package client

import (
	"context"
	"crypto/ed25519"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha512"
	"crypto/x509"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

// scramNext is the file scram.next beside the bundle: the signed request as produced, and its password.
type scramNext struct {
	Request  string `yaml:"request"`
	Password string `yaml:"password"`
}

func scramNextPath(bundle string) string { return filepath.Join(filepath.Dir(bundle), "scram.next") }

// ScramLockPath is the host lock beside the bundle at bundle, shared by every process given that bundle.
func ScramLockPath(bundle string) string { return filepath.Join(filepath.Dir(bundle), "scram.lock") }

func readSCRAMNext(bundle string) (scramNext, error) {
	encoded, err := os.ReadFile(scramNextPath(bundle))
	if errors.Is(err, os.ErrNotExist) {
		return scramNext{}, nil
	}
	if err != nil {
		return scramNext{}, err
	}
	var next scramNext
	if err := operator.DecodeYAML(encoded, &next); err != nil || next.Request == "" || next.Password == "" {
		return scramNext{}, errors.New("scram.next is not valid")
	}
	return next, nil
}

// RemoveSCRAMNext removes scram.next beside the bundle at bundle, if any.
func RemoveSCRAMNext(bundle string) error {
	if err := os.Remove(scramNextPath(bundle)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func writeSCRAMNext(bundle string, next scramNext) (err error) {
	encoded, err := yaml.Marshal(next)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(bundle), ".scram.next.*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			file.Close()
			os.Remove(file.Name())
		}
	}()
	if err = file.Chmod(0o600); err != nil {
		return err
	}
	if _, err = file.Write(encoded); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), scramNextPath(bundle))
}

// newSCRAMRequest is T-H1's new request: a random password, salted by PBKDF2-HMAC-SHA-512 at 4096 iterations.
func newSCRAMRequest(tenant, host, replaces string, key ed25519.PrivateKey) (scramNext, string, error) {
	password, err := operator.Password()
	if err != nil {
		return scramNext{}, "", err
	}
	id, err := operator.CredentialID()
	if err != nil {
		return scramNext{}, "", err
	}
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return scramNext{}, "", err
	}
	salted, err := pbkdf2.Key(sha512.New, password, salt, 4096, 64)
	if err != nil {
		return scramNext{}, "", err
	}
	request := operator.CredentialRequest{Version: 1, ID: id, Tenant: tenant, Host: host, Kind: operator.SCRAMRequestKind, Replaces: replaces,
		SCRAM: operator.SCRAMCredential{Iterations: 4096, Salt: salt, SaltedPassword: salted}}
	encoded, err := signed.Sign(operator.CredentialRequestType, signed.HostOwner(tenant, host), request, key)
	if err != nil {
		return scramNext{}, "", err
	}
	return scramNext{Request: string(encoded), Password: password}, id, nil
}

// requestStatus is section 3's scram.next status of request for principal in state.
func requestStatus(state operator.State, principal string, request operator.CredentialRequest) string {
	switch current := state.Credentials[principal]; {
	case current == request.ID:
		return "current"
	case state.Decision(principal, request.ID) != "" || request.Replaces != current:
		return "void"
	}
	return "pending"
}

// scramStep runs at "lock", before a host row takes its lock, at "read", once it has read wherry.operator and the
// files, and at "promoted", between a promotion's two file writes; tests replace it.
var scramStep = func(string) {}

// syncSCRAM is the rows of section 5.1 that write the bundle or scram.next, under the host lock: promotion and removal
// by the request's status, and, with rotate (host rotate --scram) or an unsatisfied rotate record, a new request or a
// pending one sent again.
func (h *Host) syncSCRAM(ctx context.Context, rotate bool, out io.Writer) error {
	scramStep("lock")
	release, err := LockFile(ctx, ScramLockPath(h.bundlePath), nil)
	if err != nil {
		return err
	}
	produce, err := h.scramRow(ctx, rotate, out)
	release()
	if err != nil || produce == nil {
		return err
	}
	if err := h.produce(ctx, &kgo.Record{Topic: operator.RequestsTopic, Key: []byte(operator.TenantPrincipal(h.tenant, h.host)), Value: produce}); err != nil {
		return err
	}
	h.resent.Store(true)
	return nil
}

// scramRow decides from a read of wherry.operator made holding the host lock, and returns the request to produce.
func (h *Host) scramRow(ctx context.Context, rotate bool, out io.Writer) ([]byte, error) {
	readCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	state, err := h.reader.Load(readCtx)
	if err != nil {
		if readCtx.Err() != nil && ctx.Err() == nil {
			return nil, fmt.Errorf("could not read wherry.operator within %s; nothing was changed", pingTimeout)
		}
		return nil, err
	}
	bundle, err := operator.ReadBundle(h.bundlePath)
	if err != nil {
		return nil, err
	}
	next, err := readSCRAMNext(h.bundlePath)
	if err != nil {
		return nil, err
	}
	scramStep("read")
	principal := operator.TenantPrincipal(h.tenant, h.host)
	rotate = rotate || state.RotateSCRAM[principal]
	if next.Request == "" {
		if !rotate {
			return nil, nil
		}
		current := state.Credentials[principal]
		if current == "" {
			return nil, errors.New("no credential ID is recorded for this host; ask the operator to revoke and re-enrol it")
		}
		next, id, err := newSCRAMRequest(h.tenant, h.host, current, h.key)
		if err != nil {
			return nil, err
		}
		if err := writeSCRAMNext(h.bundlePath, next); err != nil {
			return nil, err
		}
		fmt.Fprintf(out, "request %s sent\n", id)
		return []byte(next.Request), nil
	}
	request, payload, err := operator.DecodeCredentialRequest([]byte(next.Request))
	if err != nil || request.Tenant != h.tenant || request.Host != h.host {
		return nil, errors.New("scram.next is not this host's request")
	}
	switch requestStatus(state, principal, request) {
	case "current":
		if h.scram != nil {
			h.scram.set(request.ID)
		}
		if bundle.Password != next.Password {
			bundle.Password = next.Password
			if err := operator.ReplaceBundle(h.bundlePath, bundle); err != nil {
				return nil, err
			}
			fmt.Fprintf(out, "credential %s approved; now in use\n", request.ID)
		}
		scramStep("promoted")
		return nil, RemoveSCRAMNext(h.bundlePath)
	case "void":
		if err := RemoveSCRAMNext(h.bundlePath); err != nil {
			return nil, err
		}
		decision := state.Decision(principal, request.ID)
		if decision == "" {
			decision = "superseded"
		}
		fmt.Fprintf(out, "request %s %s\n", request.ID, decision)
		return nil, nil
	}
	if h.scram != nil && h.scram.flagged(request.ID) {
		fmt.Fprintf(out, "approval of %s is in progress\n", request.ID)
	}
	if !rotate || !h.rotating && h.resent.Load() {
		return nil, nil
	}
	keys := signed.Keys{signed.HostOwner(h.tenant, h.host): {Current: h.key.Public().(ed25519.PublicKey)}}
	if _, _, err := signed.VerifyStrict([]byte(next.Request), operator.CredentialRequestType, keys, &operator.CredentialRequest{}); err != nil {
		// Signed again under the host's current key, the payload bytes and so the digest an approval names are the same.
		encoded, err := signed.Sign(operator.CredentialRequestType, signed.HostOwner(h.tenant, h.host), jsontext.Value(payload), h.key)
		if err != nil {
			return nil, err
		}
		next.Request = string(encoded)
		if err := writeSCRAMNext(h.bundlePath, next); err != nil {
			return nil, err
		}
	}
	if h.rotating {
		fmt.Fprintf(out, "request %s is pending; sent again\n", request.ID)
	}
	return []byte(next.Request), nil
}

// RotateSCRAM is host rotate --scram: it files a request, or sends the pending one again.
func (h *Host) RotateSCRAM(ctx context.Context, out io.Writer) error {
	h.rotating = true
	return h.syncSCRAM(ctx, true, out)
}

// scramDue is T-L2's and T-H7's guard on the process's own fold: scram.next is current or void, or a rotate record is
// unsatisfied with nothing filed or a pending request not yet sent again by this process.
func (h *Host) scramDue(state operator.State) bool {
	principal := operator.TenantPrincipal(h.tenant, h.host)
	next, err := readSCRAMNext(h.bundlePath)
	if err != nil {
		return false
	}
	if next.Request == "" {
		return state.RotateSCRAM[principal]
	}
	request, _, err := operator.DecodeCredentialRequest([]byte(next.Request))
	if err != nil {
		return false
	}
	return requestStatus(state, principal, request) != "pending" || state.RotateSCRAM[principal] && !h.resent.Load()
}

// ConnectRetrying runs when T-H0 found some listed broker accepting a password but none every broker accepts, before
// it tries again; tests in other packages replace it.
var ConnectRetrying = func() {}

// betweenConnectReads runs between T-H0's read of scram.next and its read of the bundle; tests replace it.
var betweenConnectReads = func() {}

var errNeitherPassword = errors.New("neither the bundle password nor the requested one is accepted; ask the operator to revoke and re-enrol this host")

// connectPassword is T-H0: it reads scram.next, then the bundle, and returns the bundle and, when every listed broker
// accepts scram.next's password and not the bundle's, scram.next's request ID. It retries while some broker accepts one
// of the two, since brokers apply an upsert in turn, within operator.ProbeWindow. With no scram.next and the bundle
// password accepted, it logs in once.
func connectPassword(path string, roots *x509.CertPool) (operator.Bundle, string, error) {
	deadline := time.Now().Add(operator.ProbeWindow)
	for {
		next, err := readSCRAMNext(path)
		if err != nil {
			return operator.Bundle{}, "", err
		}
		betweenConnectReads()
		bundle, err := operator.ReadBundle(path)
		if err != nil {
			return operator.Bundle{}, "", err
		}
		// Outside a rotation the bundle password is the only one, and one login on the bundle's broker is enough.
		if next.Request == "" {
			_, err := login(bundle.Broker, bundle.Principal, bundle.Password, roots)
			if err == nil {
				return bundle, "", nil
			}
			if !errors.Is(err, kerr.SaslAuthenticationFailed) {
				return operator.Bundle{}, "", err
			}
		}
		passwords, id := []string{bundle.Password}, ""
		if next.Request != "" {
			request, _, err := operator.DecodeCredentialRequest([]byte(next.Request))
			if err != nil {
				return operator.Bundle{}, "", errors.New("scram.next is not valid")
			}
			passwords, id = append(passwords, next.Password), request.ID
		}
		accepted, some, err := logins(bundle, passwords, roots)
		switch {
		case err != nil:
			return operator.Bundle{}, "", err
		case accepted == 0:
			return bundle, "", nil
		case accepted == 1:
			return bundle, id, nil
		case !some || time.Now().After(deadline):
			return operator.Bundle{}, "", errNeitherPassword
		}
		ConnectRetrying()
		time.Sleep(time.Second)
	}
}

// logins returns the index of the first of passwords that every listed broker accepts, or -1, and whether any broker
// asked accepts any of them. The brokers are those the bundle's broker lists to a password it accepts, or that broker
// alone when it accepts none: a refused login lists nothing.
func logins(bundle operator.Bundle, passwords []string, roots *x509.CertPool) (accepted int, some bool, err error) {
	brokers := []string{bundle.Broker}
	atSeed := map[int]error{}
	for i, password := range passwords {
		listed, err := login(bundle.Broker, bundle.Principal, password, roots)
		if err != nil && !errors.Is(err, kerr.SaslAuthenticationFailed) {
			return -1, false, err
		}
		atSeed[i] = err
		if err == nil {
			brokers = listed
			break
		}
	}
	for i, password := range passwords {
		all := true
		for _, broker := range brokers {
			err, asked := atSeed[i]
			if broker != bundle.Broker || !asked {
				_, err = login(broker, bundle.Principal, password, roots)
			}
			switch {
			case errors.Is(err, kerr.SaslAuthenticationFailed):
				all = false
			case err != nil:
				return -1, false, err
			default:
				some = true
			}
		}
		if all {
			return i, true, nil
		}
	}
	return -1, some, nil
}

// login logs in as principal with password on the broker at address alone, and returns the brokers it lists.
func login(address, principal, password string, roots *x509.CertPool) ([]string, error) {
	opts, err := operator.KafkaOpts(address, principal, password, roots)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{}
	response, err := client.SeedBrokers()[0].Request(ctx, request)
	if err != nil {
		return nil, err
	}
	var brokers []string
	for _, broker := range response.(*kmsg.MetadataResponse).Brokers {
		brokers = append(brokers, net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port))))
	}
	return brokers, nil
}

// scramAuth is T-L1 and T-L3 for one process: the auth function franz-go calls at each authentication, and the
// recovery it starts when one fails.
type scramAuth struct {
	bundle     string
	roots      *x509.CertPool
	mu         sync.Mutex
	flag       string
	host       atomic.Pointer[Host]
	recovering atomic.Bool
}

func (a *scramAuth) set(id string) {
	a.mu.Lock()
	a.flag = id
	a.mu.Unlock()
}

func (a *scramAuth) flagged(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.flag == id
}

// auth reads scram.next before the bundle, so a promotion between the two reads leaves it the new password.
func (a *scramAuth) auth(context.Context) (scram.Auth, error) {
	next, err := readSCRAMNext(a.bundle)
	if err != nil {
		return scram.Auth{}, err
	}
	bundle, err := operator.ReadBundle(a.bundle)
	if err != nil {
		return scram.Auth{}, err
	}
	if next.Request != "" {
		if request, _, err := operator.DecodeCredentialRequest([]byte(next.Request)); err == nil && a.flagged(request.ID) {
			return scram.Auth{User: bundle.Principal, Pass: next.Password}, nil
		}
	}
	return scram.Auth{User: bundle.Principal, Pass: bundle.Password}, nil
}

// OnBrokerConnect starts T-L3 when a connection's authentication fails.
func (a *scramAuth) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if !errors.Is(err, kerr.SaslAuthenticationFailed) || !a.recovering.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer a.recovering.Store(false)
		if _, flag, err := connectPassword(a.bundle, a.roots); err == nil && flag != "" {
			a.set(flag)
		}
		if h := a.host.Load(); h != nil {
			if err := h.syncSCRAM(context.Background(), false, h.logger.Writer()); err != nil {
				h.logger.Printf("SCRAM credential: %v", err)
			}
		}
	}()
}
