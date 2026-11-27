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
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
// by the request's status, and, with rotate (host rotate --scram), a new request or the pending one sent again.
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
	return h.produce(ctx, &kgo.Record{Topic: operator.RequestsTopic, Key: []byte(operator.TenantPrincipal(h.tenant, h.host)), Value: produce})
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
	if h.connectedWith == request.ID {
		fmt.Fprintf(out, "approval of %s is in progress\n", request.ID)
	}
	if !rotate {
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
	fmt.Fprintf(out, "request %s is pending; sent again\n", request.ID)
	return []byte(next.Request), nil
}

// RotateSCRAM is host rotate --scram: it files a request, or sends the pending one again.
func (h *Host) RotateSCRAM(ctx context.Context, out io.Writer) error {
	return h.syncSCRAM(ctx, true, out)
}

// refusedLogin names a broker that refuses the host's login. Brokers apply an approved credential in turn, and kgo
// does not retry a request a broker refused with SASL_AUTHENTICATION_FAILED.
type refusedLogin struct{ logger *log.Logger }

func (r refusedLogin) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if errors.Is(err, kerr.SaslAuthenticationFailed) {
		r.logger.Printf("broker %d at %s refused this host's password; a broker refuses a credential an operator has just approved until it has applied it: run the command again", meta.NodeID, net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port))))
	}
}

// ConnectRetrying runs when T-H0 has found the bundle's broker refusing both passwords, before it tries again; tests in
// other packages replace it.
var ConnectRetrying = func() {}

// betweenConnectReads runs between T-H0's read of scram.next and its read of the bundle; tests replace it.
var betweenConnectReads = func() {}

var errNeitherPassword = errors.New("neither the bundle password nor the requested one is accepted; ask the operator to revoke and re-enrol this host")

// connectBundle is T-H0: it reads scram.next, then the bundle, and logs in on the bundle's broker with the bundle
// password, then with scram.next's. It returns the bundle with the password that broker accepts and, when that is
// scram.next's, its request ID. While the broker refuses both and scram.next is there, the upsert may not have reached
// it yet, so it tries again within operator.ProbeWindow.
func connectBundle(path string, roots *x509.CertPool) (operator.Bundle, string, error) {
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
		if err := login(bundle, bundle.Password, roots); !errors.Is(err, kerr.SaslAuthenticationFailed) {
			return bundle, "", err
		}
		if next.Request == "" {
			return operator.Bundle{}, "", errNeitherPassword
		}
		request, _, err := operator.DecodeCredentialRequest([]byte(next.Request))
		if err != nil {
			return operator.Bundle{}, "", errors.New("scram.next is not valid")
		}
		if err := login(bundle, next.Password, roots); !errors.Is(err, kerr.SaslAuthenticationFailed) {
			bundle.Password = next.Password
			return bundle, request.ID, err
		}
		if time.Now().After(deadline) {
			return operator.Bundle{}, "", errNeitherPassword
		}
		ConnectRetrying()
		time.Sleep(time.Second)
	}
}

// login logs in with password on the bundle's broker alone.
func login(bundle operator.Bundle, password string, roots *x509.CertPool) error {
	opts, err := operator.KafkaOpts(bundle.Broker, bundle.Principal, password, roots)
	if err != nil {
		return err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{}
	_, err = client.SeedBrokers()[0].Request(ctx, request)
	return err
}
