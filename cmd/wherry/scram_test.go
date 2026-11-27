package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/operator"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// scramEnv is an operator directory, the working directory, over a TLS kfake with tenant-a and host-a added by the
// CLI and installed by host init in HOME's default configuration, and revocation bounds of reauth milliseconds.
type scramEnv struct {
	t       *testing.T
	cluster *kfake.Cluster
	service *operator.Service
	kept    string
	bundle  string
	upserts *kfake.FaultHandle
	// hold, when set, holds an upsert it returns true for until held is closed, as a slow controller would.
	hold func(kmsg.AlterUserSCRAMCredentialsRequestUpsertion) bool
	held chan struct{}
	// While lagging, broker lagNode refuses every login of a host principal, as a broker yet to apply its last
	// credential would refuse the new password; kfake checks the old one against its current credential and refuses it
	// already.
	lagNode int32
	lagging atomic.Bool
	// afterUpsert, when set, is told each upsertion once the fixture has recorded it.
	afterUpsert func(kmsg.AlterUserSCRAMCredentialsRequestUpsertion)
	// seen is every command's output, for the sentinel.
	seen bytes.Buffer
}

// refuseAsKafka makes kfake answer a SCRAM-SHA-512 login whose proof does not match the credential it was last sent
// with SASL_AUTHENTICATION_FAILED, as Kafka does; kfake itself closes the connection. Users the cluster was never sent
// a credential for, its super users, are left to kfake.
func (e *scramEnv) refuseAsKafka() {
	var mu sync.Mutex
	credentials := map[string]kmsg.AlterUserSCRAMCredentialsRequestUpsertion{}
	firsts := map[string][2]string{}
	e.cluster.ControlKey(int16(kmsg.AlterUserSCRAMCredentials), func(request kmsg.Request) (kmsg.Response, error, bool) {
		e.cluster.KeepControl()
		alter := request.(*kmsg.AlterUserSCRAMCredentialsRequest)
		for _, upsertion := range alter.Upsertions {
			if e.hold != nil && e.hold(upsertion) {
				e.cluster.SleepControl(func() { <-e.held })
			}
		}
		mu.Lock()
		defer mu.Unlock()
		for _, deletion := range alter.Deletions {
			delete(credentials, deletion.Name)
		}
		for _, upsertion := range alter.Upsertions {
			credentials[upsertion.Name] = upsertion
			if e.afterUpsert != nil {
				e.afterUpsert(upsertion)
			}
		}
		return nil, nil, false
	})
	e.cluster.ControlKey(int16(kmsg.SASLAuthenticate), func(request kmsg.Request) (kmsg.Response, error, bool) {
		e.cluster.KeepControl()
		message := string(request.(*kmsg.SASLAuthenticateRequest).SASLAuthBytes)
		mu.Lock()
		defer mu.Unlock()
		if bare, ok := strings.CutPrefix(message, "n,,"); ok {
			user, rest, _ := strings.Cut(strings.TrimPrefix(bare, "n="), ",r=")
			if _, tracked := credentials[user]; tracked {
				firsts[rest] = [2]string{user, bare}
			}
			return nil, nil, false
		}
		withoutProof, proof64, ok := strings.Cut(message, ",p=")
		nonce := strings.TrimPrefix(strings.SplitN(withoutProof, ",", 3)[1], "r=")
		for client, first := range firsts {
			if !ok || !strings.HasPrefix(nonce, client) {
				continue
			}
			delete(firsts, client)
			credential := credentials[first[0]]
			host := strings.HasPrefix(first[0], strings.TrimSuffix(operator.TenantPrincipal("", ""), "."))
			lagging := host && e.cluster.CurrentNode() == e.lagNode && e.lagging.Load()
			auth := first[1] + ",r=" + nonce + ",s=" + base64.StdEncoding.EncodeToString(credential.Salt) + ",i=4096," + withoutProof
			mac := hmac.New(sha512.New, credential.SaltedPassword)
			mac.Write([]byte("Client Key"))
			stored := sha512.Sum512(mac.Sum(nil))
			signature := hmac.New(sha512.New, stored[:])
			signature.Write([]byte(auth))
			proof, _ := base64.StdEncoding.DecodeString(proof64)
			key := signature.Sum(nil)
			for i := range key {
				if i < len(proof) {
					key[i] ^= proof[i]
				}
			}
			if lagging || sha512.Sum512(key) != stored {
				response := request.ResponseKind().(*kmsg.SASLAuthenticateResponse)
				response.ErrorCode = kerr.SaslAuthenticationFailed.Code
				return response, nil, true
			}
		}
		return nil, nil, false
	})
}

func newSCRAMEnv(t *testing.T, reauth string) *scramEnv {
	t.Helper()
	cluster, ca := tlsCluster(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "operator.password"), []byte("operator-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	address := cluster.ListenAddrs()[0]
	if err := writeYAML(filepath.Join(dir, "operator.yaml"), operatorConfig{Broker: address, PasswordFile: "operator.password", CACert: testCA.pem}); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	noTerminal(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, variable := range []string{"XDG_CONFIG_HOME", "XDG_STATE_HOME", "WHERRY_CONFIG"} {
		t.Setenv(variable, "")
	}
	window := operator.ProbeWindow
	operator.ProbeWindow = time.Second
	t.Cleanup(func() { operator.ProbeWindow = window })
	e := &scramEnv{t: t, cluster: cluster, lagNode: -1, kept: filepath.Join(dir, "bundles", "hosts", "tenant-a", "host-a.bundle"), bundle: filepath.Join(home, ".config", "wherry", "bundle.yaml"),
		service: &operator.Service{Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"), Broker: address, Roots: testRoots(t), DeliveryCeiling: 10, LockCeilingMS: 60000}}
	var alters []kadm.AlterConfig
	for _, name := range []string{"connections.max.reauth.ms", "group.share.max.record.lock.duration.ms", "transaction.max.timeout.ms"} {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: name, Value: &reauth})
	}
	if responses, err := kadm.NewClient(e.service.Operator).AlterBrokerConfigs(context.Background(), alters); err != nil || responses[0].Err != nil {
		t.Fatalf("broker settings: %v %v", err, responses)
	}
	e.refuseAsKafka()
	// The topics and grants operator init leaves.
	admin := connectTLS(t, address, ca, "admin", "password")
	if err := operator.Delegate(context.Background(), admin, "operator-password"); err != nil {
		t.Fatal(err)
	}
	if err := (&operator.Service{Operator: admin}).EnsureTraceTopics(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.op("add", "--tenant", "tenant-a")
	e.op("add", "--host", "host-a", "--tenant", "tenant-a")
	e.upserts = cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.AlterUserSCRAMCredentials}, Observe: true, Count: -1})
	return e
}

func (e *scramEnv) run(args ...string) (string, string, error) {
	e.t.Helper()
	var stdout, stderr bytes.Buffer
	err := run(args, &stdout, &stderr)
	e.seen.WriteString(stdout.String() + stderr.String() + fmt.Sprint(err))
	return stdout.String(), stderr.String(), err
}

// op runs a wherry operator command that must succeed and returns its standard output.
func (e *scramEnv) op(args ...string) string {
	e.t.Helper()
	stdout, stderr, err := e.run(append([]string{"operator"}, args...)...)
	if err != nil {
		e.t.Fatalf("operator %v: %v\n%s%s", args, err, stdout, stderr)
	}
	return stdout
}

// init runs host init with bundle and returns what it wrote to standard error.
func (e *scramEnv) init(bundle string) string {
	e.t.Helper()
	stdout, stderr, err := e.run("host", "init", "--ca-cert", testCA.pem, bundle)
	if err != nil {
		e.t.Fatalf("host init: %v\n%s%s", err, stdout, stderr)
	}
	return stderr
}

func (e *scramEnv) next() string {
	e.t.Helper()
	encoded, err := os.ReadFile(filepath.Join(filepath.Dir(e.bundle), "scram.next"))
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return string(encoded)
}

// requestSalt is the salt of the request in the host's scram.next.
func (e *scramEnv) requestSalt() []byte {
	e.t.Helper()
	var next struct{ Request, Password string }
	if err := readYAML(filepath.Join(filepath.Dir(e.bundle), "scram.next"), &next); err != nil {
		e.t.Fatal(err)
	}
	request, _, err := operator.DecodeCredentialRequest([]byte(next.Request))
	if err != nil {
		e.t.Fatal(err)
	}
	return request.SCRAM.Salt
}

func (e *scramEnv) state() operator.State {
	e.t.Helper()
	state, err := e.service.LoadState(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return state
}

func (e *scramEnv) cur() string {
	return e.state().Credentials[operator.TenantPrincipal("tenant-a", "host-a")]
}

func (e *scramEnv) works(path string) bool {
	e.t.Helper()
	return bundleWorks(e.t, e.service, path)
}

func sentID(t *testing.T, output string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if id, ok := strings.CutPrefix(line, "request "); ok && strings.HasSuffix(id, " sent") {
			return strings.TrimSuffix(id, " sent")
		}
	}
	t.Fatalf("no request sent in %q", output)
	return ""
}

// Tests 1 and 13: host init files a request; approve upserts it; the host's next command promotes it and the old
// password is refused; rotate --storage and host init of the refreshed bundle keep the promoted password; host init
// of a revoke's bundle removes scram.next and files again.
func TestSCRAMRotationEndToEnd(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	id := sentID(t, e.init(e.kept))
	if e.next() == "" || !strings.Contains(e.op("list", "requests"), "request="+id+" tenant=tenant-a host=host-a kind=scram-sha-512 state=pending") {
		t.Fatal("host init left no pending request")
	}
	var next struct{ Request, Password string }
	if err := readYAML(filepath.Join(filepath.Dir(e.bundle), "scram.next"), &next); err != nil {
		t.Fatal(err)
	}
	first, _, err := operator.DecodeCredentialRequest([]byte(next.Request))
	if err != nil {
		t.Fatal(err)
	}
	// Test 2's sentinel: no command's output or error, and no wherry.operator record, carries a secret of the request.
	defer func() {
		reader := operator.NewReader(e.service.Operator)
		defer reader.Close()
		records, err := reader.ReadFrom(context.Background(), operator.OperatorTopic, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			e.seen.Write(record.Value)
		}
		for _, secret := range [][]byte{[]byte(next.Password), first.SCRAM.Salt, first.SCRAM.SaltedPassword} {
			for _, form := range []string{string(secret), base64.StdEncoding.EncodeToString(secret), base64.RawURLEncoding.EncodeToString(secret)} {
				if strings.Contains(e.seen.String(), form) {
					t.Fatal("a secret of the request appears in a command's output or a wherry.operator record")
				}
			}
		}
	}()
	if out := e.op("approve", id); out != "approved "+id+"\n" {
		t.Fatalf("approve = %q", out)
	}
	_, stderr, err := e.run("host", "rotate", "--scram")
	if err != nil || !strings.Contains(stderr, "credential "+id+" approved; now in use") {
		t.Fatalf("host command after approval: %v\n%s", err, stderr)
	}
	if e.next() == "" || !e.works(e.bundle) || e.works(e.kept) {
		t.Fatal("after promotion the bundle must log in, the enrolment password must not, and the new request must be filed")
	}
	promoted, err := operator.ReadBundle(e.bundle)
	if err != nil {
		t.Fatal(err)
	}
	e.op("rotate", "--tenant", "tenant-a", "--host", "host-a", "--storage")
	e.init(e.kept)
	if after, err := operator.ReadBundle(e.bundle); err != nil || after.Password != promoted.Password || !e.works(e.bundle) {
		t.Fatalf("host init of a refreshed bundle replaced the promoted password: %v", err)
	}
	if _, stderr, err := e.run("host", "receivers"); err != nil {
		t.Fatalf("host command after a refreshed bundle: %v\n%s", err, stderr)
	}
	pending := e.next()
	e.op("revoke", "--tenant", "tenant-a", "--host", "host-a")
	again := sentID(t, e.init(e.kept))
	if next := e.next(); next == "" || next == pending || again == id || !e.works(e.bundle) {
		t.Fatal("host init of a revoke's bundle kept the old scram.next or filed nothing")
	}
}

// failQuota fails the client quota of host-a until the returned function is called.
func (e *scramEnv) failQuota() func() {
	handle := e.cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.AlterClientQuotas}, Err: kerr.PolicyViolation, Count: -1})
	return handle.Remove
}

// Tests 20 and 35: a revoke stopped at its quota leaves the host with no credential ID and upserts nothing; host rotate
// --scram refuses with the hint, list requests and check report it; a revoke that completes applies the quota and
// enrols it, and host init files.
func TestRevokeStoppedAtItsQuotaAndAHostWithNoCredentialID(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	e.init(e.kept)
	kept, err := os.ReadFile(e.kept)
	if err != nil {
		t.Fatal(err)
	}
	restore := e.failQuota()
	hits := e.upserts.Hits()
	if _, _, err := e.run("operator", "revoke", "--tenant", "tenant-a", "--host", "host-a"); err == nil {
		t.Fatal("revoke with a failing quota succeeded")
	}
	if after, _ := os.ReadFile(e.kept); e.upserts.Hits() != hits || !bytes.Equal(after, kept) {
		t.Fatal("revoke whose quota failed upserted or wrote a bundle")
	}
	restore()
	if _, _, err := e.run("host", "rotate", "--scram"); err == nil || !strings.Contains(err.Error(), "no credential ID is recorded for this host; ask the operator to revoke and re-enrol it") {
		t.Fatalf("host rotate --scram with no credential ID = %v", err)
	}
	for _, args := range [][]string{{"list", "requests"}, {"check"}} {
		if out := e.op(args...); !strings.Contains(out, "tenant=tenant-a host=host-a has no credential ID") {
			t.Fatalf("operator %v = %q", args, out)
		}
	}
	quotas := e.cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.AlterClientQuotas}, Observe: true, Count: -1})
	e.op("revoke", "--tenant", "tenant-a", "--host", "host-a")
	if quotas.Hits() == 0 {
		t.Fatal("revoke applied no quota")
	}
	sentID(t, e.init(e.kept))
	if e.cur() == "" {
		t.Fatal("revoke left the host with no credential ID")
	}
}

// Test 9: a host that can still log in during revoke files no request the operator can approve afterwards.
func TestHostConnectedDuringRevokeFilesNothingApprovable(t *testing.T) {
	e := newSCRAMEnv(t, "1200")
	approved := sentID(t, e.init(e.kept))
	e.op("approve", approved)
	config, err := busclient.LoadConfig(filepath.Join(filepath.Dir(e.bundle), "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := busclient.Connect(config, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	// Revoke's upsert is held a while, so the host's password still logs in during the revoke.
	release := make(chan struct{})
	var once sync.Once
	e.held = release
	e.hold = func(upsertion kmsg.AlterUserSCRAMCredentialsRequestUpsertion) bool {
		held := false
		once.Do(func() {
			held = true
			time.AfterFunc(600*time.Millisecond, func() { close(release) })
		})
		return held
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := e.run("operator", "revoke", "--tenant", "tenant-a", "--host", "host-a")
		done <- err
	}()
	for deadline := time.Now().Add(10 * time.Second); e.cur() == approved; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("revoke recorded nothing")
		}
	}
	// From revoke's first record the connected host tries to file, as a host whose session outlives the upsert would.
	for running := true; running; {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("revoke: %v", err)
			}
			running = false
		default:
			os.Remove(filepath.Join(filepath.Dir(e.bundle), "scram.next"))
			attempt, cancel := context.WithTimeout(context.Background(), time.Second)
			host.RotateSCRAM(attempt, io.Discard)
			cancel()
			time.Sleep(20 * time.Millisecond)
		}
	}
	e.run("operator", "approve", "--all")
	if !e.works(e.kept) {
		t.Fatal("a request filed during revoke was approved over revoke's password")
	}
}

// Test 36: an upsert held past the pass's timeout and released after revoke's upsert stops revoke at its last probe
// with T-V4's line; released after revoke completes it restores the host's payload until revoke runs again.
func TestStaleUpsertAgainstRevoke(t *testing.T) {
	for _, after := range []bool{false, true} {
		e := newSCRAMEnv(t, "800")
		id := sentID(t, e.init(e.kept))
		release := make(chan struct{})
		salt := e.requestSalt()
		e.held = release
		var mu sync.Mutex
		revokes := 0
		e.hold = func(upsertion kmsg.AlterUserSCRAMCredentialsRequestUpsertion) bool {
			if bytes.Equal(upsertion.Salt, salt) {
				return true
			}
			mu.Lock()
			revokes++
			mu.Unlock()
			return false
		}
		short, err := kgo.NewClient(append(e.service.Operator.Opts(), kgo.RequestTimeoutOverhead(200*time.Millisecond), kgo.RetryTimeout(500*time.Millisecond))...)
		if err != nil {
			t.Fatal(err)
		}
		pass := *e.service
		pass.Operator = short
		var stdout bytes.Buffer
		err = pass.ApproveRequests(context.Background(), id, false, time.Now(), &stdout)
		short.Close()
		if err == nil || !strings.Contains(stdout.String(), "approval of "+id+" is still open; the upsert may still be applied") {
			t.Fatalf("approve with its upsert held = %v\n%s", err, stdout.String())
		}
		if !after {
			// Once revoke's first probe has passed and before its wait, the held upsert is released and revoke goes on
			// only when the fixture has seen it applied, so the last probe meets it.
			applied := make(chan struct{})
			var once sync.Once
			e.afterUpsert = func(upsertion kmsg.AlterUserSCRAMCredentialsRequestUpsertion) {
				if bytes.Equal(upsertion.Salt, salt) {
					once.Do(func() { close(applied) })
				}
			}
			saved := operator.RevokeProbed
			operator.RevokeProbed = func() {
				operator.RevokeProbed = saved
				close(release)
				<-applied
			}
			_, _, err := e.run("operator", "revoke", "--tenant", "tenant-a", "--host", "host-a")
			operator.RevokeProbed, e.afterUpsert = saved, nil
			if err == nil || !strings.Contains(err.Error(), "the password revoke set is refused after its wait: an upsert from an earlier command was applied after it, and an old password may work; run revoke again") {
				t.Fatalf("revoke with a stale upsert in its wait = %v", err)
			}
			mu.Lock()
			sent := revokes
			mu.Unlock()
			if sent != 1 || e.state().Credentials[operator.TenantPrincipal("tenant-a", "host-a")] != "" {
				t.Fatal("revoke sent a second upsert or recorded an enrolment after its last probe was refused")
			}
		} else {
			e.op("revoke", "--tenant", "tenant-a", "--host", "host-a")
			close(release)
			for deadline := time.Now().Add(5 * time.Second); e.works(e.kept) && time.Now().Before(deadline); {
				time.Sleep(20 * time.Millisecond)
			}
			if e.works(e.kept) {
				t.Fatal("the held upsert, applied after revoke, did not replace revoke's password in the model of the named race")
			}
		}
		if out := e.op("revoke", "--tenant", "tenant-a", "--host", "host-a"); !strings.Contains(out, "a session opened with an old password during the wait can last up to 800ms more") {
			t.Fatalf("revoke = %q", out)
		}
		if !e.works(e.kept) {
			t.Fatal("revoke run again did not repair the host")
		}
	}
}

// lockHolder is a second process that holds operator.lock as verb until it is killed.
func lockHolder(t *testing.T, verb string) *exec.Cmd {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestOperatorLockHelper$")
	command.Env = append(os.Environ(), trustHelperVariable+"=1", "WHERRY_TEST_LOCK_VERB="+verb, "WHERRY_TEST_LOCK_DIR="+mustGetwd(t))
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { command.Process.Kill(); command.Wait() })
	buffer := make([]byte, 6)
	if _, err := stdout.Read(buffer); err != nil || string(buffer) != "locked" {
		t.Fatalf("lock holder: %q, %v", buffer, err)
	}
	return command
}

func mustGetwd(t *testing.T) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestOperatorLockHelper is the second process of the lock tests; it does nothing when run as a test.
func TestOperatorLockHelper(t *testing.T) {
	verb := os.Getenv("WHERRY_TEST_LOCK_VERB")
	if verb == "" {
		return
	}
	if _, err := lockOperator(filepath.Join(os.Getenv("WHERRY_TEST_LOCK_DIR"), "operator.lock"), verb, []string{verb, "--tenant", "tenant-a"}, os.Stderr); err != nil {
		t.Fatal(err)
	}
	fmt.Print("locked")
	time.Sleep(time.Hour)
}

// Tests 21 and 32: each operator command of G-1 reads and writes nothing while another process holds operator.lock,
// names it and says whether it may be interrupted, waits past any timeout, and proceeds once it is killed; the lock file
// stays; a receiver's removal and add --host take no lock.
func TestOperatorLockOrdersTheCommandsBetweenProcesses(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	stubProbe(t, deniedProbe)
	id := sentID(t, e.init(e.kept))
	if out := e.op("add", "--host", "host-b", "--tenant", "tenant-a"); !strings.Contains(out, "wrote") {
		t.Fatalf("add --host = %q", out)
	}
	for i, test := range []struct {
		holder string
		args   []string
		line   string
	}{
		{"approve", []string{"approve", id}, "; do not interrupt it: an approve that is interrupted, or that ends on a timeout, can leave an upsert the broker makes later"},
		{"revoke", []string{"reject", id}, "; to go first, interrupt that command and run it again afterwards"},
		{"remove", []string{"revoke", "--tenant", "tenant-a", "--host", "host-b"}, "; to go first, interrupt that command and run it again afterwards"},
		{"approve", []string{"remove", "--host", "host-b", "--tenant", "tenant-a"}, "; do not interrupt it"},
		{"approve", []string{"remove", "--tenant", "tenant-a"}, "; do not interrupt it"},
	} {
		holder := lockHolder(t, test.holder)
		records := len(e.state().Hosts) + len(e.state().Decisions)
		hits := e.upserts.Hits()
		var stderr syncWriter
		done := make(chan error, 1)
		go func() { done <- run(append([]string{"operator"}, test.args...), &bytes.Buffer{}, &stderr) }()
		for deadline := time.Now().Add(10 * time.Second); !strings.Contains(stderr.String(), "waiting for") && time.Now().Before(deadline); {
			time.Sleep(10 * time.Millisecond)
		}
		// The first wait outlasts a one-second deadline on the lock, which a waiter must not have.
		if i == 0 {
			time.Sleep(1200 * time.Millisecond)
		}
		select {
		case err := <-done:
			t.Fatalf("%v ran while another process held the lock: %v", test.args, err)
		default:
		}
		want := fmt.Sprintf("waiting for wherry operator %s --tenant tenant-a (pid %d on ", test.holder, holder.Process.Pid)
		if got := stderr.String(); !strings.HasPrefix(got, want) || !strings.Contains(got, test.line) || strings.Count(got, "waiting for") != 1 {
			t.Fatalf("%v while waiting printed %q; want %q ... %q", test.args, got, want, test.line)
		}
		if e.upserts.Hits() != hits || len(e.state().Hosts)+len(e.state().Decisions) != records {
			t.Fatalf("%v changed something while waiting", test.args)
		}
		holder.Process.Signal(syscall.SIGKILL)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("%v did not proceed after the holder was killed", test.args)
		}
		if _, err := os.Stat("operator.lock"); err != nil {
			t.Fatalf("the lock file is gone: %v", err)
		}
	}
}

// Test 22 (lock half): a receiver's removal runs to its end while another process holds operator.lock.
func TestReceiverRemovalTakesNoLock(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	ctx := context.Background()
	revocationBounds(t, e.service, "60000")
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := e.service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	config := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Profiles: []operator.ProfileSettings{{Name: "fake", Plugin: "fake", Agent: "claude"}}, PluginSHA256: map[string]string{"fake": fakeSHA256}}
	if err := e.service.ReceiverAdd(ctx, config, e.service.Broker, filepath.Join(t.TempDir(), "receiver.yaml")); err != nil {
		t.Fatal(err)
	}
	revocationBounds(t, e.service, "1")
	stubProbe(t, deniedProbe)
	lockHolder(t, "approve")
	done := make(chan error, 1)
	go func() {
		_, _, err := e.run("operator", "remove", "--receiver", "receiver-a")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("remove --receiver: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("a receiver's removal waited for operator.lock")
	}
}

type syncWriter struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.String()
}

// worksWith reports whether host-a logs in with password.
func (e *scramEnv) worksWith(password string) bool {
	e.t.Helper()
	bundle, err := operator.ReadBundle(e.kept)
	if err != nil {
		e.t.Fatal(err)
	}
	bundle.Password = password
	path := filepath.Join(e.t.TempDir(), "probe.bundle")
	if err := operator.ReplaceBundle(path, bundle); err != nil {
		e.t.Fatal(err)
	}
	return e.works(path)
}

// replay sends upsertion again, as a forwarder's late copy of an earlier call.
func (e *scramEnv) replay(upsertion kmsg.AlterUserSCRAMCredentialsRequestUpsertion) {
	e.t.Helper()
	request := kmsg.NewPtrAlterUserSCRAMCredentialsRequest()
	request.Upsertions = append(request.Upsertions, upsertion)
	response, err := request.RequestWith(context.Background(), e.service.Operator)
	if err == nil && response.Results[0].ErrorCode != 0 {
		err = kerr.ErrorForCode(response.Results[0].ErrorCode)
	}
	if err != nil {
		e.t.Fatal(err)
	}
}

// Test 36, the silent forms: a second copy of an approve's upsert that returned, and of a first revoke's, applied after
// a revoke that completed, restore the password it carries with no command printing anything; revoke again repairs it.
func TestSilentStaleUpsertsAfterRevoke(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	sentID(t, e.init(e.kept))
	var mu sync.Mutex
	var copies []kmsg.AlterUserSCRAMCredentialsRequestUpsertion
	e.hold = func(upsertion kmsg.AlterUserSCRAMCredentialsRequestUpsertion) bool {
		mu.Lock()
		copies = append(copies, upsertion)
		mu.Unlock()
		return false
	}
	last := func() kmsg.AlterUserSCRAMCredentialsRequestUpsertion {
		mu.Lock()
		defer mu.Unlock()
		return copies[len(copies)-1]
	}
	var next struct{ Request, Password string }
	if err := readYAML(filepath.Join(filepath.Dir(e.bundle), "scram.next"), &next); err != nil {
		t.Fatal(err)
	}
	e.op("approve", "--all")
	approved := last()
	e.op("revoke", "--tenant", "tenant-a", "--host", "host-a")
	e.replay(approved)
	if !e.worksWith(next.Password) || e.works(e.kept) {
		t.Fatal("a second copy of the approve's upsert, applied after revoke, did not restore the host's payload")
	}
	e.op("revoke", "--tenant", "tenant-a", "--host", "host-a")
	if e.worksWith(next.Password) || !e.works(e.kept) {
		t.Fatal("revoke run again did not repair the host")
	}
	first, err := operator.ReadBundle(e.kept)
	if err != nil {
		t.Fatal(err)
	}
	firstCopy := last()
	e.op("revoke", "--tenant", "tenant-a", "--host", "host-a")
	e.replay(firstCopy)
	if !e.worksWith(first.Password) || e.works(e.kept) {
		t.Fatal("a copy of the first revoke's upsert, applied after the second, did not restore the first bundle's password")
	}
	e.op("revoke", "--tenant", "tenant-a", "--host", "host-a")
	if !e.works(e.kept) {
		t.Fatal("a third revoke did not repair the host")
	}
}

// Test 36, removal: a held upsert released before step 3 ends removal on its probe and a re-run completes; released
// after end it leaves the principal a credential and no ACLs, which the next command that converges deletes.
func TestStaleUpsertAgainstRemoval(t *testing.T) {
	for _, after := range []bool{false, true} {
		e := newSCRAMEnv(t, "1")
		id := sentID(t, e.init(e.kept))
		release := make(chan struct{})
		salt := e.requestSalt()
		e.held = release
		var mu sync.Mutex
		removals := 0
		e.hold = func(upsertion kmsg.AlterUserSCRAMCredentialsRequestUpsertion) bool {
			if bytes.Equal(upsertion.Salt, salt) {
				return true
			}
			mu.Lock()
			removals++
			mu.Unlock()
			return false
		}
		short, err := kgo.NewClient(append(e.service.Operator.Opts(), kgo.RequestTimeoutOverhead(200*time.Millisecond), kgo.RetryTimeout(500*time.Millisecond))...)
		if err != nil {
			t.Fatal(err)
		}
		pass := *e.service
		pass.Operator = short
		if err := pass.ApproveRequests(context.Background(), id, false, time.Now(), &bytes.Buffer{}); err == nil {
			t.Fatal("approve with its upsert held exited zero")
		}
		short.Close()
		principal := operator.TenantPrincipal("tenant-a", "host-a")
		if !after {
			go func() {
				for mu.Lock(); removals == 0; mu.Lock() {
					mu.Unlock()
					time.Sleep(5 * time.Millisecond)
				}
				mu.Unlock()
				close(release)
			}()
			if out, _, err := e.run("operator", "remove", "--host", "host-a", "--tenant", "tenant-a"); err == nil || !strings.Contains(out, "step=3 complete=false") {
				t.Fatalf("removal with a stale upsert before its probe = %v\n%s", err, out)
			}
			e.op("remove", "--host", "host-a", "--tenant", "tenant-a")
			continue
		}
		e.op("remove", "--host", "host-a", "--tenant", "tenant-a")
		close(release)
		described := func() bool {
			users, err := kadm.NewClient(e.service.Operator).DescribeUserSCRAMs(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_, ok := users[principal]
			return ok
		}
		for deadline := time.Now().Add(5 * time.Second); !described(); time.Sleep(20 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the held upsert was not applied after end")
			}
		}
		e.op("add", "--tenant", "tenant-b")
		if described() {
			t.Fatal("the next command that converges left the removed principal's credential")
		}
	}
}

// whileLagging runs a host command with broker node lagging, and ends the lag when T-H0 says it is about to try again,
// so the command has met the lag and retried; a command that gives up instead returns with the lag still on.
func (e *scramEnv) whileLagging(node int32, args ...string) (output string, retried bool, err error) {
	e.t.Helper()
	e.lagNode = node
	e.lagging.Store(true)
	defer e.lagging.Store(false)
	signal := make(chan struct{})
	var once sync.Once
	saved := busclient.ConnectRetrying
	busclient.ConnectRetrying = func() { once.Do(func() { e.lagging.Store(false); close(signal) }) }
	defer func() { busclient.ConnectRetrying = saved }()
	type result struct {
		output string
		err    error
	}
	done := make(chan result, 1)
	go func() {
		output, err := e.host(args...)
		done <- result{output, err}
	}()
	select {
	case r := <-done:
		select {
		case <-signal:
			retried = true
		default:
		}
		return r.output, retried, r.err
	case <-time.After(15 * time.Second):
		e.t.Fatal("the host command neither retried nor returned within 15 s")
		return "", false, nil
	}
}

func (e *scramEnv) host(args ...string) (string, error) {
	e.t.Helper()
	stdout, stderr, err := e.run(append([]string{"host"}, args...)...)
	return stdout + stderr, err
}

func (e *scramEnv) nextID() string {
	e.t.Helper()
	var next struct{ Request, Password string }
	if err := readYAML(filepath.Join(filepath.Dir(e.bundle), "scram.next"), &next); err != nil {
		e.t.Fatal(err)
	}
	request, _, err := operator.DecodeCredentialRequest([]byte(next.Request))
	if err != nil {
		e.t.Fatal(err)
	}
	return request.ID
}

// Test 11: a request whose produce failed is sent with the same ID by the next run; a run while pending sends it again
// and leaves scram.next as it was; after host rotate --key it is signed again under the new key.
func TestPendingRequestIsSentAgainNeverReplaced(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	id := sentID(t, e.init(e.kept))
	e.op("approve", id)
	e.host("receivers")
	fault := e.cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: operator.RequestsTopic, Err: kerr.TopicAuthorizationFailed, Count: -1})
	if _, err := e.host("rotate", "--scram"); err == nil {
		t.Fatal("host rotate --scram succeeded with its produce refused")
	}
	fault.Remove()
	stopped, written := e.nextID(), e.next()
	if stderr, err := e.host("rotate", "--scram"); err != nil || !strings.Contains(stderr, "request "+stopped+" is pending; sent again") || e.next() != written {
		t.Fatalf("host rotate --scram after a stopped produce: %v\n%s", err, stderr)
	}
	if _, err := e.host("rotate", "--key"); err != nil {
		t.Fatal(err)
	}
	if stderr, err := e.host("rotate", "--scram"); err != nil || !strings.Contains(stderr, "request "+stopped+" is pending; sent again") || e.next() == written || e.nextID() != stopped {
		t.Fatalf("host rotate --scram after host rotate --key: %v\n%s", err, stderr)
	}
	if out := e.op("list", "requests"); !strings.Contains(out, "request="+stopped+" tenant=tenant-a host=host-a kind=scram-sha-512 state=pending") {
		t.Fatalf("list requests = %q", out)
	}
	e.op("approve", stopped)
	if stderr, err := e.host("receivers"); err != nil || !strings.Contains(stderr, "credential "+stopped+" approved; now in use") || !e.works(e.bundle) {
		t.Fatalf("the request signed again was not promoted: %v\n%s", err, stderr)
	}
}

// Test 12: the next command promotes an approved request; a scram.next left beside a revoke's bundle is not promoted
// into it. A promotion stopped between its writes is internal/client's TestPromotionStoppedBetweenItsWritesKeepsThePassword.
func TestPromotionNeverLosesTheApprovedPassword(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	id := sentID(t, e.init(e.kept))
	e.op("approve", id)
	if stderr, err := e.host("receivers"); err != nil || !strings.Contains(stderr, "credential "+id+" approved; now in use") || e.next() != "" || !e.works(e.bundle) {
		t.Fatalf("the next command did not promote: %v\n%s", err, stderr)
	}
	e.host("rotate", "--scram")
	pending := e.next()
	e.op("approve", e.nextID())
	e.op("revoke", "--tenant", "tenant-a", "--host", "host-a")
	kept, err := os.ReadFile(e.kept)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(e.bundle, kept, 0o600); err != nil {
		t.Fatal(err)
	}
	if e.next() != pending {
		t.Fatal("the test needs the old scram.next beside the new bundle")
	}
	if _, err := e.host("receivers"); err != nil || e.next() != "" || !e.works(e.bundle) {
		t.Fatalf("an old approved scram.next beside a revoke's bundle: %v", err)
	}
	if after, _ := os.ReadFile(e.bundle); !bytes.Equal(after, kept) {
		t.Fatal("an old approved request was promoted into the revoke's bundle")
	}
}

// Test 15: an operator rotate record makes the host file once; a rejection satisfies it; with a scram.next whose request
// never reached the topic, the host's next command of any kind after rotate --scram sends the same ID.
func TestOperatorRotateRecordIsActedOnOnce(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	e.op("approve", sentID(t, e.init(e.kept)))
	e.host("receivers")
	e.op("rotate", "--tenant", "tenant-a", "--host", "host-a", "--scram")
	if _, err := e.host("receivers"); err != nil || e.next() == "" {
		t.Fatalf("a host command after rotate --scram filed nothing: %v", err)
	}
	first := e.nextID()
	e.host("receivers")
	if e.nextID() != first {
		t.Fatal("a second host command filed another request for one rotate record")
	}
	e.op("reject", first)
	e.host("receivers")
	if e.next() != "" {
		t.Fatal("a rejection did not satisfy the rotate record, or its scram.next stayed")
	}
	fault := e.cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: operator.RequestsTopic, Err: kerr.TopicAuthorizationFailed, Count: -1})
	e.host("rotate", "--scram")
	fault.Remove()
	lost := e.nextID()
	if out := e.op("list", "requests"); strings.Contains(out, lost) {
		t.Fatal("the test needs a request that never reached the topic")
	}
	e.op("rotate", "--tenant", "tenant-a", "--host", "host-a", "--scram")
	if _, err := e.host("receivers"); err != nil || !strings.Contains(e.op("list", "requests"), "request="+lost) {
		t.Fatalf("after rotate --scram the host's next command did not send the pending request: %v", err)
	}
}

// Test 18 (host half): a broker applying the upsert late is retried within the probe window and the host promotes; a
// revoked host fails at once.
func TestHostRetriesABrokerApplyingLate(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	id := sentID(t, e.init(e.kept))
	e.op("approve", id)
	if stderr, retried, err := e.whileLagging(1, "receivers"); err != nil || !retried || !strings.Contains(stderr, "credential "+id+" approved; now in use") {
		t.Fatalf("host command while a broker applies late: %v\n%s", err, stderr)
	}
	e.op("revoke", "--tenant", "tenant-a", "--host", "host-a")
	start := time.Now()
	if _, err := e.host("receivers"); err == nil || !strings.Contains(err.Error(), "neither the bundle password nor the requested one is accepted; ask the operator to revoke and re-enrol this host") || time.Since(start) > operator.ProbeWindow {
		t.Fatalf("a revoked host's command = %v after %s", err, time.Since(start))
	}
}

// Test 14: host mcp's client goes on after an approval, recovers when it re-authenticates between the upsert and the
// decision, and keeps the bundle password once another process has promoted and filed again.
func TestLongLivedHostFollowsItsCredential(t *testing.T) {
	e := newSCRAMEnv(t, "600")
	sentID(t, e.init(e.kept))
	config, err := busclient.LoadConfig(filepath.Join(filepath.Dir(e.bundle), "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := busclient.Connect(config, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	ctx := context.Background()
	var request struct{ Request, Password string }
	readYAML(filepath.Join(filepath.Dir(e.bundle), "scram.next"), &request)
	decoded, _, _ := operator.DecodeCredentialRequest([]byte(request.Request))
	results, err := kadm.NewClient(e.service.Operator).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{User: operator.TenantPrincipal("tenant-a", "host-a"), Mechanism: kadm.ScramSha512,
		Iterations: decoded.SCRAM.Iterations, Salt: decoded.SCRAM.Salt, SaltedPassword: decoded.SCRAM.SaltedPassword}})
	if err != nil || results.Error() != nil {
		t.Fatalf("upsert as a stopped pass makes it: %v %v", err, results.Error())
	}
	// T-L3 runs on a request refused at re-authentication; the requests after it authenticate.
	recovered := func(when string) {
		t.Helper()
		var err error
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
			if _, err = host.Receivers(ctx); err == nil {
				return
			}
		}
		t.Fatalf("host mcp %s: %v", when, err)
	}
	time.Sleep(time.Second)
	recovered("after an upsert with no decision")
	e.op("approve", "--all")
	time.Sleep(time.Second)
	recovered("after the decision")
	if e.next() != "" || !e.works(e.bundle) {
		t.Fatal("host mcp did not promote the approved request")
	}
	e.op("rotate", "--tenant", "tenant-a", "--host", "host-a", "--scram")
	if _, err := e.host("receivers"); err != nil || e.next() == "" {
		t.Fatalf("the one-shot command filed nothing: %v", err)
	}
	time.Sleep(time.Second)
	if _, err := host.Receivers(ctx); err != nil {
		t.Fatalf("host mcp after another process filed again: %v", err)
	}
}

// Test 18 (revoke half): revoke waits for a broker applying late; one still refusing when the probe window ends ends
// the command on the probe's error, with no second upsert, no enrolled record and no bundle; a re-run completes.
func TestRevokeProbesEveryBrokerBeforeItsWait(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	e.init(e.kept)
	var mu sync.Mutex
	upserts := 0
	e.hold = func(kmsg.AlterUserSCRAMCredentialsRequestUpsertion) bool {
		mu.Lock()
		upserts++
		mu.Unlock()
		return false
	}
	e.lagNode = 1
	e.lagging.Store(true)
	kept, _ := os.ReadFile(e.kept)
	_, _, err := e.run("operator", "revoke", "--tenant", "tenant-a", "--host", "host-a")
	if err == nil || !strings.Contains(err.Error(), "probe incomplete") || strings.Contains(err.Error(), "refused after its wait") {
		t.Fatalf("revoke with a broker refusing its password = %v", err)
	}
	mu.Lock()
	sent := upserts
	mu.Unlock()
	if after, _ := os.ReadFile(e.kept); sent != 1 || e.cur() != "" || !bytes.Equal(after, kept) {
		t.Fatalf("revoke whose probe failed: %d upserts, cur %q, bundle changed %t", sent, e.cur(), !bytes.Equal(after, kept))
	}
	e.lagging.Store(false)
	e.op("revoke", "--tenant", "tenant-a", "--host", "host-a")
	if !e.works(e.kept) || e.cur() == "" {
		t.Fatal("revoke re-run did not complete")
	}
}

// A request ID can begin with "-" (raw URL base64); approve and reject take it as their operand, wherever the flags
// are.
func TestRequestIDBeginningWithAHyphenIsAnOperand(t *testing.T) {
	id := "-" + strings.Repeat("A", 21)
	if !operator.ValidCredentialID(id) {
		t.Fatalf("%s is not a request ID", id)
	}
	for _, args := range [][]string{{"approve", id}, {"reject", id}, {"--dir", "d", "approve", id}, {"reject", id, "--dir", "d"},
		{"approve", "--dir", "d", id}, {"reject", "--dir=d", id}, {"approve", "--all", id}} {
		positional, _, err := operatorArgs(args)
		if err != nil || len(positional) != 2 || positional[1] != id {
			t.Fatalf("%q parsed to %q, %v", args, positional, err)
		}
	}
	e := newSCRAMEnv(t, "1")
	for _, verb := range []string{"approve", "reject"} {
		stdout, _, err := e.run("operator", verb, id)
		if err == nil || !strings.Contains(stdout+err.Error(), "no request "+id) {
			t.Fatalf("operator %s %s = %v\n%s", verb, id, err, stdout)
		}
	}
}

// Test 18, T-H0's predicate: the bundle names the second broker; the first listed broker has not applied the
// approved password and refuses both, and the second accepts the approved one. The host retries until every listed
// broker accepts it, and does not fail on the first broker's refusal.
func TestHostRetriesWhileOnlyAnotherBrokerAcceptsTheApprovedPassword(t *testing.T) {
	e := newSCRAMEnv(t, "1")
	kept, err := operator.ReadBundle(e.kept)
	if err != nil {
		t.Fatal(err)
	}
	kept.Broker = e.cluster.ListenAddrs()[1]
	if err := operator.ReplaceBundle(e.kept, kept); err != nil {
		t.Fatal(err)
	}
	id := sentID(t, e.init(e.kept))
	e.op("approve", id)
	stderr, retried, err := e.whileLagging(0, "receivers")
	if err != nil || !retried || !strings.Contains(stderr, "credential "+id+" approved; now in use") {
		t.Fatalf("host command while only another broker accepts the approved password: %v\n%s", err, stderr)
	}
}
