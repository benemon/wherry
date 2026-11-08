package main

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha512"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// initBroker is a broker wherry operator init delegates on: its address, CA file and Kafka super user, and a client
// connected as that super user for what a test checks beyond the commands.
type initBroker struct {
	address, caFile, superUser, superPasswordFile string
	root                                          *kgo.Client
	fake                                          *kfake.Cluster // nil on a real broker
	// login checks the broker was given the password operator.password in dir holds.
	login func(t *testing.T, dir string)
}

// init runs wherry operator init in dir with the broker's flags and logs in as its new password.
func (b initBroker) init(t *testing.T, dir string) error {
	t.Helper()
	err := runOperator([]string{"init", "--dir", dir, "--broker", b.address, "--ca-file", b.caFile, "--super-user", b.superUser, "--super-user-password-file", b.superPasswordFile}, io.Discard, io.Discard)
	if err == nil {
		b.login(t, dir)
	}
	return err
}

// scramRecorder records the last SCRAM credential cluster is given for each user; holds proves which password it was.
type scramRecorder struct {
	mu  sync.Mutex
	got map[string]kmsg.AlterUserSCRAMCredentialsRequestUpsertion
}

func recordSCRAM(cluster *kfake.Cluster) *scramRecorder {
	r := &scramRecorder{got: map[string]kmsg.AlterUserSCRAMCredentialsRequestUpsertion{}}
	cluster.ControlKey(int16(kmsg.AlterUserSCRAMCredentials), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, upsert := range request.(*kmsg.AlterUserSCRAMCredentialsRequest).Upsertions {
			r.got[upsert.Name] = upsert
		}
		return nil, nil, false
	})
	return r
}

// holds reports whether user's last credential was set from password.
func (r *scramRecorder) holds(t *testing.T, user, password string) bool {
	t.Helper()
	r.mu.Lock()
	upsert, ok := r.got[user]
	r.mu.Unlock()
	if !ok {
		return false
	}
	salted, err := pbkdf2.Key(sha512.New, password, upsert.Salt, int(upsert.Iterations), sha512.Size)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(salted, upsert.SaltedPassword)
}

// kfakeInitBroker is a TLS fake broker whose only super user is kafka-root, with the share-group ceilings pool apply
// reads, which kfake does not report until set.
func kfakeInitBroker(t *testing.T) (initBroker, *scramRecorder) {
	cluster, ca := tlsBroker(t, kfake.Superuser("SCRAM-SHA-512", "kafka-root", "root-secret-value"))
	recorder := recordSCRAM(cluster)
	dir := t.TempDir()
	b := initBroker{address: cluster.ListenAddrs()[0], caFile: filepath.Join(dir, "broker-ca.pem"), superUser: "kafka-root", superPasswordFile: filepath.Join(dir, "root-password-file"), fake: cluster}
	for path, contents := range map[string]string{b.caFile: ca, b.superPasswordFile: "root-secret-value\n"} {
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	b.root = connectTLS(t, b.address, ca, "kafka-root", "root-secret-value")
	shareCeilings(t, b.root)
	b.login = func(t *testing.T, dir string) {
		t.Helper()
		password := operatorPassword(t, dir)
		if !recorder.holds(t, operator.OperatorPrincipal, password) {
			t.Fatal("the broker does not hold the password operator.password holds")
		}
	}
	return b, recorder
}

// realInitBroker is the disposable broker test/kafka/up.sh or up-cluster.sh starts, skipping t without one.
func realInitBroker(t *testing.T) initBroker {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	b := initBroker{address: os.Getenv("WHERRY_TRUST_BROKER"), caFile: os.Getenv("WHERRY_TRUST_CA"), superUser: os.Getenv("WHERRY_TRUST_SUPER_USER"), superPasswordFile: os.Getenv("WHERRY_TRUST_SUPER_USER_PASSWORD_FILE"), login: func(*testing.T, string) {}}
	ca, err := os.ReadFile(b.caFile)
	if err != nil {
		t.Fatal(err)
	}
	password, err := os.ReadFile(b.superPasswordFile)
	if err != nil {
		t.Fatal(err)
	}
	b.root = connectTLS(t, b.address, string(ca), b.superUser, strings.TrimSpace(string(password)))
	metadata, err := kadm.NewClient(b.root).BrokerMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := os.Getenv("WHERRY_TRUST_CLUSTER_ID"); want == "" || metadata.Cluster != want {
		t.Fatalf("broker cluster ID %q is not WHERRY_TRUST_CLUSTER_ID %q: this test runs only against a broker test/kafka/up.sh or up-cluster.sh starts", metadata.Cluster, want)
	}
	return b
}

// shareCeilings sets the share-group ceilings pool apply reads on the fake broker client is connected to, which kfake
// does not report until set.
func shareCeilings(t *testing.T, client *kgo.Client) {
	t.Helper()
	var alters []kadm.AlterConfig
	for name, value := range map[string]string{"group.share.max.delivery.count.limit": "10", "group.share.max.record.lock.duration.ms": "60000"} {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: name, Value: &value})
	}
	if responses, err := kadm.NewClient(client).AlterBrokerConfigs(context.Background(), alters); err != nil || responses[0].Err != nil {
		t.Fatal(err, responses)
	}
}

func operatorPassword(t *testing.T, dir string) string {
	t.Helper()
	password, err := os.ReadFile(filepath.Join(dir, "operator.password"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(password))
}

// operatorCommands runs every wherry operator command in dir on objects named with suffix, connected as wherry.operator
// through operator.yaml, and returns the first failure. No receiver runs here to publish a key custodian snapshot, so
// root writes a record the snapshot's readers skip, which suspend and rotate-key read past to find none; rotate-key
// runs that far. The removes run only with removes: their revocation waits out the broker's timeouts.
func operatorCommands(t *testing.T, root *kgo.Client, dir, suffix string, check, removes bool) error {
	t.Helper()
	pool, tenant, host, receiver := "p"+suffix, "t"+suffix, "h"+suffix, "r"+suffix
	localFixture(t, dir, receiver)
	files := writeTree(t, map[string]string{
		"pool.yaml":     "agents:\n  - name: claude\nproducer_byte_rate: 1048576\nrequest_percentage: 10\n",
		"receiver.yaml": "pool: " + pool + "\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: claude\nagent: claude\n", "      "),
	})
	wherry := func(args ...string) error {
		if err := runOperator(append(args, "--dir", dir), io.Discard, io.Discard); err != nil {
			return fmt.Errorf("%s: %w", strings.Join(args, " "), err)
		}
		return nil
	}
	if err := wherry("pool", "apply", pool, "-f", filepath.Join(files, "pool.yaml")); err != nil {
		return err
	}
	if err := root.ProduceSync(context.Background(), &kgo.Record{Topic: operator.CustodianTopic(pool), Key: []byte("not-a-snapshot")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"tenant", "add", tenant},
		{"tenant", "pool", "add", tenant, pool},
		{"tenant", "host", "add", tenant, host},
		{"receiver", "apply", receiver, "-f", filepath.Join(files, "receiver.yaml")},
		{"receiver", "show", receiver},
		{"pool", "suspend", pool},
	} {
		if err := wherry(args...); err != nil {
			return err
		}
	}
	if err := wherry("pool", "rotate-key", pool); err == nil || !strings.Contains(err.Error(), "name the suspect keys with --suspect-key") {
		return fmt.Errorf("pool rotate-key = %v; want it to find no key custodian", err)
	}
	if err := wherry("pool", "resume", pool); err != nil {
		return err
	}
	if check {
		if err := wherry("check"); err != nil {
			return err
		}
	}
	if !removes {
		return nil
	}
	var errs []error
	for _, args := range [][]string{{"tenant", "host", "remove", tenant, host}, {"receiver", "remove", receiver}} {
		errs = append(errs, wherry(args...))
	}
	return errors.Join(errs...)
}

func suffix(t *testing.T) string {
	random := make([]byte, 3)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(random)
}

// operator init, given the Kafka super user, delegates provisioning to wherry.operator: every operator command then
// succeeds connected as wherry.operator alone, and no file init writes holds the super user.
func TestOperatorInitDelegatesTheSuperUser(t *testing.T) {
	noTerminal(t)
	b, _ := kfakeInitBroker(t)
	testOperatorInitDelegates(t, b, false)
}

func TestOperatorInitDelegatesTheSuperUserOnRealBroker(t *testing.T) {
	noTerminal(t)
	testOperatorInitDelegates(t, realInitBroker(t), true)
}

func testOperatorInitDelegates(t *testing.T, b initBroker, removes bool) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("node_modules\n*.log"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.init(t, dir); err != nil {
		t.Fatal(err)
	}
	if err := operatorCommands(t, b.root, dir, suffix(t), true, removes); err != nil {
		t.Fatal(err)
	}
	var config operatorConfig
	if err := readYAML(filepath.Join(dir, "operator.yaml"), &config); err != nil || config != (operatorConfig{b.address, b.caFile, "operator.password"}) {
		t.Fatalf("operator.yaml %+v, %v; want the broker, its CA file and operator.password", config, err)
	}
	if info, err := os.Stat(filepath.Join(dir, "operator.password")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("operator.password %v, %v; want mode 0600", info, err)
	}
	if ignored, err := os.ReadFile(filepath.Join(dir, ".gitignore")); err != nil || string(ignored) != "node_modules\n*.log\noperator.password\nbundles/\n" {
		t.Fatalf(".gitignore %q, %v; want its lines kept and operator.password and bundles/ added", ignored, err)
	}
	superPassword, err := os.ReadFile(b.superPasswordFile)
	if err != nil {
		t.Fatal(err)
	}
	superValues := []string{b.superUser, strings.TrimSpace(string(superPassword)), b.superPasswordFile, filepath.Base(b.superPasswordFile)}
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, value := range superValues {
			if bytes.Contains(contents, []byte(value)) {
				return fmt.Errorf("%s holds the super user's %q", path, value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.init(t, dir); err != nil {
		t.Fatal(err)
	}
	if ignored, err := os.ReadFile(filepath.Join(dir, ".gitignore")); err != nil || string(ignored) != "node_modules\n*.log\noperator.password\nbundles/\n" {
		t.Fatalf("after a second init, .gitignore %q, %v; want each line once", ignored, err)
	}
}

// Each ACL init grants is one a command needs. Kafka implies Describe by Read, Write or Alter, and DescribeConfigs by
// AlterConfigs, so init grants neither.
func TestOperatorInitGrantsOnlyNeededACLs(t *testing.T) {
	noTerminal(t)
	testOperatorACLs(t, func(t *testing.T) initBroker { b, _ := kfakeInitBroker(t); return b })
}

func TestOperatorInitGrantsOnlyNeededACLsOnRealBroker(t *testing.T) {
	noTerminal(t)
	b := realInitBroker(t)
	// A last init restores the grant for the tests after this one.
	t.Cleanup(func() {
		if err := b.init(t, t.TempDir()); err != nil {
			t.Error(err)
		}
	})
	testOperatorACLs(t, func(*testing.T) initBroker { return b })
}

func testOperatorACLs(t *testing.T, broker func(*testing.T) initBroker) {
	for _, acl := range operator.OperatorACLs {
		for _, dropped := range acl.Operations {
			t.Run(acl.ResourceType+"/"+dropped, func(t *testing.T) {
				b := broker(t)
				dir := t.TempDir()
				if err := b.init(t, dir); err != nil {
					t.Fatal(err)
				}
				builder := kadm.NewACLs().Allow("User:" + operator.OperatorPrincipal).AllowHosts("*").Operations(map[string]kadm.ACLOperation{
					"ALTER": kadm.OpAlter, "ALTER_CONFIGS": kadm.OpAlterConfigs, "CREATE": kadm.OpCreate, "READ": kadm.OpRead, "WRITE": kadm.OpWrite,
				}[dropped])
				switch acl.ResourceType {
				case "cluster":
					builder.Clusters().ResourcePatternType(kadm.ACLPatternLiteral)
				case "topic":
					builder.Topics(acl.Resource).ResourcePatternType(kadm.ACLPatternPrefixed)
				case "group":
					builder.Groups(acl.Resource).ResourcePatternType(kadm.ACLPatternPrefixed)
				}
				deleted, err := kadm.NewClient(b.root).DeleteACLs(context.Background(), builder)
				if err == nil && len(deleted) == 1 {
					err = deleted[0].Err
				}
				if err != nil || len(deleted) != 1 || len(deleted[0].Deleted) != 1 {
					t.Fatalf("delete %s %s: %v, %v", acl.ResourceType, dropped, deleted, err)
				}
				// ACL deletion reaches each broker in turn.
				time.Sleep(time.Second)
				// Without check, which requires every grant, a dropped grant fails only a command that needs it.
				err = operatorCommands(t, b.root, dir, suffix(t), false, false)
				if err == nil {
					t.Fatalf("every command succeeded without %s on the %s; init grants it for nothing", dropped, acl.ResourceType)
				}
				t.Logf("without %s on the %s: %v", dropped, acl.ResourceType, err)
			})
		}
	}
}

// Running init again rotates wherry.operator's password: the broker takes the new one operator.password holds and
// refuses the old.
func TestOperatorInitRotatesThePassword(t *testing.T) {
	noTerminal(t)
	b, recorder := kfakeInitBroker(t)
	dir := t.TempDir()
	if err := b.init(t, dir); err != nil {
		t.Fatal(err)
	}
	old := operatorPassword(t, dir)
	if err := b.init(t, dir); err != nil {
		t.Fatal(err)
	}
	ca, err := os.ReadFile(b.caFile)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if current := operatorPassword(t, dir); current == old || recorder.holds(t, operator.OperatorPrincipal, old) || connectTLS(t, b.address, string(ca), operator.OperatorPrincipal, current).Ping(ctx) != nil {
		t.Fatal("after a second init, operator.password does not hold a new password the broker takes")
	}
	if connectTLS(t, b.address, string(ca), operator.OperatorPrincipal, old).Ping(ctx) == nil {
		t.Fatal("the broker still takes the old password")
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 3 {
		t.Fatalf("working directory holds %v, %v; want operator.yaml, operator.password and .gitignore alone", entries, err)
	}
}

// init recovers from a failure to grant, to set the password, to wait for every broker to accept it or to rename the
// password file: the failed run leaves operator.password holding the password the broker holds, or, when the wait or
// the rename failed, the broker holding the new one, and a re-run gives both a new password. No run prints the super user's password or wherry.operator's.
// TestOperatorInitKeepsItsFilesWhenTheCredentialWaitFails covers the failed wait.
func TestOperatorInitRecoversFromAFailure(t *testing.T) {
	noTerminal(t)
	b, recorder := kfakeInitBroker(t)
	dir := t.TempDir()
	passwordFile := filepath.Join(dir, "operator.password")
	secrets := []string{"root-secret-value"}
	run := func() error {
		t.Helper()
		var stdout, stderr bytes.Buffer
		err := runOperator([]string{"init", "--dir", dir, "--broker", b.address, "--ca-file", b.caFile, "--super-user", b.superUser, "--super-user-password-file", b.superPasswordFile}, &stdout, &stderr)
		if err == nil {
			b.login(t, dir)
			secrets = append(secrets, operatorPassword(t, dir))
		} else {
			fmt.Fprintln(&stderr, err)
		}
		for _, secret := range secrets {
			if strings.Contains(stdout.String()+stderr.String(), secret) {
				t.Fatalf("init printed a password: stdout %q, stderr %q", stdout.String(), stderr.String())
			}
		}
		return err
	}
	ca, err := os.ReadFile(b.caFile)
	if err != nil {
		t.Fatal(err)
	}
	// takes reports whether the broker takes password for wherry.operator, as b.login last stored it.
	takes := func(password string) bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return connectTLS(t, b.address, string(ca), operator.OperatorPrincipal, password).Ping(ctx) == nil
	}
	// pending is the password init has written to its temporary file and not yet renamed into place.
	pending := func() string {
		matches, err := filepath.Glob(filepath.Join(dir, ".operator.password.*"))
		if err != nil || len(matches) != 1 {
			t.Errorf("temporary password files %v, %v; want one", matches, err)
			return ""
		}
		password, err := os.ReadFile(matches[0])
		if err != nil {
			t.Error(err)
		}
		secrets = append(secrets, strings.TrimSpace(string(password)))
		return strings.TrimSpace(string(password))
	}
	recovered := func(failure string) {
		t.Helper()
		if matches, _ := filepath.Glob(filepath.Join(dir, ".operator.password.*")); len(matches) != 0 {
			t.Fatalf("after a failed %s, init left %v", failure, matches)
		}
		if err := run(); err != nil {
			t.Fatalf("init after a failed %s: %v", failure, err)
		}
		if !recorder.holds(t, operator.OperatorPrincipal, operatorPassword(t, dir)) {
			t.Fatalf("after init recovered from a failed %s, the broker does not hold operator.password's password", failure)
		}
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	for request, failure := range map[kmsg.Request]string{&kmsg.CreateACLsRequest{}: "grant", &kmsg.AlterUserSCRAMCredentialsRequest{}: "password change"} {
		old := operatorPassword(t, dir)
		b.fake.ControlKey(request.Key(), func(request kmsg.Request) (kmsg.Response, error, bool) {
			pending()
			switch request := request.(type) {
			case *kmsg.CreateACLsRequest:
				response := request.ResponseKind().(*kmsg.CreateACLsResponse)
				for range request.Creations {
					result := kmsg.NewCreateACLsResponseResult()
					result.ErrorCode = kerr.ClusterAuthorizationFailed.Code
					response.Results = append(response.Results, result)
				}
				return response, nil, true
			case *kmsg.AlterUserSCRAMCredentialsRequest:
				response := request.ResponseKind().(*kmsg.AlterUserSCRAMCredentialsResponse)
				for _, upsert := range request.Upsertions {
					result := kmsg.NewAlterUserSCRAMCredentialsResponseResult()
					result.User, result.ErrorCode = upsert.Name, kerr.ClusterAuthorizationFailed.Code
					response.Results = append(response.Results, result)
				}
				return response, nil, true
			}
			return nil, nil, false
		})
		if err := run(); err == nil {
			t.Fatalf("init with a failed %s succeeded", failure)
		}
		if operatorPassword(t, dir) != old || !takes(old) {
			t.Fatalf("after a failed %s, operator.password or the broker no longer holds the old password", failure)
		}
		recovered(failure)
	}
	// The rename fails once the broker holds the new password: operator.password is then a directory.
	aside := filepath.Join(t.TempDir(), "operator.password")
	var renamed string
	// kfake keeps a control function that handles nothing, so this one acts once.
	b.fake.ControlKey(int16(kmsg.AlterUserSCRAMCredentials), func(kmsg.Request) (kmsg.Response, error, bool) {
		if renamed != "" {
			return nil, nil, false
		}
		renamed = pending()
		if err := os.Rename(passwordFile, aside); err != nil {
			t.Error(err)
		}
		if err := os.MkdirAll(filepath.Join(passwordFile, "occupied"), 0o700); err != nil {
			t.Error(err)
		}
		return nil, nil, false
	})
	if err := run(); err == nil {
		t.Fatal("init with a failed rename succeeded")
	}
	if renamed == "" || !recorder.holds(t, operator.OperatorPrincipal, renamed) {
		t.Fatal("init renamed its password file before the broker held the new password")
	}
	if err := os.RemoveAll(passwordFile); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(aside, passwordFile); err != nil {
		t.Fatal(err)
	}
	recovered("rename")
}

// Without a terminal, init given no flags refuses naming the flags it needs, and writes nothing; on a terminal it asks
// for each value no flag gives.
func TestOperatorInitWithoutATerminalNamesTheFlags(t *testing.T) {
	noTerminal(t)
	dir := t.TempDir()
	err := runOperator([]string{"init", "--dir", dir}, io.Discard, io.Discard)
	if err == nil || err.Error() != "operator init has no terminal to prompt on; give --broker, --super-user, --super-user-password-file" {
		t.Fatalf("init without a terminal or flags = %v; want it refused naming the flags", err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatalf("a refused init wrote %v, %v", entries, err)
	}
	b, _ := kfakeInitBroker(t)
	scriptForms(t, keys(b.address+"\r"), keys(down+"\r"), keys(b.caFile+"\r"), keys(b.superUser+"\r"))
	var stdout, prompts bytes.Buffer
	if err := runOperator([]string{"init", "--dir", dir, "--super-user-password-file", b.superPasswordFile}, &stdout, &prompts); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 {
		t.Fatal("init wrote stdout", stdout.String())
	}
	if strings.Contains(prompts.String(), "root-secret-value") {
		t.Fatal("super user password leaked")
	}
	b.login(t, dir)
	for _, question := range []string{"Kafka broker address", "CA certificate", "Kafka super user"} {
		if !strings.Contains(prompts.String(), question) {
			t.Errorf("init asked\n%s\nwant %q", prompts.String(), question)
		}
	}
	if strings.Contains(prompts.String(), "password file") {
		t.Errorf("init asked\n%s\nwant no question --super-user-password-file answered", prompts.String())
	}
	if err := runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
}

// A client given no CA trusts the system roots alone; given one, that CA alone.
func TestKafkaOptsTrustTheSystemRootsWithoutACA(t *testing.T) {
	_, ca := tlsBroker(t)
	for name, test := range map[string]struct {
		ca    []byte
		roots bool
	}{"no CA": {nil, false}, "a CA": {[]byte(ca), true}} {
		opts, err := operator.KafkaOpts("127.0.0.1:9093", test.ca, "user", "password")
		if err != nil {
			t.Fatal(err)
		}
		client, err := kgo.NewClient(opts...)
		if err != nil {
			t.Fatal(err)
		}
		config, _ := client.OptValue(kgo.DialTLSConfig).(*tls.Config)
		client.Close()
		if config == nil || (config.RootCAs != nil) != test.roots {
			t.Errorf("%s: TLS config %+v; want root CAs %t", name, config, test.roots)
		}
	}
	if _, err := operator.KafkaOpts("127.0.0.1:9093", []byte("no certificate"), "user", "password"); err == nil {
		t.Error("KafkaOpts took a CA holding no certificate")
	}
}

// pool apply applies the working directory's pools/NAME.yaml without prompting; with no file and no terminal it refuses
// naming the fields a file gives. The guided path writes the file before applying it, so a refused apply leaves it.
func TestPoolApplyGuidedPath(t *testing.T) {
	service := operatorEnv(t)
	shareCeilings(t, service.Operator)
	if err := os.Mkdir("pools", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("pools", "kept.yaml"), []byte("agents:\n  - name: claude\nproducer_byte_rate: 1048576\nrequest_percentage: 10\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	onTerminal(t, "")
	var prompts bytes.Buffer
	if err := runOperator([]string{"pool", "apply", "kept"}, io.Discard, &prompts); err != nil || prompts.Len() != 0 {
		t.Fatalf("pool apply with its kept file = %v, asking %q; want it applied without a prompt", err, prompts.String())
	}
	noTerminal(t)
	if err := runOperator([]string{"pool", "apply", "absent"}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "no terminal") || !strings.Contains(err.Error(), poolFields) {
		t.Fatalf("pool apply without a file or a terminal = %v; want it refused naming %s", err, poolFields)
	}
	// A start-by limit the tenant feeds' retention cannot hold is refused by apply, after the prompts.
	onTerminal(t, "claude, codex\nhaiku,sonnet\nclaude-token=CLAUDE_CODE_OAUTH_TOKEN\n\nopenai=OPENAI_API_KEY, deploy=file:deploy-key\n\n2097152\n\n9999h\n")
	if err := runOperator([]string{"pool", "apply", "guided"}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "start_by_limit") {
		t.Fatalf("guided pool apply = %v; want apply to refuse the start-by limit", err)
	}
	terms, err := readPoolConfig(filepath.Join("pools", "guided.yaml"), false)
	if err != nil {
		t.Fatalf("the guided path wrote no readable file before applying: %v", err)
	}
	if len(terms.Agents) != 2 || terms.DefaultAgent != "claude" || !slices.Equal(terms.Agents[0].Models, []string{"haiku", "sonnet"}) || terms.Agents[0].Credentials[0].Env != "CLAUDE_CODE_OAUTH_TOKEN" ||
		terms.Agents[1].Credentials[1].File != "deploy-key" || terms.ProducerByteRate != 2097152 || terms.RequestPercentage != 10 || terms.StartByLimit() != 9999*time.Hour {
		t.Fatalf("the guided path wrote %+v", terms)
	}
	if state, err := service.LoadState(context.Background()); err != nil || len(state.Pools) != 1 {
		t.Fatalf("pools %v, %v; want the kept pool alone", state.Pools, err)
	}
}

// receiver apply's guided path writes receivers/NAME.yaml with the broker the receiver uses, the operator's by default,
// and applies it; without a file or a terminal it refuses naming the fields.
func TestReceiverApplyGuidedPath(t *testing.T) {
	service := operatorEnv(t)
	localFixture(t, ".", "receiver-a")
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), receiverFields) {
		t.Fatalf("receiver apply without a file or a terminal = %v; want it refused naming %s", err, receiverFields)
	}
	onTerminal(t, "pool-a\n\n"+fakeSHA256+"\n{address: https://nomad.example:4646, credential: nomad-token}\nclaude\ntier=batch\n\n")
	if err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	config, err := readReceiverConfig(filepath.Join("receivers", "receiver-a.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	if config.Pool != "pool-a" || config.Broker != service.Broker || config.PluginSHA256["nomad"] != fakeSHA256 ||
		len(config.Profiles) != 1 || config.Profiles[0].Labels["tier"] != "batch" || config.Profiles[0].Config["credential"] != "nomad-token" {
		t.Fatalf("the guided path wrote %+v", config)
	}
	if written, err := os.ReadFile(filepath.Join("receivers", "receiver-a.yaml")); err != nil || strings.Contains(string(written), "replicas") {
		t.Fatalf("the guided path wrote replicas: %v\n%s", err, written)
	}
	state, err := service.LoadState(ctx)
	if err != nil || len(state.Receivers["receiver-a"].Profiles) != 1 {
		t.Fatalf("receivers %v, %v; want receiver-a recorded", state.Receivers, err)
	}
	if _, err := operator.ReadBundle(filepath.Join("bundles", "receivers", "receiver-a.bundle")); err != nil {
		t.Fatal(err)
	}
}

// receiver apply with another broker address in the receiver's file writes a new bundle naming it, with the same
// password.
func TestReceiverApplyNewBrokerKeepsThePassword(t *testing.T) {
	service := operatorEnv(t)
	localFixture(t, ".", "receiver-a")
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("receivers", 0o755); err != nil {
		t.Fatal(err)
	}
	kept := filepath.Join("receivers", "receiver-a.yaml")
	write := func(broker string) {
		t.Helper()
		head := "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n"
		if broker != "" {
			head += "broker: " + broker + "\n"
		}
		if err := os.WriteFile(kept, []byte(head+basePlugin+item("name: claude\nagent: claude\n", "      ")), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("")
	if err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("bundles", "receivers", "receiver-a.bundle")
	before, err := operator.ReadBundle(path)
	if err != nil || before.Broker != service.Broker {
		t.Fatalf("bundle %+v, %v; want the operator's broker", before, err)
	}
	write("kafka.platform.internal:9093")
	if err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	after, err := operator.ReadBundle(path)
	if err != nil || after != (operator.Bundle{Broker: "kafka.platform.internal:9093", Principal: before.Principal, Password: before.Password}) {
		t.Fatalf("bundle %+v, %v; want the new broker with the principal and password of %+v", after, err, before)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("bundle %v, %v; want mode 0600", info, err)
	}
}

// receiver apply of an existing receiver whose bundle is not in the working directory is refused before it records
// anything: the new broker could reach the receiver only in a bundle.
func TestReceiverApplyWithoutItsBundleIsRefused(t *testing.T) {
	service := operatorEnv(t)
	localFixture(t, ".", "receiver-a")
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	file := func(broker string) string {
		return filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nbroker: " + broker + "\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: claude\nagent: claude\n", "      ")}), "receiver.yaml")
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", file(service.Broker)}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("bundles", "receivers", "receiver-a.bundle")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	end := func() int64 {
		t.Helper()
		offsets, err := kadm.NewClient(service.Operator).ListEndOffsets(ctx, operator.OperatorTopic)
		if err != nil || offsets.Error() != nil {
			t.Fatal(err, offsets.Error())
		}
		offset, _ := offsets.Lookup(operator.OperatorTopic, 0)
		return offset.Offset
	}
	before := end()
	want := path + " does not exist, so a new broker could not reach the receiver; --force gives the receiver a new password and bundle"
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", file("kafka.platform.internal:9093")}, io.Discard, io.Discard); err == nil || err.Error() != want {
		t.Fatalf("receiver apply without its bundle = %v; want %q", err, want)
	}
	if after := end(); after != before {
		t.Fatalf("a refused receiver apply moved wherry.operator's end from %d to %d", before, after)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused receiver apply left %s: %v", path, err)
	}
}

// receiver apply --force for a receiver applied from a file outside the working directory keeps the broker its bundle
// names.
func TestReceiverApplyForceKeepsTheBundlesBroker(t *testing.T) {
	service := operatorEnv(t)
	localFixture(t, ".", "receiver-a")
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nbroker: kafka.platform.internal:9093\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: claude\nagent: claude\n", "      ")}), "receiver.yaml")
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", file}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("bundles", "receivers", "receiver-a.bundle")
	before, err := operator.ReadBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "--force"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if after, err := operator.ReadBundle(path); err != nil || after.Broker != "kafka.platform.internal:9093" || after.Password == before.Password {
		t.Fatalf("bundle after --force %+v, %v; want a new password naming kafka.platform.internal:9093", after.Broker, err)
	}
}

// host add makes existing bundle directories mode 0700, as it creates new ones.
func TestHostAddRestrictsExistingBundleDirectories(t *testing.T) {
	service := operatorEnv(t)
	if err := service.TenantAdd(t.Context(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	directories := []string{"bundles", filepath.Join("bundles", "hosts"), filepath.Join("bundles", "hosts", "tenant-a")}
	for _, directory := range directories {
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := runOperator([]string{"tenant", "host", "add", "tenant-a", "host-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, directory := range directories {
		if info, err := os.Stat(directory); err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("%s: %v, %v; want mode 0700", directory, info, err)
		}
	}
}

// A dry run writes nothing to the working directory: no bundle directory, no change to an existing one's mode, and no
// kept file from prompts.
func TestDryRunWritesNothingToTheWorkingDirectory(t *testing.T) {
	service := operatorEnv(t)
	localFixture(t, ".", "receiver-a")
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(context.Background(), "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: claude\nagent: claude\n", "      ")}), "receiver.yaml")
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", file, "--dry-run"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("bundles"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bundles after a dry run: %v; want it absent", err)
	}
	if err := os.MkdirAll(filepath.Join("bundles", "receivers"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", file, "--dry-run"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join("bundles", "receivers")); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("bundles/receivers after a dry run: %v, %v; want its mode left 0755", info, err)
	}
	onTerminal(t, "claude\n\n\n\n\n\n\n\n")
	if err := runOperator([]string{"pool", "apply", "prompted", "--dry-run"}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "--dry-run writes nothing") {
		t.Fatalf("pool apply --dry-run without a kept file = %v; want it refused", err)
	}
	if _, err := os.Stat(filepath.Join("pools", "prompted.yaml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pools/prompted.yaml after a dry run: %v; want it absent", err)
	}
}

// receiver apply --force takes the broker from the receiver's kept file over the one in its existing bundle.
func TestReceiverApplyForceTakesTheKeptFilesBroker(t *testing.T) {
	service := operatorEnv(t)
	localFixture(t, ".", "receiver-a")
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(context.Background(), "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	body := "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: claude\nagent: claude\n", "      ")
	file := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "broker: kafka.old.internal:9093\n" + body}), "receiver.yaml")
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", file}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("receivers", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("receivers", "receiver-a.yaml"), []byte("broker: kafka.kept.internal:9093\n"+body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "--force"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if bundle, err := operator.ReadBundle(filepath.Join("bundles", "receivers", "receiver-a.bundle")); err != nil || bundle.Broker != "kafka.kept.internal:9093" {
		t.Fatalf("bundle after --force names %q, %v; want the kept file's broker", bundle.Broker, err)
	}
}

// operatorLogins refuses wherry.operator's client-first SCRAM message on cluster with SASL_AUTHENTICATION_FAILED, as a
// broker that has not applied a credential does, when answer says to, given the node and how many logins it was sent
// before, and counts the logins each node was sent. Other SASLAuthenticate requests, such as the super user's and
// SCRAM's second exchange, go to kfake.
type operatorLogins struct {
	mu    sync.Mutex
	count map[int32]int
}

func answerOperatorLogins(cluster *kfake.Cluster, answer func(node int32, n int) (refuse bool)) *operatorLogins {
	logins := &operatorLogins{count: map[int32]int{}}
	cluster.ControlKey(int16(kmsg.SASLAuthenticate), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		first := request.(*kmsg.SASLAuthenticateRequest).SASLAuthBytes
		if !bytes.HasPrefix(first, []byte("n,,")) || !bytes.Contains(first, []byte("n="+operator.OperatorPrincipal+",")) {
			return nil, nil, false
		}
		node := cluster.CurrentNode()
		logins.mu.Lock()
		n := logins.count[node]
		logins.count[node]++
		logins.mu.Unlock()
		if !answer(node, n) {
			return nil, nil, false
		}
		response := request.ResponseKind().(*kmsg.SASLAuthenticateResponse)
		response.ErrorCode = kerr.SaslAuthenticationFailed.Code
		return response, nil, true
	})
	return logins
}

func (l *operatorLogins) on(node int32) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count[node]
}

// WaitOperatorCredential retries a broker's refused operator login within topicWait, shared by every listed broker,
// and returns any other failure, or the parent context's, at once.
func TestWaitOperatorCredential(t *testing.T) {
	const password = "operator-secret-value"
	// delegated starts three TLS brokers, delegates wherry.operator with password through a super-user client, and
	// returns a Service whose super-user client has sent nothing yet and whose seed is node seed.
	delegated := func(t *testing.T, seed int) (*kfake.Cluster, *operator.Service) {
		cluster, ca := tlsBroker(t, kfake.NumBrokers(3), kfake.Superuser("SCRAM-SHA-512", "kafka-root", "root-secret-value"))
		if err := operator.Delegate(context.Background(), connectTLS(t, cluster.ListenAddrs()[0], ca, "kafka-root", "root-secret-value"), password); err != nil {
			t.Fatal(err)
		}
		root := connectTLS(t, cluster.ListenAddrs()[0], ca, "kafka-root", "root-secret-value")
		return cluster, &operator.Service{Operator: root, Broker: cluster.ListenAddrs()[seed], CA: []byte(ca)}
	}
	// metadata counts the Metadata requests each node answers itself.
	metadata := func(cluster *kfake.Cluster) func(int32) int {
		var mu sync.Mutex
		count := map[int32]int{}
		cluster.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			mu.Lock()
			count[cluster.CurrentNode()]++
			mu.Unlock()
			return nil, nil, false
		})
		return func(node int32) int {
			mu.Lock()
			defer mu.Unlock()
			return count[node]
		}
	}
	// sleeper holds a control until release, which cleanup also closes before the cluster closes.
	sleeper := func(t *testing.T) (hold func(*kfake.Cluster), release func()) {
		released := make(chan struct{})
		release = sync.OnceFunc(func() { close(released) })
		t.Cleanup(release)
		return func(cluster *kfake.Cluster) { cluster.SleepControl(func() { <-released }) }, release
	}
	isRefusal := func(err error) bool { return errors.Is(err, kerr.SaslAuthenticationFailed) }

	t.Run("a broker refusing twice", func(t *testing.T) {
		t.Parallel()
		cluster, service := delegated(t, 0)
		requests := metadata(cluster)
		logins := answerOperatorLogins(cluster, func(node int32, n int) bool {
			return node == 2 && n < 2
		})
		if err := service.WaitOperatorCredential(context.Background(), password); err != nil {
			t.Fatal(err)
		}
		if logins.on(2) != 3 {
			t.Fatalf("broker 2 was sent %d logins; want 2 refused, then 1 accepted", logins.on(2))
		}
		for _, node := range []int32{0, 1, 2} {
			if requests(node) == 0 {
				t.Fatalf("broker %d answered no Metadata request; want every broker probed", node)
			}
		}
	})

	t.Run("every broker refusing", func(t *testing.T) {
		t.Parallel()
		cluster, service := delegated(t, 0)
		answerOperatorLogins(cluster, func(int32, int) bool { return true })
		start := time.Now()
		err := service.WaitOperatorCredential(context.Background(), password)
		elapsed := time.Since(start)
		if err == nil || !strings.HasPrefix(err.Error(), "broker 0 as "+operator.OperatorPrincipal+": ") || !isRefusal(err) || strings.Contains(err.Error(), password) {
			t.Fatalf("wait = %v; want broker 0's refusal, without the password", err)
		}
		if elapsed < 10*time.Second || elapsed > 15*time.Second {
			t.Fatalf("wait failed after %v; want about 10s", elapsed)
		}
	})

	t.Run("one allowance for every broker", func(t *testing.T) {
		t.Parallel()
		cluster, service := delegated(t, 1)
		start := time.Now()
		logins := answerOperatorLogins(cluster, func(node int32, _ int) bool {
			return node == 2 || node == 0 && time.Since(start) < 7*time.Second
		})
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := service.WaitOperatorCredential(ctx, password)
		elapsed := time.Since(start)
		if err == nil || !strings.HasPrefix(err.Error(), "broker 2 as ") || !isRefusal(err) {
			t.Fatalf("wait = %v; want broker 2's refusal", err)
		}
		if logins.on(2) == 0 || logins.on(0) < 2 {
			t.Fatalf("broker 0 was sent %d logins and broker 2 %d; want broker 0 accepted after refusals, and broker 2 tried", logins.on(0), logins.on(2))
		}
		if elapsed > 17*time.Second {
			t.Fatalf("wait failed after %v; want one 10s allowance, not one per broker", elapsed)
		}
	})

	t.Run("its own deadline during a login after a refusal", func(t *testing.T) {
		t.Parallel()
		cluster, service := delegated(t, 0)
		hold, _ := sleeper(t)
		answerOperatorLogins(cluster, func(node int32, n int) bool {
			if node == 1 && n > 0 {
				hold(cluster)
			}
			return node == 1
		})
		if err := service.WaitOperatorCredential(context.Background(), password); !strings.HasPrefix(fmt.Sprint(err), "broker 1 as ") || !isRefusal(err) {
			t.Fatalf("wait = %v; want broker 1's saved refusal", err)
		}
	})

	t.Run("its own deadline with no refusal", func(t *testing.T) {
		t.Parallel()
		cluster, service := delegated(t, 0)
		hold, _ := sleeper(t)
		answerOperatorLogins(cluster, func(node int32, _ int) bool {
			if node == 1 {
				hold(cluster)
			}
			return false
		})
		if err := service.WaitOperatorCredential(context.Background(), password); !errors.Is(err, context.DeadlineExceeded) || isRefusal(err) {
			t.Fatalf("wait = %v; want the deadline, not a refusal", err)
		}
	})

	t.Run("a login failing otherwise", func(t *testing.T) {
		t.Parallel()
		cluster, service := delegated(t, 0)
		var handshakes atomic.Int32
		cluster.ControlKey(int16(kmsg.SASLHandshake), func(request kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			if cluster.CurrentNode() != 1 {
				return nil, nil, false
			}
			handshakes.Add(1)
			response := request.ResponseKind().(*kmsg.SASLHandshakeResponse)
			response.ErrorCode = kerr.UnsupportedSaslMechanism.Code
			return response, nil, true
		})
		if err := service.WaitOperatorCredential(context.Background(), password); !errors.Is(err, kerr.UnsupportedSaslMechanism) || handshakes.Load() != 1 {
			t.Fatalf("wait = %v after %d handshakes on broker 1; want its error after one", err, handshakes.Load())
		}
	})

	t.Run("a failed listing", func(t *testing.T) {
		t.Parallel()
		_, service := delegated(t, 0)
		service.Operator.Close()
		if err := service.WaitOperatorCredential(context.Background(), password); err == nil || !strings.HasPrefix(err.Error(), "list brokers: ") || strings.HasSuffix(err.Error(), "none listed") {
			t.Fatalf("wait = %v; want the listing's error", err)
		}
	})

	t.Run("no brokers listed", func(t *testing.T) {
		t.Parallel()
		cluster, service := delegated(t, 0)
		cluster.ControlKey(int16(kmsg.Metadata), func(request kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			if request.(*kmsg.MetadataRequest).Topics != nil {
				return nil, nil, false
			}
			return request.ResponseKind(), nil, true
		})
		if err := service.WaitOperatorCredential(context.Background(), password); err == nil || err.Error() != "list brokers: none listed" {
			t.Fatalf("wait = %v; want no brokers reported", err)
		}
	})

	t.Run("the parent cancelled during a backoff", func(t *testing.T) {
		t.Parallel()
		cluster, service := delegated(t, 0)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancelled := make(chan time.Time, 1)
		answerOperatorLogins(cluster, func(node int32, n int) bool {
			if node == 1 && n == 0 {
				time.AfterFunc(10*time.Millisecond, func() {
					cancelled <- time.Now()
					cancel()
				})
			}
			return node == 1
		})
		err := service.WaitOperatorCredential(ctx, password)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait = %v; want the cancellation", err)
		}
		// A wait that ignored the cancellation would return about 90ms after it, at the end of the 100ms backoff.
		if elapsed := time.Since(<-cancelled); elapsed >= 50*time.Millisecond {
			t.Fatalf("wait returned %v after the cancellation; want under 50ms", elapsed)
		}
	})

	t.Run("the parent cancelled during a login after a refusal", func(t *testing.T) {
		t.Parallel()
		cluster, service := delegated(t, 0)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hold, _ := sleeper(t)
		answerOperatorLogins(cluster, func(node int32, n int) bool {
			if node == 1 && n > 0 {
				cancel()
				hold(cluster)
			}
			return node == 1
		})
		if err := service.WaitOperatorCredential(ctx, password); !errors.Is(err, context.Canceled) {
			t.Fatalf("wait = %v; want the cancellation", err)
		}
	})

	t.Run("the parent's earlier deadline during a login after a refusal", func(t *testing.T) {
		t.Parallel()
		cluster, service := delegated(t, 0)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		hold, _ := sleeper(t)
		answerOperatorLogins(cluster, func(node int32, n int) bool {
			if node == 1 && n > 0 {
				hold(cluster)
			}
			return node == 1
		})
		if err := service.WaitOperatorCredential(ctx, password); !errors.Is(err, context.DeadlineExceeded) || isRefusal(err) {
			t.Fatalf("wait = %v; want the parent's deadline, not the refusal", err)
		}
	})
}

// operator init waits for every broker to accept the new operator password before it reports success.
func TestOperatorInitWaitsForTheCredential(t *testing.T) {
	b, _ := kfakeInitBroker(t)
	logins := answerOperatorLogins(b.fake, func(node int32, n int) bool {
		return node == 1 && n < 2
	})
	if err := b.init(t, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if logins.on(1) < 2 {
		t.Fatalf("init returned after broker 1 was sent %d operator logins; want both refusals consumed", logins.on(1))
	}
}

// A second init whose new password no broker accepts fails, leaves the first init's files as they were, and does not
// print the password; with the refusal gone init succeeds.
func TestOperatorInitKeepsItsFilesWhenTheCredentialWaitFails(t *testing.T) {
	b, recorder := kfakeInitBroker(t)
	dir := t.TempDir()
	if err := b.init(t, dir); err != nil {
		t.Fatal(err)
	}
	read := func() map[string][]byte {
		files := map[string][]byte{}
		for _, name := range []string{"operator.password", "operator.yaml", ".gitignore"} {
			contents, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			files[name] = contents
		}
		return files
	}
	before := read()
	old := operatorPassword(t, dir)
	var refusing atomic.Bool
	refusing.Store(true)
	var pending atomic.Value
	answerOperatorLogins(b.fake, func(int32, int) bool {
		if matches, _ := filepath.Glob(filepath.Join(dir, ".operator.password.*")); len(matches) == 1 {
			if password, err := os.ReadFile(matches[0]); err == nil {
				pending.Store(strings.TrimSpace(string(password)))
			}
		}
		return refusing.Load()
	})
	var stdout, stderr bytes.Buffer
	err := runOperator([]string{"init", "--dir", dir, "--broker", b.address, "--ca-file", b.caFile, "--super-user", b.superUser, "--super-user-password-file", b.superPasswordFile}, &stdout, &stderr)
	if err == nil {
		t.Fatal("init succeeded while every broker refused the new password")
	}
	generated, _ := pending.Load().(string)
	if generated == "" || recorder.holds(t, operator.OperatorPrincipal, old) || !recorder.holds(t, operator.OperatorPrincipal, generated) {
		t.Fatal("the broker was not given the failed init's new password")
	}
	output := err.Error() + stdout.String() + stderr.String()
	if strings.Contains(output, generated) || strings.Contains(output, old) || strings.Contains(output, "can provision") {
		t.Fatalf("failed init printed %q", output)
	}
	if after := read(); !maps.EqualFunc(before, after, bytes.Equal) {
		t.Fatal("failed init changed operator.password, operator.yaml or .gitignore")
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, ".operator.password.*")); len(matches) != 0 {
		t.Fatalf("failed init left %v", matches)
	}
	refusing.Store(false)
	if err := b.init(t, dir); err != nil {
		t.Fatalf("init after the refusal ended: %v", err)
	}
}
