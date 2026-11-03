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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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
	// login makes the broker take the password operator.password in dir holds.
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

// scramRecorder records the last SCRAM credential cluster is given for each user. kfake's handshake names 4096
// iterations, so one set at wherry's 8192 cannot log in until re-salted; holds proves which password it was.
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
		upsert := kadm.UpsertSCRAM{User: operator.OperatorPrincipal, Mechanism: kadm.ScramSha512, Iterations: 4096, Password: password}
		if altered, err := kadm.NewClient(b.root).AlterUserSCRAMs(context.Background(), nil, []kadm.UpsertSCRAM{upsert}); err != nil || altered.Error() != nil {
			t.Fatal(err, altered.Error())
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
func operatorCommands(t *testing.T, root *kgo.Client, dir, suffix string, removes bool) error {
	t.Helper()
	pool, tenant, host, receiver := "p"+suffix, "t"+suffix, "h"+suffix, "r"+suffix
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
		{"pool", "entitle", pool, "--tenant", tenant},
		{"host", "add", host, "--tenant", tenant},
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
	for _, args := range [][]string{{"pool", "resume", pool}, {"check"}} {
		if err := wherry(args...); err != nil {
			return err
		}
	}
	if !removes {
		return nil
	}
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, args := range [][]string{{"host", "remove", host, "--tenant", tenant}, {"receiver", "remove", receiver}} {
		wg.Go(func() { errs[i] = wherry(args...) })
	}
	wg.Wait()
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
	if err := operatorCommands(t, b.root, dir, suffix(t), removes); err != nil {
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

// Each ACL init grants is one a command needs, but for those Kafka implies by another it grants: Describe by Read,
// Write or Alter, DescribeConfigs by AlterConfigs.
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
	implied := map[string][]string{"DESCRIBE": {"READ", "WRITE", "ALTER"}, "DESCRIBE_CONFIGS": {"ALTER_CONFIGS"}}
	for _, acl := range operator.OperatorACLs {
		for _, dropped := range acl.Operations {
			t.Run(acl.ResourceType+"/"+dropped, func(t *testing.T) {
				b := broker(t)
				dir := t.TempDir()
				if err := b.init(t, dir); err != nil {
					t.Fatal(err)
				}
				builder := kadm.NewACLs().Allow("User:" + operator.OperatorPrincipal).AllowHosts("*").Operations(map[string]kadm.ACLOperation{
					"ALTER": kadm.OpAlter, "DESCRIBE": kadm.OpDescribe, "ALTER_CONFIGS": kadm.OpAlterConfigs, "DESCRIBE_CONFIGS": kadm.OpDescribeConfigs,
					"CREATE": kadm.OpCreate, "READ": kadm.OpRead, "WRITE": kadm.OpWrite,
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
				covered := slices.ContainsFunc(implied[dropped], func(other string) bool { return slices.Contains(acl.Operations, other) })
				err = operatorCommands(t, b.root, dir, suffix(t), false)
				if covered && err != nil {
					t.Fatalf("without %s on the %s, which Kafka implies by another grant: %v", dropped, acl.ResourceType, err)
				}
				if !covered && err == nil {
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

// init recovers from a failure to grant, to set the password or to rename the password file: the failed run leaves
// operator.password holding the password the broker holds, or, when only the rename failed, the broker holding the new
// one, and a re-run gives both a new password. No run prints the super user's password or wherry.operator's.
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
	onTerminal(t, b.address+"\n"+b.caFile+"\n"+b.superUser+"\n")
	var prompts bytes.Buffer
	if err := runOperator([]string{"init", "--dir", dir, "--super-user-password-file", b.superPasswordFile}, io.Discard, &prompts); err != nil {
		t.Fatal(err)
	}
	b.login(t, dir)
	for _, question := range []string{"Kafka broker address", "CA file", "Kafka super user: "} {
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
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), receiverFields) {
		t.Fatalf("receiver apply without a file or a terminal = %v; want it refused naming %s", err, receiverFields)
	}
	onTerminal(t, "pool-a\n\n"+fakeSHA256+"\n{address: https://nomad.example:4646, credential: nomad-token}\nclaude\ntier=batch\n2\n\n")
	if err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	config, err := readReceiverConfig(filepath.Join("receivers", "receiver-a.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	if config.Pool != "pool-a" || config.Broker != service.Broker || config.Capacity != 2 || config.PluginSHA256["nomad"] != fakeSHA256 ||
		len(config.Profiles) != 1 || config.Profiles[0].Labels["tier"] != "batch" || config.Profiles[0].Config["credential"] != "nomad-token" {
		t.Fatalf("the guided path wrote %+v", config)
	}
	state, err := service.LoadState(ctx)
	if err != nil || state.Receivers["receiver-a"].Capacity != 2 {
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
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir("receivers", 0o755); err != nil {
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
	operatorEnv(t)
	directories := []string{"bundles", filepath.Join("bundles", "hosts"), filepath.Join("bundles", "hosts", "tenant-a")}
	for _, directory := range directories {
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := runOperator([]string{"host", "add", "host-a", "--tenant", "tenant-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, directory := range directories {
		if info, err := os.Stat(directory); err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("%s: %v, %v; want mode 0700", directory, info, err)
		}
	}
}
