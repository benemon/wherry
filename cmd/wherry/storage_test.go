package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/operator"
	"github.com/charmbracelet/x/ansi"
	"github.com/creack/pty"
)

// pipeStandardInput gives standard input text, as a password manager pipes it, for the rest of t.
func pipeStandardInput(t *testing.T, text string) {
	t.Helper()
	saved := stdin
	t.Cleanup(func() { stdin = saved })
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	go func() { io.WriteString(w, text); w.Close() }()
	stdin = r
}

// storageKeysTerms is a pool whose terms name storage.
func storageKeysTerms() operator.Terms {
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	terms.Storage = &operator.Storage{Endpoint: "https://s3.example.test", Bucket: "bucket-a"}
	return terms
}

// tenant storage records a tenant's pair from standard input, mode 0600 under storage/ and kept out of git, and names
// each live host's refresh. Every host bundle written or refreshed from then on carries the pair, a reissue keeps it
// under the new password, no receiver bundle carries it, and a refresh interrupted between hosts converges when rerun.
// Anything but exactly two non-empty lines, a key holding whitespace, an absent tenant, a missing - without a terminal
// and bad arguments are refused naming no value, and a refusal writes nothing and keeps the pair on file.
func TestTenantStorageCommand(t *testing.T) {
	service := operatorEnv(t)
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", storageKeysTerms(), false); err != nil {
		t.Fatal(err)
	}
	localFixture(t, ".", "receiver-a")
	receiverFile := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: claude\nagent: claude\n", "      ")}), "receiver.yaml")
	for _, args := range [][]string{{"tenant", "add", "tenant-a"}, {"tenant", "host", "add", "tenant-a", "host-a"}} {
		if err := runOperator(args, io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	hostA, hostB := filepath.Join("bundles", "hosts", "tenant-a", "host-a.bundle"), filepath.Join("bundles", "hosts", "tenant-a", "host-b.bundle")
	receiverBundle := filepath.Join("bundles", "receivers", "receiver-a.bundle")
	var stdout bytes.Buffer
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", receiverFile}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if want := "wrote " + receiverBundle + "; hand it to the receiver's platform, whose install guide's bundle step installs it, then start the receiver\n"; stdout.String() != want {
		t.Fatalf("receiver apply printed %q; want %q", stdout.String(), want)
	}
	keysPath := filepath.Join("storage", "tenant-a.yaml")
	const access, secret = "AKIATESTACCESSKEY", "test-secret-key-value-sentinel"
	refuse := func(t *testing.T, args []string, input, want string) {
		t.Helper()
		if input != "" {
			pipeStandardInput(t, input)
		}
		err := runOperator(args, io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%v = %v; want %q", args, err, want)
		}
		for _, value := range []string{access, secret, "AKIAOTHER", "other-secret"} {
			if strings.Contains(err.Error(), value) {
				t.Fatalf("%v = %v; want no value in the error", args, err)
			}
		}
	}
	for name, input := range map[string]string{
		"one line":     access + "\n",
		"three lines":  access + "\n" + secret + "\nextra\n",
		"blank secret": access + "\n\n",
		"blank access": "\n" + secret + "\n",
		"empty":        "\n",
	} {
		t.Run(name, func(t *testing.T) {
			refuse(t, []string{"tenant", "storage", "tenant-a", "-"}, input, "standard input must hold exactly two lines: the access key, then the secret key")
		})
	}
	refuse(t, []string{"tenant", "storage", "tenant-a", "-"}, access+"\nother secret\n", "secret key holds whitespace or a control character")
	refuse(t, []string{"tenant", "storage", "tenant-a", "-"}, "AKIA\tOTHER\n"+secret+"\n", "access key holds whitespace or a control character")
	refuse(t, []string{"tenant", "storage", "tenant-b", "-"}, access+"\n"+secret+"\n", "tenant absent")
	refuse(t, []string{"tenant", "storage", "tenant-a"}, "", "tenant storage has no terminal to prompt on; pipe the access key and the secret key in, one per line, with -")
	refuse(t, []string{"tenant", "storage", "not a name", "-"}, access+"\n"+secret+"\n", "is not a valid name")
	for _, args := range [][]string{{"tenant", "storage"}, {"tenant", "storage", "tenant-a", "extra"}, {"tenant", "storage", "tenant-a", "-", "extra"}, {"tenant", "storage", "tenant-a", "-", "-"}} {
		if err := runOperator(args, io.Discard, io.Discard); err == nil || err.Error() != operatorUsage["tenant storage"] {
			t.Fatalf("%v = %v; want its usage", args, err)
		}
	}
	if err := runOperator([]string{"tenant", "storage", "tenant-a", "--force"}, io.Discard, io.Discard); err == nil || err.Error() != "unknown option --force" {
		t.Fatalf("tenant storage --force = %v; want it refused", err)
	}
	if _, err := os.Stat("storage"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused tenant storage wrote storage/: %v", err)
	}
	pipeStandardInput(t, access+"\n"+secret)
	stdout.Reset()
	if err := runOperator([]string{"tenant", "storage", "tenant-a", "-"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if want := "recorded the storage keys of tenant-a in " + keysPath + "; tenant host add writes them into each new host's bundle\nrefresh host-a's bundle: wherry operator tenant host refresh tenant-a host-a\n"; stdout.String() != want {
		t.Fatalf("tenant storage printed %q; want %q", stdout.String(), want)
	}
	keys := operator.StorageKeys{AccessKey: access, SecretKey: secret}
	if kept, err := operator.ReadStorageKeys(keysPath); err != nil || kept != keys {
		t.Fatalf("%s holds %+v, %v; want the piped pair", keysPath, kept, err)
	}
	for path, mode := range map[string]os.FileMode{"storage": 0o700, keysPath: 0o600} {
		if info, err := os.Stat(path); err != nil || info.Mode().Perm() != mode {
			t.Fatalf("%s: %v, %v; want mode %v", path, info, err, mode)
		}
	}
	if ignored, err := os.ReadFile(".gitignore"); err != nil || !strings.Contains(string(ignored), "storage/\n") {
		t.Fatalf(".gitignore %q, %v; want storage/ in it", ignored, err)
	}
	// The pair reaches no record: wherry.operator holds nothing of it.
	records, err := operatorRecords(t, service)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if bytes.Contains(record, []byte(access)) || bytes.Contains(record, []byte(secret)) {
			t.Fatalf("a wherry.operator record holds a storage key:\n%s", record)
		}
	}
	// The bundle written before carries none until refreshed; the refresh keeps the password.
	before, err := operator.ReadBundle(hostA)
	if err != nil || before.Storage != (operator.StorageKeys{}) {
		t.Fatalf("bundle %+v, %v; want no storage keys before the refresh", before, err)
	}
	stdout.Reset()
	if err := runOperator([]string{"tenant", "host", "refresh", "tenant-a", "host-a"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if want := "refreshed " + hostA + "; hand it to the host, where wherry host init host-a.bundle installs it\n"; stdout.String() != want {
		t.Fatalf("refresh printed %q; want %q", stdout.String(), want)
	}
	after, err := operator.ReadBundle(hostA)
	if err != nil || after != (operator.Bundle{Broker: before.Broker, Principal: before.Principal, Password: before.Password, Storage: keys}) {
		t.Fatalf("refreshed bundle %+v, %v; want the pair with the password of %+v", after, err, before)
	}
	stdout.Reset()
	if err := runOperator([]string{"tenant", "host", "refresh", "tenant-a", "host-a"}, &stdout, io.Discard); err != nil || stdout.String() != hostA+" is current\n" {
		t.Fatalf("refresh again: %v, %q", err, stdout.String())
	}
	// A host added now carries the pair, and the add names the next step; a reissue keeps the pair under its new password.
	stdout.Reset()
	if err := runOperator([]string{"tenant", "host", "add", "tenant-a", "host-b"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if want := "wrote " + hostB + "; hand it to the host, where wherry host init host-b.bundle installs it\n"; stdout.String() != want {
		t.Fatalf("host add printed %q; want %q", stdout.String(), want)
	}
	if added, err := operator.ReadBundle(hostB); err != nil || added.Storage != keys {
		t.Fatalf("bundle %+v, %v; want the pair in a host added after it was recorded", added, err)
	}
	stdout.Reset()
	if err := runOperator([]string{"tenant", "host", "add", "tenant-a", "host-a", "--force"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if want := "wrote " + hostA + "; hand it to the host, where wherry host init host-a.bundle installs it\n"; stdout.String() != want {
		t.Fatalf("host add --force printed %q; want %q", stdout.String(), want)
	}
	if reissued, err := operator.ReadBundle(hostA); err != nil || reissued.Storage != keys || reissued.Password == after.Password {
		t.Fatalf("bundle after --force %+v, %v; want the pair under a new password", reissued, err)
	}
	// Receiver bundles never carry it, reissued or not.
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "--force"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if encoded, err := os.ReadFile(receiverBundle); err != nil || strings.Contains(string(encoded), "storage") || strings.Contains(string(encoded), access) {
		t.Fatalf("receiver bundle holds\n%s\n%v; want no storage keys", encoded, err)
	}
	// A refused rerun keeps the pair on file.
	refuse(t, []string{"tenant", "storage", "tenant-a", "-"}, "AKIAOTHER\n", "exactly two lines")
	if kept, err := operator.ReadStorageKeys(keysPath); err != nil || kept != keys {
		t.Fatalf("after a refused tenant storage %s holds %+v, %v; want the old pair", keysPath, kept, err)
	}
	// A changed pair names every live host's refresh; a refresh interrupted after host-a is rerun and converges.
	next := operator.StorageKeys{AccessKey: "AKIAOTHER", SecretKey: "other-secret"}
	pipeStandardInput(t, next.AccessKey+"\n"+next.SecretKey+"\n")
	stdout.Reset()
	if err := runOperator([]string{"tenant", "storage", "tenant-a", "-"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if want := "recorded the storage keys of tenant-a in " + keysPath + "; tenant host add writes them into each new host's bundle\nrefresh host-a's bundle: wherry operator tenant host refresh tenant-a host-a\nrefresh host-b's bundle: wherry operator tenant host refresh tenant-a host-b\n"; stdout.String() != want {
		t.Fatalf("tenant storage printed %q; want %q", stdout.String(), want)
	}
	if err := runOperator([]string{"tenant", "host", "refresh", "tenant-a", "host-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if stale, err := operator.ReadBundle(hostB); err != nil || stale.Storage != keys {
		t.Fatalf("host-b's bundle %+v, %v; want the old pair until its own refresh", stale, err)
	}
	for host, want := range map[string]string{"host-a": hostA + " is current\n", "host-b": "refreshed " + hostB + "; hand it to the host, where wherry host init host-b.bundle installs it\n"} {
		stdout.Reset()
		if err := runOperator([]string{"tenant", "host", "refresh", "tenant-a", host}, &stdout, io.Discard); err != nil || stdout.String() != want {
			t.Fatalf("rerun refresh of %s: %v, %q; want %q", host, err, stdout.String(), want)
		}
	}
	for _, path := range []string{hostA, hostB} {
		if bundle, err := operator.ReadBundle(path); err != nil || bundle.Storage != next {
			t.Fatalf("%s holds %+v, %v; want the new pair", path, bundle, err)
		}
	}
}

// operatorRecords is the value of every record on wherry.operator.
func operatorRecords(t *testing.T, service *operator.Service) ([][]byte, error) {
	t.Helper()
	reader := operator.NewReader(service.Operator)
	defer reader.Close()
	records, err := reader.ReadFrom(context.Background(), operator.OperatorTopic, 0)
	if err != nil {
		return nil, err
	}
	var values [][]byte
	for _, record := range records {
		values = append(values, record.Value)
	}
	return values, nil
}

// On a terminal, - is refused before standard input is read, and the masked prompts take the pair without showing it:
// an empty access key is refused with the validator's message, no answer line is printed, Ctrl-C writes nothing, and a
// completed pair is written.
func TestTenantStoragePrompts(t *testing.T) {
	service := operatorEnv(t)
	if err := service.TenantAdd(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	keysPath := filepath.Join("storage", "tenant-a.yaml")
	t.Run("terminal standard input", func(t *testing.T) {
		master, tty, err := pty.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer tty.Close()
		defer master.Close()
		saved := stdin
		t.Cleanup(func() { stdin = saved })
		stdin = tty
		io.WriteString(master, "AKIATYPED\ntyped-secret-marker\n")
		err = runOperator([]string{"tenant", "storage", "tenant-a", "-"}, io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "standard input is a terminal") || strings.Contains(err.Error(), "typed-secret-marker") {
			t.Fatalf("tenant storage - on a terminal = %v; want it refused naming no value", err)
		}
		if _, err := os.Stat(keysPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a refused tenant storage wrote %s: %v", keysPath, err)
		}
	})
	const access, secret = "AKIAPROMPTEDACCESSKEY", "prompted-secret-key-sentinel"
	leaked := func(t *testing.T, out string, err error) {
		t.Helper()
		for _, value := range []string{access, secret} {
			if strings.Contains(out, value) || strings.Contains(ansi.Strip(out), value) || (err != nil && strings.Contains(err.Error(), value)) {
				t.Fatalf("a storage key was shown: %q, %v", out, err)
			}
		}
		if lines := collapsedLines(out); len(lines) != 0 {
			t.Fatalf("a storage key prompt printed an answer line %q", lines)
		}
	}
	t.Run("cancel", func(t *testing.T) {
		scriptForms(t, keys(access+"\r"), []string{secret, "\x03"})
		var out bytes.Buffer
		err := runOperator([]string{"tenant", "storage", "tenant-a"}, io.Discard, &out)
		if !errors.Is(err, errInterrupted) {
			t.Fatalf("cancelled prompt = %v; want interrupted", err)
		}
		leaked(t, out.String(), err)
		if _, err := os.Stat(keysPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a cancelled tenant storage wrote %s: %v", keysPath, err)
		}
	})
	t.Run("recorded", func(t *testing.T) {
		scriptForms(t, []string{"\r", access + "\r"}, keys(secret+"\r"))
		var stdout, out bytes.Buffer
		if err := runOperator([]string{"tenant", "storage", "tenant-a"}, &stdout, &out); err != nil {
			t.Fatalf("%v\n%s", err, out.String())
		}
		leaked(t, out.String(), nil)
		text := ansi.Strip(out.String())
		for _, want := range []string{"Storage of tenant tenant-a", "S3 access key", "access key is required", "S3 secret key"} {
			if !strings.Contains(text, want) {
				t.Fatalf("prompts %q lack %q", text, want)
			}
		}
		if kept, err := operator.ReadStorageKeys(keysPath); err != nil || kept != (operator.StorageKeys{AccessKey: access, SecretKey: secret}) {
			t.Fatalf("%s holds %+v, %v; want the typed pair", keysPath, kept, err)
		}
		if !strings.HasPrefix(stdout.String(), "recorded the storage keys of tenant-a in "+keysPath) {
			t.Fatalf("tenant storage printed %q", stdout.String())
		}
	})
}

// A bundle carrying the tenant's storage keys serves every entitled pool with storage: host init lists no pool's
// storage as missing, asks no Storage question, and writes no storage entry; the keys stay in the installed copy. A
// bundle without them lists the pool's storage as before.
func TestHostInitAsksNoStorageWhenTheBundleCarriesTheKeys(t *testing.T) {
	h := newInitHost(t, map[string]operator.Terms{"pool-a": storageKeysTerms()})
	bundled := operator.StorageKeys{AccessKey: "AKIABUNDLEACCESSKEY", SecretKey: "bundle-secret-key-value"}
	editBundle(t, h.bundle, func(bundle *operator.Bundle) { bundle.Storage = bundled })
	t.Run("without a terminal", func(t *testing.T) {
		noTerminal(t)
		_, prompts := h.init(t, h.bundle)
		if strings.Contains(prompts, "storage of pool") {
			t.Fatalf("init listed the pool's storage as missing:\n%s", prompts)
		}
		if config := h.written(t); len(config.Storage)+len(config.Credentials) != 0 {
			t.Fatalf("config %+v; want no storage entry", config)
		}
		if installed, err := operator.ReadBundle(filepath.Join(h.dir, "bundle.yaml")); err != nil || installed.Storage != bundled {
			t.Fatalf("installed bundle %+v, %v; want the keys kept", installed, err)
		}
	})
	t.Run("on a terminal", func(t *testing.T) {
		scriptForms(t)
		_, prompts := h.init(t)
		if strings.Contains(prompts, "Storage") || !strings.Contains(prompts, "Summary  1 of 1") {
			t.Fatalf("init asked about storage:\n%s", prompts)
		}
		if config := h.written(t); len(config.Storage) != 0 {
			t.Fatalf("config %+v; want no storage entry", config)
		}
	})
	t.Run("without the keys", func(t *testing.T) {
		editBundle(t, h.bundle, func(bundle *operator.Bundle) { bundle.Storage = operator.StorageKeys{} })
		noTerminal(t)
		if _, prompts := h.init(t, h.bundle); !strings.Contains(prompts, "storage of pool pool-a") {
			t.Fatalf("init with a bundle carrying no keys printed\n%s\nwant the pool's storage listed", prompts)
		}
	})
}
