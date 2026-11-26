package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/charmbracelet/huh"
	"github.com/creack/pty"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func localFixture(t *testing.T, dir, name string) {
	t.Helper()
	path := filepath.Join(dir, "receivers", name+".config.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("bundle: /etc/wherry/receiver.bundle\ncredentials:\n  nomad-token: {env: NOMAD_TOKEN}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func localReceiver() operator.ReceiverConfig {
	return operator.ReceiverConfig{Receiver: "r", Broker: "broker", Profiles: []operator.ProfileSettings{{Name: "a", Plugin: "nomad", Config: map[string]any{"credential": "nomad-token"}}}}
}

// localForms accept each variable as derived: the only questions apply asks are the credential sources. No question
// asks for the bundle, the directories, the stop window, the metrics address or the broker's CA.
func localForms(variables ...string) []receiverForm {
	var forms []receiverForm
	for _, variable := range variables {
		forms = append(forms, pickForm("env", "\r"), inputForm(variable, "\r"))
	}
	return forms
}

func askLocal(config operator.ReceiverConfig, out *bytes.Buffer) (map[string]credential.Source, error) {
	return promptReceiverCredentials(&questions{context.Background(), stdin, &promptLatch{out: out}}, receiverCredentials(config))
}

func retypeLocal(bad, good string) []string { return []string{"\x15" + bad, "\r", "\x15", good + "\r"} }

// receiverPlatform points the platform variables at dir, which holds a bundle, so a file that leaves bundle out loads.
func receiverPlatform(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bundle"), []byte("broker: b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CREDENTIALS_DIRECTORY", dir)
	t.Setenv("STATE_DIRECTORY", "")
	t.Setenv("NOMAD_SECRETS_DIR", "")
	return dir
}

// apply asks for the credential sources alone, under one heading, and the file it writes loads with the platform's
// bundle and state directory, the default plugin directory, a 20s drain and metrics on :9464.
func TestReceiverLocalPrompts(t *testing.T) {
	config := localReceiver()
	var out bytes.Buffer
	scriptReceiver(t, &out, localForms("NOMAD_TOKEN")...)
	sources, err := askLocal(config, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sources, map[string]credential.Source{"nomad-token": {Env: "NOMAD_TOKEN"}}) {
		t.Fatalf("sources = %+v", sources)
	}
	if got := collapsedLines(out.String()); !reflect.DeepEqual(got, []string{"? Credential nomad-token  Environment variable NOMAD_TOKEN"}) {
		t.Fatalf("prompt order: %q", got)
	}
	if !strings.Contains(out.String(), "\nCredentials\n") || strings.Contains(out.String(), "Receiver host") {
		t.Fatalf("headings: %q", out.String())
	}
	for _, question := range []string{"Bundle path", "State directory", "Plugin directory", "Stop window", "Metrics"} {
		if strings.Contains(out.String(), question) {
			t.Fatalf("apply asked %q: %q", question, out.String())
		}
	}
	platform := receiverPlatform(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := writeYAML(path, receiverLocalWritten{MetricsAddr: defaultMetricsAddr, DrainTimeout: defaultDrainTimeout, Credentials: sources}); err != nil {
		t.Fatal(err)
	}
	local, _, err := loadReceiverLocalConfig(path)
	want := receiverLocalConfig{Bundle: filepath.Join(platform, "bundle"), StateDir: defaultStateDir, PluginDir: defaultPluginDir, MetricsAddr: ":9464", DrainTimeout: 20 * time.Second, Credentials: sources}
	if err != nil || !reflect.DeepEqual(local, want) {
		t.Fatalf("the written file loads as %+v, %v; want %+v", local, err, want)
	}
}

// The full flow writes the exact YAML: the sources asked for, one from a variable and one from a file, with the metrics
// address and drain timeout a receiver without a file gets, and never a bundle, a directory or a ca_file.
func TestReceiverLocalFullFlow(t *testing.T) {
	config := localReceiver()
	config.Profiles = append(config.Profiles, operator.ProfileSettings{Name: "b", Plugin: "kubernetes", Config: map[string]any{"credential": "kube-token"}})
	forms := append(localForms("KUBE_TOKEN"), pickForm("env", down+"\r"), inputForm("", "/run/secrets/nomad\r"))
	var out bytes.Buffer
	scriptReceiver(t, &out, forms...)
	dir := t.TempDir()
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, config); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "receivers", "r.config.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "plugin_dir: /usr/local/libexec/wherry/plugins\nmetrics_addr: :9464\ndrain_timeout: 20s\ncredentials:\n    kube-token:\n        env: KUBE_TOKEN\n    nomad-token:\n        file: /run/secrets/nomad\n"
	if string(data) != want {
		t.Fatalf("YAML: got\n%s\nwant\n%s", data, want)
	}
	expected := receiverLocalConfig{PluginDir: defaultPluginDir, MetricsAddr: ":9464", DrainTimeout: 20 * time.Second, Credentials: map[string]credential.Source{"kube-token": {Env: "KUBE_TOKEN"}, "nomad-token": {File: "/run/secrets/nomad"}}}
	if local, _, err := readReceiverLocalConfig(path); err != nil || !reflect.DeepEqual(local, expected) {
		t.Fatalf("read back: %+v, %v", local, err)
	}
	lines := collapsedLines(out.String())
	credentials := []string{"? Credential kube-token  Environment variable KUBE_TOKEN", "? Credential nomad-token  File /run/secrets/nomad"}
	if !reflect.DeepEqual(lines, credentials) {
		t.Fatalf("credential lines: %q", lines)
	}
	if strings.Contains(out.String(), "warning") || strings.Contains(out.String(), "CA") {
		t.Fatalf("the local configuration questions mention the CA: %q", out.String())
	}
}

// With no platform credential named, apply writes no file and says the receiver runs without one, with a terminal or
// without, and a dry run says the same.
func TestReceiverLocalWithoutCredentialsWritesNoFile(t *testing.T) {
	config := localReceiver()
	config.Profiles = []operator.ProfileSettings{{Name: "a", Plugin: "nomad", Config: map[string]any{"address": "https://nomad.example:4646"}}}
	for _, dryRun := range []bool{false, true} {
		dir := t.TempDir()
		path := filepath.Join(dir, "receivers", "r.config.yaml")
		var out bytes.Buffer
		scriptReceiver(t, &out)
		if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, dryRun, &out, config); err != nil {
			t.Fatalf("dry run %t: %v", dryRun, err)
		}
		if want := path + " is absent and no profile names a platform credential; the receiver runs without a file\n"; out.String() != want {
			t.Fatalf("dry run %t printed %q, want %q", dryRun, out.String(), want)
		}
		if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("dry run %t wrote under receivers: %v", dryRun, err)
		}
	}
}

func TestReceiverLocalFile(t *testing.T) {
	dir := t.TempDir()
	config := localReceiver()
	noTerminal(t)
	path := filepath.Join(dir, "receivers", "r.config.yaml")
	var out bytes.Buffer
	err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, config)
	if err == nil || err.Error() != path+" does not exist and there is no terminal to prompt on; write it with a credentials source for nomad-token" {
		t.Fatalf("missing: %v", err)
	}
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, true, &out, config); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), path+" is absent; apply will ask for a source for each platform credential\n") {
		t.Fatal(out.String())
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("dry run created files: %v", err)
	}
	scriptReceiver(t, &out, localForms("NOMAD_TOKEN")...)
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, config); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "wrote "+path+"\n") {
		t.Fatal(out.String())
	}
	local, _, err := readReceiverLocalConfig(path)
	if err != nil || local.DrainTimeout != 20*time.Second || local.Credentials["nomad-token"].Env != "NOMAD_TOKEN" {
		t.Fatalf("load: %+v %v", local, err)
	}
	before, _ := os.Stat(path)
	contents, _ := os.ReadFile(path)
	if before.Mode().Perm() != 0o600 {
		t.Fatal(before.Mode())
	}
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(path)
	kept, _ := os.ReadFile(path)
	if !bytes.Equal(contents, kept) || before.Mode() != after.Mode() || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("provided file changed")
	}
	// A provided file is read on the operator's machine without the receiver's platform: a bundle it leaves out is not
	// looked for here.
	provided := t.TempDir()
	if err := os.WriteFile(filepath.Join(provided, "config.yaml"), []byte("drain_timeout: 0s\ncredentials:\n  nomad-token: {command: [token]}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := receiverLocalFile(context.Background(), dir, "r", provided, true, true, &out, config); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "would use local configuration "+filepath.Join(provided, "config.yaml")) || strings.Contains(out.String(), "warning:") {
		t.Fatal(out.String())
	}
	given := filepath.Join(provided, "config.yaml")
	before, _ = os.Stat(given)
	contents, _ = os.ReadFile(given)
	if err := receiverLocalFile(context.Background(), dir, "r", provided, true, false, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	after, _ = os.Stat(given)
	kept, _ = os.ReadFile(given)
	if !bytes.Equal(contents, kept) || after.Mode().Perm() != 0o640 || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("provided file changed")
	}
}

func TestReceiverLocalRefusesSymlink(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "escaped.yaml")
	if err := os.MkdirAll(filepath.Join(dir, "receivers"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "receivers", "r.config.yaml")); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	scriptReceiver(t, &out, localForms("NOMAD_TOKEN")...)
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, localReceiver()); err == nil {
		t.Fatal("wrote through a dangling symlink")
	}
	if _, err := os.Stat(outside); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink target written: %v", err)
	}
}

// An invalid answer shows its error inline, and the corrected value is stored.
func TestReceiverLocalCorrections(t *testing.T) {
	for _, test := range []struct {
		name    string
		field   int
		edit    func([]receiverForm) []receiverForm
		message string
		stored  credential.Source
	}{
		{"leading digit variable", 1, func(forms []receiverForm) []receiverForm {
			forms[1] = inputForm("NOMAD_TOKEN", retypeLocal("1X", "GOOD")...)
			return forms
		}, "invalid environment variable name", credential.Source{Env: "GOOD"}},
		{"dashed variable", 1, func(forms []receiverForm) []receiverForm {
			forms[1] = inputForm("NOMAD_TOKEN", retypeLocal("A-B", " A_B ")...)
			return forms
		}, "invalid environment variable name", credential.Source{Env: "A_B"}},
		{"blank file path", 1, func(forms []receiverForm) []receiverForm {
			forms[0] = pickForm("env", down+"\r")
			forms[1] = inputForm("", retypeLocal("", "/run/token")...)
			return forms
		}, "credential file path must not be blank", credential.Source{File: "/run/token"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			frames := scriptReceiver(t, &out, test.edit(localForms("NOMAD_TOKEN"))...)
			sources, err := askLocal(localReceiver(), &out)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(strings.Fields((*frames)[test.field]), " "), test.message) {
				t.Fatalf("form %d did not show %q: %q", test.field, test.message, (*frames)[test.field])
			}
			if !reflect.DeepEqual(sources["nomad-token"], test.stored) {
				t.Fatalf("corrected value not stored: %+v", sources)
			}
		})
	}
}

// The credentials are asked once each in sorted order, with a derived variable that cannot start with a digit.
func TestReceiverLocalMergedCredentials(t *testing.T) {
	path := writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\nplugins:\n  nomad:\n    config: {credential: z-token}\n    profiles:\n      - {name: a, agent: claude}\n      - {name: b, agent: claude}\n      - {name: c, agent: claude, config: {credential: null}}\n      - {name: d, agent: claude, config: {credential: 9-token}}\n"})
	config, err := readReceiverConfig(filepath.Join(path, "receiver.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	frames := scriptReceiver(t, &out, localForms("_9_TOKEN", "Z_TOKEN")...)
	sources, err := askLocal(config, &out)
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 2 || sources["9-token"].Env != "_9_TOKEN" || sources["z-token"].Env != "Z_TOKEN" {
		t.Fatalf("credentials: %+v", sources)
	}
	if !strings.Contains((*frames)[0], "Credential 9-token") || !strings.Contains((*frames)[2], "Credential z-token") {
		t.Fatalf("question order: %q", *frames)
	}
	lines := collapsedLines(out.String())
	if want := []string{"? Credential 9-token  Environment variable _9_TOKEN", "? Credential z-token  Environment variable Z_TOKEN"}; !reflect.DeepEqual(lines, want) || strings.Count(strings.Join(lines, "\n"), "Credential z-token") != 1 {
		t.Fatalf("collapse order: %q", lines)
	}
}

// Two credentials are asked, and collapse, in receiverCredentials' order, one from its derived variable and one from a
// file.
func TestReceiverLocalTwoCredentials(t *testing.T) {
	config := localReceiver()
	config.Profiles = append(config.Profiles, operator.ProfileSettings{Name: "b", Plugin: "nomad", Config: map[string]any{"credential": "9-token"}}, operator.ProfileSettings{Name: "c", Plugin: "nomad", Config: map[string]any{"credential": "nomad-token"}})
	var out bytes.Buffer
	forms := append(localForms("_9_TOKEN"), pickForm("env", down+"\r"), inputForm("", "/run/nomad\r"))
	frames := scriptReceiver(t, &out, forms...)
	sources, err := askLocal(config, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sources, map[string]credential.Source{"9-token": {Env: "_9_TOKEN"}, "nomad-token": {File: "/run/nomad"}}) {
		t.Fatalf("credentials: %+v", sources)
	}
	if !strings.Contains((*frames)[0], "Credential 9-token") || !strings.Contains((*frames)[1], "Variable") || !strings.Contains((*frames)[2], "Credential nomad-token") || !strings.Contains((*frames)[3], "File path") {
		t.Fatalf("question order: %q", *frames)
	}
	lines := collapsedLines(out.String())
	if want := []string{"? Credential 9-token  Environment variable _9_TOKEN", "? Credential nomad-token  File /run/nomad"}; !reflect.DeepEqual(lines, want) {
		t.Fatalf("collapse order: %q", lines)
	}
}

// A credential file path is written raw, and its collapse line escapes it.
func TestReceiverLocalEscape(t *testing.T) {
	var out bytes.Buffer
	forms := []receiverForm{pickForm("env", down+"\r"), inputForm("", "/run/\u202etoken\r")}
	scriptReceiver(t, &out, forms...)
	dir := t.TempDir()
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, localReceiver()); err != nil {
		t.Fatal(err)
	}
	local, _, err := readReceiverLocalConfig(filepath.Join(dir, "receivers", "r.config.yaml"))
	if err != nil || local.Credentials["nomad-token"].File != "/run/\u202etoken" {
		t.Fatalf("written: %+v, %v", local, err)
	}
	lines := strings.Join(collapsedLines(out.String()), "\n")
	if !strings.Contains(lines, `? Credential nomad-token  File /run/\u202etoken`) || strings.Contains(lines, "\u202e") {
		t.Fatalf("escaped file collapse: %q", lines)
	}
}

// receiverApplyEnv applies pool-a and returns a receiver file r can be applied from without the object questions.
func receiverApplyEnv(t *testing.T) (*operator.Service, string) {
	service := operatorEnv(t)
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", ProducerByteRate: 1 << 20, RequestPercentage: 50, DeliveryLimit: 5, LockDurationMS: 15000}
	if err := service.PoolApply(context.Background(), "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\nplugins:\n  nomad:\n    profiles:\n      - name: a\n        agent: claude\n        config: {credential: nomad-token}\n"}), "receiver.yaml")
	return service, file
}

// receiverUnapplied fails t if r has a local file, a bundle or a record.
func receiverUnapplied(t *testing.T, service *operator.Service) {
	t.Helper()
	for _, written := range []string{"receivers", "bundles"} {
		if _, err := os.Stat(written); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("created %s: %v", written, err)
		}
	}
	state, err := service.LoadState(context.Background())
	if (err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition)) || len(state.Receivers) != 0 {
		t.Fatalf("applied receiver: %v, %v", state.Receivers, err)
	}
}

// receiver apply of a receiver whose profiles name no platform credential writes no local file, with no terminal to
// ask on; one that names a credential asks for its source alone and writes the file.
func TestReceiverApplyAsksForCredentialsAlone(t *testing.T) {
	service, _ := receiverApplyEnv(t)
	without := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\nplugins:\n  nomad:\n    profiles:\n      - name: a\n        agent: claude\n"}), "receiver.yaml")
	var out bytes.Buffer
	if err := runOperator([]string{"apply", "--receiver", "r", "--file", without}, io.Discard, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), filepath.Join("receivers", "r.config.yaml")+" is absent and no profile names a platform credential; the receiver runs without a file\n") {
		t.Fatalf("apply printed %q", out.String())
	}
	if _, err := os.Stat("receivers"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("apply wrote under receivers: %v", err)
	}
	state, err := service.LoadState(context.Background())
	if err != nil || len(state.Receivers) != 1 {
		t.Fatalf("receivers %v, %v; want r applied", state.Receivers, err)
	}
	named := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\nplugins:\n  nomad:\n    profiles:\n      - name: a\n        agent: claude\n        config: {credential: nomad-token}\n"}), "receiver.yaml")
	out.Reset()
	scriptReceiver(t, &out, localForms("NOMAD_TOKEN")...)
	if err := runOperator([]string{"apply", "--receiver", "r", "--file", named}, io.Discard, &out); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(filepath.Join("receivers", "r.config.yaml"))
	if want := "plugin_dir: /usr/local/libexec/wherry/plugins\nmetrics_addr: :9464\ndrain_timeout: 20s\ncredentials:\n    nomad-token:\n        env: NOMAD_TOKEN\n"; err != nil || string(written) != want {
		t.Fatalf("apply wrote %q, %v; want %q", written, err, want)
	}
	if lines := collapsedLines(out.String()); !reflect.DeepEqual(lines, []string{"? Credential nomad-token  Environment variable NOMAD_TOKEN"}) {
		t.Fatalf("apply asked %q", lines)
	}
}

func TestReceiverLocalInterrupts(t *testing.T) {
	for _, mode := range []string{"ctrl-c at credential", "eof at variable"} {
		t.Run(mode, func(t *testing.T) {
			service, file := receiverApplyEnv(t)
			var out bytes.Buffer
			var forms []receiverForm
			if mode == "ctrl-c at credential" {
				forms = append(forms, pickForm("env", "\x03"))
			} else {
				forms = append(forms, pickForm("env", "\r"))
			}
			scriptReceiver(t, &out, forms...)
			if mode == "eof at variable" {
				scripted := runQuestion
				n := 0
				runQuestion = func(q *questions, f huh.Field) error {
					n++
					if n == 2 {
						next := *q
						next.in = strings.NewReader("NOMAD_TOKEN")
						return next.runForm(f)
					}
					return scripted(q, f)
				}
			}
			err := runOperator([]string{"apply", "--receiver", "r", "--file", file}, io.Discard, &out)
			if !errors.Is(err, errInterrupted) {
				t.Fatalf("interrupt: %v", err)
			}
			receiverUnapplied(t, service)
		})
	}
}

func TestReceiverLocalEligibility(t *testing.T) {
	master, tty, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer tty.Close()
	service, file := receiverApplyEnv(t)
	old := runQuestion
	t.Cleanup(func() { runQuestion = old })
	runQuestion = func(*questions, huh.Field) error { t.Fatal("ineligible prompt asked"); return nil }
	for _, mode := range []string{"stdin", "dumb", "writer"} {
		t.Run(mode, func(t *testing.T) {
			stdin = tty
			var out io.Writer = tty
			t.Setenv("TERM", "xterm")
			switch mode {
			case "stdin":
				stdin = strings.NewReader("")
			case "dumb":
				t.Setenv("TERM", "dumb")
			case "writer":
				out = io.Discard
			}
			err := runOperator([]string{"apply", "--receiver", "r", "--file", file}, io.Discard, out)
			want := filepath.Join("receivers", "r.config.yaml") + " does not exist and there is no terminal to prompt on; write it with a credentials source for nomad-token"
			if err == nil || err.Error() != want {
				t.Fatalf("eligibility error: %v", err)
			}
			receiverUnapplied(t, service)
		})
	}
}

func TestReceiverLocalFailedWriter(t *testing.T) {
	service, file := receiverApplyEnv(t)
	old := promptsEligible
	t.Cleanup(func() { promptsEligible = old })
	promptsEligible = func(io.Writer) bool { return true }
	if err := runOperator([]string{"apply", "--receiver", "r", "--file", file}, io.Discard, failingPrompt{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer error: %v", err)
	}
	receiverUnapplied(t, service)
}

func TestReceiverLocalApplyRefusals(t *testing.T) {
	for _, test := range []struct {
		name, profile, local, want string
		terminal                   bool
	}{
		{"absent", "credential: nomad-token", "", "receivers/r.config.yaml", false},
		{"missing-source", "credential: nomad-token", "bundle: /bundle\n", "nomad-token", false},
		{"invalid-receiver", "credential: bad/name", "", "valid name", true},
		{"outside-terms", "", "", "outside pool terms", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := operatorEnv(t)
			ctx := context.Background()
			terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", ProducerByteRate: 1 << 20, RequestPercentage: 50, DeliveryLimit: 5, LockDurationMS: 15000}
			if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
				t.Fatal(err)
			}
			agent := "claude"
			if test.name == "outside-terms" {
				agent = "other"
			}
			file := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\nplugins:\n  nomad:\n    profiles:\n      - name: a\n        agent: " + agent + "\n        config: {" + test.profile + "}\n"}), "receiver.yaml")
			if test.local != "" {
				if err := os.Mkdir("receivers", 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile("receivers/r.config.yaml", []byte(test.local), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if test.terminal {
				scriptForms(t)
			}
			end := func() int64 {
				offsets, err := kadm.NewClient(service.Operator).ListEndOffsets(ctx, operator.OperatorTopic)
				if err != nil || offsets.Error() != nil {
					t.Fatal(err)
				}
				offset, _ := offsets.Lookup(operator.OperatorTopic, 0)
				return offset.Offset
			}
			before := end()
			var out bytes.Buffer
			err := runOperator([]string{"apply", "--receiver", "r", "--file", file}, io.Discard, &out)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %s", err, test.want)
			}
			if end() != before {
				t.Fatal("recorded before local checks")
			}
			if _, err := os.Stat("bundles"); !os.IsNotExist(err) {
				t.Fatalf("bundle directories created: %v", err)
			}
			if test.local == "" {
				if _, err := os.Stat("receivers/r.config.yaml"); !os.IsNotExist(err) {
					t.Fatalf("local file created: %v", err)
				}
			}
			if test.terminal && out.Len() != 0 {
				t.Fatalf("prompted before checking: %s", out.String())
			}
		})
	}
}

func TestReceiverLocalApplyDryRun(t *testing.T) {
	for _, present := range []bool{false, true} {
		for _, tty := range []bool{false, true} {
			t.Run(fmt.Sprintf("present=%t/terminal=%t", present, tty), func(t *testing.T) {
				service := operatorEnv(t)
				ctx := context.Background()
				terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", ProducerByteRate: 1 << 20, RequestPercentage: 50, DeliveryLimit: 5, LockDurationMS: 15000}
				if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
					t.Fatal(err)
				}
				file := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: claude\nagent: claude\n", "      ")}), "receiver.yaml")
				if present {
					localFixture(t, ".", "r")
				}
				if tty {
					scriptForms(t)
				}
				var stdout, stderr bytes.Buffer
				if err := runOperator([]string{"apply", "--receiver", "r", "--file", file, "--dry-run"}, &stdout, &stderr); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(stderr.String(), "receivers/r.config.yaml") {
					t.Fatal(stderr.String())
				}
				state, err := service.LoadState(ctx)
				if err != nil || len(state.Receivers) != 0 {
					t.Fatal("dry run recorded", err)
				}
				if _, err := os.Stat("bundles"); !os.IsNotExist(err) {
					t.Fatal("dry run wrote bundle")
				}
				if !present {
					if _, err := os.Stat("receivers"); !os.IsNotExist(err) {
						t.Fatal("dry run wrote local config")
					}
					localFixture(t, ".", "r")
				}
				if err := runOperator([]string{"apply", "--receiver", "r", "--file", file}, io.Discard, io.Discard); err != nil {
					t.Fatal(err)
				}
				var show bytes.Buffer
				if err := runOperator([]string{"show", "--receiver", "r"}, &show, io.Discard); err != nil {
					t.Fatal(err)
				}
				// show reports the recorded receiver's liveness first; a dry run has no receiver to report on.
				if "status: silent\n"+stdout.String() != show.String() {
					t.Fatalf("dry run differs from show: %s / %s", stdout.String(), show.String())
				}
			})
		}
	}
}
