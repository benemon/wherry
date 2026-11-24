package main

import (
	"bytes"
	"cmp"
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

// localForms accept every default: a metrics address, and each variable as derived. No question asks for the broker's
// CA: the receiver's machine trusts it through its system roots.
func localForms(variables ...string) []receiverForm {
	forms := []receiverForm{inputForm("/etc/wherry/receiver.bundle", "\r"), inputForm("/var/lib/wherry/state", "\r"), inputForm(defaultPluginDir, "\r"), inputForm("30s", "\r"), pickForm("listen", "\r"), inputForm(":9464", "\r")}
	for _, variable := range variables {
		forms = append(forms, pickForm("env", "\r"), inputForm(variable, "\r"))
	}
	return forms
}

func askLocal(config operator.ReceiverConfig, out *bytes.Buffer) (receiverLocalConfig, error) {
	return promptReceiverLocal(&questions{context.Background(), stdin, &promptLatch{out: out}}, config)
}

func retypeLocal(bad, good string) []string { return []string{"\x15" + bad, "\r", "\x15", good + "\r"} }

func TestReceiverLocalPrompts(t *testing.T) {
	for _, test := range []struct {
		name            string
		kinds, profiles int
		edit            func([]receiverForm) []receiverForm
		window          string
		drain           time.Duration
		metrics         string
	}{
		{"defaults", 1, 1, nil, "30s", 20 * time.Second, ":9464"},
		{"minimum", 1, 1, func(forms []receiverForm) []receiverForm {
			forms[3] = inputForm("30s", "\x1515s\r")
			return forms
		}, "15s", 5 * time.Second, ":9464"},
		{"three", 3, 3, nil, "30s", 18 * time.Second, ":9464"},
		{"repeated", 1, 3, nil, "30s", 20 * time.Second, ":9464"},
		{"no listener", 1, 1, func(forms []receiverForm) []receiverForm {
			forms[4] = pickForm("listen", down+"\r")
			return append(forms[:5], forms[6:]...)
		}, "30s", 20 * time.Second, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := localReceiver()
			for i := 1; i < test.profiles; i++ {
				plugin := "nomad"
				if test.kinds > 1 {
					plugin = []string{"kubernetes", "systemd"}[i-1]
				}
				config.Profiles = append(config.Profiles, operator.ProfileSettings{Plugin: plugin})
			}
			forms := localForms("NOMAD_TOKEN")
			if test.edit != nil {
				forms = test.edit(forms)
			}
			var out bytes.Buffer
			scriptReceiver(t, &out, forms...)
			local, err := askLocal(config, &out)
			if err != nil {
				t.Fatal(err)
			}
			if local.DrainTimeout != test.drain || local.Bundle != "/etc/wherry/receiver.bundle" || local.StateDir != "/var/lib/wherry/state" || local.PluginDir != defaultPluginDir || local.Credentials["nomad-token"].Env != "NOMAD_TOKEN" {
				t.Fatalf("config = %+v", local)
			}
			if local.MetricsAddr != test.metrics {
				t.Fatalf("config = %+v", local)
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := writeYAML(path, local); err != nil {
				t.Fatal(err)
			}
			if _, err := loadReceiverLocalConfig(path); err != nil {
				t.Fatal(err)
			}
			encoded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(encoded, []byte("ca_file:")) {
				t.Fatalf("CA field present: %s", encoded)
			}
			metrics := cmp.Or(test.metrics, "No listener")
			want := []string{"? Bundle path  /etc/wherry/receiver.bundle", "? State directory (must survive restarts)  /var/lib/wherry/state", "? Plugin directory  " + defaultPluginDir,
				"? Stop window (how long its platform waits before killing it)  " + test.window, "? Metrics  " + metrics, "? Credential nomad-token  Environment variable NOMAD_TOKEN"}
			if got := collapsedLines(out.String()); !reflect.DeepEqual(got, want) {
				t.Fatalf("prompt order: %q", got)
			}
			if !inOrder(out.String(), "\nReceiver host\n", "\nCredentials\n") {
				t.Fatalf("headings: %q", out.String())
			}
		})
	}
}

// The full flow writes the exact YAML for each Metrics choice, with one credential from a variable and one from a file,
// and never a ca_file: the receiver's machine trusts the broker's CA through its system roots.
func TestReceiverLocalFullFlow(t *testing.T) {
	config := localReceiver()
	config.Profiles = append(config.Profiles, operator.ProfileSettings{Name: "b", Plugin: "kubernetes", Config: map[string]any{"credential": "kube-token"}})
	for _, listener := range []bool{true, false} {
		t.Run(fmt.Sprintf("listener=%t", listener), func(t *testing.T) {
			forms := localForms("KUBE_TOKEN")
			forms = append(forms, pickForm("env", down+"\r"), inputForm("", "/run/secrets/nomad\r"))
			if !listener {
				forms[4] = pickForm("listen", down+"\r")
				forms = append(forms[:5], forms[6:]...)
			}
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
			want := "bundle: /etc/wherry/receiver.bundle\nstate_dir: /var/lib/wherry/state\nplugin_dir: " + defaultPluginDir + "\nmetrics_addr: :9464\ndrain_timeout: 20s\ncredentials:\n    kube-token:\n        env: KUBE_TOKEN\n    nomad-token:\n        file: /run/secrets/nomad\n"
			expected := receiverLocalConfig{Bundle: "/etc/wherry/receiver.bundle", StateDir: "/var/lib/wherry/state", PluginDir: defaultPluginDir, MetricsAddr: ":9464", DrainTimeout: 20 * time.Second,
				Credentials: map[string]credential.Source{"kube-token": {Env: "KUBE_TOKEN"}, "nomad-token": {File: "/run/secrets/nomad"}}}
			if !listener {
				want = strings.Replace(want, "metrics_addr: :9464", `metrics_addr: ""`, 1)
				expected.MetricsAddr = ""
			}
			if string(data) != want {
				t.Fatalf("YAML: got\n%s\nwant\n%s", data, want)
			}
			if local, err := loadReceiverLocalConfig(path); err != nil || !reflect.DeepEqual(local, expected) {
				t.Fatalf("read back: %+v, %v", local, err)
			}
			lines := collapsedLines(out.String())
			credentials := []string{"? Credential kube-token  Environment variable KUBE_TOKEN", "? Credential nomad-token  File /run/secrets/nomad"}
			if !reflect.DeepEqual(lines[len(lines)-2:], credentials) {
				t.Fatalf("credential lines: %q", lines)
			}
			if strings.Contains(out.String(), "warning") || strings.Contains(out.String(), "CA") {
				t.Fatalf("the local configuration questions mention the CA: %q", out.String())
			}
		})
	}
}

func TestReceiverLocalFile(t *testing.T) {
	dir := t.TempDir()
	config := localReceiver()
	noTerminal(t)
	path := filepath.Join(dir, "receivers", "r.config.yaml")
	var out bytes.Buffer
	err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, config)
	if err == nil || err.Error() != path+" does not exist and there is no terminal to prompt on; write it with "+receiverLocalFields {
		t.Fatalf("missing: %v", err)
	}
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, true, &out, config); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), path+" is absent; apply will ask for local configuration\n") {
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
	local, err := loadReceiverLocalConfig(path)
	if err != nil || local.DrainTimeout != 20*time.Second {
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
	provided := t.TempDir()
	if err := os.WriteFile(filepath.Join(provided, "config.yaml"), []byte("bundle: /provided\ndrain_timeout: 0s\ncredentials:\n  nomad-token: {command: [token]}\n"), 0o640); err != nil {
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

// A typed none after choosing an address is written literally.
func TestReceiverLocalLiteralNone(t *testing.T) {
	forms := localForms("NOMAD_TOKEN")
	forms[5] = inputForm(":9464", "\x15none\r")
	var out bytes.Buffer
	scriptReceiver(t, &out, forms...)
	dir := t.TempDir()
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, localReceiver()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "receivers", "r.config.yaml")
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "metrics_addr: none\n") {
		t.Fatalf("YAML: %s, %v", data, err)
	}
	if local, err := loadReceiverLocalConfig(path); err != nil || local.MetricsAddr != "none" {
		t.Fatalf("read back: %+v, %v", local, err)
	}
	lines := strings.Join(collapsedLines(out.String()), "\n")
	if !strings.Contains(lines, "? Metrics  none") {
		t.Fatalf("collapse: %q", lines)
	}
}

// An invalid answer shows its error inline, and the corrected value is stored.
func TestReceiverLocalCorrections(t *testing.T) {
	margin := "stop window leaves less than 5s to drain; raise kill_timeout, terminationGracePeriodSeconds or TimeoutStopSec"
	for _, test := range []struct {
		name    string
		field   int
		edit    func([]receiverForm) []receiverForm
		message string
		stored  func(receiverLocalConfig) bool
	}{
		{"huge negative window", 3, func(forms []receiverForm) []receiverForm {
			forms[3] = inputForm("30s", retypeLocal("-2562047h47m16.854775808s", "40s")...)
			return forms
		}, margin, func(l receiverLocalConfig) bool { return l.DrainTimeout == 30*time.Second }},
		{"short window", 3, func(forms []receiverForm) []receiverForm {
			forms[3] = inputForm("30s", retypeLocal("14s", "15s")...)
			return forms
		}, margin, func(l receiverLocalConfig) bool { return l.DrainTimeout == 5*time.Second }},
		{"unparsed window", 3, func(forms []receiverForm) []receiverForm {
			forms[3] = inputForm("30s", retypeLocal("abc", "40s")...)
			return forms
		}, `stop window: time: invalid duration "abc"`, func(l receiverLocalConfig) bool { return l.DrainTimeout == 30*time.Second }},
		{"leading digit variable", 7, func(forms []receiverForm) []receiverForm {
			forms[7] = inputForm("NOMAD_TOKEN", retypeLocal("1X", "GOOD")...)
			return forms
		}, "invalid environment variable name", func(l receiverLocalConfig) bool {
			return reflect.DeepEqual(l.Credentials["nomad-token"], credential.Source{Env: "GOOD"})
		}},
		{"dashed variable", 7, func(forms []receiverForm) []receiverForm {
			forms[7] = inputForm("NOMAD_TOKEN", retypeLocal("A-B", " A_B ")...)
			return forms
		}, "invalid environment variable name", func(l receiverLocalConfig) bool {
			return reflect.DeepEqual(l.Credentials["nomad-token"], credential.Source{Env: "A_B"})
		}},
		{"blank file path", 7, func(forms []receiverForm) []receiverForm {
			forms[6] = pickForm("env", down+"\r")
			forms[7] = inputForm("", retypeLocal("", "/run/token")...)
			return forms
		}, "credential file path must not be blank", func(l receiverLocalConfig) bool {
			return reflect.DeepEqual(l.Credentials["nomad-token"], credential.Source{File: "/run/token"})
		}},
		{"blank bundle path", 0, func(forms []receiverForm) []receiverForm {
			forms[0] = inputForm("/etc/wherry/receiver.bundle", retypeLocal("  ", "/srv/receiver.bundle")...)
			return forms
		}, "bundle path must not be blank", func(l receiverLocalConfig) bool { return l.Bundle == "/srv/receiver.bundle" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			frames := scriptReceiver(t, &out, test.edit(localForms("NOMAD_TOKEN"))...)
			local, err := askLocal(localReceiver(), &out)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(strings.Fields((*frames)[test.field]), " "), test.message) {
				t.Fatalf("form %d did not show %q: %q", test.field, test.message, (*frames)[test.field])
			}
			if !test.stored(local) {
				t.Fatalf("corrected value not stored: %+v", local)
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
	forms := append(localForms("_9_TOKEN"), pickForm("env", "\r"), inputForm("Z_TOKEN", "\r"))
	frames := scriptReceiver(t, &out, forms...)
	local, err := askLocal(config, &out)
	if err != nil {
		t.Fatal(err)
	}
	if len(local.Credentials) != 2 || local.Credentials["9-token"].Env != "_9_TOKEN" || local.Credentials["z-token"].Env != "Z_TOKEN" {
		t.Fatalf("credentials: %+v", local.Credentials)
	}
	if !strings.Contains((*frames)[6], "Credential 9-token") || !strings.Contains((*frames)[8], "Credential z-token") {
		t.Fatalf("question order: %q", (*frames)[6:])
	}
	lines := collapsedLines(out.String())
	if want := []string{"? Credential 9-token  Environment variable _9_TOKEN", "? Credential z-token  Environment variable Z_TOKEN"}; !reflect.DeepEqual(lines[len(lines)-2:], want) || strings.Count(strings.Join(lines, "\n"), "Credential z-token") != 1 {
		t.Fatalf("collapse order: %q", lines)
	}
	config.Profiles = nil
	out.Reset()
	scriptReceiver(t, &out, localForms()...)
	if _, err := askLocal(config, &out); err != nil || strings.Contains(out.String(), "Credential") {
		t.Fatalf("no credentials: %v %s", err, out.String())
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
	local, err := askLocal(config, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(local.Credentials, map[string]credential.Source{"9-token": {Env: "_9_TOKEN"}, "nomad-token": {File: "/run/nomad"}}) {
		t.Fatalf("credentials: %+v", local.Credentials)
	}
	if !strings.Contains((*frames)[6], "Credential 9-token") || !strings.Contains((*frames)[7], "Variable") || !strings.Contains((*frames)[8], "Credential nomad-token") || !strings.Contains((*frames)[9], "File path") {
		t.Fatalf("question order: %q", (*frames)[6:])
	}
	lines := collapsedLines(out.String())
	if want := []string{"? Credential 9-token  Environment variable _9_TOKEN", "? Credential nomad-token  File /run/nomad"}; !reflect.DeepEqual(lines[len(lines)-2:], want) {
		t.Fatalf("collapse order: %q", lines)
	}
}

// A credential file path is written raw, and its collapse line escapes it.
func TestReceiverLocalEscape(t *testing.T) {
	var out bytes.Buffer
	forms := localForms("NOMAD_TOKEN")
	forms[6] = pickForm("env", down+"\r")
	forms[7] = inputForm("", "/run/\u202etoken\r")
	scriptReceiver(t, &out, forms...)
	dir := t.TempDir()
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, localReceiver()); err != nil {
		t.Fatal(err)
	}
	local, err := loadReceiverLocalConfig(filepath.Join(dir, "receivers", "r.config.yaml"))
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

func TestReceiverLocalInterrupts(t *testing.T) {
	for _, mode := range []string{"ctrl-c at credential", "eof at stop window"} {
		t.Run(mode, func(t *testing.T) {
			service, file := receiverApplyEnv(t)
			var out bytes.Buffer
			forms := localForms()
			if mode == "ctrl-c at credential" {
				forms = append(forms, pickForm("env", "\x03"))
			} else {
				forms = forms[:3]
			}
			scriptReceiver(t, &out, forms...)
			if mode == "eof at stop window" {
				scripted := runQuestion
				n := 0
				runQuestion = func(q *questions, f huh.Field) error {
					n++
					if n == 4 {
						next := *q
						next.in = strings.NewReader("40s")
						return next.runForm(f)
					}
					return scripted(q, f)
				}
			}
			err := runOperator([]string{"receiver", "apply", "r", "-f", file}, io.Discard, &out)
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
			err := runOperator([]string{"receiver", "apply", "r", "-f", file}, io.Discard, out)
			want := filepath.Join("receivers", "r.config.yaml") + " does not exist and there is no terminal to prompt on; write it with " + receiverLocalFields
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
	if err := runOperator([]string{"receiver", "apply", "r", "-f", file}, io.Discard, failingPrompt{}); !errors.Is(err, io.ErrClosedPipe) {
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
			err := runOperator([]string{"receiver", "apply", "r", "-f", file}, io.Discard, &out)
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
				if err := runOperator([]string{"receiver", "apply", "r", "-f", file, "--dry-run"}, &stdout, &stderr); err != nil {
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
				if err := runOperator([]string{"receiver", "apply", "r", "-f", file}, io.Discard, io.Discard); err != nil {
					t.Fatal(err)
				}
				var show bytes.Buffer
				if err := runOperator([]string{"receiver", "show", "r"}, &show, io.Discard); err != nil {
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
