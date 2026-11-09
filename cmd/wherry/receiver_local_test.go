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

// localForms accept every default: CA file first when needsCA, a metrics address, and each variable as derived.
func localForms(needsCA bool, variables ...string) []receiverForm {
	ca := "roots"
	if needsCA {
		ca = "file"
	}
	forms := []receiverForm{inputForm("/etc/wherry/receiver.bundle", "\r"), pickForm(ca, "\r")}
	if needsCA {
		forms = append(forms, inputForm("/etc/wherry/kafka-ca.pem", "\r"))
	}
	forms = append(forms, inputForm("/var/lib/wherry/state", "\r"), inputForm(defaultPluginDir, "\r"), inputForm("30s", "\r"), pickForm("listen", "\r"), inputForm(":9464", "\r"))
	for _, variable := range variables {
		forms = append(forms, pickForm("env", "\r"), inputForm(variable, "\r"))
	}
	return forms
}

func askLocal(config operator.ReceiverConfig, needsCA bool, out *bytes.Buffer) (receiverLocalConfig, error) {
	return promptReceiverLocal(&questions{context.Background(), stdin, &promptLatch{out: out}}, config, needsCA)
}

func retypeLocal(bad, good string) []string { return []string{"\x15" + bad, "\r", "\x15", good + "\r"} }

func TestReceiverLocalPrompts(t *testing.T) {
	for _, test := range []struct {
		name            string
		ca              bool
		kinds, profiles int
		edit            func([]receiverForm) []receiverForm
		window          string
		drain           time.Duration
		caFile, metrics string
	}{
		{"defaults", true, 1, 1, nil, "30s", 20 * time.Second, "/etc/wherry/kafka-ca.pem", ":9464"},
		{"minimum", false, 1, 1, func(forms []receiverForm) []receiverForm {
			forms[4] = inputForm("30s", "\x1515s\r")
			return forms
		}, "15s", 5 * time.Second, "", ":9464"},
		{"three", false, 3, 3, nil, "30s", 18 * time.Second, "", ":9464"},
		{"repeated", false, 1, 3, nil, "30s", 20 * time.Second, "", ":9464"},
		{"roots and no listener", true, 1, 1, func(forms []receiverForm) []receiverForm {
			forms[1] = pickForm("file", down+"\r")
			forms[6] = pickForm("listen", down+"\r")
			return append(forms[:2], append(forms[3:7], forms[8:]...)...)
		}, "30s", 20 * time.Second, "", ""},
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
			forms := localForms(test.ca, "NOMAD_TOKEN")
			if test.edit != nil {
				forms = test.edit(forms)
			}
			var out bytes.Buffer
			scriptReceiver(t, &out, forms...)
			local, err := askLocal(config, test.ca, &out)
			if err != nil {
				t.Fatal(err)
			}
			if local.DrainTimeout != test.drain || local.Bundle != "/etc/wherry/receiver.bundle" || local.StateDir != "/var/lib/wherry/state" || local.PluginDir != defaultPluginDir || local.Credentials["nomad-token"].Env != "NOMAD_TOKEN" {
				t.Fatalf("config = %+v", local)
			}
			if local.CAFile != test.caFile || local.MetricsAddr != test.metrics {
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
			if test.caFile == "" && bytes.Contains(encoded, []byte("ca_file:")) {
				t.Fatalf("CA field present: %s", encoded)
			}
			ca, metrics := cmp.Or(test.caFile, "System roots"), cmp.Or(test.metrics, "No listener")
			want := []string{"? Bundle path  /etc/wherry/receiver.bundle", "? Broker CA  " + ca, "? State directory (must survive restarts)  /var/lib/wherry/state", "? Plugin directory  " + defaultPluginDir,
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

// The full flow writes the exact YAML for each Broker CA and Metrics choice, under both needsCA values, with one
// credential from a variable and one from a file.
func TestReceiverLocalFullFlow(t *testing.T) {
	config := localReceiver()
	config.Profiles = append(config.Profiles, operator.ProfileSettings{Name: "b", Plugin: "kubernetes", Config: map[string]any{"credential": "kube-token"}})
	for _, needsCA := range []bool{false, true} {
		for _, roots := range []bool{false, true} {
			t.Run(fmt.Sprintf("needsCA=%t/roots=%t", needsCA, roots), func(t *testing.T) {
				forms := localForms(needsCA, "KUBE_TOKEN")
				forms = append(forms, pickForm("env", down+"\r"), inputForm("", "/run/secrets/nomad\r"))
				switch {
				case roots && needsCA:
					forms[1] = pickForm("file", down+"\r")
					forms = append(forms[:2], forms[3:]...)
				case !roots && !needsCA:
					forms[1] = pickForm("roots", "\x1b[A\r")
					forms = append(forms[:2], append([]receiverForm{inputForm("/etc/wherry/kafka-ca.pem", "\r")}, forms[2:]...)...)
				}
				if roots {
					forms[5] = pickForm("listen", down+"\r")
					forms = append(forms[:6], forms[7:]...)
				}
				var out bytes.Buffer
				scriptReceiver(t, &out, forms...)
				dir := t.TempDir()
				if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, config, "broker", needsCA); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(dir, "receivers", "r.config.yaml")
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				want := "bundle: /etc/wherry/receiver.bundle\nca_file: /etc/wherry/kafka-ca.pem\nstate_dir: /var/lib/wherry/state\nplugin_dir: " + defaultPluginDir + "\nmetrics_addr: :9464\ndrain_timeout: 20s\ncredentials:\n    kube-token:\n        env: KUBE_TOKEN\n    nomad-token:\n        file: /run/secrets/nomad\n"
				expected := receiverLocalConfig{Bundle: "/etc/wherry/receiver.bundle", CAFile: "/etc/wherry/kafka-ca.pem", StateDir: "/var/lib/wherry/state", PluginDir: defaultPluginDir, MetricsAddr: ":9464", DrainTimeout: 20 * time.Second,
					Credentials: map[string]credential.Source{"kube-token": {Env: "KUBE_TOKEN"}, "nomad-token": {File: "/run/secrets/nomad"}}}
				if roots {
					want = strings.Replace(strings.Replace(want, "ca_file: /etc/wherry/kafka-ca.pem\n", "", 1), "metrics_addr: :9464", `metrics_addr: ""`, 1)
					expected.CAFile, expected.MetricsAddr = "", ""
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
				if strings.Contains(out.String(), "warning: local configuration has no ca_file") != (roots && needsCA) {
					t.Fatalf("CA warning: %q", out.String())
				}
			})
		}
	}
}

func TestReceiverLocalFile(t *testing.T) {
	dir := t.TempDir()
	config := localReceiver()
	op := operatorConfig{Broker: "broker", CAFile: "ca.pem"}
	noTerminal(t)
	path := filepath.Join(dir, "receivers", "r.config.yaml")
	var out bytes.Buffer
	err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, config, op.Broker, op.CAFile != "")
	if err == nil || err.Error() != path+" does not exist and there is no terminal to prompt on; write it with "+receiverLocalFields {
		t.Fatalf("missing: %v", err)
	}
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, true, &out, config, op.Broker, op.CAFile != ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), path+" is absent; apply will ask for local configuration\n") {
		t.Fatal(out.String())
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("dry run created files: %v", err)
	}
	scriptReceiver(t, &out, localForms(true, "NOMAD_TOKEN")...)
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, config, op.Broker, op.CAFile != ""); err != nil {
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
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, io.Discard, config, op.Broker, op.CAFile != ""); err != nil {
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
	if err := receiverLocalFile(context.Background(), dir, "r", provided, true, true, &out, config, op.Broker, op.CAFile != ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "would use local configuration "+filepath.Join(provided, "config.yaml")) || !strings.Contains(out.String(), "warning:") {
		t.Fatal(out.String())
	}
	given := filepath.Join(provided, "config.yaml")
	before, _ = os.Stat(given)
	contents, _ = os.ReadFile(given)
	if err := receiverLocalFile(context.Background(), dir, "r", provided, true, false, io.Discard, config, op.Broker, op.CAFile != ""); err != nil {
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
	scriptReceiver(t, &out, localForms(false, "NOMAD_TOKEN")...)
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, localReceiver(), "", false); err == nil {
		t.Fatal("wrote through a dangling symlink")
	}
	if _, err := os.Stat(outside); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink target written: %v", err)
	}
}

// A typed none after choosing CA file or an address is written literally.
func TestReceiverLocalLiteralNone(t *testing.T) {
	forms := localForms(true, "NOMAD_TOKEN")
	forms[2] = inputForm("/etc/wherry/kafka-ca.pem", "\x15none\r")
	forms[7] = inputForm(":9464", "\x15none\r")
	var out bytes.Buffer
	scriptReceiver(t, &out, forms...)
	dir := t.TempDir()
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, localReceiver(), "broker", true); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "receivers", "r.config.yaml")
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "ca_file: none\n") || !strings.Contains(string(data), "metrics_addr: none\n") {
		t.Fatalf("YAML: %s, %v", data, err)
	}
	if local, err := loadReceiverLocalConfig(path); err != nil || local.CAFile != "none" || local.MetricsAddr != "none" {
		t.Fatalf("read back: %+v, %v", local, err)
	}
	lines := strings.Join(collapsedLines(out.String()), "\n")
	if !strings.Contains(lines, "? Broker CA  none") || !strings.Contains(lines, "? Metrics  none") {
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
		{"huge negative window", 4, func(forms []receiverForm) []receiverForm {
			forms[4] = inputForm("30s", retypeLocal("-2562047h47m16.854775808s", "40s")...)
			return forms
		}, margin, func(l receiverLocalConfig) bool { return l.DrainTimeout == 30*time.Second }},
		{"short window", 4, func(forms []receiverForm) []receiverForm {
			forms[4] = inputForm("30s", retypeLocal("14s", "15s")...)
			return forms
		}, margin, func(l receiverLocalConfig) bool { return l.DrainTimeout == 5*time.Second }},
		{"unparsed window", 4, func(forms []receiverForm) []receiverForm {
			forms[4] = inputForm("30s", retypeLocal("abc", "40s")...)
			return forms
		}, `stop window: time: invalid duration "abc"`, func(l receiverLocalConfig) bool { return l.DrainTimeout == 30*time.Second }},
		{"leading digit variable", 8, func(forms []receiverForm) []receiverForm {
			forms[8] = inputForm("NOMAD_TOKEN", retypeLocal("1X", "GOOD")...)
			return forms
		}, "invalid environment variable name", func(l receiverLocalConfig) bool {
			return reflect.DeepEqual(l.Credentials["nomad-token"], credential.Source{Env: "GOOD"})
		}},
		{"dashed variable", 8, func(forms []receiverForm) []receiverForm {
			forms[8] = inputForm("NOMAD_TOKEN", retypeLocal("A-B", " A_B ")...)
			return forms
		}, "invalid environment variable name", func(l receiverLocalConfig) bool {
			return reflect.DeepEqual(l.Credentials["nomad-token"], credential.Source{Env: "A_B"})
		}},
		{"blank file path", 8, func(forms []receiverForm) []receiverForm {
			forms[7] = pickForm("env", down+"\r")
			forms[8] = inputForm("", retypeLocal("", "/run/token")...)
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
			frames := scriptReceiver(t, &out, test.edit(localForms(false, "NOMAD_TOKEN"))...)
			local, err := askLocal(localReceiver(), false, &out)
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
	forms := append(localForms(false, "_9_TOKEN"), pickForm("env", "\r"), inputForm("Z_TOKEN", "\r"))
	frames := scriptReceiver(t, &out, forms...)
	local, err := askLocal(config, false, &out)
	if err != nil {
		t.Fatal(err)
	}
	if len(local.Credentials) != 2 || local.Credentials["9-token"].Env != "_9_TOKEN" || local.Credentials["z-token"].Env != "Z_TOKEN" {
		t.Fatalf("credentials: %+v", local.Credentials)
	}
	if !strings.Contains((*frames)[7], "Credential 9-token") || !strings.Contains((*frames)[9], "Credential z-token") {
		t.Fatalf("question order: %q", (*frames)[7:])
	}
	lines := collapsedLines(out.String())
	if want := []string{"? Credential 9-token  Environment variable _9_TOKEN", "? Credential z-token  Environment variable Z_TOKEN"}; !reflect.DeepEqual(lines[len(lines)-2:], want) || strings.Count(strings.Join(lines, "\n"), "Credential z-token") != 1 {
		t.Fatalf("collapse order: %q", lines)
	}
	config.Profiles = nil
	out.Reset()
	scriptReceiver(t, &out, localForms(false)...)
	if _, err := askLocal(config, false, &out); err != nil || strings.Contains(out.String(), "Credential") {
		t.Fatalf("no credentials: %v %s", err, out.String())
	}
}

// Two credentials are asked, and collapse, in receiverCredentials' order, one from its derived variable and one from a
// file.
func TestReceiverLocalTwoCredentials(t *testing.T) {
	config := localReceiver()
	config.Profiles = append(config.Profiles, operator.ProfileSettings{Name: "b", Plugin: "nomad", Config: map[string]any{"credential": "9-token"}}, operator.ProfileSettings{Name: "c", Plugin: "nomad", Config: map[string]any{"credential": "nomad-token"}})
	var out bytes.Buffer
	forms := append(localForms(false, "_9_TOKEN"), pickForm("env", down+"\r"), inputForm("", "/run/nomad\r"))
	frames := scriptReceiver(t, &out, forms...)
	local, err := askLocal(config, false, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(local.Credentials, map[string]credential.Source{"9-token": {Env: "_9_TOKEN"}, "nomad-token": {File: "/run/nomad"}}) {
		t.Fatalf("credentials: %+v", local.Credentials)
	}
	if !strings.Contains((*frames)[7], "Credential 9-token") || !strings.Contains((*frames)[8], "Variable") || !strings.Contains((*frames)[9], "Credential nomad-token") || !strings.Contains((*frames)[10], "File path") {
		t.Fatalf("question order: %q", (*frames)[7:])
	}
	lines := collapsedLines(out.String())
	if want := []string{"? Credential 9-token  Environment variable _9_TOKEN", "? Credential nomad-token  File /run/nomad"}; !reflect.DeepEqual(lines[len(lines)-2:], want) {
		t.Fatalf("collapse order: %q", lines)
	}
}

// A credential file path is written raw, and its collapse line escapes it.
func TestReceiverLocalEscape(t *testing.T) {
	var out bytes.Buffer
	forms := localForms(false, "NOMAD_TOKEN")
	forms[7] = pickForm("env", down+"\r")
	forms[8] = inputForm("", "/run/\u202etoken\r")
	scriptReceiver(t, &out, forms...)
	dir := t.TempDir()
	if err := receiverLocalFile(context.Background(), dir, "r", "outside.yaml", false, false, &out, localReceiver(), "", false); err != nil {
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
			forms := localForms(true)
			if mode == "ctrl-c at credential" {
				forms = append(forms, pickForm("env", "\x03"))
			} else {
				forms = forms[:5]
			}
			scriptReceiver(t, &out, forms...)
			if mode == "eof at stop window" {
				scripted := runQuestion
				n := 0
				runQuestion = func(q *questions, f huh.Field) error {
					n++
					if n == 6 {
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
				if stdout.String() != show.String() {
					t.Fatalf("dry run differs from show: %s / %s", stdout.String(), show.String())
				}
			})
		}
	}
}

func TestReceiverLocalCA(t *testing.T) {
	for _, test := range []struct {
		name, broker, ca, answer string
		warn                     bool
	}{
		{"same", "broker", "ca.pem", "", true},
		{"different", "other", "ca.pem", "", false},
		{"no-ca", "broker", "", "", false},
		{"provided-ca", "broker", "ca.pem", "ca_file: /ca\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := localReceiver()
			config.Broker = test.broker
			op := operatorConfig{Broker: "broker", CAFile: test.ca}
			needsCA := test.broker == op.Broker && test.ca != ""
			dir := t.TempDir()
			var out bytes.Buffer
			scriptReceiver(t, &out, localForms(needsCA, "NOMAD_TOKEN")...)
			if err := receiverLocalFile(context.Background(), dir, "r", "external/receiver.yaml", false, false, &out, config, op.Broker, op.CAFile != ""); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "receivers/r.config.yaml")
			local, err := loadReceiverLocalConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			expected := ""
			if needsCA {
				expected = "/etc/wherry/kafka-ca.pem"
			}
			if local.CAFile != expected {
				t.Fatalf("CA = %q want %q", local.CAFile, expected)
			}
			if err := os.WriteFile(path, []byte("bundle: /bundle\n"+test.answer+"credentials: {nomad-token: {env: NOMAD_TOKEN}}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			out.Reset()
			if err := receiverLocalFile(context.Background(), dir, "r", "external/receiver.yaml", false, false, &out, config, op.Broker, op.CAFile != ""); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(out.String(), "warning:") != test.warn {
				t.Fatal(out.String())
			}
		})
	}
}
