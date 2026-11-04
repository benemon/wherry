package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/twmb/franz-go/pkg/kadm"
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

func TestReceiverLocalPrompts(t *testing.T) {
	for _, test := range []struct {
		name, answers string
		ca            bool
		kinds         int
		drain         time.Duration
		failure       bool
	}{
		{"defaults", strings.Repeat("\n", 8), true, 1, 20 * time.Second, false},
		{"minimum", "\n\n\n\n15s\n\n\n\n", false, 1, 5 * time.Second, false},
		{"short", "\n\n\n\n14s\n", false, 1, 0, true},
		{"three", strings.Repeat("\n", 8), false, 3, 18 * time.Second, false},
		{"none", "\nnone\n\n\n\nnone\n\n\n", true, 1, 20 * time.Second, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := localReceiver()
			for i := 1; i < test.kinds; i++ {
				config.Profiles = append(config.Profiles, operator.ProfileSettings{Plugin: []string{"kubernetes", "systemd"}[i-1]})
			}
			var out bytes.Buffer
			local, err := promptReceiverLocal(&prompter{bufio.NewReader(strings.NewReader(test.answers)), &out}, config, test.ca)
			if test.failure {
				if err == nil || !strings.Contains(err.Error(), "kill_timeout, terminationGracePeriodSeconds or TimeoutStopSec") {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if local.DrainTimeout != test.drain || local.Bundle != "/etc/wherry/receiver.bundle" || local.StateDir != "/var/lib/wherry/state" || local.PluginDir != defaultPluginDir || local.Credentials["nomad-token"].Env != "NOMAD_TOKEN" {
				t.Fatalf("config = %+v", local)
			}
			wantCA := ""
			if test.ca && test.name != "none" {
				wantCA = "/etc/wherry/kafka-ca.pem"
			}
			metrics := ":9464"
			if test.name == "none" {
				metrics = ""
			}
			if local.CAFile != wantCA || local.MetricsAddr != metrics {
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
			if wantCA == "" && bytes.Contains(encoded, []byte("ca_file:")) {
				t.Fatalf("CA field present: %s", encoded)
			}

			order := []string{"read its bundle?", "broker's CA?", "keep its state?", "its plugins?", "stop before killing it?", "serve metrics?", "Credential nomad-token:"}
			previous := -1
			for _, question := range order {
				index := strings.Index(out.String(), question)
				if index <= previous {
					t.Fatalf("prompt order: %s", out.String())
				}
				previous = index
			}
		})
	}
}

func TestReceiverLocalFile(t *testing.T) {
	dir := t.TempDir()
	config := localReceiver()
	op := operatorConfig{Broker: "broker", CAFile: "ca.pem"}
	noTerminal(t)
	path := filepath.Join(dir, "receivers", "r.config.yaml")
	var out bytes.Buffer
	if err := receiverLocalFile(dir, "r", "outside.yaml", false, false, &out, config, op.Broker, op.CAFile != ""); err == nil || !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), receiverLocalFields) {
		t.Fatalf("missing: %v", err)
	}
	if err := receiverLocalFile(dir, "r", "outside.yaml", false, true, &out, config, op.Broker, op.CAFile != ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("dry run created files: %v", err)
	}
	onTerminal(t, strings.Repeat("\n", 8))
	if err := receiverLocalFile(dir, "r", "outside.yaml", false, false, &out, config, op.Broker, op.CAFile != ""); err != nil {
		t.Fatal(err)
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
	onTerminal(t, "")
	if err := receiverLocalFile(dir, "r", "outside.yaml", false, false, io.Discard, config, op.Broker, op.CAFile != ""); err != nil {
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
	if err := receiverLocalFile(dir, "r", provided, true, true, &out, config, op.Broker, op.CAFile != ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), provided) || !strings.Contains(out.String(), "warning:") {
		t.Fatal(out.String())
	}
	given := filepath.Join(provided, "config.yaml")
	before, _ = os.Stat(given)
	contents, _ = os.ReadFile(given)
	if err := receiverLocalFile(dir, "r", provided, true, false, io.Discard, config, op.Broker, op.CAFile != ""); err != nil {
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
	onTerminal(t, strings.Repeat("\n", 8))
	if err := receiverLocalFile(dir, "r", "outside.yaml", false, false, io.Discard, localReceiver(), "", false); err == nil {
		t.Fatal("wrote through a dangling symlink")
	}
	if _, err := os.Stat(outside); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink target written: %v", err)
	}
}

func TestReceiverLocalHugeNegativeWindow(t *testing.T) {
	onTerminal(t, "\n\n\n\n-2562047h47m16.854775808s\n")
	if _, err := promptReceiverLocal(newPrompter(io.Discard), localReceiver(), false); err == nil || !strings.Contains(err.Error(), "less than 5s") {
		t.Fatalf("got %v", err)
	}
}

func TestReceiverLocalMergedCredentials(t *testing.T) {
	path := writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\nplugins:\n  nomad:\n    config: {credential: z-token}\n    profiles:\n      - {name: a, agent: claude}\n      - {name: b, agent: claude}\n      - {name: c, agent: claude, config: {credential: null}}\n      - {name: d, agent: claude, config: {credential: 9-token}}\n"})
	config, err := readReceiverConfig(filepath.Join(path, "receiver.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	local, err := promptReceiverLocal(&prompter{bufio.NewReader(strings.NewReader(strings.Repeat("\n", 10))), &out}, config, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(local.Credentials) != 2 || local.Credentials["9-token"].Env != "_9_TOKEN" || local.Credentials["z-token"].Env != "Z_TOKEN" {
		t.Fatalf("credentials: %+v", local.Credentials)
	}
	if strings.Count(out.String(), "Credential z-token:") != 1 || strings.Index(out.String(), "Credential 9-token:") > strings.Index(out.String(), "Credential z-token:") {
		t.Fatal(out.String())
	}
	config.Profiles = nil
	out.Reset()
	if _, err := promptReceiverLocal(&prompter{bufio.NewReader(strings.NewReader(strings.Repeat("\n", 6))), &out}, config, false); err != nil || strings.Contains(out.String(), "Credential ") {
		t.Fatalf("no credentials: %v %s", err, out.String())
	}
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
				onTerminal(t, strings.Repeat("\n", 8))
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
					onTerminal(t, "")
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
			dir := t.TempDir()
			onTerminal(t, strings.Repeat("\n", 8))
			if err := receiverLocalFile(dir, "r", "external/receiver.yaml", false, false, io.Discard, config, op.Broker, op.CAFile != ""); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "receivers/r.config.yaml")
			local, err := loadReceiverLocalConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			expected := ""
			if test.broker == op.Broker && test.ca != "" {
				expected = "/etc/wherry/kafka-ca.pem"
			}
			if local.CAFile != expected {
				t.Fatalf("CA = %q want %q", local.CAFile, expected)
			}
			if err := os.WriteFile(path, []byte("bundle: /bundle\n"+test.answer+"credentials: {nomad-token: {env: NOMAD_TOKEN}}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err := receiverLocalFile(dir, "r", "external/receiver.yaml", false, false, &out, config, op.Broker, op.CAFile != ""); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(out.String(), "warning:") != test.warn {
				t.Fatal(out.String())
			}
		})
	}
}

func TestReceiverLocalInterruptedAndInvalidAnswers(t *testing.T) {
	for _, answers := range []string{"", "\n\n", "\n\n\n\ninvalid\n", "\n\n\n\n\n\nenv\n9INVALID\n", "\n\n\n\n\n\nfile\n\n"} {
		t.Run(fmt.Sprintf("%q", answers), func(t *testing.T) {
			dir := t.TempDir()
			onTerminal(t, answers)
			if err := receiverLocalFile(dir, "r", "outside.yaml", false, false, io.Discard, localReceiver(), "", false); err == nil {
				t.Fatal("accepted incomplete or invalid answers")
			}
			if _, err := os.Stat(filepath.Join(dir, "receivers")); !os.IsNotExist(err) {
				t.Fatalf("partial file written: %v", err)
			}
		})
	}
}
