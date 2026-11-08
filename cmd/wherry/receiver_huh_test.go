package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/operator"
	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/x/ansi"
	"github.com/creack/pty"
	"github.com/twmb/franz-go/pkg/kerr"
)

var receiverSHA = strings.Repeat("0123456789abcdef", 4)

const receiverBroker = "broker.example:9093"

// receiverForm is one scripted form: whether it is a pick, the value it starts at, and the keys typed into it.
type receiverForm struct {
	pick    bool
	initial string
	keys    []string
}

func pickForm(initial string, keys ...string) receiverForm { return receiverForm{true, initial, keys} }
func inputForm(initial string, keys ...string) receiverForm {
	return receiverForm{false, initial, keys}
}

// scriptReceiver scripts forms, refusing a form of another kind or starting value before it runs, and returns each
// form's output with its ANSI sequences stripped.
func scriptReceiver(t *testing.T, out *bytes.Buffer, forms ...receiverForm) *[]string {
	t.Helper()
	steps := make([][]string, len(forms))
	for i, form := range forms {
		steps[i] = form.keys
	}
	scriptForms(t, steps...)
	scripted := runQuestion
	n := 0
	frames := &[]string{}
	runQuestion = func(q *questions, f huh.Field) error {
		i := n
		n++
		_, pick := f.(*huh.Select[string])
		if i < len(forms) && (pick != forms[i].pick || f.GetValue() != forms[i].initial) {
			t.Errorf("form %d: pick %t starting at %q; want pick %t starting at %q", i, pick, f.GetValue(), forms[i].pick, forms[i].initial)
			return errors.New("unexpected form")
		}
		before := out.Len()
		err := scripted(q, f)
		*frames = append(*frames, ansi.Strip(out.String()[before:]))
		return err
	}
	return frames
}

// inOrder reports whether each of labels appears in text, the first occurrences in order.
func inOrder(text string, labels ...string) bool {
	last := -1
	for _, label := range labels {
		i := strings.Index(text, label)
		if i <= last {
			return false
		}
		last = i
	}
	return true
}

func receiverState() operator.State {
	state := operator.NewState()
	state.Pools["pool-b"] = operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}}
	state.Pools["pool-a"] = operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}, {Name: "codex"}}}
	return state
}

func askReceiver(t *testing.T, state operator.State, out *bytes.Buffer) (receiverFile, error) {
	t.Helper()
	value, err := promptReceiver(&questions{context.Background(), stdin, &promptLatch{out: out}}, "receiver-a", state, receiverBroker)
	if err != nil {
		return receiverFile{}, err
	}
	return value.(receiverFile), nil
}

// baseReceiverForms answer receiverState's pool-a with nomad, config {}, one claude profile without labels and the
// default broker.
func baseReceiverForms() []receiverForm {
	return []receiverForm{
		pickForm("pool-a", "\r"), pickForm("nomad", "\r"), inputForm("", receiverSHA+"\r"), inputForm("{}", "\r"),
		pickForm("claude", "\r"), inputForm("", "\r"), pickForm("add", down+"\r"), inputForm(receiverBroker, "\r"),
	}
}

func TestReceiverForms(t *testing.T) {
	var out bytes.Buffer
	frames := scriptReceiver(t, &out,
		pickForm("pool-a", "\r"), pickForm("nomad", "\r"), inputForm("", receiverSHA+"\r"),
		inputForm("{}", "\x15{address: https://nomad.example:4646, credential: nomad-token, tls: {ca_file: /etc/nomad/ca.pem, verify: true}}\r"),
		pickForm("claude", "\r"), inputForm("", "tier=batch\r"), inputForm("", "gpu=a100\r"), inputForm("", "\r"), pickForm("add", "\r"),
		pickForm("claude", down+"\r"), inputForm("", "\r"), pickForm("add", down+"\r"), inputForm(receiverBroker, "\r"))
	dir := t.TempDir()
	path, _, err := objectFile(context.Background(), dir, "receivers", "receiver-a", nil, false, &out, receiverFields, func(q *questions) (any, error) {
		return promptReceiver(q, "receiver-a", receiverState(), receiverBroker)
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "pool: pool-a\nbroker: broker.example:9093\nplugin_sha256:\n    nomad: " + receiverSHA + "\nplugins:\n    nomad:\n        labels: {}\n        config:\n            address: https://nomad.example:4646\n            credential: nomad-token\n            tls:\n                ca_file: /etc/nomad/ca.pem\n                verify: true\n        profiles:\n            - name: claude\n              agent: claude\n              labels:\n                gpu: a100\n                tier: batch\n              config: {}\n            - name: codex\n              agent: codex\n              labels: {}\n              config: {}\n"
	if string(data) != want {
		t.Fatalf("YAML shape: got\n%s\nwant\n%s", data, want)
	}
	config, err := readReceiverConfig(path, false)
	nested := func() map[string]any {
		return map[string]any{"address": "https://nomad.example:4646", "credential": "nomad-token", "tls": map[string]any{"ca_file": "/etc/nomad/ca.pem", "verify": true}}
	}
	expected := operator.ReceiverConfig{Pool: "pool-a", Broker: receiverBroker, PluginSHA256: map[string]string{"nomad": receiverSHA}, Profiles: []operator.ProfileSettings{
		{Name: "claude", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"tier": "batch", "gpu": "a100"}, Config: nested()},
		{Name: "codex", Plugin: "nomad", Agent: "codex", Config: nested()},
	}}
	if err != nil || !reflect.DeepEqual(config, expected) {
		t.Fatalf("read back: %+v, %v; want %+v", config, err, expected)
	}
	wantLines := []string{"? Pool  pool-a", "? Plugin type  nomad", "? SHA-256 of the nomad plugin binary  " + receiverSHA,
		"? nomad plugin config, as a YAML flow mapping  {address: https://nomad.example:4646, credential: nomad-token, tls: {ca_file: /etc/nomad/ca.pem, verify: true}}",
		"? Agent  claude", "? Labels of claude  tier=batch, gpu=a100", "? Agent  codex", "? Labels of codex  none", "? Broker address the receiver connects to  " + receiverBroker}
	if !reflect.DeepEqual(collapsedLines(out.String()), wantLines) {
		t.Fatalf("collapse lines: %q", collapsedLines(out.String()))
	}
	if !inOrder(out.String(), "\nReceiver\n", "\nProfiles\n", "\nBroker\n") {
		t.Fatalf("headings: %q", out.String())
	}
	for _, heading := range []string{"\nReceiver\n", "\nProfiles\n", "\nBroker\n"} {
		if strings.Count(out.String(), heading) != 1 {
			t.Fatalf("heading %q", heading)
		}
	}
	if !inOrder((*frames)[0], "Pool", "pool-a", "pool-b", "Other pool...") || !inOrder((*frames)[1], "Plugin type", "nomad", "kubernetes", "systemd", "Other type...") ||
		!inOrder((*frames)[4], "Agent", "claude", "codex", "Other agent...") || !strings.Contains((*frames)[2], "The line for the binary in the release's signed checksums.txt") {
		t.Fatalf("pick options: %q", (*frames)[:5])
	}
	if strings.Contains(strings.Join((*frames)[:5], ""), "Another profile") || !strings.Contains((*frames)[8], "Another profile") {
		t.Fatal("Another profile asked before the first Agent")
	}
}

func TestReceiverOptions(t *testing.T) {
	state := operator.NewState()
	if pools := receiverPools(state, "receiver-a"); len(pools) != 0 {
		t.Fatalf("no pools: %q", pools)
	}
	for _, pool := range []string{"pool-c", "pool-a", "pool-b"} {
		state.Pools[pool] = operator.Terms{}
	}
	if pools := receiverPools(state, "receiver-a"); !slices.Equal(pools, []string{"pool-a", "pool-b", "pool-c"}) {
		t.Fatalf("sorted pools: %q", pools)
	}
	state.Receivers["receiver-a"] = operator.ReceiverConfig{Pool: "pool-b"}
	if pools := receiverPools(state, "receiver-a"); !slices.Equal(pools, []string{"pool-b", "pool-a", "pool-c"}) {
		t.Fatalf("receiver's pool first: %q", pools)
	}
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "codex"}, {Name: "claude"}, {Name: "bad name"}, {Name: "codex"}, {Name: "x\u202e"}, {Name: "gemini"}}}
	if agents := receiverAgents(terms); !slices.Equal(agents, []string{"codex", "claude", "gemini"}) {
		t.Fatalf("terms-order agents: %q", agents)
	}
	if agents := receiverAgents(operator.Terms{Agents: []operator.AgentTerms{{Name: "bad name"}}}); len(agents) != 0 {
		t.Fatalf("invalid agents: %q", agents)
	}
}

func TestReceiverPicksFromState(t *testing.T) {
	t.Run("no pools", func(t *testing.T) {
		var out bytes.Buffer
		frames := scriptReceiver(t, &out, pickForm("Other...", "\r"), inputForm("", "pool-c\r"), pickForm("nomad", "\r"), inputForm("", receiverSHA+"\r"), inputForm("{}", "\r"),
			pickForm("Other...", "\r"), inputForm("", "claude\r"), inputForm("", "\r"), pickForm("add", down+"\r"), inputForm(receiverBroker, "\r"))
		f, err := askReceiver(t, operator.NewState(), &out)
		if err != nil {
			t.Fatal(err)
		}
		if f.Pool != "pool-c" || f.Plugins["nomad"].Profiles[0].Agent != "claude" || strings.Contains((*frames)[0], "pool-") || !strings.Contains((*frames)[0], "Other pool...") {
			t.Fatalf("other only: %+v %q", f, (*frames)[0])
		}
		if !slices.Contains(collapsedLines(out.String()), "? Pool  pool-c") || !slices.Contains(collapsedLines(out.String()), "? Agent  claude") {
			t.Fatalf("collapse lines: %q", collapsedLines(out.String()))
		}
	})
	t.Run("no valid agents", func(t *testing.T) {
		state := operator.NewState()
		state.Pools["pool-a"] = operator.Terms{Agents: []operator.AgentTerms{{Name: "bad name"}}}
		var out bytes.Buffer
		frames := scriptReceiver(t, &out, pickForm("pool-a", "\r"), pickForm("nomad", "\r"), inputForm("", receiverSHA+"\r"), inputForm("{}", "\r"),
			pickForm("Other...", "\r"), inputForm("", "claude\r"), inputForm("", "\r"), pickForm("add", down+"\r"), inputForm(receiverBroker, "\r"))
		f, err := askReceiver(t, state, &out)
		if err != nil {
			t.Fatal(err)
		}
		if f.Plugins["nomad"].Profiles[0].Agent != "claude" || strings.Contains((*frames)[4], "bad name") || !strings.Contains((*frames)[4], "Other agent...") {
			t.Fatalf("other agent only: %+v %q", f, (*frames)[4])
		}
	})
	t.Run("existing receiver", func(t *testing.T) {
		state := receiverState()
		state.Receivers["receiver-a"] = operator.ReceiverConfig{Pool: "pool-b"}
		var out bytes.Buffer
		forms := baseReceiverForms()
		forms[0] = pickForm("pool-b", "\r")
		frames := scriptReceiver(t, &out, forms...)
		f, err := askReceiver(t, state, &out)
		if err != nil {
			t.Fatal(err)
		}
		if f.Pool != "pool-b" || !inOrder((*frames)[0], "pool-b", "pool-a", "Other pool...") {
			t.Fatalf("receiver's pool first: %+v %q", f, (*frames)[0])
		}
	})
	t.Run("other paths", func(t *testing.T) {
		var out bytes.Buffer
		forms := baseReceiverForms()
		forms[0] = pickForm("pool-a", down+down+"\r")
		forms[1] = pickForm("nomad", down+down+down+"\r")
		forms[4] = pickForm("Other...", "\r")
		forms = slices.Insert(forms, 5, inputForm("", "gemini\r"))
		forms = slices.Insert(forms, 2, inputForm("", "docker\r"))
		forms = slices.Insert(forms, 1, inputForm("", "pool-c\r"))
		scriptReceiver(t, &out, forms...)
		f, err := askReceiver(t, receiverState(), &out)
		if err != nil {
			t.Fatal(err)
		}
		if f.Pool != "pool-c" || f.PluginSHA256["docker"] != receiverSHA || f.Plugins["docker"].Profiles[0].Agent != "gemini" {
			t.Fatalf("other paths: %+v", f)
		}
		for _, line := range []string{"? Pool  pool-c", "? Plugin type  docker", "? SHA-256 of the docker plugin binary  " + receiverSHA, "? Agent  gemini"} {
			if !slices.Contains(collapsedLines(out.String()), line) {
				t.Fatalf("collapse line %q missing: %q", line, collapsedLines(out.String()))
			}
		}
	})
}

func TestReceiverValidation(t *testing.T) {
	retype := func(bad, good string) []string { return []string{"\x15" + bad, "\r", "\x15", good + "\r"} }
	profiles := func(f receiverFile) []receiverProfile { return f.Plugins["nomad"].Profiles }
	label := func(f receiverFile, key string) string {
		if value := profiles(f)[0].Labels[key]; value != nil {
			return *value
		}
		return ""
	}
	cases := []struct {
		name    string
		field   int
		edit    func([]receiverForm) []receiverForm
		message string
		stored  func(receiverFile) bool
	}{
		{"pool name", 1, func(forms []receiverForm) []receiverForm {
			forms[0] = pickForm("pool-a", down+down+"\r")
			return slices.Insert(forms, 1, inputForm("", retype("bad name", "pool-b")...))
		}, "invalid name", func(f receiverFile) bool { return f.Pool == "pool-b" }},
		{"type name", 2, func(forms []receiverForm) []receiverForm {
			forms[1] = pickForm("nomad", down+down+down+"\r")
			return slices.Insert(forms, 2, inputForm("", retype("bad name", "docker")...))
		}, "invalid name", func(f receiverFile) bool {
			return f.Plugins["docker"].Profiles != nil && f.PluginSHA256["docker"] == receiverSHA
		}},
		{"agent name", 5, func(forms []receiverForm) []receiverForm {
			forms[4] = pickForm("claude", down+down+"\r")
			return slices.Insert(forms, 5, inputForm("", retype("bad name", "gemini")...))
		}, "invalid name", func(f receiverFile) bool { return profiles(f)[0].Agent == "gemini" && profiles(f)[0].Name == "gemini" }},
		{"duplicate agent picked", 7, func(forms []receiverForm) []receiverForm {
			forms[6] = pickForm("add", "\r")
			return slices.Insert(forms, 7, pickForm("claude", "\r", down, "\r"), inputForm("", "\r"), pickForm("add", down+"\r"))
		}, "agent claude already has a profile", func(f receiverFile) bool {
			return len(profiles(f)) == 2 && profiles(f)[1].Agent == "codex"
		}},
		{"duplicate agent typed", 8, func(forms []receiverForm) []receiverForm {
			forms[6] = pickForm("add", "\r")
			return slices.Insert(forms, 7, pickForm("claude", down+down+"\r"), inputForm("", retype("claude", "gemini")...), inputForm("", "\r"), pickForm("add", down+"\r"))
		}, "agent claude already has a profile", func(f receiverFile) bool {
			return len(profiles(f)) == 2 && profiles(f)[1].Agent == "gemini"
		}},
		{"sha uppercase", 2, nil, "SHA-256 must be 64 lowercase hex digits", nil},
		{"sha short", 2, nil, "SHA-256 must be 64 lowercase hex digits", nil},
		{"sha non-hex", 2, nil, "SHA-256 must be 64 lowercase hex digits", nil},
		{"config list", 3, nil, "plugin config is not a YAML mapping", nil},
		{"config scalar", 3, nil, "plugin config is not a YAML mapping", nil},
		{"config null", 3, nil, "plugin nomad sets config key a to null", nil},
		{"config null in list", 3, nil, "plugin nomad sets config key b[1] to null", nil},
		{"config credential number", 3, nil, "credential is not a string", nil},
		{"config credential name", 3, nil, `credential "bad name" is not a valid name`, nil},
		{"label form", 5, nil, `label "k" needs the form KEY=VALUE`, nil},
		{"label key", 5, nil, `label "-k" must have a valid key and value`, nil},
		{"label value", 5, nil, `label "k" must have a valid key and value`, nil},
		{"label empty value", 5, nil, `label "k" must have a valid key and value`, nil},
		{"label empty key", 5, nil, `label "" must have a valid key and value`, nil},
		{"duplicate label key", 6, func(forms []receiverForm) []receiverForm {
			forms[5] = inputForm("", "tier=batch\r")
			return slices.Insert(forms, 6, inputForm("", retype("tier=other", "gpu=a100")...), inputForm("", "\r"))
		}, "duplicate label key", func(f receiverFile) bool { return label(f, "tier") == "batch" && label(f, "gpu") == "a100" }},
		{"broker port", 7, nil, "host:0 is not a valid Kafka broker address", nil},
		{"broker bracket", 7, nil, "[::1]x9092 is not a valid Kafka broker address", nil},
	}
	bad := map[string][2]string{
		"sha uppercase": {strings.ToUpper(receiverSHA), receiverSHA}, "sha short": {receiverSHA[:63], receiverSHA}, "sha non-hex": {receiverSHA[:63] + "g", receiverSHA},
		"config list": {"[1]", "{credential: nomad-token}"}, "config scalar": {"x", "{credential: nomad-token}"},
		"config null": {"{a: null}", "{credential: nomad-token}"}, "config null in list": {"{b: [1, null]}", "{credential: nomad-token}"},
		"config credential number": {"{credential: 1}", "{credential: nomad-token}"}, "config credential name": {`{credential: "bad name"}`, "{credential: nomad-token}"},
		"label form": {"k", "tier=batch"}, "label key": {"-k=v", "tier=batch"}, "label value": {"k=bad value", "tier=batch"},
		"label empty value": {"k=", "tier=batch"}, "label empty key": {"=v", "tier=batch"},
		"broker port": {"host:0", "kafka.example:9093"}, "broker bracket": {"[::1]x9092", "kafka.example:9093"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			forms := baseReceiverForms()
			stored := tc.stored
			if tc.edit != nil {
				forms = tc.edit(forms)
			} else {
				values := bad[tc.name]
				forms[tc.field].keys = retype(values[0], values[1])
				if tc.field == 5 {
					forms = slices.Insert(forms, 6, inputForm("", "\r"))
				}
				stored = map[int]func(receiverFile) bool{
					2: func(f receiverFile) bool { return f.PluginSHA256["nomad"] == receiverSHA },
					3: func(f receiverFile) bool {
						return reflect.DeepEqual(f.Plugins["nomad"].Config, map[string]any{"credential": "nomad-token"})
					},
					5: func(f receiverFile) bool { return label(f, "tier") == "batch" && len(profiles(f)[0].Labels) == 1 },
					7: func(f receiverFile) bool { return f.Broker == "kafka.example:9093" },
				}[tc.field]
			}
			var out bytes.Buffer
			frames := scriptReceiver(t, &out, forms...)
			f, err := askReceiver(t, receiverState(), &out)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains((*frames)[tc.field], tc.message) {
				t.Fatalf("form %d did not show %q: %q", tc.field, tc.message, (*frames)[tc.field])
			}
			if !stored(f) {
				t.Fatalf("corrected value not stored: %+v", f)
			}
		})
	}
}

func TestReceiverPermittedValues(t *testing.T) {
	blank := "pool: pool-a\nbroker: kafka.example\nplugin_sha256:\n    nomad: " + receiverSHA + "\nplugins:\n    nomad:\n        labels: {}\n        config: {}\n        profiles:\n            - name: claude\n              agent: claude\n              labels: {}\n              config: {}\n"
	for _, config := range []string{"", "null", "~"} {
		t.Run("config "+cmpName(config), func(t *testing.T) {
			forms := baseReceiverForms()
			forms[3] = inputForm("{}", "\x15"+config+"\r")
			forms[7] = inputForm(receiverBroker, "\x15kafka.example\r")
			var out bytes.Buffer
			scriptReceiver(t, &out, forms...)
			dir := t.TempDir()
			path, _, err := objectFile(context.Background(), dir, "receivers", "receiver-a", nil, false, &out, receiverFields, func(q *questions) (any, error) {
				return promptReceiver(q, "receiver-a", receiverState(), receiverBroker)
			})
			if err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(path); err != nil || string(data) != blank {
				t.Fatalf("config %q wrote %v\n%s", config, err, data)
			}
		})
	}
}

func cmpName(s string) string {
	if s == "" {
		return "blank"
	}
	return s
}

func TestReceiverConfigEscape(t *testing.T) {
	forms := baseReceiverForms()
	forms[3] = inputForm("{}", "\x15{note: \"\u202ex\"}\r")
	var out bytes.Buffer
	scriptReceiver(t, &out, forms...)
	dir := t.TempDir()
	path, _, err := objectFile(context.Background(), dir, "receivers", "receiver-a", nil, false, &out, receiverFields, func(q *questions) (any, error) {
		return promptReceiver(q, "receiver-a", receiverState(), receiverBroker)
	})
	if err != nil {
		t.Fatal(err)
	}
	var written receiverFile
	if err := readYAML(path, &written); err != nil || written.Plugins["nomad"].Config["note"] != "\u202ex" {
		t.Fatalf("written config: %+v, %v", written, err)
	}
	lines := strings.Join(collapsedLines(out.String()), "\n")
	if !strings.Contains(lines, `? nomad plugin config, as a YAML flow mapping  {note: "\u202ex"}`) || strings.Contains(lines, "\u202e") {
		t.Fatalf("escaped config collapse: %q", lines)
	}
}

func TestReceiverInterruptBeforeWrite(t *testing.T) {
	for _, mode := range []string{"ctrl-c at first agent", "ctrl-c at label", "eof at broker"} {
		eof := mode == "eof at broker"
		t.Run(mode, func(t *testing.T) {
			service := operatorEnv(t)
			terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
			if err := service.PoolApply(context.Background(), "pool-a", terms, false); err != nil {
				t.Fatal(err)
			}
			forms := []receiverForm{pickForm("pool-a", "\r"), pickForm("nomad", "\r"), inputForm("", receiverSHA+"\r"), inputForm("{}", "\r"), pickForm("claude", "\r")}
			if mode == "ctrl-c at first agent" {
				forms[4] = pickForm("claude", "\x03")
			} else if eof {
				forms = append(forms, inputForm("", "\r"), pickForm("add", down+"\r"))
			} else {
				forms = append(forms, inputForm("", "\x03"))
			}
			var out bytes.Buffer
			scriptReceiver(t, &out, forms...)
			if eof {
				scripted := runQuestion
				n := 0
				runQuestion = func(q *questions, f huh.Field) error {
					n++
					if n == 8 {
						next := *q
						next.in = strings.NewReader("")
						return next.runForm(f)
					}
					return scripted(q, f)
				}
			}
			err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, &out)
			if !errors.Is(err, errInterrupted) {
				t.Fatalf("interrupt: %v", err)
			}
			for _, written := range []string{"receivers", "bundles"} {
				if _, err := os.Stat(written); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("interrupted prompt created %s: %v", written, err)
				}
			}
			state, loadErr := service.LoadState(context.Background())
			if (loadErr != nil && !errors.Is(loadErr, kerr.UnknownTopicOrPartition)) || len(state.Receivers) != 0 {
				t.Fatalf("interrupted prompt applied receiver: %v, %v", state.Receivers, loadErr)
			}
		})
	}
}

func TestReceiverEligibility(t *testing.T) {
	master, tty, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer tty.Close()
	operatorEnv(t)
	path := filepath.Join("receivers", "receiver-a.yaml")
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
			err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, out)
			want := path + " does not exist and there is no terminal to prompt on; write it, or give -f FILE, with " + receiverFields
			if err == nil || err.Error() != want {
				t.Fatalf("eligibility error: %v", err)
			}
		})
	}
	stdin = tty
	t.Setenv("TERM", "xterm")
	err = runOperator([]string{"receiver", "apply", "receiver-a", "--dry-run"}, io.Discard, tty)
	want := path + " does not exist and --dry-run writes nothing; write it, or give -f FILE, with " + receiverFields
	if err == nil || err.Error() != want {
		t.Fatalf("dry-run refusal: %v", err)
	}
	for _, directory := range []bool{false, true} {
		file := filepath.Join(t.TempDir(), "receiver.yaml")
		if directory {
			if err := os.Mkdir(file, 0o755); err != nil {
				t.Fatal(err)
			}
		} else if err := os.WriteFile(file, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		got, isDir, err := objectFile(context.Background(), ".", "receivers", "receiver-a", map[string]string{"f": file}, false, tty, receiverFields, nil)
		if err != nil || got != file || isDir != directory {
			t.Fatalf("-f %s: %s %t %v", file, got, isDir, err)
		}
	}
}

func TestReceiverFailedWriter(t *testing.T) {
	old := promptsEligible
	t.Cleanup(func() { promptsEligible = old })
	promptsEligible = func(io.Writer) bool { return true }
	dir := t.TempDir()
	_, _, err := objectFile(context.Background(), dir, "receivers", "receiver-a", nil, false, failingPrompt{}, receiverFields, func(q *questions) (any, error) {
		return promptReceiver(q, "receiver-a", receiverState(), receiverBroker)
	})
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "receivers")); !os.IsNotExist(err) {
		t.Fatal("failed writer created directory")
	}
}
