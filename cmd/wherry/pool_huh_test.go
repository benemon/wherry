package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/charmbracelet/huh"
	"github.com/creack/pty"
	"github.com/twmb/franz-go/pkg/kerr"
)

func poolSteps() [][]string {
	return [][]string{keys("claude\r"), keys("haiku\r"), keys("sonnet\r"), keys("\r"), keys("\r"), keys("token\r"), keys("\r"), keys("TOKEN\r"), keys("\r"), keys("Claude subscription token\r"), keys("\r"), keys("deploy\r"), keys(down + "\r"), keys("deploy-key\r"), keys(down + "\r"), keys("\r"), keys(down + "\r"), keys("\r"), keys("codex\r"), keys("\r"), keys(down + "\r"), keys(down + "\r"), keys(down + "\r"), keys("\x152097152\r"), keys("\x1520\r"), keys("\x151h\r")}
}

func TestPoolForms(t *testing.T) {
	scriptForms(t, poolSteps()...)
	dir := t.TempDir()
	var out bytes.Buffer
	path, _, err := objectFile(context.Background(), dir, "pools", "a", nil, false, &out, poolFields, promptPool)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "agents:\n    - name: claude\n      models:\n        - haiku\n        - sonnet\n      credentials:\n        - description: Claude subscription token\n          env: TOKEN\n          name: token\n        - file: deploy-key\n          name: deploy\n          required: false\n    - name: codex\n      credentials: []\ndefault_agent: codex\nproducer_byte_rate: 2097152\nrequest_percentage: 20\nstart_by_limit: 1h\n"
	if string(data) != want {
		t.Fatalf("YAML shape: got\n%s\nwant\n%s", data, want)
	}
	terms, err := readPoolConfig(path, false)
	optional := false
	expected := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude", Models: []string{"haiku", "sonnet"}, Credentials: []operator.AgentCredential{{Description: "Claude subscription token"}, {Required: &optional}}}, {Name: "codex"}}, DefaultAgent: "codex", ProducerByteRate: 2097152, RequestPercentage: 20, StartByLimitMS: operator.Millis(time.Hour / time.Millisecond)}
	expected.Agents[0].Credentials[0].Name = "token"
	expected.Agents[0].Credentials[0].Env = "TOKEN"
	expected.Agents[0].Credentials[1].Name = "deploy"
	expected.Agents[0].Credentials[1].File = "deploy-key"
	expected.Agents[1].Credentials = []operator.AgentCredential{}
	if err != nil || !reflect.DeepEqual(terms, expected) {
		t.Fatalf("read back: %+v, %v; want %+v", terms, err, expected)
	}
	wantLines := []string{"? Agent name  claude", "? Models of claude  haiku, sonnet", "? Credential token  Environment variable TOKEN, required, \"Claude subscription token\"", "? Credential deploy  File deploy-key, optional", "? Agent name  codex", "? Models of codex  none", "? Default agent  codex", "? Producer byte rate per host, bytes a second  2097152", "? Request percentage per host  20", "? Start-by limit  1h"}
	if !reflect.DeepEqual(collapsedLines(out.String()), wantLines) {
		t.Fatalf("collapse lines: %q", collapsedLines(out.String()))
	}
	for _, heading := range []string{"\nAgents\n", "\nPool\n"} {
		if strings.Count(out.String(), heading) != 1 {
			t.Fatalf("heading %q", heading)
		}
	}
}

func minimalPoolSteps() [][]string {
	return [][]string{keys("agent\r"), keys("\r"), keys(down + "\r"), keys(down + "\r"), keys("\r"), keys("\r"), keys("\r"), keys("\r")}
}

func TestPoolDefaults(t *testing.T) {
	scriptForms(t, minimalPoolSteps()...)
	var out bytes.Buffer
	value, err := promptPool(&questions{context.Background(), stdin, &promptLatch{out: &out}})
	if err != nil {
		t.Fatal(err)
	}
	f := value.(poolFile)
	if f.DefaultAgent != "agent" || f.ProducerByteRate != 1048576 || f.RequestPercentage != 10 || f.StartByLimit != "72h0m0s" || !strings.Contains(out.String(), "? Default agent  agent\n") {
		t.Fatalf("defaults: %+v", f)
	}
}

func TestPoolValidation(t *testing.T) {
	cases := []struct {
		name               string
		index              int
		bad, good, message string
	}{
		{"agent", 0, "bad name", "claude", "invalid name"},
		{"credential", 5, "bad name", "token", "invalid name"},
		{"duplicate", 11, "token", "deploy", "duplicate credential name"},
		{"env", 7, "bad-var", "TOKEN", "env must match"},
		{"reserved", 7, "HOME", "TOKEN", "env must match"},
		{"file", 13, "../key", "deploy-key", "file must be a portable file name"},
		{"long description", 9, strings.Repeat("界", 81), strings.Repeat("界", 80), "invalid description"},
		{"separator description", 9, "bad\u2028", "good", "invalid description"},
		{"rate zero", 23, "0", "2097152", "producer byte rate must be greater than 0"},
		{"rate text", 23, "abc", "2097152", "producer byte rate must be greater than 0"},
		{"rate negative", 23, "-1", "2097152", "producer byte rate must be greater than 0"},
		{"percentage", 24, "0", "20", "request percentage must be greater than 0"},
		{"percentage NaN", 24, "NaN", "20", "request percentage must be greater than 0"},
		{"duration negative", 25, "-1h", "1h", "start-by limit must be a non-negative duration"},
		{"duration text", 25, "soon", "1h", "start-by limit must be a non-negative duration"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			steps := poolSteps()
			steps[tc.index] = []string{"\x15" + tc.bad, "\r", "\x15", tc.good + "\r"}
			scriptForms(t, steps...)
			var out bytes.Buffer
			value, err := promptPool(&questions{context.Background(), stdin, &promptLatch{out: &out}})
			if err != nil {
				t.Fatal(err)
			}
			f := value.(poolFile)
			if tc.message != "" && !strings.Contains(out.String(), tc.message) {
				t.Fatalf("missing validation error %q", tc.message)
			}
			if tc.index == 7 && f.Agents[0].Credentials[0]["env"] != tc.good {
				t.Fatalf("target validation and correction: %+v", f)
			}
			if tc.index == 13 && f.Agents[0].Credentials[1]["file"] != tc.good {
				t.Fatalf("file correction: %+v", f)
			}
		})
	}
}

func TestPoolDescriptionEscape(t *testing.T) {
	steps := poolSteps()
	steps[9] = keys("hello\u202e\r")
	scriptForms(t, steps...)
	var out bytes.Buffer
	dir := t.TempDir()
	path, _, err := objectFile(context.Background(), dir, "pools", "a", nil, false, &out, poolFields, promptPool)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if !bytes.Contains(data, []byte("hello\u202e")) {
		t.Fatalf("raw YAML description: %s", data)
	}
	lines := strings.Join(collapsedLines(out.String()), "\n")
	if !strings.Contains(lines, `"hello\u202e"`) || strings.Contains(lines, "\u202e") {
		t.Fatalf("escaped description collapse: %q", lines)
	}
}

func TestPoolDescriptionTrimmed(t *testing.T) {
	for _, tc := range []struct{ typed, want string }{{"  padded  ", "padded"}, {"   ", ""}} {
		steps := poolSteps()
		steps[9] = keys(tc.typed + "\r")
		scriptForms(t, steps...)
		var out bytes.Buffer
		value, err := promptPool(&questions{context.Background(), stdin, &promptLatch{out: &out}})
		if err != nil {
			t.Fatal(err)
		}
		got, ok := value.(poolFile).Agents[0].Credentials[0]["description"]
		if tc.want == "" && ok || tc.want != "" && got != tc.want {
			t.Fatalf("description %q stored as %v, %v", tc.typed, got, ok)
		}
		if tc.want != "" && !strings.Contains(out.String(), `, "padded"`+"\n") {
			t.Fatalf("collapse: %q", out.String())
		}
	}
}

func TestPoolInterruptBeforeWrite(t *testing.T) {
	for _, eof := range []bool{false, true} {
		t.Run(map[bool]string{false: "ctrl-c", true: "eof"}[eof], func(t *testing.T) {
			service := operatorEnv(t)
			steps := minimalPoolSteps()
			if eof {
				steps = steps[:7]
			} else {
				steps = steps[:3]
				steps[2] = keys("\x03")
			}
			scriptForms(t, steps...)
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
			dir := "."
			err := runOperator([]string{"pool", "apply", "a"}, io.Discard, io.Discard)
			if !errors.Is(err, errInterrupted) {
				t.Fatalf("interrupt: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, "pools")); !os.IsNotExist(err) {
				t.Fatal("interrupted prompt created pools directory")
			}
			state, loadErr := service.LoadState(context.Background())
			if (loadErr != nil && !errors.Is(loadErr, kerr.UnknownTopicOrPartition)) || len(state.Pools) != 0 {
				t.Fatalf("interrupted prompt applied pool: %v, %v", state.Pools, loadErr)
			}
		})
	}
}

func TestPoolEligibility(t *testing.T) {
	master, tty, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer tty.Close()
	oldIn := stdin
	t.Cleanup(func() { stdin = oldIn })
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
			dir := t.TempDir()
			path := filepath.Join(dir, "pools", "a.yaml")
			_, _, err := objectFile(context.Background(), dir, "pools", "a", nil, false, out, poolFields, func(*questions) (any, error) { t.Fatal("ineligible prompt called"); return nil, nil })
			want := path + " does not exist and there is no terminal to prompt on; write it, or give -f FILE, with " + poolFields
			if err == nil || err.Error() != want {
				t.Fatalf("eligibility error: %v", err)
			}
		})
	}
}

func TestPoolSharedTarget(t *testing.T) {
	steps := poolSteps()
	steps[12] = keys("\r")
	steps[13] = []string{"TOKEN", "\r", "\x15", "OTHER\r"}
	scriptForms(t, steps...)
	var out bytes.Buffer
	value, err := promptPool(&questions{context.Background(), stdin, &promptLatch{out: &out}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "share a target") || value.(poolFile).Agents[0].Credentials[1]["env"] != "OTHER" {
		t.Fatal("shared target validation or correction missing")
	}
}

func TestPoolFailedWriter(t *testing.T) {
	old := promptsEligible
	t.Cleanup(func() { promptsEligible = old })
	promptsEligible = func(io.Writer) bool { return true }
	dir := t.TempDir()
	_, _, err := objectFile(context.Background(), dir, "pools", "a", nil, false, failingPrompt{}, poolFields, promptPool)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pools")); !os.IsNotExist(err) {
		t.Fatal("failed writer created directory")
	}
}

func TestPoolObjectFileBypasses(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "directory"}[directory], func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "pools", "a.yaml")
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				t.Fatal(err)
			}
			if directory {
				if err := os.Mkdir(path, 0755); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(path, []byte("agents: []\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, isDir, err := objectFile(context.Background(), dir, "pools", "a", nil, false, io.Discard, poolFields, func(*questions) (any, error) { t.Fatal("existing path prompted"); return nil, nil })
			if err != nil || got != path || isDir != directory {
				t.Fatalf("existing path: %s %t %v", got, isDir, err)
			}
		})
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "pools", "a.yaml")
	_, _, err := objectFile(context.Background(), dir, "pools", "a", nil, true, io.Discard, poolFields, nil)
	want := path + " does not exist and --dry-run writes nothing; write it, or give -f FILE, with " + poolFields
	if err == nil || err.Error() != want {
		t.Fatalf("dry-run refusal: %v", err)
	}
}

type poolRawValue string

func (v poolRawValue) Get() string { return string(v) }
func (v poolRawValue) Set(string)  {}

func TestPoolFieldValidators(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index int
		bad   string
	}{
		{"credential", 5, "bad name"}, {"duplicate", 11, "token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			steps := poolSteps()[:tc.index]
			scriptForms(t, steps...)
			scripted := runQuestion
			n := 0
			stop := errors.New("validated field")
			runQuestion = func(q *questions, f huh.Field) error {
				current := n
				n++
				if current == tc.index {
					input, ok := f.(*huh.Input)
					if !ok {
						t.Fatalf("expected input, got %T", f)
					}
					input.Accessor(poolRawValue(tc.bad))
					input.Blur()
					if input.Error() == nil {
						t.Fatalf("field accepted invalid value %q", tc.bad)
					}
					return stop
				}
				return scripted(q, f)
			}
			_, err := promptPool(&questions{context.Background(), stdin, &promptLatch{out: io.Discard}})
			if !errors.Is(err, stop) {
				t.Fatalf("field validation: %v", err)
			}
		})
	}
}

func TestPoolPermittedValues(t *testing.T) {
	steps := poolSteps()
	steps[0] = keys(" claude \r")
	steps[1] = keys(" model\u202e \r")
	steps[18] = keys("claude\r")
	steps[23] = keys("\x15 1 \r")
	steps[24] = keys("\x15 +Inf \r")
	steps[25] = keys("\x15 0 \r")
	scriptForms(t, steps...)
	var out bytes.Buffer
	value, err := promptPool(&questions{context.Background(), stdin, &promptLatch{out: &out}})
	if err != nil {
		t.Fatal(err)
	}
	f := value.(poolFile)
	if f.Agents[0].Name != "claude" || f.Agents[1].Name != "claude" || f.Agents[0].Models[0] != "model\u202e" || f.ProducerByteRate != 1 || f.StartByLimit != "0" {
		t.Fatalf("permitted values: %+v", f)
	}
	if !strings.Contains(strings.Join(collapsedLines(out.String()), "\n"), `? Models of claude  model\u202e, sonnet`) {
		t.Fatal("model collapse not escaped")
	}
}

func TestPoolExistingDirectoryNoPrompt(t *testing.T) {
	service := operatorEnv(t)
	shareCeilings(t, service.Operator)
	dir := filepath.Join("pools", "directory.yaml")
	if err := os.MkdirAll(filepath.Join(dir, "agents"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pool.yaml"), []byte("producer_byte_rate: 1048576\nrequest_percentage: 10\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "agents", "agent.yaml"), []byte("name: agent\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runOperator([]string{"pool", "apply", "directory"}, io.Discard, &out); err != nil || out.Len() != 0 {
		t.Fatalf("existing directory apply: %v, prompts %q", err, out.String())
	}
	state, err := service.LoadState(context.Background())
	if err != nil || len(state.Pools) != 1 {
		t.Fatalf("existing directory not applied: %v, %v", state.Pools, err)
	}
}
