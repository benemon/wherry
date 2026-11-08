package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/x/ansi"
	"github.com/creack/pty"
	"golang.org/x/sys/unix"
)

type signalledOutput struct {
	io.Writer
	once  sync.Once
	ready chan struct{}
}

type initialFrameOutput struct {
	io.Writer
	mu        sync.Mutex
	buffer    bytes.Buffer
	frame     string
	ready     chan struct{}
	readyOnce sync.Once
}

func (w *initialFrameOutput) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.mu.Lock()
	w.buffer.Write(p[:n])
	if strings.Contains(w.buffer.String(), "↑ up") {
		w.readyOnce.Do(func() {
			w.frame = w.buffer.String()
			close(w.ready)
		})
	}
	w.mu.Unlock()
	return n, err
}

func (w *initialFrameOutput) initial() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.frame
}

func (w *signalledOutput) Write(p []byte) (int, error) {
	n, e := w.Writer.Write(p)
	w.once.Do(func() { close(w.ready) })
	return n, e
}

// scriptForms supplies each form only after its first render, with refresh time between separate key batches.
func scriptForms(t *testing.T, answers ...[]string) {
	t.Helper()
	t.Setenv("TERM", "xterm-256color")
	old, eligible := runQuestion, promptsEligible
	t.Cleanup(func() { runQuestion, promptsEligible = old, eligible })
	promptsEligible = func(io.Writer) bool { return true }
	i := 0
	runQuestion = func(q *questions, field huh.Field) error {
		if i >= len(answers) {
			t.Errorf("unexpected question %T", field)
			return errors.New("script exhausted")
		}
		steps := answers[i]
		i++
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		defer w.Close()
		ready := make(chan struct{})
		out := &signalledOutput{Writer: q.out, ready: ready}
		next := *q
		ctx, cancel := context.WithTimeout(q.ctx, 5*time.Second)
		defer cancel()
		next.ctx = ctx
		next.in = r
		next.out = &promptLatch{out: out}
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-ready:
			case <-done:
				return
			}
			for _, step := range steps {
				select {
				case <-time.After(80 * time.Millisecond):
				case <-done:
					return
				}
				if _, err := io.WriteString(w, step); err != nil {
					return
				}
			}
		}()
		return next.runForm(field)
	}
	t.Cleanup(func() {
		if i != len(answers) {
			t.Errorf("consumed %d forms, want %d", i, len(answers))
		}
	})
}
func keys(s string) []string { return []string{s} }

const down = "\x1b[B"

func collapsedLines(output string) []string {
	var lines []string
	for {
		start := strings.Index(output, "? ")
		if start < 0 {
			return lines
		}
		output = output[start:]
		end := strings.IndexByte(output, '\n')
		if end < 0 {
			end = len(output)
		}
		lines = append(lines, strings.TrimSuffix(output[:end], "\r"))
		output = output[end:]
	}
}

func TestHostHuhSourcesAndSummary(t *testing.T) {
	t.Setenv("TARGET_A", "secret-a")
	t.Setenv("TARGET_B", "")
	a := envCredential("token", "TARGET_A")
	a.Description = "API token"
	h := newInitHost(t, map[string]operator.Terms{"pool-a": agentTerms(a), "pool-b": agentTerms(envCredential("token", "TARGET_B"))})
	scriptForms(t, keys("\r"), keys("\r"))
	stdout, out := h.init(t, h.bundle)
	h.registered(t, strings.TrimSpace(stdout))
	if !reflect.DeepEqual(h.written(t).Credentials, map[string]credential.Source{"token": {Env: "TARGET_A"}}) {
		t.Fatal(h.written(t))
	}
	for _, s := range []string{"? API token  Environment variable TARGET_A", "Credentials  1 of 2", "Summary  2 of 2", "Host key ", "Wrote "} {
		if !strings.Contains(out, s) {
			t.Errorf("missing %q in %s", s, out)
		}
	}
	if strings.Contains(out, "secret-a") {
		t.Fatal("value rendered")
	}
	if strings.Count(stdout, "\n") != 1 || strings.Contains(out, stdout) {
		t.Fatalf("stdout %q prompts %q", stdout, out)
	}
	if strings.Count(out, "Wrote "+h.config) != 1 {
		t.Fatalf("write notices in %q", out)
	}
}

func TestHostSourceCollapsedLines(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.WriteFile("source", nil, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TOKEN", "value")
	for _, test := range []struct {
		name       string
		nested     bool
		configured map[string]credential.Source
		answers    [][]string
		want       string
	}{
		{"environment", false, nil, [][]string{keys("\r"), keys("\r")}, "? label  Environment variable TOKEN"},
		{"file", false, nil, [][]string{keys(down + "\r"), keys("./source\r")}, "? label  File ./source"},
		{"command", false, nil, [][]string{keys(down + down + "\r"), keys("printf token\r")}, "? label  Command printf token"},
		{"configured", true, map[string]credential.Source{"shared": {Env: "SHARED"}}, [][]string{keys("\r"), keys("\r")}, "? label  Configured credential shared"},
		{"skipped", false, nil, [][]string{keys(down + down + down + "\r")}, "? label  Skipped"},
		{"env-skipped", false, nil, [][]string{keys("\r"), keys(down + down + down + "\r")}, "? label  Skipped"},
	} {
		t.Run(test.name, func(t *testing.T) {
			scriptForms(t, test.answers...)
			var out bytes.Buffer
			q := &questions{context.Background(), nil, &promptLatch{out: &out}}
			result, err := q.source("key", "label", []credentialUse{{target: wire.TaskCredential{Env: "TOKEN"}}}, test.configured, test.nested)
			skipped := strings.HasSuffix(test.want, "Skipped")
			if err != nil || !skipped && !result.ok || skipped && result.ok {
				t.Fatal(result, err)
			}
			if got := collapsedLines(out.String()); !reflect.DeepEqual(got, []string{test.want}) {
				t.Fatalf("collapsed lines %q, want %q\n%s", got, test.want, out.String())
			}
		})
	}
}

func TestHostSourceShowsEveryOption(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	old := runQuestion
	t.Cleanup(func() { runQuestion = old })
	initial := make(chan string, 1)
	runQuestion = func(q *questions, field huh.Field) error {
		r, w, err := os.Pipe()
		if err != nil {
			return err
		}
		defer r.Close()
		defer w.Close()
		frame := &initialFrameOutput{Writer: q.out, ready: make(chan struct{})}
		next := *q
		next.in = r
		next.out = &promptLatch{out: frame}
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-frame.ready:
				initial <- frame.initial()
				io.WriteString(w, down+down+down+"\r")
			case <-done:
			}
		}()
		return next.runForm(field.WithWidth(40))
	}
	uses := []credentialUse{{pool: "one"}, {pool: "two"}, {pool: "three"}}
	q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
	if _, err := q.source("key", "label", uses, nil, false); err != nil {
		t.Fatal(err)
	}
	text := ansi.Strip(<-initial)
	for _, want := range []string{"pool one", "pool two", "pool three", "Environment variable", "File", "Command", "Skip"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in initial frame %q", want, text)
		}
	}
}
func TestHostHuhStorageAtomicAndAllPools(t *testing.T) {
	terms := agentTerms()
	terms.Storage = &operator.Storage{Endpoint: "https://s3.test", Bucket: "bucket"}
	h := newInitHost(t, map[string]operator.Terms{"a": terms, "b": terms})
	// Cancel after staging access, then start again; neither source is cached.
	scriptForms(t, keys("\r"), keys(down+"\r"), keys("\r"), keys("ACCESS\r"), keys(down+down+down+down+"\r"), keys("\r"), keys(down+"\r"), keys("\r"), keys("NEW_ACCESS\r"), keys(down+"\r"), keys("\r"), keys("SECRET\r"))
	_, out := h.init(t, h.bundle)
	f := h.written(t)
	if !reflect.DeepEqual(f.Credentials, map[string]credential.Source{"s3-access-key": {Env: "NEW_ACCESS"}, "s3-secret-key": {Env: "SECRET"}}) {
		t.Fatal(f.Credentials)
	}
	if len(f.Storage) != 2 || f.Storage["a"] != f.Storage["b"] {
		t.Fatal(f.Storage)
	}
	if !strings.Contains(out, "Storage  1 of 2") {
		t.Fatal(out)
	}
	for _, want := range []string{"? S3 access key  Environment variable NEW_ACCESS", "? S3 secret key  Environment variable SECRET"} {
		if !slices.Contains(collapsedLines(out), want) {
			t.Fatalf("missing %q in %q", want, collapsedLines(out))
		}
	}
}
func TestHostHuhSSHAndValidation(t *testing.T) {
	terms := agentTerms()
	terms.VCS = []string{"git.test"}
	h := newInitHost(t, map[string]operator.Terms{"a": terms})
	known := filepath.Join(t.TempDir(), "known_hosts")
	os.WriteFile(known, []byte("host key"), 0600)
	scriptForms(t, keys(down+down+"\r"), []string{"https://other.test/\r", "\x15", "ssh://git@git.test/\r"}, keys(down+"\r"), keys("\r"), keys("SSH_KEY\r"), []string{"/no-such-wherry-known-hosts\r", "\x15" + known + "\r"})
	_, out := h.init(t, h.bundle)
	f := h.written(t)
	if len(f.Git) != 1 || f.Git[0].HostKey != known || f.Git[0].Credential != "git-git-test-ssh" {
		t.Fatal(f.Git)
	}
	if !slices.Contains(collapsedLines(out), "? git.test  SSH ssh://git@git.test/") {
		t.Fatal(collapsedLines(out))
	}
}
func TestHostHuhAskedOnce(t *testing.T) {
	terms := agentTerms(envCredential("git-git-example-test", "TOKEN"))
	terms.VCS = []string{"git.example.test"}
	h := newInitHost(t, map[string]operator.Terms{"a": terms})
	scriptForms(t, keys(down+down+down+"\r"), keys(down+"\r"), keys(down+down+"\r"))
	_, out := h.init(t, h.bundle)
	if len(h.written(t).Git) != 0 || strings.Count(out, "? git-git-example-test  Skipped") != 1 || !strings.Contains(out, "was skipped earlier") || !slices.Contains(collapsedLines(out), "? git.example.test  Skipped") {
		t.Fatal(out)
	}
}
func TestHostHuhInterruptAfterBootstrap(t *testing.T) {
	h := newInitHost(t, map[string]operator.Terms{"a": agentTerms(envCredential("key", "KEY"))})
	scriptForms(t, keys("\x03"))
	var out bytes.Buffer
	err := hostInit(h.ctx, h.config, h.bundle, &out, &out)
	if !errors.Is(err, errInterrupted) {
		t.Fatal(err)
	}
	if h.written(t).Bundle == "" || len(h.written(t).Credentials) != 0 {
		t.Fatal(h.written(t))
	}
}
func TestHostHuhNoEntitlementSummary(t *testing.T) {
	h := newInitHost(t, nil)
	scriptForms(t)
	_, out := h.init(t, h.bundle)
	if !strings.Contains(out, "Summary  1 of 1") {
		t.Fatal(out)
	}
}

func TestHostKeyIDStaysInTerminalSummary(t *testing.T) {
	h := newInitHost(t, nil)
	scriptForms(t)
	master, tty, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	done := make(chan struct{})
	go func() {
		io.Copy(&output, master)
		close(done)
	}()
	if err := hostInit(h.ctx, h.config, h.bundle, tty, tty); err != nil {
		t.Fatal(err)
	}
	tty.Close()
	<-done
	master.Close()
	var keyID string
	for _, line := range strings.Split(strings.ReplaceAll(output.String(), "\r", ""), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 4 && fields[0] == "Host" && fields[1] == "key" && fields[3] == "registered" {
			keyID = fields[2]
		}
	}
	if keyID == "" {
		t.Fatal(output.String())
	}
	h.registered(t, keyID)
	for _, line := range strings.Split(strings.ReplaceAll(output.String(), "\r", ""), "\n") {
		if line == keyID {
			t.Fatalf("bare key ID printed in %q", output.String())
		}
	}
}

func TestHostNamesReservations(t *testing.T) {
	n := newGitNames(nil)
	seen := map[string]string{}
	for _, host := range []string{"a.b", "a-b", "a-ssh", "a", "host:8443", strings.Repeat("a", 60) + ".test"} {
		for _, transport := range []string{"https", "ssh"} {
			name := n.allocate(host, transport)
			pair := gitPair(host, transport)
			if len(name) > 32 || seen[name] != "" && seen[name] != pair {
				t.Fatal(name)
			}
			seen[name] = pair
			if n.allocate(host, transport) != name {
				t.Fatal("unstable reservation")
			}
		}
	}
	if n.allocate("a.b", "https") != "git-a-b" || n.allocate("a-b", "https") != "git-a-b-2" {
		t.Fatal(n)
	}
	n = newGitNames([]busclient.GitCredential{{URL: "https://other/", Credential: "git-a"}})
	if n.allocate("a", "https") != "git-a-2" {
		t.Fatal(n)
	}
	for i := 0; i < 12; i++ {
		host := strings.Repeat("a", 60) + strings.Repeat("b", i)
		name := n.allocate(host, "https")
		if len(name) > 32 {
			t.Fatal(name)
		}
	}
}
func TestHostLabelsTargetsAndEscapes(t *testing.T) {
	t.Setenv("PRESENT", "value")
	t.Setenv("EMPTY", "")
	uses := []credentialUse{{pool: "a", target: envCredential("x", "PRESENT").TaskCredential, optional: true}, {pool: "b", target: envCredential("x", "EMPTY").TaskCredential}}
	if sourceLabel("x", "description", uses) != "description" || sourceLabel("x", "", uses) != "x" {
		t.Fatal("label")
	}
	if got := targetVariables(uses); !reflect.DeepEqual(got, []string{"PRESENT"}) {
		t.Fatal(got)
	}
	for _, s := range []string{"\x1b]52;c;evil\a", "\x1b[2J", "\xff", "\u2029", "\x9b"} {
		if display(s) == s || strings.Contains(display(s), "\x1b") {
			t.Fatal(s)
		}
	}
	err := presentationError{errors.New("\x1b[2J")}
	if strings.Contains(err.Error(), "\x1b") || errors.Unwrap(err) == nil {
		t.Fatal(err)
	}
}
func TestHostCARejectsUnsafeFiles(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	os.Symlink(fifo, link)
	dirLink := filepath.Join(dir, "directory-link")
	os.Symlink(dir, dirLink)
	for _, p := range []string{fifo, link, dir, dirLink} {
		done := make(chan error, 1)
		go func() { done <- readCA(p) }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal(p)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked", p)
		}
	}
	_, ca := tlsCluster(t)
	regular := filepath.Join(dir, "ca")
	os.WriteFile(regular, []byte(ca), 0600)
	os.Remove(link)
	os.Symlink(regular, link)
	if err := readCA(link); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(regular, bytes.Repeat([]byte("x"), (1<<20)+1), 0600)
	if readCA(link) == nil {
		t.Fatal("oversized accepted")
	}
}
func TestHostCompletionRules(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	os.MkdirAll(filepath.Join(dir, "one", "two"), 0700)
	os.WriteFile(filepath.Join(dir, "one", "two", "file"), nil, 0600)
	os.WriteFile(filepath.Join(dir, ".hidden"), nil, 0600)
	os.WriteFile(filepath.Join(dir, "bad\x1b"), nil, 0600)
	for text, want := range map[string]string{"~/o": "~/one/", "~/one/t": "~/one/two/", "~/one/two/f": "~/one/two/file", "~/.h": "~/.hidden"} {
		if got := completions(text, "path"); !reflect.DeepEqual(got, []string{want}) {
			t.Fatalf("%s: %v", text, got)
		}
	}
	if got := completions("cat ~/one/t", "command"); !reflect.DeepEqual(got, []string{"cat ~/one/two/"}) {
		t.Fatal(got)
	}
	if got := completions("~/one/two/file/", "path"); len(got) != 0 {
		t.Fatal(got)
	}
	t.Setenv("PATH", ":"+dir+":relative")
	os.WriteFile(filepath.Join(dir, "executable"), nil, 0700)
	if got := completions("exec", "command"); !reflect.DeepEqual(got, []string{"executable"}) {
		t.Fatal(got)
	}
	if got := completions("~/", "path"); !reflect.DeepEqual(got, []string{"~/executable", "~/one/"}) {
		t.Fatal(got)
	}
}
func TestHostHuhTabRefreshAndNoMatch(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	os.MkdirAll(filepath.Join(dir, "one", "two"), 0700)
	scriptForms(t, []string{"~/o", "\t", "t", "\t", "\r"}, []string{"no-match", "\t", "\r"})
	q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
	for _, want := range []string{"~/one/two/", "no-match"} {
		got, err := q.input("File", "", "", "path", func(string) error { return nil })
		if err != nil || got != want {
			t.Fatalf("%q, %v; want %s", got, err, want)
		}
	}
}
func TestQuestionsTrimAndAsk(t *testing.T) {
	scriptForms(t, keys("   \r"), keys("  value  \r"))
	var out bytes.Buffer
	q := &questions{context.Background(), nil, &promptLatch{out: &out}}
	var seen []string
	validate := func(s string) error { seen = append(seen, s); return nil }
	if value, err := q.input("File", "", "", "path", validate); err != nil || value != "" {
		t.Fatalf("whitespace-only answer: %q, %v", value, err)
	}
	if value, err := q.ask("Name", "Shown \x1b under the title", "", "", validate); err != nil || value != "value" {
		t.Fatalf("trimmed answer: %q, %v", value, err)
	}
	for _, s := range seen {
		if s != strings.TrimSpace(s) {
			t.Fatalf("validator saw untrimmed %q", s)
		}
	}
	if !strings.Contains(out.String(), "? Name  value\n") || strings.Contains(out.String(), "\x1b under") {
		t.Fatalf("ask output: %q", out.String())
	}
}
func TestHostHuhBlankAndEscapedValidation(t *testing.T) {
	scriptForms(t, keys("\r"), []string{"bad\r", "\x15good\r"})
	var out bytes.Buffer
	q := &questions{context.Background(), nil, &promptLatch{out: &out}}
	if value, err := q.input("File", "", "", "path", func(s string) error {
		if s == "" {
			return nil
		}
		return errors.New("refused")
	}); err != nil || value != "" {
		t.Fatal(value, err)
	}
	value, err := q.input("Variable", "", "", "variable", func(s string) error {
		if s == "bad" {
			return errors.New("bad\x1b]52;c;evil\a")
		}
		return nil
	})
	if err != nil || value != "good" {
		t.Fatal(value, err)
	}
	if strings.Contains(out.String(), "\x1b]52;c;evil") {
		t.Fatal("raw error")
	}
}

type failingPrompt struct{}

func (failingPrompt) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestHostPromptFailuresAndEOF(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	q := &questions{ctx, strings.NewReader(""), &promptLatch{out: failingPrompt{}}}
	if _, err := q.input("File", "", "", "path", func(string) error { return nil }); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	eofCtx, eofCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer eofCancel()
	q = &questions{eofCtx, strings.NewReader(""), &promptLatch{out: io.Discard}}
	started := time.Now()
	if _, err := q.input("File", "", "", "path", func(string) error { return nil }); err == nil {
		t.Fatal("EOF succeeded")
	}
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("EOF took %v", elapsed)
	}
}

func TestHostSelectFilterCannotWedgeInterrupt(t *testing.T) {
	scriptForms(t, []string{"/", "zzz", "\r\x03"})
	started := time.Now()
	q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
	prev := "a"
	_, err := q.pick("Choice", "", []huh.Option[string]{option("a", "a")}, &prev)
	if !errors.Is(err, errInterrupted) || time.Since(started) >= time.Second {
		t.Fatal(err, time.Since(started))
	}
}

func TestHostSelectSlashDoesNotFilter(t *testing.T) {
	scriptForms(t, []string{"/", "zzz", "\r"})
	q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
	prev := "a"
	if got, err := q.pick("Choice", "", []huh.Option[string]{option("a", "a")}, &prev); err != nil || got != "a" {
		t.Fatal(got, err)
	}
}
func TestHostPagesEligibility(t *testing.T) {
	// The slave side: Linux also reports a pty master as a terminal, macOS does not.
	master, tty, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer tty.Close()
	old := stdin
	t.Cleanup(func() { stdin = old })
	stdin = tty
	t.Setenv("TERM", "xterm-256color")
	if !promptsEligible(tty) {
		t.Fatal("terminal stdin and prompts rejected")
	}
	if promptsEligible(io.Discard) {
		t.Fatal("redirected prompts accepted")
	}
	stdin = strings.NewReader("")
	if promptsEligible(tty) {
		t.Fatal("redirected stdin accepted")
	}
	stdin = tty
	t.Setenv("TERM", "dumb")
	if promptsEligible(tty) {
		t.Fatal("dumb accepted")
	}
}
func TestHostRenderedDescriptionEscapes(t *testing.T) {
	scriptForms(t, keys(down+down+down+"\r"))
	var out bytes.Buffer
	q := &questions{context.Background(), nil, &promptLatch{out: &out}}
	_, err := q.source("evil\x1b]52;c;x\a", "label\x1b[2J", []credentialUse{{pool: "pool\x1b]8;;evil\a"}}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"\x1b]52", "\x1b]8", "\x1b[2J"} {
		if strings.Contains(out.String(), raw) {
			t.Fatal("unsafe output")
		}
	}
}
func TestHostCAPipeWithCertificate(t *testing.T) {
	_, ca := tlsCluster(t)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = io.WriteString(w, ca); err != nil {
		t.Fatal(err)
	}
	w.Close()
	path := fmt.Sprintf("/proc/self/fd/%d", r.Fd())
	if _, err = os.Stat(path); os.IsNotExist(err) {
		t.Skip("requires proc fd paths")
	}
	if readCA(path) == nil {
		t.Fatal("accepted PEM from a non-regular file")
	}
}

func TestHostSharedInputTypeahead(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	for _, tail := range []string{"discarded\r", "\x03"} {
		t.Run(fmt.Sprintf("%q", tail), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			defer w.Close()
			q := &questions{ctx, r, nil}
			for i, want := range []string{"one", "two"} {
				ready := make(chan struct{})
				q.out = &promptLatch{out: &signalledOutput{Writer: io.Discard, ready: ready}}
				sent := make(chan struct{})
				go func(i int) {
					defer close(sent)
					select {
					case <-ready:
					case <-ctx.Done():
						return
					}
					time.Sleep(80 * time.Millisecond)
					text := want + "\r"
					if i == 0 {
						text += tail
					}
					io.WriteString(w, text)
				}(i)
				got, err := q.input("File", "", "", "path", func(string) error { return nil })
				<-sent
				if i == 0 && errors.Is(err, errInterrupted) {
					continue
				}
				if err != nil || got != want {
					t.Fatalf("form %d: %q %v", i, got, err)
				}
			}
		})
	}
}

type quitField struct{ huh.Field }

func (f quitField) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if _, ok := msg.(tea.KeyMsg); ok {
		return f, tea.Quit
	}
	model, cmd := f.Field.Update(msg)
	if field, ok := model.(huh.Field); ok {
		f.Field = field
	}
	return f, cmd
}
func TestHostIncompleteFormIsInterrupted(t *testing.T) {
	scriptForms(t, keys("x"))
	q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
	if err := q.run(quitField{huh.NewInput().Title("File")}); !errors.Is(err, errInterrupted) {
		t.Fatal(err)
	}
}
func TestHostClipboardIsDisabled(t *testing.T) {
	if os.Getenv("HOST_CLIPBOARD_CHILD") == "1" {
		scriptForms(t, []string{"\x16", "\r"}, []string{"\x16", "\r"})
		q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
		if got, err := q.input("File", "", "", "path", func(string) error { return nil }); got != "" || err != nil {
			t.Fatal(got, err)
		}
		prev := "a"
		if _, err := q.pick("Choice", "", []huh.Option[string]{option("a", "a")}, &prev); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(os.Getenv("HOST_CLIPBOARD_MARKER")); !os.IsNotExist(err) {
			t.Fatal("clipboard invoked", err)
		}
		return
	}
	dir := t.TempDir()
	for _, name := range []string{"xclip", "xsel", "pbpaste", "wl-copy", "wl-paste", "powershell.exe", "clip.exe", "termux-clipboard-get", "termux-clipboard-set"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nprintf invoked > \"$HOST_CLIPBOARD_MARKER\"\nprintf fake\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	goPath, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestHostClipboardIsDisabled$", "-test.timeout=10s")
	command.Env = append(os.Environ(), "HOST_CLIPBOARD_CHILD=1", "HOST_CLIPBOARD_MARKER="+filepath.Join(dir, "invoked"), "PATH="+dir+":"+filepath.Dir(goPath)+":/usr/bin:/bin", "WAYLAND_DISPLAY=")
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestHostReadablePathRejectsNonRegularFiles(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{fifo, dir} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			scriptForms(t, []string{path + "\r", "\x03"})
			var out bytes.Buffer
			q := &questions{context.Background(), nil, &promptLatch{out: &out}}
			started := time.Now()
			_, err := q.input("known_hosts file", "", "", "path", func(s string) error {
				_, err := readablePath(s)
				return err
			})
			if !errors.Is(err, errInterrupted) || time.Since(started) >= time.Second {
				t.Fatal(err, time.Since(started))
			}
			text := strings.Join(strings.Fields(ansi.Strip(out.String())), " ")
			if !strings.Contains(text, "is not a regular file") {
				t.Fatal(out.String())
			}
		})
	}
}

func TestHostCADoesNotTrustPreOpenMetadata(t *testing.T) {
	_, ca := tlsCluster(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "ca")
	os.WriteFile(path, []byte(ca), 0600)
	old := openFile
	t.Cleanup(func() { openFile = old })
	called := false
	openFile = func(p string, flags int, mode os.FileMode) (*os.File, error) {
		called = true
		if flags&unix.O_NONBLOCK == 0 {
			t.Error("CA open can block")
		}
		os.Remove(p)
		if err := unix.Mkfifo(p, 0600); err != nil {
			return nil, err
		}
		return old(p, flags, mode)
	}
	done := make(chan error, 1)
	go func() { done <- readCA(path) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("accepted replacement")
		}
	case <-time.After(time.Second):
		t.Fatal("replacement blocked")
	}
	if !called {
		t.Fatal("CA bypassed safe open")
	}
}
func TestHostCompletionDoesNotReadNonDirectories(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	os.WriteFile(path, nil, 0600)
	old := readDirectory
	t.Cleanup(func() { readDirectory = old })
	readDirectory = func(path string) ([]os.DirEntry, error) {
		t.Error("ReadDir called before directory check")
		return old(path)
	}
	if got := completions(path+"/", "path"); len(got) != 0 {
		t.Fatal(got)
	}
	t.Setenv("PATH", path)
	if got := completions("exe", "command"); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestHostBlankNavigationKeepsParent(t *testing.T) {
	scriptForms(t, keys("\r"), keys("\r"), keys("\r"), keys(down+"\r"), keys(down+"\r"), keys("\r"), keys(down+"\r"), keys("\r"), keys(down+"\r"))
	q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
	if s, err := q.source("key", "key", nil, nil, false); err != nil || s.ok {
		t.Fatal(s, err)
	}
}
func TestHostGitCancellationDoesNotCacheSource(t *testing.T) {
	terms := agentTerms()
	terms.VCS = []string{"git.test"}
	h := newInitHost(t, map[string]operator.Terms{"a": terms})
	known := filepath.Join(t.TempDir(), "known_hosts")
	os.WriteFile(known, nil, 0600)
	scriptForms(t, keys(down+down+"\r"), keys("\r"), keys(down+"\r"), keys("\r"), keys("FIRST\r"), keys("\r"), keys("\r"), keys("\r"), keys(down+"\r"), keys("\r"), keys("SECOND\r"), keys(known+"\r"))
	h.init(t, h.bundle)
	f := h.written(t)
	if !reflect.DeepEqual(f.Credentials, map[string]credential.Source{"git-git-test-ssh": {Env: "SECOND"}}) || len(f.Git) != 1 || f.Git[0].URL != "ssh://git@git.test/" {
		t.Fatal(f)
	}
}
func TestHostStorageCollisionAndConfiguredReuse(t *testing.T) {
	terms := agentTerms()
	terms.Storage = &operator.Storage{Endpoint: "https://s3.test", Bucket: "bucket"}
	h := newInitHost(t, map[string]operator.Terms{"a": terms, "b": terms, "c": terms})
	original := busclient.Config{Bundle: h.bundle, Credentials: map[string]credential.Source{"s3-access-key": {Env: "OLD"}, "shared": {Env: "SHARED"}}, Storage: map[string]busclient.StorageConfig{"c": {AccessKey: "shared", SecretKey: "shared"}}}
	if err := writeYAML(h.config, original); err != nil {
		t.Fatal(err)
	}
	scriptForms(t, keys("\r"), keys(down+"\r"), keys("\r"), keys("NEW\r"), keys("\r"), keys(down+"\r"))
	h.init(t)
	f := h.written(t)
	if f.Credentials["s3-access-key"].Env != "OLD" || f.Credentials["s3-access-key-2"].Env != "NEW" || f.Storage["a"].SecretKey != "shared" || f.Storage["c"] != original.Storage["c"] || f.Storage["a"] != f.Storage["b"] {
		t.Fatal(f)
	}
}
func TestHostDescriptionPoolPrecedenceAndMissing(t *testing.T) {
	first := envCredential("key", "FIRST")
	first.Description = "First description"
	second := envCredential("key", "SECOND")
	second.Description = "Second description"
	h := newInitHost(t, map[string]operator.Terms{"a": agentTerms(first), "b": agentTerms(second)})
	scriptForms(t, keys(down+down+down+"\r"))
	_, out := h.init(t, h.bundle)
	if !strings.Contains(out, "? First description  Skip") || !strings.Contains(out, "First description, credential key (pool a as FIRST, required; pool b as SECOND, required)") {
		t.Fatal(out)
	}
}

func TestHostSSHRequiresSchemeHostAndNewPrefix(t *testing.T) {
	for _, prefix := range []string{"https://git.test/", "ssh://other.test/", "ssh://git@git.test/existing"} {
		entries := []busclient.GitCredential{{URL: "ssh://git@git.test/existing"}}
		if sshPrefix(prefix, "git.test", entries) == nil {
			t.Fatal(prefix)
		}
	}
	if err := sshPrefix("ssh://git@git.test/", "git.test", nil); err != nil {
		t.Fatal(err)
	}
}
func TestHostReturnedErrorIsEscapedAndWrapped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host\x1b]52;c;x\a.yaml")
	var out bytes.Buffer
	err := hostInit(context.Background(), path, "", &out, &out)
	if err == nil || strings.Contains(err.Error(), "\x1b") || errors.Unwrap(err) == nil {
		t.Fatal(err)
	}
}
func TestHostCancellationKeepsPairReservations(t *testing.T) {
	terms := agentTerms()
	terms.VCS = []string{"a.b", "a-b"}
	h := newInitHost(t, map[string]operator.Terms{"a": terms})
	old := newGitNames
	t.Cleanup(func() { newGitNames = old })
	var names *gitNames
	newGitNames = func(entries []busclient.GitCredential) *gitNames { names = old(entries); return names }
	scriptForms(t, keys(down+"\r"), keys(down+down+down+down+"\r"), keys(down+down+"\r"), keys(down+down+down+"\r"))
	h.init(t, h.bundle)
	for _, host := range terms.VCS {
		for _, transport := range []string{"https", "ssh"} {
			pair := gitPair(host, transport)
			name := names.pairs[pair]
			if name == "" || names.owners[name] != pair {
				t.Fatalf("lost %s: %+v", pair, names)
			}
		}
	}
}

func TestHostBrokerAndBundleModes(t *testing.T) {
	h := newInitHost(t, nil)
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.Rename(filepath.Join(h.dir, "ca.pem"), caPath); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	scriptForms(t, keys("\r"))
	_, out := h.init(t, h.bundle)
	if !strings.Contains(out, "\nBroker\n") || strings.Contains(out, "Broker  1") || !strings.Contains(out, "Summary  1 of 1") || strings.Count(out, "Wrote "+h.config) != 1 {
		t.Fatal(out)
	}
	if f := h.written(t); f.CAFile != caPath {
		t.Fatal(f)
	}
	for path, want := range map[string]os.FileMode{h.dir: 0700, filepath.Join(h.dir, "bundle.yaml"): 0600, h.config: 0600, filepath.Join(h.dir, "host.key"): 0600} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != want {
			t.Fatal(path, info, err)
		}
	}
	a, err := operator.ReadBundle(h.bundle)
	if err != nil {
		t.Fatal(err)
	}
	b, err := operator.ReadBundle(filepath.Join(h.dir, "bundle.yaml"))
	if err != nil || a != b {
		t.Fatal("bundle changed", err)
	}
}
func TestHostCAOptionsAndRevalidation(t *testing.T) {
	_, ca := tlsCluster(t)
	dir := t.TempDir()
	t.Chdir(dir)
	for i := 0; i < 10; i++ {
		os.WriteFile(fmt.Sprintf("%02d.pem", i), []byte(ca), 0600)
	}
	os.WriteFile(".hidden", []byte(ca), 0600)
	os.WriteFile("invalid", []byte("not PEM"), 0600)
	options := caOptions()
	if len(options) != 10 {
		t.Fatal(options)
	}
	for i := 0; i < 8; i++ {
		if options[i].Key != fmt.Sprintf("%02d.pem", i) {
			t.Fatal(options)
		}
	}
	for i := 1; i < 10; i++ {
		os.Remove(fmt.Sprintf("%02d.pem", i))
	}
	scriptForms(t, keys("\r"), keys("\r"))
	script := runQuestion
	once := true
	runQuestion = func(q *questions, field huh.Field) error {
		err := script(q, field)
		if once {
			once = false
			os.Remove("00.pem")
		}
		return err
	}
	var out bytes.Buffer
	q := &questions{context.Background(), nil, &promptLatch{out: &out}}
	if p, err := q.ca(); err != nil || p != "" {
		t.Fatal(p, err)
	}
	if !strings.Contains(out.String(), "no such file") {
		t.Fatal(out.String())
	}
}
func TestHostCompletionCapDedupAndUnsafePrefixes(t *testing.T) {
	var candidates []string
	for i := 220; i >= 0; i-- {
		candidates = append(candidates, fmt.Sprintf("item-%03d", i), fmt.Sprintf("item-%03d", i))
	}
	candidates = append(candidates, "bad\x1b", "bad\xff")
	got := safeCandidates(candidates)
	if len(got) != 200 || got[0] != "item-000" || got[199] != "item-199" {
		t.Fatal(got)
	}
	dir := filepath.Join(t.TempDir(), "bad\x1b")
	os.Mkdir(dir, 0700)
	os.WriteFile(filepath.Join(dir, "file"), nil, 0600)
	if got := completions(dir+"/f", "path"); len(got) != 0 {
		t.Fatal(got)
	}
}
func TestHostCommandAndVariableTab(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "test-executable"), nil, 0700)
	t.Setenv("PATH", dir)
	t.Setenv("UNIQUE_COMPLETION_VARIABLE", "secret")
	scriptForms(t, []string{"test-ex", "\t", "\r"}, []string{"UNIQUE_COMPLETION_V", "\t", "\r"})
	q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
	for _, item := range []struct{ kind, want string }{{"command", "test-executable"}, {"variable", "UNIQUE_COMPLETION_VARIABLE"}} {
		if got, err := q.input("Input", "", "", item.kind, func(string) error { return nil }); err != nil || got != item.want {
			t.Fatal(got, err)
		}
	}
}
func TestHostGitExistingSourceReuse(t *testing.T) {
	terms := agentTerms()
	terms.VCS = []string{"git.test"}
	h := newInitHost(t, map[string]operator.Terms{"a": terms})
	original := busclient.Config{Bundle: h.bundle, Credentials: map[string]credential.Source{"git-git-test": {Env: "EXISTING"}}}
	writeYAML(h.config, original)
	scriptForms(t, keys(down+"\r"))
	_, out := h.init(t)
	f := h.written(t)
	if !reflect.DeepEqual(f.Credentials, original.Credentials) || len(f.Git) != 1 || f.Git[0].Credential != "git-git-test" {
		t.Fatal(f)
	}
	if !slices.Contains(collapsedLines(out), "? git.test  HTTPS https://git.test/") {
		t.Fatal(collapsedLines(out))
	}
}

func TestHostInterruptBeforeBootstrap(t *testing.T) {
	h := newInitHost(t, nil)
	os.Remove(filepath.Join(h.dir, "ca.pem"))
	scriptForms(t, keys("\x03"))
	var out bytes.Buffer
	if err := hostInit(h.ctx, h.config, h.bundle, &out, &out); !errors.Is(err, errInterrupted) {
		t.Fatal(err)
	}
	if _, err := os.Stat(h.config); !os.IsNotExist(err) {
		t.Fatal("bootstrap written before broker answer", err)
	}
}
func TestHostRerunKeepsEntries(t *testing.T) {
	h := newInitHost(t, map[string]operator.Terms{"a": agentTerms(envCredential("model", "UNSET_TARGET"))})
	t.Setenv("UNSET_TARGET", "")
	scriptForms(t, keys("\r"), keys("\r"), keys("MY_MODEL\r"))
	h.init(t, h.bundle)
	before := h.written(t)
	h.entitle(t, "b", agentTerms(envCredential("other", "UNSET_TARGET")))
	scriptForms(t, keys("\r"), keys("\r"), keys("MY_OTHER\r"))
	_, out := h.init(t)
	after := h.written(t)
	if !reflect.DeepEqual(after.Credentials["other"], credential.Source{Env: "MY_OTHER"}) {
		t.Fatal(after)
	}
	delete(after.Credentials, "other")
	if !reflect.DeepEqual(after, before) || strings.Contains(out, "? model") {
		t.Fatal(after, out)
	}
	scriptForms(t)
	_, out = h.init(t)
	if !strings.Contains(out, "Summary  1 of 1") || strings.Contains(out, "Not configured:") || strings.Contains(out, "Wrote ") {
		t.Fatal(out)
	}
}
func TestHostOffTerminalDescription(t *testing.T) {
	c := envCredential("key", "TARGET")
	c.Description = "Account token"
	h := newInitHost(t, map[string]operator.Terms{"a": agentTerms(c)})
	noTerminal(t)
	stdout, out := h.init(t, h.bundle)
	if strings.Contains(out, "\x1b") || !strings.Contains(out, "Account token, credential key (pool a as TARGET, required)") {
		t.Fatal(out)
	}
	h.registered(t, strings.TrimSpace(stdout))
	if strings.Count(stdout, "\n") != 1 {
		t.Fatal(stdout)
	}
}

func TestHostNoMatchTabKeepsInputOpen(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	ready := make(chan struct{})
	q := &questions{ctx, r, &promptLatch{out: &signalledOutput{Writer: io.Discard, ready: ready}}}
	done := make(chan error, 1)
	go func() { _, err := q.input("File", "", "", "path", func(string) error { return nil }); done <- err }()
	<-ready
	io.WriteString(w, "/no-such-wherry-directory/zzz")
	time.Sleep(80 * time.Millisecond)
	io.WriteString(w, "\t")
	select {
	case err := <-done:
		t.Fatal("Tab submitted unmatched input", err)
	case <-time.After(100 * time.Millisecond):
	}
	io.WriteString(w, "\r")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestHostNarrowDescriptionKeepsScopeWarning(t *testing.T) {
	scriptForms(t, keys(down+down+down+"\r"))
	script := runQuestion
	runQuestion = func(q *questions, field huh.Field) error { return script(q, field.WithWidth(32).WithHeight(5)) }
	var out bytes.Buffer
	q := &questions{context.Background(), nil, &promptLatch{out: &out}}
	if _, err := q.source("token", "token", nil, nil, false); err != nil {
		t.Fatal(err)
	}
	text := strings.Join(strings.Fields(strings.ReplaceAll(ansi.Strip(out.String()), "┃", "")), " ")
	for _, part := range []string{"An environment source sends what the variable holds", "every pool that names token, now or later.", "It is read where submits run, including the MCP server your harness starts."} {
		if !strings.Contains(text, part) {
			t.Fatalf("missing %q: %s", part, text)
		}
	}
}

func TestHostGrowingCAIsBounded(t *testing.T) {
	_, ca := tlsCluster(t)
	path := filepath.Join(t.TempDir(), "ca.pem")
	os.WriteFile(path, []byte(ca), 0600)
	old := readCAData
	t.Cleanup(func() { readCAData = old })
	readCAData = func(r io.Reader) ([]byte, error) {
		if err := os.WriteFile(path, append([]byte(ca), bytes.Repeat([]byte("x"), 2<<20)...), 0600); err != nil {
			return nil, err
		}
		data, err := old(r)
		if len(data) > (1<<20)+1 {
			t.Error("read exceeded bound")
		}
		return data, err
	}
	if readCA(path) == nil {
		t.Fatal("accepted a file grown after Fstat")
	}
}

func TestHostGitAnonymous(t *testing.T) {
	terms := agentTerms()
	terms.VCS = []string{"git.test"}
	h := newInitHost(t, map[string]operator.Terms{"a": terms})
	scriptForms(t, keys("\r"))
	_, out := h.init(t, h.bundle)
	if f := h.written(t); !reflect.DeepEqual(f.Git, []busclient.GitCredential{{URL: "https://git.test/"}}) || len(f.Credentials) != 0 {
		t.Fatal(f)
	}
	if !slices.Contains(collapsedLines(out), "? git.test  Anonymous over https") {
		t.Fatal(collapsedLines(out))
	}
}
func TestHostConfiguredNoneCannotYieldCredential(t *testing.T) {
	scriptForms(t, keys("\r"), keys("\r"), keys(down+down+down+down+"\r"))
	q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
	if s, err := q.source("key", "key", nil, nil, true); err != nil || s.ok {
		t.Fatal(s, err)
	}
}
func TestHostNestedInterrupts(t *testing.T) {
	for _, item := range []struct {
		nested bool
		steps  [][]string
	}{{false, [][]string{keys("\r"), keys("\x03")}}, {false, [][]string{keys(down + "\r"), keys("\x03")}}, {true, [][]string{keys("\r"), keys("\x03")}}} {
		t.Run(fmt.Sprint(item), func(t *testing.T) {
			scriptForms(t, item.steps...)
			q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
			if _, err := q.source("key", "key", nil, nil, item.nested); !errors.Is(err, errInterrupted) {
				t.Fatal(err)
			}
		})
	}
}
func TestHostContextCancellation(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	q := &questions{ctx, r, &promptLatch{out: io.Discard}}
	if _, err := q.input("File", "", "", "path", func(string) error { return nil }); err == nil {
		t.Fatal("cancelled context accepted")
	}
}
func TestHostNamesTrimTruncatedPunctuation(t *testing.T) {
	n := newGitNames(nil)
	host := strings.Repeat("a", 27) + ".example.test"
	if name := n.allocate(host, "https"); name != "git-"+strings.Repeat("a", 27) {
		t.Fatal(name)
	}
	if name := n.allocate(host, "ssh"); !strings.HasSuffix(name, "-ssh") || len(name) > 32 {
		t.Fatal(name)
	}
}

func TestHostCAHiddenPathAndHostileName(t *testing.T) {
	_, ca := tlsCluster(t)
	dir := t.TempDir()
	t.Chdir(dir)
	name := "evil\x1b]52;c;x\a.pem"
	os.WriteFile(name, []byte(ca), 0600)
	os.WriteFile(".hidden", []byte(ca), 0600)
	os.Mkdir("directory", 0700)
	if err := unix.Mkfifo("fifo", 0600); err != nil {
		t.Fatal(err)
	}
	options := caOptions()
	if len(options) != 3 || strings.Contains(options[0].Key, "\x1b") {
		t.Fatal(options)
	}
	scriptForms(t, keys("\r"), keys(down+down+"\r"), keys(filepath.Join(dir, ".hidden")+"\r"))
	var out bytes.Buffer
	q := &questions{context.Background(), nil, &promptLatch{out: &out}}
	if got, err := q.ca(); err != nil || got != filepath.Join(dir, name) {
		t.Fatal(got, err)
	}
	if got, err := q.ca(); err != nil || got != filepath.Join(dir, ".hidden") {
		t.Fatal(got, err)
	}
	if strings.Contains(out.String(), "\x1b]52") {
		t.Fatal("unsafe filename rendered")
	}
}

func TestHostAllOptionalLabel(t *testing.T) {
	if got := sourceLabel("key", "Account", []credentialUse{{optional: true}, {optional: true}}); got != "Account (optional)" {
		t.Fatal(got)
	}
}

func TestHostNestedListSkipEndsSourceRequest(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprint(nested), func(t *testing.T) {
			scriptForms(t, keys("\r"), keys(down+down+"\r"))
			q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
			if s, err := q.source("key", "key", nil, nil, nested); err != nil || s.ok {
				t.Fatal(s, err)
			}
		})
	}
}

func TestHostSSHDoesNotSuggestFilesystemPaths(t *testing.T) {
	t.Chdir(t.TempDir())
	os.WriteFile("unexpected-file", nil, 0600)
	if got := completions("", ""); len(got) != 0 {
		t.Fatal(got)
	}
	scriptForms(t, []string{"\t", "\r"})
	q := &questions{context.Background(), nil, &promptLatch{out: io.Discard}}
	if got, err := q.input("SSH URL prefix", "", "", "", func(string) error { return nil }); err != nil || got != "" {
		t.Fatal(got, err)
	}
}

// A special file is refused before it is opened, since opening some devices acts.
func TestHostSpecialFilesAreNotOpened(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	old := openFile
	t.Cleanup(func() { openFile = old })
	var opened []string
	openFile = func(p string, flags int, mode os.FileMode) (*os.File, error) {
		opened = append(opened, p)
		return old(p, flags, mode)
	}
	for _, p := range []string{dir, fifo, "/dev/null"} {
		if readCA(p) == nil {
			t.Fatalf("CA %s accepted", p)
		}
		if _, err := readablePath(p); err == nil {
			t.Fatalf("known_hosts %s accepted", p)
		}
	}
	if len(opened) != 0 {
		t.Fatalf("opened %q", opened)
	}
}
