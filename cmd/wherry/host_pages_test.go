package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"golang.org/x/term"
)

const down = "\x1b[B"

func requestsFor(input string, file *busclient.Config, out io.Writer) *sourceRequests {
	return &sourceRequests{p: testPicker(input, out), file: file, asked: map[string]sourceResult{}}
}
func TestHostLabelsAndMissing(t *testing.T) {
	a := envCredential("token", "TARGET")
	a.Description = "Account token"
	b := fileCredential("token", "key")
	b.Description = "Other label"
	uses, descriptions, _, _ := hostNeeds(map[string]operator.Terms{"pool-b": agentTerms(b), "pool-a": agentTerms(a, envCredential("plain", "PLAIN"))}, busclient.Config{})
	if descriptions["token"] != "Account token" || descriptions["plain"] != "" || len(uses["token"]) != 2 {
		t.Fatal(descriptions, uses)
	}
	lines := missingHost(busclient.Config{}, uses, descriptions, nil, nil)
	want := []string{"credential plain (pool pool-a as PLAIN, required)", "Account token, credential token (pool pool-a as TARGET, required; pool pool-b as file key, optional)"}
	if !reflect.DeepEqual(lines, want) {
		t.Fatal(lines)
	}
	h := newInitHost(t, map[string]operator.Terms{"pool-a": agentTerms(a, envCredential("plain", "PLAIN")), "pool-b": agentTerms(b)})
	hostOnTerminal(t, "\x1b\x1b")
	_, output := h.init(t, h.bundle)
	if !strings.Contains(output, "? Account token  Skipped") || !strings.Contains(output, "? plain  Skipped") || strings.Contains(output, "Account token (optional)") {
		t.Fatal(output)
	}
}
func TestHostEnvironmentCandidates(t *testing.T) {
	t.Setenv("SET_A", "private-a")
	t.Setenv("SET_B", "private-b")
	t.Setenv("EMPTY", "")
	t.Setenv("UNSET", "")
	if err := os.Unsetenv("UNSET"); err != nil {
		t.Fatal(err)
	}
	file := busclient.Config{}
	var out bytes.Buffer
	uses := []credentialUse{{"a", wire.TaskCredential{Env: "SET_A"}, false}, {"b", wire.TaskCredential{Env: "EMPTY"}, false}, {"b", wire.TaskCredential{Env: "UNSET"}, false}, {"b", wire.TaskCredential{Env: "SET_B"}, false}, {"c", wire.TaskCredential{Env: "SET_A"}, false}}
	uses[3], uses[4] = uses[4], uses[3]
	r := requestsFor("\n"+down+"\n", &file, &out)
	result, err := r.request("token", "Token", uses, false)
	if err != nil || result.source.Env != "SET_B" {
		t.Fatal(result, err)
	}
	if strings.Contains(out.String(), "EMPTY") || strings.Contains(out.String(), "UNSET") || strings.Contains(out.String(), "private-") {
		t.Fatal(out.String())
	}

}
func TestHostSourceNavigation(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		nested      bool
	}{
		{"skip", "\x1b", false},
		{"environment back", "\n\x1b\x1b", false},
		{"blank variable", "\n\n\n\x1b\x1b", false},
		{"blank file", down + "\n\n\x1b", false},
		{"blank command", down + down + "\n\n\x1b", false},
		{"empty configured", "\n\n\x1b\x1b", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := busclient.Config{}
			var out bytes.Buffer
			r := requestsFor(tc.input, &file, &out)
			result, err := r.request("token", "Token", nil, tc.nested)
			if err != nil || result.ok || result.cancelled != tc.nested || len(file.Credentials) > 0 {
				t.Fatal(result, err, out.String())
			}
			if tc.name == "empty configured" && !strings.Contains(out.String(), "(none)") {
				t.Fatal(out.String())
			}
			if tc.name == "blank command" && !strings.Contains(out.String(), "> Command\r\n") {
				t.Fatal("selection lost")
			}
		})
	}
}
func TestHostCentralAskedOnce(t *testing.T) {
	file := busclient.Config{}
	var out bytes.Buffer
	r := requestsFor("\x1b", &file, &out)
	for range 2 {
		result, err := r.request("same", "Same", nil, false)
		if result.ok || err != nil {
			t.Fatal(result, err)
		}
	}
	if strings.Count(out.String(), "? Same  Skipped") != 1 {
		t.Fatal(out.String())
	}
}
func TestHostStorageAllPoolsAndCollision(t *testing.T) {
	file := busclient.Config{Credentials: map[string]credential.Source{"s3-access-key": {Env: "OLD"}}, Storage: map[string]busclient.StorageConfig{"configured": {AccessKey: "old-access", SecretKey: "old-secret"}}}
	r := requestsFor("\n"+down+"\n\nACCESS\n"+down+"\n\nSECRET\n", &file, io.Discard)
	ok, err := askStorage(r, []string{"a", "b", "configured"})
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	if file.Storage["a"] != (busclient.StorageConfig{AccessKey: "s3-access-key-2", SecretKey: "s3-secret-key"}) || file.Storage["b"] != file.Storage["a"] || file.Storage["configured"].AccessKey != "old-access" || file.Credentials["s3-access-key"].Env != "OLD" {
		t.Fatal(file)
	}
}
func TestHostStorageReuseAndCancel(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		file := busclient.Config{Credentials: map[string]credential.Source{"existing": {Env: "EXISTING"}}}
		input := "\n\n\n\n\n"
		if cancel {
			input = "\n" + down + "\n\nNEW\n\x1b\x1b"
		}
		r := requestsFor(input, &file, io.Discard)
		ok, err := askStorage(r, []string{"a", "b"})
		if err != nil || ok == cancel {
			t.Fatal(ok, err)
		}
		if cancel {
			if len(file.Credentials) != 1 || len(file.Storage) != 0 {
				t.Fatal("partial pair committed", file)
			}
		} else if file.Storage["a"].AccessKey != "existing" || file.Storage["b"].SecretKey != "existing" {
			t.Fatal(file)
		}
	}
}
func TestHostGitRoutes(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.WriteFile("known_hosts", []byte("host key"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"anonymous", "https", "ssh", "cancel"} {
		t.Run(route, func(t *testing.T) {
			file := busclient.Config{}
			input := "\n"
			if route == "https" {
				input = down + "\n" + down + "\n\nTOKEN\n"
			}
			if route == "ssh" {
				input = down + down + "\nssh://git@wrong.test/\n\n" + down + "\n\nSSH_KEY\nmissing\nknown_hosts\n"
			}
			if route == "cancel" {
				input = down + down + "\n\n" + down + "\n\nSSH_KEY\n\n\x1b"
			}
			var out bytes.Buffer
			r := requestsFor(input, &file, &out)
			entry, result, err := askGitPage(r, "git.example.test", newGitNames([]string{"git.example.test"}, file))
			if err != nil {
				t.Fatal(err)
			}
			if len(file.Credentials) != 0 {
				t.Fatal("source committed early")
			}
			if route == "cancel" {
				if entry.URL != "" {
					t.Fatal(entry)
				}
				return
			}
			if entry.URL == "" || route != "anonymous" && !result.ok {
				t.Fatal(entry, result)
			}
			if route == "ssh" {
				want, _ := filepath.Abs("known_hosts")
				if entry.HostKey != want || entry.URL != "ssh://git@git.example.test/" || !strings.Contains(out.String(), "? SSH URL prefix  ssh://git@git.example.test/") {
					t.Fatal(entry)
				}
			}
		})
	}
	if validGitPrefix("https://wrong.test/", "git.example.test", nil) || validGitPrefix("https://git.example.test/", "git.example.test", []busclient.GitCredential{{URL: "https://git.example.test/"}}) {
		t.Fatal("invalid prefix accepted")
	}
}
func TestHostGitNames(t *testing.T) {
	hosts := []string{"github.com", "host:8443", "a.b", "a-b", strings.Repeat("long", 20) + "a.test", strings.Repeat("long", 20) + "b.test", strings.Repeat("a", 27) + ".test", "a", "a-ssh"}
	slices.Sort(hosts)
	n := newGitNames(hosts, busclient.Config{})
	seen := map[string]string{}
	for _, host := range hosts {
		for _, ssh := range []bool{false, true} {
			name := n.allocate(host, ssh)
			pair := gitPair(host, ssh)
			if !wire.ValidName(name) || len(name) > 32 || ssh && !strings.Contains(name, "-ssh") {
				t.Fatal(name)
			}
			if owner, ok := seen[name]; ok && owner != pair {
				t.Fatalf("reused %s for %s and %s", name, owner, pair)
			}
			seen[name] = pair
			if n.allocate(host, ssh) != name {
				t.Fatal("repeat allocation changed")
			}
		}
	}
	if n.allocate("github.com", false) != "git-github-com" || n.allocate("host:8443", false) != "git-host-8443" {
		t.Fatal(n.pairs)
	}
	file := busclient.Config{Credentials: map[string]credential.Source{"git-github-com": {Env: "TOKEN"}}, Git: []busclient.GitCredential{{URL: "https://different.test/", Credential: "git-github-com"}}}
	if newGitNames([]string{"github.com"}, file).allocate("github.com", false) != "git-github-com-2" {
		t.Fatal("associated name reused")
	}
	file.Git = nil
	if newGitNames([]string{"github.com"}, file).allocate("github.com", false) != "git-github-com" {
		t.Fatal("unassociated name not reused")
	}
	r := requestsFor("", &file, io.Discard)
	result, err := r.request("git-github-com", "Git", nil, true)
	if err != nil || !result.ok || result.source.Env != "TOKEN" {
		t.Fatal(result, err)
	}
	n = newGitNames(nil, busclient.Config{})
	for i := 1; i <= 10; i++ {
		host := strings.Repeat("a", 50) + fmt.Sprint(i)
		name := n.allocate(host, true)
		if i == 10 && !strings.HasSuffix(name, "-ssh-10") {
			t.Fatal(name)
		}
		if len(name) > 32 {
			t.Fatal(name)
		}
	}
}
func TestHostCACapAndRevalidation(t *testing.T) {
	_, ca := tlsCluster(t)
	t.Chdir(t.TempDir())
	for i := range 10 {
		if err := os.WriteFile(fmt.Sprintf("%02d.pem", i), []byte(ca), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile("00-not-pem", []byte("no"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := caOptions()
	if len(options) != 10 || !strings.HasSuffix(options[0], "00.pem") || !strings.HasSuffix(options[7], "07.pem") {
		t.Fatal(options)
	}
	var out bytes.Buffer
	p := testPicker("\n"+strings.Repeat(down, 8)+"\n", &out)
	original := p.makeRaw
	p.makeRaw = func() (*term.State, error) {
		if err := os.Remove("00.pem"); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		return original()
	}
	path, err := askCA(p)
	if err != nil || path != "" || !strings.Contains(out.String(), "holds no PEM certificate") {
		t.Fatal(path, err, out.String())
	}
}
func TestHostPagesSummary(t *testing.T) {
	h := newInitHost(t, nil)
	hostOnTerminal(t, "")
	stdout, out := h.init(t, h.bundle)
	if !strings.Contains(out, "Summary  1 of 1") || !strings.Contains(out, "Host key "+strings.TrimSpace(stdout)+" registered") || strings.Contains(out, "Not configured:") {
		t.Fatal(out)
	}
	if strings.Index(out, "Wrote ") < strings.Index(out, "Summary") {
		t.Fatal("write notice before summary")
	}
}
func TestHostPageSnapshot(t *testing.T) {
	h := newInitHost(t, map[string]operator.Terms{"pool-a": agentTerms(envCredential("token", "TARGET"))})
	hostOnTerminal(t, "\x1b")
	_, out := h.init(t, h.bundle)
	if !strings.Contains(out, "Credentials  1 of 2") || !strings.Contains(out, "Summary  2 of 2") || strings.Contains(out, "Git  ") {
		t.Fatal(out)
	}
}
func TestHostWriteErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "directory")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeHostConfig(path, busclient.Config{}, io.Discard); err == nil {
		t.Fatal("write error lost")
	}
	h := newInitHost(t, nil)
	hostOnTerminal(t, "")
	if err := hostInit(h.ctx, h.config, h.bundle, io.Discard, pickerBadWriter{}); err == nil {
		t.Fatal("prompt error lost")
	}
}

func TestHostDisplayReferencesUnchanged(t *testing.T) {
	hostile := "key\x1b\n\u0085\u2029"
	file := busclient.Config{Credentials: map[string]credential.Source{hostile: {File: "/tmp/" + hostile}}}
	var out bytes.Buffer
	r := requestsFor("\n\n", &file, &out)
	result, err := r.request("new", "Source", nil, true)
	if err != nil || result.name != hostile || result.source.File != "/tmp/"+hostile {
		t.Fatal(result, err)
	}
	if strings.Contains(out.String(), hostile) || !strings.Contains(out.String(), display(hostile)) {
		t.Fatal(out.String())
	}
	_, ca := tlsCluster(t)
	t.Chdir(t.TempDir())
	if err := os.WriteFile(hostile, []byte(ca), 0o600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	path, err := askCA(testPicker("\n", &out))
	want, _ := filepath.Abs(hostile)
	if err != nil || path != want || strings.Contains(out.String(), hostile) || !strings.Contains(out.String(), display(hostile)) {
		t.Fatal(path, err, out.String())
	}
}

func TestHostOffTerminalDescription(t *testing.T) {
	c := envCredential("token", "TOKEN")
	c.Description = "Account token"
	h := newInitHost(t, map[string]operator.Terms{"pool-a": agentTerms(c, envCredential("plain", "PLAIN"))})
	noTerminal(t)
	stdout, out := h.init(t, h.bundle)
	for _, line := range []string{"  Account token, credential token (pool pool-a as TOKEN, required)\n", "  credential plain (pool pool-a as PLAIN, required)\n"} {
		if !strings.Contains(out, line) {
			t.Fatal(out)
		}
	}
	if strings.Contains(out, "\x1b") || strings.Count(stdout, "\n") != 1 || strings.Contains(stdout, " ") {
		t.Fatal(stdout, out)
	}
}

func TestHostSourceHelpAndParentSelection(t *testing.T) {
	file := busclient.Config{}
	var out bytes.Buffer
	r := requestsFor(down+"??\n\n\n/tmp/key\n", &file, &out)
	uses := []credentialUse{{"pool-a", wire.TaskCredential{Env: "TOKEN"}, false}, {"pool-b", wire.TaskCredential{File: "token"}, true}}
	result, err := r.request("token", "Account token", uses, false)
	if err != nil || result.source.File != "/tmp/key" {
		t.Fatal(result, err)
	}
	for _, line := range []string{"credential token", "pool pool-a: TOKEN, required", "pool pool-b: file token, optional", "An environment source sends what the variable holds to every pool", "that names token, now or later.", "It is read where submits run, including the MCP server your harness starts.", "? Account token  File /tmp/key"} {
		if !strings.Contains(out.String(), line) {
			t.Fatalf("missing %q", line)
		}
	}
}
func TestHostGitSharedOwnershipAndReverseAllocation(t *testing.T) {
	file := busclient.Config{Credentials: map[string]credential.Source{"git-a": {Env: "OLD"}}, Git: []busclient.GitCredential{{URL: "https://b/", Credential: "git-a"}, {URL: "https://a/", Credential: "git-a"}}}
	n := newGitNames([]string{"a"}, file)
	if n.allocate("a", false) != "git-a-2" {
		t.Fatal("shared source implicitly reused")
	}
	hosts := []string{"a", "a-ssh", "a.b", "a-b"}
	n = newGitNames(hosts, busclient.Config{})
	before := map[string]string{}
	for _, host := range hosts {
		for _, ssh := range []bool{false, true} {
			before[gitPair(host, ssh)] = n.allocate(host, ssh)
		}
	}
	slices.Reverse(hosts)
	for _, host := range hosts {
		for _, ssh := range []bool{true, false} {
			if n.allocate(host, ssh) != before[gitPair(host, ssh)] {
				t.Fatal("reverse navigation renamed pair")
			}
		}
	}
}

func TestHostResolvedCredentialsPage(t *testing.T) {
	uses := map[string][]credentialUse{"token": {{pool: "a", target: wire.TaskCredential{Env: "TOKEN"}}}}
	file := busclient.Config{}
	// The topic snapshot precedes answers that configure a source.
	file.Credentials = map[string]credential.Source{"token": {Env: "TOKEN"}}
	var out bytes.Buffer
	r := requestsFor("", &file, &out)
	changed, err := askCredentials(r, uses, nil)
	if err != nil || changed || out.String() != "Already configured.\r\n" {
		t.Fatal(changed, err, out.String())
	}
}

type hostSummaryWriter struct {
	io.Writer
	beforeWrite func()
}

func (w *hostSummaryWriter) Write(value []byte) (int, error) {
	if strings.Contains(string(value), "Summary  ") && w.beforeWrite != nil {
		f := w.beforeWrite
		w.beforeWrite = nil
		f()
	}
	return w.Writer.Write(value)
}
func TestHostFinalWriteError(t *testing.T) {
	h := newInitHost(t, map[string]operator.Terms{"pool-a": agentTerms(envCredential("token", "TOKEN"))})
	hostOnTerminal(t, "\n\nREFERENCE\n")
	writer := &hostSummaryWriter{Writer: io.Discard, beforeWrite: func() {
		if err := os.Remove(h.config); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(h.config, 0o700); err != nil {
			t.Fatal(err)
		}
	}}
	if err := hostInit(h.ctx, h.config, h.bundle, io.Discard, writer); err == nil {
		t.Fatal("final config write error lost")
	}
}
func TestHostStorageSameSource(t *testing.T) {
	file := busclient.Config{Credentials: map[string]credential.Source{"s3-access-key": {Env: "SAME", Command: []string{}}}}
	r := requestsFor("\n"+down+"\n\nSAME\n\n\n", &file, io.Discard)
	ok, err := askStorage(r, []string{"a"})
	if err != nil || !ok || file.Storage["a"].AccessKey != "s3-access-key" || file.Storage["a"].SecretKey != "s3-access-key" || len(file.Credentials) != 1 {
		t.Fatal(file, err)
	}
}
func TestHostGitAndStorageHelp(t *testing.T) {
	file := busclient.Config{}
	var out bytes.Buffer
	r := requestsFor("?\x1b", &file, &out)
	if _, _, err := askGitPage(r, "host.test", newGitNames([]string{"host.test"}, file)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Configured credential allows deliberate sharing.") || !strings.Contains(out.String(), "presumed to belong") {
		t.Fatal(out.String())
	}
	out.Reset()
	r = requestsFor("?\x1b", &file, &out)
	if _, err := askStorage(r, []string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "S3 storage for pools a, b and c") || !strings.Contains(out.String(), "Set per-pool keys by editing the config.") {
		t.Fatal(out.String())
	}
}

func TestHostInterruptReturns(t *testing.T) {
	h := newInitHost(t, map[string]operator.Terms{"pool-a": agentTerms(envCredential("token", "TOKEN"))})
	hostOnTerminal(t, "\x03")
	if err := hostInit(h.ctx, h.config, h.bundle, io.Discard, io.Discard); !errors.Is(err, errPickInterrupt) {
		t.Fatal(err)
	}
	if len(h.written(t).Credentials) != 0 {
		t.Fatal("interrupt wrote a source")
	}
}
