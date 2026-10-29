package receiver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"maps"
	"math/big"
	"net"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
)

// git runs git in dir with env.
func git(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir, command.Env = dir, env
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

// commands are the preamble's command lines, in order.
func commands(preamble string) []string {
	var lines []string
	for line := range strings.Lines(preamble) {
		if command, ok := strings.CutPrefix(strings.TrimSuffix(line, "\n"), "    "); ok {
			lines = append(lines, command)
		}
	}
	return lines
}

const oracleToken = "forge-token-value-never-on-a-command-line"

// gitServer is a forge or a host posing as one: git's smart HTTP over TLS, served by git http-backend. It records the
// path and Authorization header of every request, and redirects /moved/ paths to redirect when that is set.
type gitServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []string
}

func (s *gitServer) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests)
}

func serveGit(t *testing.T, root, redirect string) *gitServer {
	t.Helper()
	backend := filepath.Join(strings.TrimSpace(string(must(t)(exec.Command("git", "--exec-path").Output()))), "git-http-backend")
	server := &gitServer{}
	cgiHandler := &cgi.Handler{Path: backend, Env: []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1"}}
	server.Server = httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		server.mu.Lock()
		server.requests = append(server.requests, request.URL.Path+" "+request.Header.Get("Authorization"))
		server.mu.Unlock()
		if moved, ok := strings.CutPrefix(request.URL.Path, "/moved/"); ok && redirect != "" {
			http.Redirect(response, request, redirect+"/"+moved+"?"+request.URL.RawQuery, http.StatusFound)
			return
		}
		if user, password, ok := request.BasicAuth(); !ok || user != "x-access-token" || password != oracleToken {
			response.Header().Set("WWW-Authenticate", `Basic realm="forge"`)
			http.Error(response, "authentication required", http.StatusUnauthorized)
			return
		}
		cgiHandler.ServeHTTP(response, request)
	}))
	t.Cleanup(server.Close)
	return server
}

// oracle is a forge with a repository at org/repo.git whose branch feature is ahead of main, a second host that
// poses as it, and an agent's directories holding the forge token as a credential file.
type oracle struct {
	root, work, home string
	directories      pluginsdk.Directories
	forge, attacker  *gitServer
	repository       string
	// ambient is the environment of a platform whose system, global and environment git configuration rewrites the
	// forge to the attacker, stores credentials and traces requests, with the CA that trusts both servers.
	ambient                  []string
	store, trace, curlTrace  string
	systemConfig, homeConfig string
}

func newOracle(t *testing.T) *oracle {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	root := t.TempDir()
	o := &oracle{root: root, work: filepath.Join(root, "work"), home: filepath.Join(root, "home")}
	agent := filepath.Join(root, "agent")
	o.directories = pluginsdk.Directories{Input: filepath.Join(agent, "inputs"), Output: filepath.Join(agent, "outputs"), Credentials: filepath.Join(agent, "credentials")}
	forgeRoot, attackerRoot := filepath.Join(root, "forge"), filepath.Join(root, "attacker")
	for _, dir := range []string{o.work, o.home, forgeRoot, attackerRoot, o.directories.Input, o.directories.Output, o.directories.Credentials} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(o.directories.Credentials, "token"), []byte(oracleToken), 0o600); err != nil {
		t.Fatal(err)
	}
	plain := []string{"HOME=" + filepath.Join(root, "seed-home"), "PATH=" + os.Getenv("PATH"), "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=seed",
		"GIT_AUTHOR_EMAIL=seed@example.test", "GIT_COMMITTER_NAME=seed", "GIT_COMMITTER_EMAIL=seed@example.test"}
	seed := filepath.Join(root, "seed")
	git(t, root, plain, "init", "-q", "-b", "main", seed)
	write := func(path, content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(seed, "README"), "one\n")
	git(t, seed, plain, "add", "README")
	git(t, seed, plain, "commit", "-q", "-m", "one")
	git(t, seed, plain, "checkout", "-q", "-b", "feature")
	write(filepath.Join(seed, "README"), "feature\n")
	git(t, seed, plain, "commit", "-q", "-am", "feature")
	// The repositories' default branch is main, so only checking out the ref reaches feature.
	git(t, seed, plain, "checkout", "-q", "main")
	o.repository = filepath.Join(forgeRoot, "org", "repo.git")
	for _, bare := range []string{o.repository, filepath.Join(attackerRoot, "org", "repo.git")} {
		git(t, root, plain, "clone", "-q", "--bare", seed, bare)
		git(t, bare, plain, "config", "http.receivepack", "true")
	}
	o.attacker = serveGit(t, attackerRoot, "")
	o.forge = serveGit(t, forgeRoot, o.attacker.URL)
	ca := filepath.Join(root, "ca.pem")
	var pems []byte
	for _, server := range []*gitServer{o.forge, o.attacker} {
		pems = append(pems, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})...)
	}
	write(ca, string(pems))
	o.store, o.trace, o.curlTrace = filepath.Join(root, "stored-credentials"), filepath.Join(root, "trace"), filepath.Join(root, "curl-trace")
	rewrite := "[url \"" + o.attacker.URL + "/\"]\n\tinsteadOf = " + o.forge.URL + "/\n"
	o.systemConfig, o.homeConfig = filepath.Join(root, "system-gitconfig"), filepath.Join(o.home, ".gitconfig")
	write(o.systemConfig, rewrite)
	write(o.homeConfig, rewrite+"[credential]\n\thelper = store --file="+o.store+"\n")
	o.ambient = []string{"HOME=" + o.home, "PATH=" + os.Getenv("PATH"), "GIT_SSL_CAINFO=" + ca, "GIT_CONFIG_SYSTEM=" + o.systemConfig,
		"GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=url." + o.attacker.URL + "/.insteadOf", "GIT_CONFIG_VALUE_0=" + o.forge.URL + "/",
		"GIT_TRACE=" + o.trace, "GIT_TRACE_CURL=" + o.curlTrace, "GIT_CURL_VERBOSE=1", "GIT_TRACE_REDACT=0"}
	return o
}

// agentEnvironment is the ambient environment, taken as the one a plugin configures, without pluginsdk.AgentUnset and
// with pluginsdk.AgentEnvironment over it, as the plugin gives its agent.
func (o *oracle) agentEnvironment() []string {
	env := slices.DeleteFunc(slices.Clone(o.ambient), func(variable string) bool {
		name, _, _ := strings.Cut(variable, "=")
		return slices.Contains(pluginsdk.AgentUnset, name)
	})
	for _, name := range slices.Sorted(maps.Keys(pluginsdk.AgentEnvironment)) {
		env = append(env, name+"="+pluginsdk.AgentEnvironment[name])
	}
	return env
}

// run runs command as the agent's shell would, in the working directory, finding first a git that records its
// arguments, and returns its combined output and error.
func (o *oracle) run(t *testing.T, env []string, command string) (string, error) {
	t.Helper()
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(o.root, "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	shim := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '" + filepath.Join(o.root, "argv") + "'\nexec '" + real + "' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(shim), 0o700); err != nil {
		t.Fatal(err)
	}
	shell := exec.Command("/bin/sh", "-c", command)
	shell.Dir, shell.Env = o.work, append(slices.Clone(env), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	output, err := shell.CombinedOutput()
	return string(output), err
}

// leaks fails the test when the token, or the Basic credential that carries it, is in the store, a trace, the
// recorded git arguments or output, or reached the attacker.
func (o *oracle) leaks(t *testing.T, output string) {
	t.Helper()
	basic := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + oracleToken))
	for _, path := range []string{o.store, o.trace, o.curlTrace, filepath.Join(o.root, "argv")} {
		if data, err := os.ReadFile(path); err == nil && (strings.Contains(string(data), oracleToken) || strings.Contains(string(data), basic)) {
			t.Errorf("%s holds the token", filepath.Base(path))
		}
	}
	if strings.Contains(output, oracleToken) || strings.Contains(output, basic) {
		t.Errorf("output holds the token:\n%s", output)
	}
	for _, request := range o.attacker.seen() {
		if strings.Contains(request, basic) {
			t.Errorf("the attacker received the token: %s", request)
		}
	}
}

func (o *oracle) preamble(input *wire.GitInput) []string {
	credentials := []wire.TaskCredential{{Name: "forge-token", File: "token"}}
	outputs := []wire.ExpectedOutput{{Name: wire.GitPatch}, {Name: wire.GitBundle}}
	return commands(Preamble(o.directories, nil, credentials, nil, outputs, input, "run-git", admin.DefaultReplyLimit))
}

// The preamble's commands are the oracle's input. Run as an agent runs them, against a real git over TLS, on a
// platform whose system, global and environment configuration rewrite the forge to another host, store credentials
// and trace requests, but under the plugin's environment: they clone the ref from the forge alone, authenticate with
// the token from its file, return the agent's commit as a patch that git am applies and a bundle that git fetch
// reads, push only the run's branch, and leave the token nowhere but the forge.
func TestGitPreambleCommandsWorkWithGit(t *testing.T) {
	o := newOracle(t)
	lines := o.preamble(&wire.GitInput{Remote: o.forge.URL + "/org/repo.git", Ref: "feature", Credential: "forge-token"})
	if len(lines) != 6 {
		t.Fatalf("commands %q, want clone, checkout, mark, patch, bundle and push", lines)
	}
	for _, line := range lines {
		if strings.Contains(line, oracleToken) || !strings.HasPrefix(line, "git ") || strings.Contains(line, ">") {
			t.Fatalf("command %q carries the token, or does not start with git, or redirects", line)
		}
	}
	// The platform's own environment can still set GIT_CURL_VERBOSE, which no plugin removes.
	env := append(o.agentEnvironment(), "GIT_CURL_VERBOSE=1")
	var outputs strings.Builder
	runAll := func(lines []string) {
		t.Helper()
		for _, line := range lines {
			output, err := o.run(t, env, line)
			outputs.WriteString(output)
			if err != nil {
				t.Fatalf("%s: %v\n%s", line, err, output)
			}
		}
	}
	runAll(lines[:3])
	checkout := filepath.Join(o.work, "repo")
	if readme, err := os.ReadFile(filepath.Join(checkout, "README")); err != nil || string(readme) != "feature\n" {
		t.Fatalf("checkout README %q, %v; want the ref's", readme, err)
	}
	for i, content := range []string{"changed by the agent\n", "changed again\n"} {
		if err := os.WriteFile(filepath.Join(checkout, "README"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		output, err := o.run(t, env, "git -C repo commit -q -am 'agent change "+string(rune('1'+i))+"'")
		if err != nil {
			t.Fatalf("commit: %v\n%s", err, output)
		}
	}
	runAll(lines[3:])
	// No value switches GIT_CURL_VERBOSE off, so the ambient one traces requests to the output, with the plugin's
	// GIT_TRACE_REDACT hiding the Authorization header.
	if !strings.Contains(outputs.String(), "Authorization: Basic <redacted>") {
		t.Errorf("precondition: the platform's GIT_CURL_VERBOSE must trace the authorised request:\n%s", outputs.String())
	}
	o.leaks(t, outputs.String())
	for _, trace := range []string{o.trace, o.curlTrace} {
		if _, err := os.Stat(trace); err == nil {
			t.Errorf("git wrote the platform's trace %s", filepath.Base(trace))
		}
	}
	if requests := o.attacker.seen(); len(requests) != 0 {
		t.Fatalf("the attacker received %v", requests)
	}
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+oracleToken))
	if !slices.ContainsFunc(o.forge.seen(), func(request string) bool { return strings.HasSuffix(request, " "+basic) }) {
		t.Fatalf("the forge never received the token: %v", o.forge.seen())
	}
	if recorded, err := os.ReadFile(filepath.Join(o.root, "argv")); err != nil || strings.Count(string(recorded), "\n") != len(lines)+2 {
		t.Fatalf("git arguments %q, %v; want every command's", recorded, err)
	}
	patch := filepath.Join(o.directories.Output, wire.GitPatch)
	if info, err := os.Lstat(patch); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("%s is %v, %v; want a regular file", wire.GitPatch, info, err)
	}
	if data := string(must(t)(os.ReadFile(patch))); strings.Count(data, "\nSubject: [PATCH ") != 2 {
		t.Fatalf("patch holds %d patches, want both commits:\n%s", strings.Count(data, "\nSubject: [PATCH "), data)
	}
	plain := []string{"HOME=" + filepath.Join(o.root, "parent-home"), "PATH=" + os.Getenv("PATH"), "GIT_CONFIG_NOSYSTEM=1", "GIT_COMMITTER_NAME=parent", "GIT_COMMITTER_EMAIL=parent@example.test"}
	parent := filepath.Join(o.root, "parent")
	git(t, o.root, plain, "clone", "-q", "--branch", "feature", o.repository, parent)
	git(t, parent, plain, "am", "-q", patch)
	if readme, err := os.ReadFile(filepath.Join(parent, "README")); err != nil || string(readme) != "changed again\n" {
		t.Fatalf("README after git am %q, %v", readme, err)
	}
	git(t, parent, plain, "reset", "-q", "--hard", "HEAD~2")
	git(t, parent, plain, "fetch", "-q", filepath.Join(o.directories.Output, wire.GitBundle), "HEAD")
	if fetched := git(t, parent, plain, "log", "-1", "--format=%s", "FETCH_HEAD"); fetched != "agent change 2" {
		t.Fatalf("bundle's head %q, want the agent's last commit", fetched)
	}
	if branches := git(t, o.repository, plain, "for-each-ref", "--format=%(refname)", "refs/heads"); branches != "refs/heads/feature\nrefs/heads/main\nrefs/heads/wherry/run-git" {
		t.Fatalf("forge branches %q, want the run's branch pushed beside the others", branches)
	}
}

// Without the plugin's environment the platform's rewrite applies, so the clone reaches the other host; the
// clone's own settings still keep the token from it and from the platform's credential store.
func TestGitCloneKeepsTheTokenFromAnotherHost(t *testing.T) {
	o := newOracle(t)
	lines := o.preamble(&wire.GitInput{Remote: o.forge.URL + "/org/repo.git", Ref: "feature", Credential: "forge-token"})
	output, err := o.run(t, append(slices.Clone(o.ambient), "GIT_TERMINAL_PROMPT=0"), lines[0])
	if err == nil {
		t.Fatalf("clone through the rewrite succeeded:\n%s", output)
	}
	if len(o.attacker.seen()) == 0 {
		t.Fatalf("precondition: the rewrite must send the clone to the other host; output:\n%s", output)
	}
	o.leaks(t, output)
}

// Without the plugin's environment, a platform's credential store is still never given the token.
func TestGitCloneStoresTheTokenNowhere(t *testing.T) {
	o := newOracle(t)
	if err := os.WriteFile(o.homeConfig, []byte("[credential]\n\thelper = store --file="+o.store+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ambient := slices.DeleteFunc(slices.Clone(o.ambient), func(variable string) bool {
		return strings.HasPrefix(variable, "GIT_CONFIG_") || strings.HasPrefix(variable, "GIT_TRACE") || strings.HasPrefix(variable, "GIT_CURL")
	})
	lines := o.preamble(&wire.GitInput{Remote: o.forge.URL + "/org/repo.git", Ref: "feature", Credential: "forge-token"})
	output, err := o.run(t, append(ambient, "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0"), lines[0])
	if err != nil {
		t.Fatalf("clone: %v\n%s", err, output)
	}
	o.leaks(t, output)
	if _, err := os.Stat(o.store); err == nil {
		t.Fatalf("the platform's credential store was written")
	}
}

// A forge that redirects the clone elsewhere is refused, and the other host is never asked.
func TestGitCloneRefusesARedirect(t *testing.T) {
	o := newOracle(t)
	lines := o.preamble(&wire.GitInput{Remote: o.forge.URL + "/moved/org/repo.git", Ref: "feature", Credential: "forge-token"})
	output, err := o.run(t, o.agentEnvironment(), lines[0])
	if err == nil {
		t.Fatalf("clone through a redirect succeeded:\n%s", output)
	}
	if requests := o.attacker.seen(); len(requests) != 0 || len(o.forge.seen()) == 0 {
		t.Fatalf("attacker requests %v, forge requests %v; want the forge asked and the redirect not followed", requests, o.forge.seen())
	}
	o.leaks(t, output)
}

// A rewrite to another protocol is refused, so the clone reaches nothing but the remote's scheme.
func TestGitCloneAllowsOnlyTheRemotesProtocol(t *testing.T) {
	o := newOracle(t)
	rewrite := "[url \"file://" + filepath.Dir(filepath.Dir(o.repository)) + "/\"]\n\tinsteadOf = " + o.forge.URL + "/\n"
	if err := os.WriteFile(o.homeConfig, []byte(rewrite), 0o600); err != nil {
		t.Fatal(err)
	}
	lines := o.preamble(&wire.GitInput{Remote: o.forge.URL + "/org/repo.git", Ref: "feature", Credential: "forge-token"})
	output, err := o.run(t, []string{"HOME=" + o.home, "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0"}, lines[0])
	if err == nil || !strings.Contains(output, "transport 'file' not allowed") {
		t.Fatalf("clone through a file rewrite = %v:\n%s", err, output)
	}
}

// sshForge is an sshd of the test's own on a free local port, serving the current user's repositories with hostKey,
// and authorising clientKey alone.
func sshForge(t *testing.T, dir string) (port, hostKey, clientKey string) {
	t.Helper()
	sshd, err := exec.LookPath("sshd")
	if err != nil {
		if sshd, err = exec.LookPath("/usr/sbin/sshd"); err != nil {
			t.Skip("sshd not installed")
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ = net.SplitHostPort(listener.Addr().String())
	listener.Close()
	hostKey, clientKey = filepath.Join(dir, "host"), filepath.Join(dir, "client")
	for _, key := range []string{hostKey, clientKey} {
		if output, err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", key).CombinedOutput(); err != nil {
			t.Fatalf("ssh-keygen: %v\n%s", err, output)
		}
	}
	config := filepath.Join(dir, "sshd_config")
	settings := "Port " + port + "\nListenAddress 127.0.0.1\nHostKey " + hostKey + "\nAuthorizedKeysFile " + clientKey + ".pub\nStrictModes no\nUsePAM no\n" +
		"PasswordAuthentication no\nKbdInteractiveAuthentication no\nPidFile " + filepath.Join(dir, "sshd.pid") + "\n"
	if err := os.WriteFile(config, []byte(settings), 0o600); err != nil {
		t.Fatal(err)
	}
	server := exec.Command(sshd, "-D", "-e", "-f", config)
	if err := server.Start(); err != nil {
		t.Skipf("sshd: %v", err)
	}
	t.Cleanup(func() { server.Process.Kill(); server.Wait() })
	listening := false
	for range 50 {
		if conn, err := net.Dial("tcp", "127.0.0.1:"+port); err == nil {
			conn.Close()
			listening = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !listening {
		t.Skip("sshd did not listen on 127.0.0.1:" + port)
	}
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	// An sshd run without privileges cannot open a session everywhere, so the oracle needs one that does.
	probe := exec.Command("ssh", "-F", "/dev/null", "-i", clientKey, "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-p", port, current.Username+"@127.0.0.1", "git-upload-pack --help >/dev/null 2>&1; echo ready")
	if output, err := probe.CombinedOutput(); err != nil || !strings.Contains(string(output), "ready") {
		t.Skipf("sshd on 127.0.0.1:%s accepts no session that runs git (%v): %s", port, err, output)
	}
	return port, hostKey, clientKey
}

// sshCommand is the ssh command a preamble's clone sets.
func sshCommand(t *testing.T, clone string) string {
	t.Helper()
	_, rest, ok := strings.Cut(clone, "core.sshCommand='")
	command, _, closed := strings.Cut(rest, "'")
	if !ok || !closed {
		t.Fatalf("clone %q sets no ssh command", clone)
	}
	return command
}

// The preamble's ssh clone, run against a real sshd: it clones with the deploy key when the host key is the server's,
// fails host key verification when it is not, and its ssh offers no identity but the credential and trusts no
// known_hosts but the remote's.
func TestGitSSHCloneTrustsOnlyTheHostKey(t *testing.T) {
	o := newOracle(t)
	port, hostKey, clientKey := sshForge(t, t.TempDir())
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(o.directories.Credentials, "id_ed25519"), must(t)(os.ReadFile(clientKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	knownHost := func(key string) string {
		fields := strings.Fields(string(must(t)(os.ReadFile(key + ".pub"))))
		return "[127.0.0.1]:" + port + " " + fields[0] + " " + fields[1] + "\n"
	}
	other := filepath.Join(t.TempDir(), "other")
	if output, err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", other).CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen: %v\n%s", err, output)
	}
	remote := "ssh://" + current.Username + "@127.0.0.1:" + port + o.repository
	credentials := []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}}
	for name, test := range map[string]struct {
		hostKey, credential, want string
	}{
		"server's key":    {knownHost(hostKey), "deploy-key", ""},
		"another key":     {knownHost(other), "deploy-key", "Host key verification failed"},
		"without the key": {knownHost(hostKey), "", "Permission denied"},
	} {
		t.Run(name, func(t *testing.T) {
			input := &wire.GitInput{Remote: remote, Ref: "feature", Path: name[:3], Credential: test.credential, HostKey: test.hostKey}
			if err := input.Validate(credentials); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(o.directories.Input, wire.KnownHostsFile), []byte(test.hostKey), 0o600); err != nil {
				t.Fatal(err)
			}
			lines := commands(Preamble(o.directories, nil, credentials, nil, nil, input, "run-ssh", admin.DefaultReplyLimit))
			ssh := sshCommand(t, lines[0])
			resolved, err := o.run(t, o.agentEnvironment(), ssh+" -G -p "+port+" "+current.Username+"@127.0.0.1")
			if err != nil {
				t.Fatalf("ssh -G: %v\n%s", err, resolved)
			}
			identity := filepath.Join(o.directories.Credentials, "id_ed25519")
			if test.credential == "" {
				identity = "/dev/null"
			}
			var identities []string
			for line := range strings.Lines(resolved) {
				if file, ok := strings.CutPrefix(strings.TrimSpace(line), "identityfile "); ok {
					identities = append(identities, file)
				}
			}
			for _, want := range []string{"identityagent none", "identitiesonly yes", "batchmode yes", "stricthostkeychecking true",
				"globalknownhostsfile /dev/null", "userknownhostsfile " + filepath.Join(o.directories.Input, wire.KnownHostsFile)} {
				if !strings.Contains(resolved, "\n"+want+"\n") {
					t.Errorf("ssh resolves without %q", want)
				}
			}
			if !slices.Equal(identities, []string{identity}) || strings.Contains(resolved, "\nproxycommand ") {
				t.Errorf("ssh offers %v and resolves a proxy command %t; want %s alone and none", identities, strings.Contains(resolved, "\nproxycommand "), identity)
			}
			output, err := o.run(t, o.agentEnvironment(), lines[0])
			if test.want == "" {
				if err != nil {
					t.Fatalf("clone: %v\n%s", err, output)
				}
				if output, err := o.run(t, o.agentEnvironment(), lines[1]); err != nil {
					t.Fatalf("checkout: %v\n%s", err, output)
				}
				if readme, err := os.ReadFile(filepath.Join(o.work, name[:3], "README")); err != nil || string(readme) != "feature\n" {
					t.Fatalf("checkout README %q, %v", readme, err)
				}
				return
			}
			if err == nil || !strings.Contains(output, test.want) {
				t.Fatalf("clone = %v, want %q:\n%s", err, test.want, output)
			}
		})
	}
}

// Under the plugin's environment the platform's trace variables write nothing.
func TestGitCloneTracesNothing(t *testing.T) {
	o := newOracle(t)
	lines := o.preamble(&wire.GitInput{Remote: o.forge.URL + "/org/repo.git", Ref: "feature", Credential: "forge-token"})
	env := slices.DeleteFunc(o.agentEnvironment(), func(variable string) bool { return strings.HasPrefix(variable, "GIT_CURL_VERBOSE=") })
	if output, err := o.run(t, env, lines[0]); err != nil {
		t.Fatalf("clone: %v\n%s", err, output)
	}
	for _, trace := range []string{o.trace, o.curlTrace} {
		if _, err := os.Stat(trace); err == nil {
			t.Errorf("git wrote the platform's trace %s", filepath.Base(trace))
		}
	}
}

// writeTemplate is a git template directory whose configuration rewrites the forge to the attacker and whose
// post-checkout hook leaves marker.
func (o *oracle) writeTemplate(t *testing.T) (dir, marker string) {
	t.Helper()
	dir, marker = filepath.Join(o.root, "template"), filepath.Join(o.root, "template-hook-ran")
	if err := os.MkdirAll(filepath.Join(dir, "hooks"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := "[url \"" + o.attacker.URL + "/\"]\n\tinsteadOf = " + o.forge.URL + "/\n[planted]\n\tby = template\n"
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "hooks", "post-checkout"), []byte("#!/bin/sh\ntouch '"+marker+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return dir, marker
}

// A platform's template directory seeds neither the clone nor a repository the agent creates.
func TestGitCloneTakesNoTemplate(t *testing.T) {
	o := newOracle(t)
	template, marker := o.writeTemplate(t)
	lines := o.preamble(&wire.GitInput{Remote: o.forge.URL + "/org/repo.git", Ref: "feature", Credential: "forge-token"})
	planted := func(t *testing.T, repository string) {
		t.Helper()
		if _, err := os.Stat(marker); err == nil {
			t.Fatal("the template's hook ran")
		}
		if config, err := os.ReadFile(filepath.Join(repository, ".git", "config")); err != nil || strings.Contains(string(config), "planted") {
			t.Fatalf("repository configuration %q, %v; want none of the template's", config, err)
		}
	}
	t.Run("plugin environment", func(t *testing.T) {
		o.ambient = append(o.ambient, "GIT_TEMPLATE_DIR="+template)
		if output, err := o.run(t, o.agentEnvironment(), "git init -q own"); err != nil {
			t.Fatalf("init: %v\n%s", err, output)
		}
		planted(t, filepath.Join(o.work, "own"))
	})
	t.Run("clone alone", func(t *testing.T) {
		env := []string{"HOME=" + filepath.Join(o.root, "empty-home"), "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0", "GIT_TEMPLATE_DIR=" + template,
			"GIT_SSL_CAINFO=" + filepath.Join(o.root, "ca.pem")}
		output, err := o.run(t, env, lines[0])
		if err != nil {
			t.Fatalf("clone: %v\n%s", err, output)
		}
		planted(t, filepath.Join(o.work, "repo"))
		if len(o.attacker.seen()) != 0 {
			t.Fatal("the template's rewrite reached the attacker")
		}
	})
}

// GIT_DIR, GIT_WORK_TREE and GIT_EXEC_PATH in the environment a plugin configures do not retarget the clone or the
// commands after it, or replace the programs git runs.
func TestGitCommandsKeepTheirRepository(t *testing.T) {
	o := newOracle(t)
	elsewhere, tree, programs := filepath.Join(o.root, "elsewhere.git"), filepath.Join(o.root, "elsewhere-tree"), filepath.Join(o.root, "programs")
	for _, dir := range []string{tree, programs} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	replaced := filepath.Join(o.root, "replaced-helper-ran")
	if err := os.WriteFile(filepath.Join(programs, "git-remote-https"), []byte("#!/bin/sh\ntouch '"+replaced+"'\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	o.ambient = append(o.ambient, "GIT_DIR="+elsewhere, "GIT_WORK_TREE="+tree, "GIT_EXEC_PATH="+programs)
	lines := o.preamble(&wire.GitInput{Remote: o.forge.URL + "/org/repo.git", Ref: "feature", Credential: "forge-token"})
	for _, line := range lines[:3] {
		if output, err := o.run(t, o.agentEnvironment(), line); err != nil {
			t.Fatalf("%s: %v\n%s", line, err, output)
		}
	}
	if readme, err := os.ReadFile(filepath.Join(o.work, "repo", "README")); err != nil || string(readme) != "feature\n" {
		t.Fatalf("checkout README %q, %v; want the ref's, in the clone", readme, err)
	}
	entries, _ := os.ReadDir(tree)
	if _, err := os.Stat(elsewhere); err == nil || len(entries) != 0 {
		t.Fatalf("the commands wrote to the planted repository or work tree")
	}
	if _, err := os.Stat(replaced); err == nil {
		t.Fatal("git ran the planted remote helper")
	}
}

// GIT_SSL_NO_VERIFY in the environment a plugin configures, or http.sslVerify=false in the platform's configuration,
// does not let the clone trust a certificate its CA does not.
func TestGitCloneVerifiesCertificates(t *testing.T) {
	o := newOracle(t)
	// httptest's servers share one certificate, so a CA that trusts neither is one of the test's own.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "other ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	untrusting := filepath.Join(o.root, "other-ca.pem")
	if err := os.WriteFile(untrusting, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}), 0o600); err != nil {
		t.Fatal(err)
	}
	lines := o.preamble(&wire.GitInput{Remote: o.forge.URL + "/org/repo.git", Ref: "feature", Credential: "forge-token"})
	for name, env := range map[string]func() []string{
		"plugin environment": func() []string {
			o.ambient = append(o.ambient, "GIT_SSL_NO_VERIFY=1")
			return append(o.agentEnvironment(), "GIT_SSL_CAINFO="+untrusting)
		},
		"platform configuration": func() []string {
			if err := os.WriteFile(o.homeConfig, []byte("[http]\n\tsslVerify = false\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			return []string{"HOME=" + o.home, "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0", "GIT_SSL_CAINFO=" + untrusting}
		},
	} {
		t.Run(name, func(t *testing.T) {
			output, err := o.run(t, env(), strings.Replace(lines[0], "'repo'", "'"+strings.ReplaceAll(name, " ", "-")+"'", 1))
			if err == nil || !strings.Contains(output, "certificate") {
				t.Fatalf("clone from an untrusted forge = %v:\n%s", err, output)
			}
		})
	}
}
