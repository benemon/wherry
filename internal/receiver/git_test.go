package receiver

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// testHostKey is a known_hosts line as ssh-keyscan prints one.
const testHostKey = "git.example.test ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl\n"

func vcsTerms(hosts ...string) admin.Terms {
	terms := testTerms()
	terms.VCS = hosts
	return terms
}

func withGit(git wire.GitInput) func(*wire.Submission) {
	return func(task *wire.Submission) { task.Wherry.Git = &git }
}

func TestGitInputAdmittedOnlyFromThePoolsVCSHosts(t *testing.T) {
	git := wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"}
	for name, test := range map[string]struct {
		hosts  []string
		reason string
	}{
		"listed":            {[]string{"github.com", "git.example.test"}, ""},
		"unlisted":          {[]string{"github.com", "git.example.test:8443"}, "git remote host git.example.test is not in pool " + testPool + "'s vcs hosts"},
		"without vcs hosts": {nil, "git remote host git.example.test is not in pool " + testPool + "'s vcs hosts"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixtureWith(t, resultStream, vcsTerms(test.hosts...), testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
			f.answer(t, credentialName)
			record := f.submit(t, f.host, withGit(git))
			if test.reason != "" {
				f.rejected(t, record, test.reason)
				statuses := f.feedStatuses(t)
				if len(statuses) != 1 || statuses[0].StatusUpdate.Status.Message.Parts[0].Text != test.reason {
					t.Fatalf("statuses %+v, want the rejection naming the host and the pool", statuses)
				}
				return
			}
			if status := f.execute(t, record, 1); status != kgo.AckAccept {
				t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
			}
			if prompt := string(f.lastRun().Prompt); !strings.Contains(prompt, "    git clone --template= --config credential.helper= --config http.followRedirects=false --config http.sslVerify=true --config protocol.allow=never "+
				"--config protocol.https.allow=always --config user.name=wherry --config user.email=wherry@invalid -- 'https://git.example.test/org/repo.git' 'repo'\n") {
				t.Fatalf("prompt %q, want the clone command", prompt)
			}
		})
	}
}

// A continuation is admitted under the pool's current VCS hosts, against the git input its first turn declared.
func TestGitContinuationRechecksTheVCSHosts(t *testing.T) {
	f := newFixtureWith(t, resultStream, vcsTerms("git.example.test"), testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	// Unanswered, the first turn's seal request leaves it AUTH_REQUIRED.
	if status := f.execute(t, f.submit(t, f.host, withGit(wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"})), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateAuthRequired {
		t.Fatalf("task %#v, want AUTH_REQUIRED", task.Status)
	}
	continuation := f.turn(t, "run-a", 2)
	if admitted := f.admitted(t, continuation); admitted.git == nil || admitted.git.Remote != "https://git.example.test/org/repo.git" {
		t.Fatalf("continuation admitted git %+v, want the first turn's", admitted.git)
	}
	terms := vcsTerms("github.com")
	encoded := must(t)(admin.EncodeRecord(admin.Record{Kind: "pool", Pool: testPool, Terms: &terms}))
	if err := f.service.Operator.ProduceSync(f.ctx, &kgo.Record{Topic: admin.AdminTopic, Value: encoded}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.rejected(t, continuation, "git remote host git.example.test is not in pool "+testPool+"'s vcs hosts")
}

func TestGitInputReachesTheAgentAsFilesAndCommands(t *testing.T) {
	f := newFixtureWith(t, resultStream, vcsTerms("git.example.test:2222"), testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	hostKey := "[git.example.test]:2222" + testHostKey[len("git.example.test"):]
	git := wire.GitInput{Remote: "ssh://git@git.example.test:2222/org/repo.git", Ref: "v1.2.0", Path: "src/repo", Credential: "deploy-key", HostKey: hostKey}
	done := f.executing(t, f.ctx, f.submit(t, f.host, func(task *wire.Submission) {
		withDeployKey(task)
		task.Wherry.Git = &git
		task.Wherry.Outputs = []wire.ExpectedOutput{{Name: wire.GitPatch}}
		task.Message.Parts = append(task.Message.Parts, wire.Part{Raw: wire.StdBytes("a,b\n"), Filename: "data.csv"})
	}), 1)
	f.sealFor(t, f.request(t, 1), map[string][]byte{credentialName: []byte(testToken), "deploy-key": []byte(deployValue)})
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	run := f.lastRun()
	want := []adapter.File{{Name: wire.KnownHostsFile, Data: []byte(hostKey)}, {Name: "data.csv", Data: []byte("a,b\n")}}
	if !slices.EqualFunc(run.Inputs, want, func(a, b adapter.File) bool { return a.Name == b.Name && bytes.Equal(a.Data, b.Data) }) {
		t.Fatalf("inputs %q, want known_hosts then the input", run.Inputs)
	}
	ssh := `ssh -F /dev/null -i "/task/secrets/credentials/id_ed25519" -o IdentitiesOnly=yes -o IdentityAgent=none -o UserKnownHostsFile="/task/inputs/known_hosts" ` +
		`-o GlobalKnownHostsFile=/dev/null -o StrictHostKeyChecking=yes -o BatchMode=yes -o ProxyCommand=none`
	prompt := string(run.Prompt)
	for _, line := range []string{
		"Input files: /task/inputs/known_hosts, /task/inputs/data.csv.",
		"Credential files: /task/secrets/credentials/id_ed25519. Never print or copy their contents.",
		"Clone the git repository ssh://git@git.example.test:2222/org/repo.git at v1.2.0 into src/repo, relative to your working directory, with these commands. " +
			"They need git 2.30 or later: run git version first, and if it is older, stop and report that.",
		"    git clone --template= --config credential.helper= --config core.sshCommand='" + ssh + "' --config http.followRedirects=false --config http.sslVerify=true --config protocol.allow=never " +
			"--config protocol.ssh.allow=always --config user.name=wherry --config user.email=wherry@invalid -- 'ssh://git@git.example.test:2222/org/repo.git' 'src/repo'",
		"    git -C 'src/repo' checkout 'v1.2.0' --",
		"    git -C 'src/repo' format-patch --output=\"/task/outputs/changes.patch\" refs/wherry/start..HEAD",
		"    git -C 'src/repo' push origin HEAD:refs/heads/wherry/run-a",
	} {
		if !strings.Contains(prompt, line+"\n") {
			t.Errorf("prompt lacks %q:\n%s", line, prompt)
		}
	}
	if strings.Contains(prompt, deployValue) || strings.Contains(prompt, testToken) || !strings.HasSuffix(prompt, "\n\nhi") {
		t.Fatalf("prompt %q, want no credential value and the text after the preamble", prompt)
	}
}

func TestKnownHostsInputConflictsAcrossTurns(t *testing.T) {
	git := &wire.GitInput{Remote: "ssh://git@git.example.test/org/repo.git", Ref: "main", HostKey: testHostKey}
	_, rejection := deliverables([]wire.Message{{Parts: []wire.Part{{Text: "hi"}}}, {Parts: []wire.Part{{Raw: wire.StdBytes("x"), Filename: wire.KnownHostsFile}}}}, git, nil)
	if rejection != "input names repeat across turns" {
		t.Fatalf("rejection %q, want a continuation's known_hosts refused beside the host key", rejection)
	}
}
