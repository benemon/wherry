package client

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
)

// Each kind of source delivers its value byte for byte, and no value reaches the logs.
func TestEachSourceKindIsSealed(t *testing.T) {
	f := newFixture(t)
	deployKey := filepath.Join(t.TempDir(), "deploy-key")
	if err := os.WriteFile(deployKey, []byte("deploy-key-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WHERRY_TEST_MODEL_TOKEN", "model-from-env")
	f.host.credentials["model"] = Source{Env: "WHERRY_TEST_MODEL_TOKEN"}
	f.host.credentials["deploy-key"] = Source{File: deployKey}
	f.host.credentials["vault-token"] = Source{Command: []string{"sh", "-c", `printf 'vault-from-command\n'; echo command-stderr >&2`}}
	receiver, task := f.admittedWith(t, []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}, {Name: "vault-token", Env: "VAULT_TOKEN"}})
	runID := task.submission.Wherry.Run
	asked := newRequest(t, receiver.binding(task, 1), "model")
	asked.record.TaskCredentials = []string{"deploy-key", "vault-token"}
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	f.waitSeals(t, "host-a", 1)
	values := asked.open(t, f, "host-a")
	if string(values["model"]) != "model-from-env" || string(values["deploy-key"]) != "deploy-key-value\n" || string(values["vault-token"]) != "vault-from-command\n" || len(values) != 3 {
		t.Fatalf("sealed %q, want each source's value as it is", values)
	}
	for _, value := range []string{"model-from-env", "deploy-key-value", "vault-from-command", "command-stderr"} {
		if strings.Contains(f.logs.String(), value) {
			t.Fatalf("logs carry %q:\n%s", value, f.logs.String())
		}
	}
}

// A source that fails to resolve leaves the request unsettled, logged by credential name and cause alone: no value, no
// output of the command and no path reaches the logs.
func TestAFailingSourceIsLoggedByNameAlone(t *testing.T) {
	previous := commandTimeout
	commandTimeout = 300 * time.Millisecond
	t.Cleanup(func() { commandTimeout = previous })
	const secret = "source-output-never-logged"
	for name, test := range map[string]struct {
		source Source
		want   string
	}{
		"env unset": {Source{Env: "WHERRY_TEST_UNSET_TOKEN"}, "its environment variable is unset or empty"},
		"command fails": {Source{Command: []string{"sh", "-c", "printf " + secret + "; echo " + secret + " >&2; exit 3"}},
			"its command exited with status 3"},
		"command times out": {Source{Command: []string{"sh", "-c", "printf " + secret + "; echo " + secret + " >&2; sleep 5"}},
			"its command did not finish within 300ms"},
		"command over size":      {Source{Command: []string{"sh", "-c", "yes " + secret + " | head -c 70000"}}, "its command wrote over 65536 bytes"},
		"command writes nothing": {Source{Command: []string{"sh", "-c", "echo " + secret + " >&2"}}, "its command wrote nothing"},
		"command missing":        {Source{Command: []string{filepath.Join(t.TempDir(), secret)}}, "its command did not start"},
		"file missing":           {Source{File: filepath.Join(t.TempDir(), secret)}, "no such file or directory"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			if name == "env unset" {
				t.Setenv("WHERRY_TEST_UNSET_TOKEN", "")
			}
			f.host.credentials["model"] = test.source
			receiver, task := f.admitted(t)
			runID := task.submission.Wherry.Run
			asked := newRequest(t, receiver.binding(task, 1), "model")
			receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
			answering(t, f, f.host)
			want := "run " + runID + ": resolve credential model: " + test.want
			for !strings.Contains(f.logs.String(), want) {
				if f.ctx.Err() != nil {
					t.Fatalf("logs lack %q:\n%s", want, f.logs.String())
				}
				time.Sleep(50 * time.Millisecond)
			}
			if logs := f.logs.String(); strings.Contains(logs, secret) {
				t.Fatalf("logs carry the source's output, value or path:\n%s", logs)
			}
			if seals := f.seals(t, "host-a"); seals != 0 {
				t.Fatalf("host topic holds %d seals", seals)
			}
			// The request stays unsettled, so a source that resolves on a later pass is sealed.
			if name == "env unset" {
				os.Setenv("WHERRY_TEST_UNSET_TOKEN", "model-from-env")
				f.waitSeals(t, "host-a", 1)
				if values := asked.open(t, f, "host-a"); string(values["model"]) != "model-from-env" {
					t.Fatalf("sealed %q, want the value set after the first pass", values)
				}
			}
		})
	}
}

// A command source runs before the attempt's lock is taken, so a slow one never holds up the host's other answerers.
func TestACommandSourceRunsOutsideTheAttemptLock(t *testing.T) {
	f := newFixture(t)
	started := filepath.Join(t.TempDir(), "started")
	f.host.credentials["model"] = Source{Command: []string{"sh", "-c", "touch " + started + "; sleep 3; printf model-token"}}
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	for _, err := os.Stat(started); err != nil; _, err = os.Stat(started) {
		if f.ctx.Err() != nil {
			t.Fatalf("the command never started; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	release, err := f.host.lockAttempt(ctx, runID, 1)
	if err != nil {
		t.Fatalf("the attempt's lock is held while the command runs: %v", err)
	}
	release()
	f.waitSeals(t, "host-a", 1)
}

func TestSubmitRefusesARequiredAgentCredentialWithoutASource(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	delete(f.host.credentials, "model")
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "p", Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "auth required: credential model is not in this host's configuration") {
		t.Fatalf("submit = %v; want the agent's credential named", err)
	}
	if records, _ := admin.ReadTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic("pool-a")); len(records) != 0 {
		t.Fatal("published a task whose agent's credential this host has no source for")
	}
}

// An optional agent credential the host has no source for is named unsupplied, and the submission goes ahead.
func TestSubmitGoesWithoutAnOptionalCredential(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = bothOffers() })
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Agent: "codex", Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}},
		Prompt: "p", Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if task := receiver.claim(t, f.ctx, submitted.RunID)[0]; !slices.Equal(task.submission.Wherry.Unsupplied, []string{"github-token"}) {
		t.Fatalf("unsupplied %v, want github-token", task.submission.Wherry.Unsupplied)
	}
}

func TestSubmitRefusesATaskCredentialOnAnAgentTarget(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "p", Credentials: []wire.TaskCredential{{Name: "openai", Env: "MODEL_API_KEY"}},
		Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "task credential openai takes the environment variable MODEL_API_KEY of one of agent claude's credentials") {
		t.Fatalf("submit = %v; want the collision named", err)
	}
	if records, _ := admin.ReadTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic("pool-a")); len(records) != 0 {
		t.Fatal("published a task whose credential takes the agent's target")
	}
}

func TestConfigSourcesAndGit(t *testing.T) {
	dir := t.TempDir()
	hostKey := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(hostKey, []byte("git.example.test ssh-ed25519 AAAA\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.json")
	for name, test := range map[string]struct{ config, want string }{
		"two kinds":          {`"credentials":{"a":{"file":"x","env":"A"}}`, "config credential a needs exactly one of file, env and command"},
		"no kind":            {`"credentials":{"a":{}}`, "config credential a needs exactly one of file, env and command"},
		"a literal":          {`"credentials":{"a":"value"}`, "decode config"},
		"an empty command":   {`"credentials":{"a":{"command":[""]}}`, "config credential a: command names no program"},
		"a git url":          {`"git":[{"url":"git@github.com:"}]`, "config git url git@github.com: must start with https:// or ssh://"},
		"a git url twice":    {`"git":[{"url":"https://a/"},{"url":"https://a/"}]`, "config git url https://a/ is listed twice"},
		"an unknown source":  {`"git":[{"url":"https://a/","credential":"absent"}]`, "names credential absent, which credentials does not configure"},
		"an unreadable key":  {`"git":[{"url":"ssh://a/","host_key":"` + filepath.Join(dir, "absent") + `"}]`, "config git url ssh://a/: host_key is not a readable file"},
		"a directory as key": {`"git":[{"url":"ssh://a/","host_key":"` + dir + `"}]`, "config git url ssh://a/: host_key is not a readable file"},
	} {
		if err := os.WriteFile(path, []byte(`{"bundle":"bundle.yaml",`+test.config+`}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("config with %s: %v, want %q", name, err, test.want)
		}
	}
	if err := os.WriteFile(path, []byte(`{"bundle":"bundle.yaml","credentials":{"a":{"command":["vault","print","token"]}},"git":[{"url":"ssh://a/","credential":"a","host_key":"`+hostKey+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if config, err := LoadConfig(path); err != nil || !slices.Equal(config.Credentials["a"].Command, []string{"vault", "print", "token"}) || config.Git[0].HostKey != hostKey {
		t.Fatalf("config %+v, %v", config, err)
	}
}
