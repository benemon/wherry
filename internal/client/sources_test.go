package client

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

// Each kind of source delivers its value byte for byte into the envelope, and no value reaches the logs.
func TestEachSourceKindIsEnveloped(t *testing.T) {
	f := newFixture(t)
	deployKey := filepath.Join(t.TempDir(), "deploy-key")
	if err := os.WriteFile(deployKey, []byte("deploy-key-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WHERRY_TEST_MODEL_TOKEN", "model-from-env")
	f.host.credentials["model"] = credential.Source{Env: "WHERRY_TEST_MODEL_TOKEN"}
	f.host.credentials["deploy-key"] = credential.Source{File: deployKey}
	f.host.credentials["vault-token"] = credential.Source{Command: []string{"sh", "-c", `printf 'vault-from-command\n'; echo command-stderr >&2`}}
	_, task := f.admittedWith(t, []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}, {Name: "vault-token", Env: "VAULT_TOKEN"}})
	_, opened, err := open(t, f, task.submission.Wherry.Run, f.poolKeys["pool-a"])
	if err != nil {
		t.Fatal(err)
	}
	values := opened.Credentials
	if string(values["model"]) != "model-from-env" || string(values["deploy-key"]) != "deploy-key-value\n" || string(values["vault-token"]) != "vault-from-command\n" || len(values) != 3 {
		t.Fatalf("enveloped %q, want each source's value as it is", values)
	}
	for _, value := range []string{"model-from-env", "deploy-key-value", "vault-from-command", "command-stderr"} {
		if strings.Contains(f.logs.String(), value) {
			t.Fatalf("logs carry %q:\n%s", value, f.logs.String())
		}
	}
}

// A source that fails to resolve refuses the submission by credential name and cause alone: no value, no output of the
// command and no path reaches the error, and nothing is published.
func TestAFailingSourceIsNamedAlone(t *testing.T) {
	const secret = "source-output-never-logged"
	for name, test := range map[string]struct {
		source credential.Source
		want   string
	}{
		"env unset": {credential.Source{Env: "WHERRY_TEST_UNSET_TOKEN"}, "its environment variable is unset or empty"},
		"command fails": {credential.Source{Command: []string{"sh", "-c", "printf " + secret + "; echo " + secret + " >&2; exit 3"}},
			"its command exited with status 3"},
		"command over size":      {credential.Source{Command: []string{"sh", "-c", "yes " + secret + " | head -c 70000"}}, "its command wrote over 65536 bytes"},
		"command writes nothing": {credential.Source{Command: []string{"sh", "-c", "echo " + secret + " >&2"}}, "its command wrote nothing"},
		"command missing":        {credential.Source{Command: []string{filepath.Join(t.TempDir(), secret)}}, "its command did not start"},
		"file missing":           {credential.Source{File: filepath.Join(t.TempDir(), secret)}, "no such file or directory"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			if name == "env unset" {
				t.Setenv("WHERRY_TEST_UNSET_TOKEN", "")
			}
			f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
			f.host.credentials["model"] = test.source
			err := f.submitErr(t)
			if want := "resolve credential model: " + test.want; err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), secret) {
				t.Fatalf("submit = %v; want %q alone", err, want)
			}
			if logs := f.logs.String(); strings.Contains(logs, secret) {
				t.Fatalf("logs carry the source's output, value or path:\n%s", logs)
			}
			if records, _ := readTopic(f.ctx, f.service.SuperUser, operator.PoolTasksTopic("pool-a")); len(records) != 0 {
				t.Fatal("published a task whose credential did not resolve")
			}
		})
	}
}

func TestSubmitRefusesARequiredAgentCredentialWithoutASource(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	delete(f.host.credentials, "model")
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "p", EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "auth required: credential model is not in this host's configuration") {
		t.Fatalf("submit = %v; want the agent's credential named", err)
	}
	if records, _ := readTopic(f.ctx, f.service.SuperUser, operator.PoolTasksTopic("pool-a")); len(records) != 0 {
		t.Fatal("published a task whose agent's credential this host has no source for")
	}
}

// An optional agent credential the host has no source for is named unsupplied, and the submission goes ahead.
func TestSubmitGoesWithoutAnOptionalCredential(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = bothOffers() })
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Agent: "codex", Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}},
		Prompt: "p", EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
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
		EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "task credential openai takes the environment variable MODEL_API_KEY of one of agent claude's credentials") {
		t.Fatalf("submit = %v; want the collision named", err)
	}
	if records, _ := readTopic(f.ctx, f.service.SuperUser, operator.PoolTasksTopic("pool-a")); len(records) != 0 {
		t.Fatal("published a task whose credential takes the agent's target")
	}
}

func TestConfigSourcesAndGit(t *testing.T) {
	dir := t.TempDir()
	hostKey := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(hostKey, []byte("git.example.test ssh-ed25519 AAAA\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.yaml")
	for name, test := range map[string]struct{ config, want string }{
		"two kinds":                      {"credentials:\n  a: {file: x, env: A}\n", "config credential a: needs exactly one of file, env and command"},
		"no kind":                        {"credentials:\n  a: {}\n", "config credential a: needs exactly one of file, env and command"},
		"a literal":                      {"credentials:\n  a: value\n", "decode config"},
		"an empty command":               {"credentials:\n  a:\n    command: ['']\n", "config credential a: command names no program"},
		"a git url":                      {"git:\n  - url: 'git@github.com:'\n", "config git url git@github.com: must start with https:// or ssh://"},
		"a git url twice":                {"git:\n  - url: https://a/\n  - url: https://a/\n", "config git url https://a/ is listed twice"},
		"an unknown source":              {"git:\n  - url: https://a/\n    credential: absent\n", "names credential absent, which credentials does not configure"},
		"an unreadable key":              {"git:\n  - url: ssh://a/\n    host_key: " + filepath.Join(dir, "absent") + "\n", "config git url ssh://a/: host_key is not a readable file"},
		"a directory as key":             {"git:\n  - url: ssh://a/\n    host_key: " + dir + "\n", "config git url ssh://a/: host_key is not a readable file"},
		"storage without its secret key": {"credentials:\n  a: {env: A}\nstorage:\n  access_key: a\n", "config storage requires access_key and secret_key"},
		"storage without its access key": {"credentials:\n  a: {env: A}\nstorage:\n  secret_key: a\n", "config storage requires access_key and secret_key"},
		"an unknown access key": {"credentials:\n  a: {env: A}\nstorage:\n  access_key: absent\n  secret_key: a\n",
			"config storage access_key names credential absent, which credentials does not configure"},
		"an unknown secret key": {"credentials:\n  a: {env: A}\nstorage:\n  access_key: a\n  secret_key: absent\n",
			"config storage secret_key names credential absent, which credentials does not configure"},
		"storage key files": {"storage:\n  access_key_file: a\n  secret_key_file: b\n", "decode config"},
	} {
		if err := os.WriteFile(path, []byte("bundle: bundle.yaml\n"+test.config), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("config with %s: %v, want %q", name, err, test.want)
		}
	}
	config := "bundle: bundle.yaml\ncredentials:\n  a:\n    command: [vault, print, token]\n  s3: {env: S3}\ngit:\n  - url: ssh://a/\n    credential: a\n    host_key: " + hostKey +
		"\nstorage:\n  access_key: s3\n  secret_key: a\n"
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if config, err := LoadConfig(path); err != nil || !slices.Equal(config.Credentials["a"].Command, []string{"vault", "print", "token"}) || config.Git[0].HostKey != hostKey ||
		*config.Storage != (StorageConfig{AccessKey: "s3", SecretKey: "a"}) {
		t.Fatalf("config %+v, %v", config, err)
	}
}
