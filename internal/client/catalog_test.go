package client

import (
	"context"
	"errors"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/catalog"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
)

// Variables the fixture catalog reads, all set or cleared by each test so the process's environment plays no part.
const (
	alphaVariable  = "WHERRY_TEST_ALPHA_KEY"
	m1Variable     = "WHERRY_TEST_M1"
	m2Variable     = "WHERRY_TEST_M2"
	m3Variable     = "WHERRY_TEST_M3"
	githubVariable = "WHERRY_TEST_GITHUB_TOKEN"
	// notesVariable is the target of a terms credential and in no catalog entry: never read implicitly.
	notesVariable = "WHERRY_TEST_NOT_IN_CATALOG"
)

// testCatalog is a catalog of three entries: alpha reads one variable, multi three, tool one that an optional
// credential of both agents targets.
func testCatalog(t *testing.T, multiReads ...string) *catalog.Catalog {
	t.Helper()
	if multiReads == nil {
		multiReads = []string{m1Variable, m2Variable, m3Variable}
	}
	quoted := make([]string, 0, len(multiReads))
	for _, variable := range multiReads {
		quoted = append(quoted, `"`+variable+`"`)
	}
	c, err := catalog.Parse([]byte(`{
		"alpha": {"id": "alpha", "name": "Alpha", "env": ["` + alphaVariable + `"], "doc": "https://alpha.example/keys"},
		"multi": {"id": "multi", "name": "Multi", "env": [` + strings.Join(quoted, ", ") + `], "doc": "https://multi.example/keys"},
		"tool": {"id": "tool", "name": "Tool", "env": ["` + githubVariable + `"], "doc": "https://tool.example/tokens"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// catalogTerms are the terms pool apply would record for agents alpha and multi under testCatalog, plus an optional
// github-token on a catalog variable and an optional notes credential on a variable no entry reads.
func catalogTerms() operator.Terms {
	optional := func() *bool { f := false; return &f }
	github := operator.AgentCredential{TaskCredential: wire.TaskCredential{Name: "github-token", Env: githubVariable}, Description: "GitHub token", Required: optional()}
	notes := operator.AgentCredential{TaskCredential: wire.TaskCredential{Name: "notes", Env: notesVariable}, Required: optional()}
	return operator.Terms{
		Agents: []operator.AgentTerms{
			{Name: "alpha", Credentials: []operator.AgentCredential{{TaskCredential: wire.TaskCredential{Name: "alpha-key", Env: alphaVariable}, Description: "Alpha"}, github, notes}},
			{Name: "multi", Credentials: []operator.AgentCredential{
				{TaskCredential: wire.TaskCredential{Name: "m1", Env: m1Variable}, Description: "Multi", Required: optional()},
				{TaskCredential: wire.TaskCredential{Name: "m2", Env: m2Variable}, Description: "Multi", Required: optional()},
				{TaskCredential: wire.TaskCredential{Name: "m3", Env: m3Variable}, Description: "Multi", Required: optional()},
				github,
			}},
		},
		DefaultAgent:  "alpha",
		DeliveryLimit: 3, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50,
	}
}

// catalogFixture is a fixture whose host reads testCatalog and has no explicit source, with pool-c under catalogTerms and
// a live receiver whose profiles one and many run alpha and multi. Every fixture variable starts unset.
func catalogFixture(t *testing.T) (*fixture, *testReceiver) {
	t.Helper()
	for _, variable := range []string{alphaVariable, m1Variable, m2Variable, m3Variable, githubVariable, notesVariable} {
		t.Setenv(variable, "")
	}
	f := newFixture(t)
	f.host.catalog = testCatalog(t)
	f.host.credentials = map[string]credential.Source{}
	if err := f.service.PoolApply(f.ctx, "pool-c", catalogTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-c"); err != nil {
		t.Fatal(err)
	}
	receiver := f.addReceiverConfig(t, operator.ReceiverConfig{Receiver: "receiver-c", Pool: "pool-c", Profiles: []operator.ProfileSettings{
		{Name: "one", Plugin: "nomad", Agent: "alpha", Labels: map[string]string{"runtime": "exec"}},
		{Name: "many", Plugin: "nomad", Agent: "multi", Labels: map[string]string{"runtime": "exec"}},
	}, PluginSHA256: map[string]string{"nomad": strings.Repeat("ab", 32)}})
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) {
		ad.Offers = []wire.Offer{{Profile: "one", Agent: "alpha"}, {Profile: "many", Agent: "multi"}}
	})
	return f, receiver
}

// catalogTask is a task for agent on pool-c.
func catalogTask(agent string, requested ...string) SubmitInput {
	return SubmitInput{Agent: agent, Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}, Prompt: "p", Requested: requested,
		StartBy: time.Now().Add(time.Minute), EndBy: time.Now().Add(time.Minute)}
}

// published is the number of tasks on pool-c's task topics.
func (f *fixture) published(t *testing.T) int {
	t.Helper()
	records, err := readTasks(f.ctx, f.service.Operator, "pool-c")
	if err != nil {
		t.Fatal(err)
	}
	return len(records)
}

// noValueLeaks fails t when any of values reaches the host's log or pool-c's task topics in the clear.
func (f *fixture) noValueLeaks(t *testing.T, values ...string) {
	t.Helper()
	records, err := readTasks(f.ctx, f.service.Operator, "pool-c")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if strings.Contains(f.logs.String(), value) {
			t.Errorf("the host's log carries a credential value:\n%s", f.logs.String())
		}
		for _, record := range records {
			if strings.Contains(string(record.Value), value) {
				t.Errorf("a task record carries a credential value in the clear")
			}
		}
	}
}

// A host reads a terms credential from its target variable only when the variable is in its catalog: alpha-key and
// github-token, set and with no configured source, are supplied from the process's environment, where notes, set as
// well but targeting a variable no entry reads, is not, and the optional ones left out are named unsupplied. No value
// reaches the log or the task topic in the clear.
func TestImplicitReadIsOfCatalogVariablesAlone(t *testing.T) {
	f, receiver := catalogFixture(t)
	t.Setenv(alphaVariable, "alpha-from-the-environment")
	t.Setenv(githubVariable, "github-from-the-environment")
	t.Setenv(notesVariable, "notes-from-the-environment")
	submitted, err := f.host.Submit(f.ctx, catalogTask("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(submitted.Unsupplied, []string{"notes"}) {
		t.Fatalf("unsupplied %v; want notes alone", submitted.Unsupplied)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	if !slices.Equal(task.submission.Wherry.Supplied, []string{"alpha-key", "github-token"}) {
		t.Fatalf("supplied %v; want alpha-key and github-token, read from their catalog variables", task.submission.Wherry.Supplied)
	}
	_, values, err := open(t, f, submitted.RunID, f.poolKeys["pool-c"])
	if err != nil {
		t.Fatal(err)
	}
	if string(values.Credentials["alpha-key"]) != "alpha-from-the-environment" || string(values.Credentials["github-token"]) != "github-from-the-environment" || len(values.Credentials) != 2 {
		t.Fatalf("enveloped %v; want the two variables' values and nothing for notes", slices.Sorted(maps.Keys(values.Credentials)))
	}
	f.noValueLeaks(t, "alpha-from-the-environment", "github-from-the-environment", "notes-from-the-environment")
	// host receivers reports the variable-resolved credentials configured and ready, and notes missing.
	receivers, err := f.host.Receivers(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	profile := receivers[slices.IndexFunc(receivers, func(r Receiver) bool { return r.ReceiverID == "receiver-c" })].Profiles[0]
	want := []Credential{
		{Name: "alpha-key", Env: alphaVariable, Description: "Alpha", Configured: true, Source: SourceVariable, Variable: alphaVariable, Status: StatusReady},
		{Name: "github-token", Env: githubVariable, Description: "GitHub token", Optional: true, Configured: true, Source: SourceVariable, Variable: githubVariable, Status: StatusReady},
		{Name: "notes", Env: notesVariable, Optional: true, Status: StatusMissing},
	}
	if !slices.Equal(profile.Credentials, want) {
		t.Fatalf("receivers lists %+v; want %+v", profile.Credentials, want)
	}
}

// A source the configuration names wins over the credential's variable, whatever its kind, and is never passed over for
// the variable when it fails.
func TestAnExplicitSourceWinsOverTheVariable(t *testing.T) {
	f, _ := catalogFixture(t)
	t.Setenv(alphaVariable, "alpha-from-the-environment")
	t.Setenv("WHERRY_TEST_OTHER", "alpha-from-another-variable")
	file := filepath.Join(t.TempDir(), "alpha")
	if err := os.WriteFile(file, []byte("alpha-from-a-file"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		source credential.Source
		want   string
	}{
		"file":    {credential.Source{File: file}, "alpha-from-a-file"},
		"env":     {credential.Source{Env: "WHERRY_TEST_OTHER"}, "alpha-from-another-variable"},
		"command": {credential.Source{Command: []string{"sh", "-c", "printf alpha-from-a-command"}}, "alpha-from-a-command"},
	} {
		t.Run(name, func(t *testing.T) {
			f.host.credentials["alpha-key"] = test.source
			submitted, err := f.host.Submit(f.ctx, catalogTask("alpha"))
			if err != nil {
				t.Fatal(err)
			}
			if _, values, err := open(t, f, submitted.RunID, f.poolKeys["pool-c"]); err != nil || string(values.Credentials["alpha-key"]) != test.want {
				t.Fatalf("enveloped %q, %v; want the configured source's %q over the variable", values.Credentials["alpha-key"], err, test.want)
			}
		})
	}
	published := f.published(t)
	f.host.credentials["alpha-key"] = credential.Source{Command: []string{"sh", "-c", "exit 3"}}
	_, err := f.host.Submit(f.ctx, catalogTask("alpha"))
	if err == nil || !strings.Contains(err.Error(), "resolve credential alpha-key: its command exited with status 3") {
		t.Fatalf("submit = %v; want the explicit source's failure, not the variable", err)
	}
	if f.published(t) != published {
		t.Fatal("published a task whose explicit source failed")
	}
}

// An optional credential the task names must resolve: with its catalog variable unset the submit fails before anything
// is published, naming the variable, where unnamed it is simply left out.
func TestANamedOptionalCredentialMustResolve(t *testing.T) {
	f, _ := catalogFixture(t)
	t.Setenv(m1Variable, "m1-from-the-environment")
	submitted, err := f.host.Submit(f.ctx, catalogTask("multi"))
	if err != nil || !slices.Equal(submitted.Unsupplied, []string{"m2", "m3", "github-token"}) {
		t.Fatalf("submit = %+v, %v; want m2, m3 and github-token left out", submitted, err)
	}
	_, err = f.host.Submit(f.ctx, catalogTask("multi", "m2"))
	want := "auth required: credential m2 has no source: " + m2Variable + " is unset in the process that submits, and this host's configuration names no source for it"
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), want) {
		t.Fatalf("submit naming m2 = %v; want %q", err, want)
	}
	if f.published(t) != 1 {
		t.Fatal("published a task whose named credential did not resolve")
	}
}

// A required credential whose catalog variable is unset refuses the submit before any record is produced, naming the
// variable to set and the configuration as the alternative, as when host init saw the variable and a later process
// lacks it.
func TestARequiredCredentialWithItsVariableUnsetFailsBeforePublishing(t *testing.T) {
	f, _ := catalogFixture(t)
	_, err := f.host.Submit(f.ctx, catalogTask("alpha"))
	want := "auth required: credential alpha-key has no source: " + alphaVariable + " is unset in the process that submits, and this host's configuration names no source for it"
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), want) {
		t.Fatalf("submit = %v; want %q", err, want)
	}
	if f.published(t) != 0 {
		t.Fatal("published a task whose required credential had no source")
	}
	if entries, err := os.ReadDir(filepath.Join(f.stateDir, "runs")); err == nil && len(entries) != 0 {
		t.Fatalf("%d local run records; want none", len(entries))
	}
}

// An agent whose name is a catalog entry reading several variables needs one of the terms credentials on those
// variables: with none set the submit is refused before publishing, naming them and their variables; with one set that
// one alone is supplied. Another credential of the agent, optional or required, neither satisfies nor disables the
// check, and only the terms targets the host's entry reads count, so a host whose entry shares no variable with the
// terms skips it.
func TestAtLeastOneModelCredentialMustResolve(t *testing.T) {
	refused := func(t *testing.T, f *fixture, published int) {
		t.Helper()
		_, err := f.host.Submit(f.ctx, catalogTask("multi"))
		want := "auth required: agent multi needs one of its model credentials m1, m2, m3: set one of " + m1Variable + ", " + m2Variable + ", " + m3Variable + " in the process that submits"
		if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), want) {
			t.Fatalf("submit = %v; want %q", err, want)
		}
		if f.published(t) != published {
			t.Fatal("published a task of an agent with no model credential")
		}
	}
	t.Run("none set", func(t *testing.T) {
		f, _ := catalogFixture(t)
		refused(t, f, 0)
	})
	t.Run("only the tool's variable set", func(t *testing.T) {
		f, _ := catalogFixture(t)
		t.Setenv(githubVariable, "github-from-the-environment")
		refused(t, f, 0)
	})
	t.Run("one set", func(t *testing.T) {
		f, receiver := catalogFixture(t)
		t.Setenv(m3Variable, "m3-from-the-environment")
		submitted, err := f.host.Submit(f.ctx, catalogTask("multi"))
		if err != nil {
			t.Fatal(err)
		}
		if task := receiver.claim(t, f.ctx, submitted.RunID)[0]; !slices.Equal(task.submission.Wherry.Supplied, []string{"m3"}) {
			t.Fatalf("supplied %v; want m3 alone", task.submission.Wherry.Supplied)
		}
		if _, values, err := open(t, f, submitted.RunID, f.poolKeys["pool-c"]); err != nil || !slices.Equal(slices.Sorted(maps.Keys(values.Credentials)), []string{"m3"}) {
			t.Fatalf("enveloped %v, %v; want m3 alone", slices.Sorted(maps.Keys(values.Credentials)), err)
		}
	})
	t.Run("a required credential beside them", func(t *testing.T) {
		f, _ := catalogFixture(t)
		terms := catalogTerms()
		terms.Agents[1].Credentials = append(terms.Agents[1].Credentials, operator.AgentCredential{TaskCredential: wire.TaskCredential{Name: "tool-key", Env: "WHERRY_TEST_TOOL"}})
		if err := f.service.PoolApply(f.ctx, "pool-c", terms, false); err != nil {
			t.Fatal(err)
		}
		f.host.credentials["tool-key"] = credential.Source{File: f.secret}
		refused(t, f, 0)
	})
	t.Run("a host entry reading other variables", func(t *testing.T) {
		f, _ := catalogFixture(t)
		// The host's snapshot has multi reading m1, m2 and a variable the terms lack: m3 is then no catalog variable
		// here, so it is not read, and only m1 and m2 count.
		f.host.catalog = testCatalog(t, m1Variable, m2Variable, "WHERRY_TEST_M9")
		t.Setenv(m3Variable, "m3-from-the-environment")
		_, err := f.host.Submit(f.ctx, catalogTask("multi"))
		if want := "agent multi needs one of its model credentials m1, m2: set one of " + m1Variable + ", " + m2Variable; !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), want) {
			t.Fatalf("submit = %v; want %q", err, want)
		}
	})
	t.Run("a host entry sharing no variable", func(t *testing.T) {
		f, receiver := catalogFixture(t)
		f.host.catalog = testCatalog(t, "WHERRY_TEST_M8", "WHERRY_TEST_M9")
		submitted, err := f.host.Submit(f.ctx, catalogTask("multi"))
		if err != nil {
			t.Fatalf("submit = %v; want the check skipped and delivery as the terms say", err)
		}
		if task := receiver.claim(t, f.ctx, submitted.RunID)[0]; len(task.submission.Wherry.Supplied) != 0 {
			t.Fatalf("supplied %v; want nothing", task.submission.Wherry.Supplied)
		}
	})
}

// counting is a command source whose runs count in a file, and the count.
func counting(t *testing.T) (credential.Source, func() int) {
	t.Helper()
	counter := filepath.Join(t.TempDir(), "runs")
	source := credential.Source{Command: []string{"sh", "-c", `n=$(cat "$1" 2>/dev/null || echo 0); echo $((n+1)) > "$1"; printf value-from-the-command`, "sh", counter}}
	return source, func() int {
		data, err := os.ReadFile(counter)
		if err != nil {
			return 0
		}
		n := 0
		for _, c := range strings.TrimSpace(string(data)) {
			n = n*10 + int(c-'0')
		}
		return n
	}
}

// A submission runs each distinct source once: the source alpha-key and both storage keys share runs once for all three,
// its bytes serving selection, the envelope and the storage client alike; a second submission runs it again.
func TestASourceRunsOncePerSubmission(t *testing.T) {
	f, _ := catalogFixture(t)
	source, runs := counting(t)
	f.host.credentials["alpha-key"], f.host.credentials["s3-access"], f.host.credentials["s3-secret"] = source, source, source
	f.host.storage = map[string]StorageConfig{"pool-c": {AccessKey: "s3-access", SecretKey: "s3-secret"}}
	agent := catalogTerms().Agents[0]
	resolver := newResolver(f.ctx, wire.RFC3339(time.Now().Add(time.Minute).Unix()))
	supplied, _, err := f.host.supplied(agent, nil, resolver)
	if err != nil || !slices.Equal(supplied, []string{"alpha-key"}) {
		t.Fatalf("supplied %v, %v", supplied, err)
	}
	if _, err := f.host.bucket(f.ctx, "pool-c", &operator.Storage{Endpoint: "https://s3.example.test", Bucket: "bucket"}, resolver); err != nil {
		t.Fatal(err)
	}
	if runs() != 1 {
		t.Fatalf("the shared source ran %d times for one submission; want once", runs())
	}
	submitted, err := f.host.Submit(f.ctx, catalogTask("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	if runs() != 2 {
		t.Fatalf("the source ran %d times after a submit; want once more", runs())
	}
	if _, values, err := open(t, f, submitted.RunID, f.poolKeys["pool-c"]); err != nil || string(values.Credentials["alpha-key"]) != "value-from-the-command" {
		t.Fatalf("enveloped %q, %v", values.Credentials["alpha-key"], err)
	}
}

// Listing receivers runs no command: a command source is reported configured and unchecked, its command never run.
func TestReceiversRunsNoCommandSource(t *testing.T) {
	f, _ := catalogFixture(t)
	source, runs := counting(t)
	f.host.credentials["alpha-key"] = source
	receivers, err := f.host.Receivers(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	profile := receivers[slices.IndexFunc(receivers, func(r Receiver) bool { return r.ReceiverID == "receiver-c" })].Profiles[0]
	if c := profile.Credentials[0]; c.Name != "alpha-key" || !c.Configured || c.Source != SourceCommand || c.Status != StatusUnchecked {
		t.Fatalf("receivers lists %+v; want alpha-key configured, unchecked", c)
	}
	if runs() != 0 {
		t.Fatalf("receivers ran the command source %d times", runs())
	}
}

// keyLost submits input through receiver and ends the run key-lost after the receiver claims it.
func (f *fixture) keyLost(t *testing.T, receiver *testReceiver, input SubmitInput) SubmitResult {
	t.Helper()
	submitted, err := f.host.Submit(f.ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	receiver.keyLost(t, f.ctx, task)
	return submitted
}

// A resubmission resolves afresh: an optional credential read from a catalog variable at submit is left out when the
// variable is unset at resubmit, while an explicit optional source that now fails is an error that leaves the run
// without a replacement until a later pass.
func TestAReplacementReresolvesCatalogVariablesAndExplicitSources(t *testing.T) {
	t.Run("a variable since unset is left out", func(t *testing.T) {
		f, receiver := catalogFixture(t)
		t.Setenv(alphaVariable, "alpha-from-the-environment")
		t.Setenv(githubVariable, "github-from-the-environment")
		submitted := f.keyLost(t, receiver, catalogTask("alpha"))
		t.Setenv(githubVariable, "")
		status, err := f.host.Status(f.ctx, submitted.RunID)
		if err != nil || status.Replacement == "" {
			t.Fatalf("status %+v, %v; want a replacement; logs:\n%s", status, err, f.logs.String())
		}
		replacement, values, err := open(t, f, status.Replacement, f.poolKeys["pool-c"])
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(replacement.Wherry.Supplied, []string{"alpha-key"}) || !slices.Equal(slices.Sorted(maps.Keys(values.Credentials)), []string{"alpha-key"}) {
			t.Fatalf("replacement supplied %v and enveloped %v; want alpha-key alone, github-token's variable being unset", replacement.Wherry.Supplied, slices.Sorted(maps.Keys(values.Credentials)))
		}
	})
	t.Run("an explicit source that fails is an error", func(t *testing.T) {
		f, receiver := catalogFixture(t)
		t.Setenv(alphaVariable, "alpha-from-the-environment")
		// The command succeeds once and fails after.
		once := filepath.Join(t.TempDir(), "once")
		f.host.credentials["github-token"] = credential.Source{Command: []string{"sh", "-c", `test ! -e "$1" && touch "$1" && printf github-once`, "sh", once}}
		submitted := f.keyLost(t, receiver, catalogTask("alpha"))
		status, err := f.host.Status(f.ctx, submitted.RunID)
		if err != nil || status.Replacement != "" {
			t.Fatalf("status %+v, %v; want no replacement", status, err)
		}
		if logs := f.logs.String(); !strings.Contains(logs, "resubmit: resolve credential github-token: its command exited with status 1") {
			t.Fatalf("logs:\n%s\nwant the resubmission refused on the explicit source's failure", logs)
		}
		if replacements := f.replacements(t, "pool-c", submitted.RunID); len(replacements) != 0 {
			t.Fatalf("%d replacements published", len(replacements))
		}
	})
}

// After a host restart, the markers of a credential the run supplied from a catalog variable, which the configuration
// never named, translate and count: the persisted submission's supplied names give them.
func TestMarkersOfAnImplicitlySuppliedCredentialSurviveARestart(t *testing.T) {
	f, receiver := catalogFixture(t)
	t.Setenv(alphaVariable, "alpha-from-the-environment")
	submitted, err := f.host.Submit(f.ctx, catalogTask("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	_, values, err := open(t, f, submitted.RunID, f.poolKeys["pool-c"])
	if err != nil {
		t.Fatal(err)
	}
	marker := signed.RedactionMarker(values.RedactionKey, "alpha-key")
	receiver.feed(t, f.ctx, submitted.RunID,
		receiver.echo(t, task),
		receiver.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) {
			result.Task.Artifacts[0].Parts = []wire.Part{{Text: "printed " + marker + " twice " + marker}}
		}),
		receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	// The restarted host has the same key and state directory, and still no configured source.
	restarted, err := NewHost(f.host.kafka, f.host.key, operator.TenantPrincipal("tenant-a", "host-a"), f.stateDir, map[string]credential.Source{}, nil, log.New(f.logs, "restarted: ", 0))
	if err != nil {
		t.Fatal(err)
	}
	restarted.catalog = f.host.catalog
	awaited, err := restarted.Await(context.Background(), submitted.RunID, 10*time.Second)
	if err != nil || awaited.Task == nil {
		t.Fatalf("await %#v, %v", awaited, err)
	}
	if text := awaited.Task.Artifacts[0].Parts[0].Text; text != "printed [redacted:alpha-key] twice [redacted:alpha-key]" || !maps.Equal(awaited.Redacted, map[string]int{"alpha-key": 2}) {
		t.Fatalf("await shows %q counting %v; want alpha-key's markers named and counted after the restart", text, awaited.Redacted)
	}
}
