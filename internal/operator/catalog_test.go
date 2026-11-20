package operator

import (
	"context"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/catalog"
	"github.com/benemon/wherry/internal/wire"
)

func optional() *bool {
	value := false
	return &value
}

// catalogTerms are terms with a claude-code agent, a google agent and one outside the catalog with its own credential.
func catalogTerms() Terms {
	terms := modelTerms()
	terms.Agents = append([]AgentTerms{{Name: "claude-code", Credentials: []AgentCredential{}}, {Name: "google", Credentials: []AgentCredential{}}}, terms.Agents...)
	terms.DefaultAgent = "claude-code"
	return terms
}

var (
	claudeCodeCredentials = []AgentCredential{{TaskCredential: wire.TaskCredential{Name: "claude-code-oauth-token", Env: "CLAUDE_CODE_OAUTH_TOKEN"}, Description: "Claude Code"}}
	googleCredentials     = []AgentCredential{
		{TaskCredential: wire.TaskCredential{Name: "google-api-key", Env: "GOOGLE_API_KEY"}, Description: "Google", Required: optional()},
		{TaskCredential: wire.TaskCredential{Name: "google-generative-ai-api-key", Env: "GOOGLE_GENERATIVE_AI_API_KEY"}, Description: "Google", Required: optional()},
		{TaskCredential: wire.TaskCredential{Name: "gemini-api-key", Env: "GEMINI_API_KEY"}, Description: "Google", Required: optional()},
	}
)

// claude-code gives one required claude-code-oauth-token targeting CLAUDE_CODE_OAUTH_TOKEN; google gives three optional
// credentials named after its three variables; beside each other in one pool each agent gets only its own, and an
// agent outside the catalog keeps the list the operator wrote.
func TestExpandGivesEachCatalogAgentItsOwnCredentials(t *testing.T) {
	c, err := catalog.Embedded()
	if err != nil {
		t.Fatal(err)
	}
	expanded, err := Expand(catalogTerms(), c)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]AgentCredential{"claude-code": claudeCodeCredentials, "google": googleCredentials, "agent-a": modelTerms().Agents[0].Credentials}
	got := map[string][]AgentCredential{}
	for _, agent := range expanded.Agents {
		got[agent.Name] = agent.Credentials
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expanded credentials %+v; want %+v", got, want)
	}
	for _, agent := range expanded.Agents {
		for _, credential := range agent.Credentials {
			if agent.Name == "google" && !credential.Optional() || agent.Name != "google" && credential.Optional() {
				t.Errorf("agent %s credential %s optional %t", agent.Name, credential.Name, credential.Optional())
			}
		}
	}
	if err := validateTerms(withDefaults(expanded)); err != nil {
		t.Errorf("expanded terms: %v", err)
	}
	// Expansion leaves the terms it was given alone.
	if original := catalogTerms(); len(original.Agents[0].Credentials) != 0 {
		t.Error("the original terms changed")
	}
}

// A catalog agent may list further credentials, kept after the derived ones; one that repeats a derived name or target
// is refused naming it, and the pool's uniqueness checks run across both.
func TestExpandKeepsExtraCredentialsAndRefusesRepeats(t *testing.T) {
	c, err := catalog.Embedded()
	if err != nil {
		t.Fatal(err)
	}
	extra := AgentCredential{TaskCredential: wire.TaskCredential{Name: "github-token", Env: "GH_TOKEN"}, Required: optional()}
	terms := catalogTerms()
	terms.Agents[0].Credentials = []AgentCredential{extra}
	expanded, err := Expand(terms, c)
	if err != nil {
		t.Fatal(err)
	}
	if want := append(append([]AgentCredential{}, claudeCodeCredentials...), extra); !reflect.DeepEqual(expanded.Agents[0].Credentials, want) {
		t.Errorf("claude-code credentials %+v; want %+v", expanded.Agents[0].Credentials, want)
	}
	for _, repeat := range []wire.TaskCredential{{Name: "claude-code-oauth-token", Env: "TOKEN"}, {Name: "token", Env: "CLAUDE_CODE_OAUTH_TOKEN"}} {
		terms := catalogTerms()
		terms.Agents[0].Credentials = []AgentCredential{{TaskCredential: repeat}}
		if _, err := Expand(terms, c); err == nil || !strings.Contains(err.Error(), "agent claude-code: credential "+repeat.Name+" is one the credential catalog derives") {
			t.Errorf("repeat %+v: %v", repeat, err)
		}
	}
	// Two extra credentials sharing a target fail the terms as they always did.
	terms = catalogTerms()
	terms.Agents[0].Credentials = []AgentCredential{extra, {TaskCredential: wire.TaskCredential{Name: "deploy", Env: "GH_TOKEN"}}}
	expanded, err = Expand(terms, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateTerms(withDefaults(expanded)); err == nil || !strings.Contains(err.Error(), "share a target") {
		t.Errorf("shared target: %v", err)
	}
}

// pool apply records the expanded terms, and applying the same file again finds them unchanged.
func TestPoolApplyRecordsExpandedTerms(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	for range 2 {
		if err := service.PoolApply(ctx, "pool-a", catalogTerms(), false); err != nil {
			t.Fatal(err)
		}
	}
	state, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recorded := state.Pools["pool-a"]
	got := map[string][]AgentCredential{}
	for _, agent := range recorded.Agents {
		got[agent.Name] = agent.Credentials
	}
	want := map[string][]AgentCredential{"claude-code": claudeCodeCredentials, "google": googleCredentials, "agent-a": modelTerms().Agents[0].Credentials}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("recorded credentials %+v; want %+v", got, want)
	}
	terms := catalogTerms()
	terms.Agents[0].Credentials = []AgentCredential{{TaskCredential: wire.TaskCredential{Name: "claude-code-oauth-token", Env: "TOKEN"}}}
	if err := service.PoolApply(ctx, "pool-a", terms, true); err == nil || !strings.Contains(err.Error(), "credential catalog derives") {
		t.Errorf("repeated derived credential: %v", err)
	}
}

// An operator adopting catalog expansion renames an agent in stages, since pool apply refuses terms under which a
// live receiver's profile names no agent: the catalog-named agent is added beside the old one, each receiver's
// profiles move to it through receiver apply, and the old agent goes in a further apply. The single-step rename is
// refused, in a dry run and in an apply, and leaves the terms and the receiver as they were.
func TestPoolApplyRenamesAnAgentToItsCatalogIdInStages(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	old := AgentTerms{Name: "claude", Credentials: []AgentCredential{{TaskCredential: wire.TaskCredential{Name: "claude-oauth-token", Env: "CLAUDE_CODE_OAUTH_TOKEN"}}}}
	catalogNamed := AgentTerms{Name: "claude-code", Credentials: []AgentCredential{}}
	terms := modelTerms()
	terms.Agents = []AgentTerms{old}
	terms.DefaultAgent = "claude"
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	config := ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Profiles: []ProfileSettings{{Name: "container", Plugin: "fake", Agent: "claude", Config: map[string]any{"image": "claude-agent:1.0"}}},
		PluginSHA256: map[string]string{"fake": strings.Repeat("ab", 32)}}
	if err := service.ReceiverAdd(ctx, config, service.Broker, filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	agents := func() (map[string][]AgentCredential, string, ReceiverConfig) {
		t.Helper()
		state, err := service.LoadState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string][]AgentCredential{}
		for _, agent := range state.Pools["pool-a"].Agents {
			got[agent.Name] = agent.Credentials
		}
		return got, state.Pools["pool-a"].DefaultAgent, state.Receivers["receiver-a"]
	}

	// The rename in one step, as the terms would read after editing the name in place.
	renamed := terms
	renamed.Agents = []AgentTerms{catalogNamed}
	renamed.DefaultAgent = "claude-code"
	for _, dryRun := range []bool{true, false} {
		err := service.PoolApply(ctx, "pool-a", renamed, dryRun)
		if err == nil || err.Error() != "pool terms would strand live receivers: receiver receiver-a plugin fake profile container: receiver agent is outside pool terms" {
			t.Fatalf("pool apply renaming claude to claude-code in one step, dry run %t = %v", dryRun, err)
		}
	}
	if got, defaultAgent, recorded := agents(); !reflect.DeepEqual(got, map[string][]AgentCredential{"claude": old.Credentials}) || defaultAgent != "claude" || recorded.Profiles[0].Agent != "claude" {
		t.Fatalf("after the refused rename: agents %+v default %s receiver %+v; want claude alone and the receiver on it", got, defaultAgent, recorded)
	}

	// Stage one: the catalog-named agent beside the old one, which keeps its explicit credential.
	both := terms
	both.Agents = []AgentTerms{old, catalogNamed}
	if err := service.PoolApply(ctx, "pool-a", both, false); err != nil {
		t.Fatalf("pool apply adding claude-code beside claude = %v", err)
	}
	if got, _, recorded := agents(); !reflect.DeepEqual(got, map[string][]AgentCredential{"claude": old.Credentials, "claude-code": claudeCodeCredentials}) || recorded.Profiles[0].Agent != "claude" {
		t.Fatalf("after stage one: agents %+v receiver %+v; want both agents, claude-code expanded, and the receiver still on claude", got, recorded)
	}

	// Stage two: the receiver's profile moves to the new agent, its deployment settings with it.
	moved := config
	moved.Profiles = []ProfileSettings{{Name: "container", Plugin: "fake", Agent: "claude-code", Config: map[string]any{"image": "claude-code-agent:1.0"}}}
	if _, err := service.ReceiverUpdate(ctx, moved, true); err != nil {
		t.Fatalf("receiver apply --dry-run moving the profile to claude-code = %v", err)
	}
	recorded, err := service.ReceiverUpdate(ctx, moved, false)
	if err != nil {
		t.Fatalf("receiver apply moving the profile to claude-code = %v", err)
	}
	if recorded.Profiles[0].Agent != "claude-code" || recorded.Profiles[0].Config["image"] != "claude-code-agent:1.0" {
		t.Fatalf("recorded profile %+v; want agent claude-code with its new settings", recorded.Profiles[0])
	}
	topics, err := service.taskTopics(ctx, "pool-a")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(topics, SignatureTopic("pool-a", "claude-code", nil)) {
		t.Fatalf("task topics %v; want the moved profile's signature topic", topics)
	}

	// Stage three: the old agent goes, and the default moves with it.
	for _, dryRun := range []bool{true, false} {
		if err := service.PoolApply(ctx, "pool-a", renamed, dryRun); err != nil {
			t.Fatalf("pool apply removing claude, dry run %t = %v", dryRun, err)
		}
	}
	if got, defaultAgent, recorded := agents(); !reflect.DeepEqual(got, map[string][]AgentCredential{"claude-code": claudeCodeCredentials}) || defaultAgent != "claude-code" || recorded.Profiles[0].Agent != "claude-code" {
		t.Fatalf("after stage three: agents %+v default %s receiver %+v; want claude-code alone", got, defaultAgent, recorded)
	}
}

// Every usable id of the embedded catalog expands into terms pool apply accepts, so no entry derives a credential a
// pool refuses, and its description is one the terms allow.
func TestEveryUsableEmbeddedIdExpandsIntoValidTerms(t *testing.T) {
	c, err := catalog.Embedded()
	if err != nil {
		t.Fatal(err)
	}
	usable := 0
	for _, id := range c.IDs() {
		if !c.Usable(id) {
			continue
		}
		usable++
		terms := modelTerms()
		terms.Agents = []AgentTerms{{Name: id, Credentials: []AgentCredential{}}}
		terms.DefaultAgent = id
		expanded, err := Expand(terms, c)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateTerms(withDefaults(expanded)); err != nil {
			t.Errorf("%s: %v", id, err)
		}
	}
	if usable < 300 {
		t.Errorf("%d usable ids", usable)
	}
}
