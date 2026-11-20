package operator

import (
	"context"
	"reflect"
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
