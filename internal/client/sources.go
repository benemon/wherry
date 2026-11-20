package client

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/benemon/wherry/internal/catalog"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
)

// Status values discovery gives one of an agent's credentials on a host. Discovery runs no command: it says where the
// value would come from and, where that is cheap to know, whether it is there.
const (
	// StatusReady is a file source, or a variable that is set: an explicit env source's, or the credential's catalog
	// variable in this process.
	StatusReady = "ready"
	// StatusUnchecked is a command source, which only a submit runs.
	StatusUnchecked = "unchecked"
	// StatusUnset is an explicit env source whose variable is unset or empty; a submit fails on it, as on any explicit
	// source that fails, and never falls back to the credential's own variable.
	StatusUnset = "unset"
	// StatusMissing is no source at all: no entry in the configuration, and no catalog variable to read.
	StatusMissing = "missing"
)

// Source kinds discovery reports.
const (
	SourceFile    = "file"
	SourceEnv     = "env"
	SourceCommand = "command"
	// SourceVariable is the credential's own target variable, read in the process that submits because the variable is
	// in the host's catalog; the configuration names nothing.
	SourceVariable = "variable"
)

// agentSource is one of an agent's credentials with where this host takes its value: the explicit source its
// configuration names for the credential's name, else, when the credential's target is an environment variable that
// appears in the host's catalog, that variable in this process. Any other environment target and every file target
// has no source on this host without an explicit one, so terms an operator writes cannot make a host read a variable
// of their choosing.
type agentSource struct {
	operator.AgentCredential
	source credential.Source
	// explicit says the configuration names the source, which then wins over the variable and whose failure is an
	// error; a variable read implicitly is left out when it is unset.
	explicit   bool
	configured bool
}

// sources are agent's credentials with their sources on a host whose configuration names configured, under catalog c,
// in terms order.
func sources(configured map[string]credential.Source, c *catalog.Catalog, agent operator.AgentTerms) []agentSource {
	result := make([]agentSource, 0, len(agent.Credentials))
	for _, entry := range agent.Credentials {
		s := agentSource{AgentCredential: entry}
		if source, ok := configured[entry.Name]; ok {
			s.source, s.explicit, s.configured = source, true, true
		} else if entry.Env != "" && c != nil && c.Reads(entry.Env) {
			s.source, s.configured = credential.Source{Env: entry.Env}, true
		}
		result = append(result, s)
	}
	return result
}

// sources is sources for this host.
func (h *Host) sources(agent operator.AgentTerms) []agentSource {
	return sources(h.credentials, h.catalog, agent)
}

// kind is the source's kind: SourceFile, SourceEnv or SourceCommand for an explicit source, SourceVariable for the
// catalog variable, "" for none.
func (s agentSource) kind() string {
	switch {
	case !s.configured:
		return ""
	case !s.explicit:
		return SourceVariable
	case s.source.File != "":
		return SourceFile
	case len(s.source.Command) > 0:
		return SourceCommand
	}
	return SourceEnv
}

// status is what discovery says of the source, running no command.
func (s agentSource) status() string {
	switch s.kind() {
	case "":
		return StatusMissing
	case SourceCommand:
		return StatusUnchecked
	case SourceFile:
		return StatusReady
	}
	if os.Getenv(s.source.Env) == "" {
		if s.explicit {
			return StatusUnset
		}
		return StatusMissing
	}
	return StatusReady
}

// variable is the environment variable an env or variable source reads, "" for the other kinds.
func (s agentSource) variable() string {
	if kind := s.kind(); kind == SourceEnv || kind == SourceVariable {
		return s.source.Env
	}
	return ""
}

// resolver resolves the sources of one submission, each distinct source once: the bytes a source gives serve every
// credential and storage key that names it, selection, encryption and storage signing alike. endBy is the task's
// task_ended_by for command sources, empty outside a submission, as when outputs are downloaded.
type resolver struct {
	ctx    context.Context
	endBy  string
	values map[string][]byte
	errs   map[string]error
}

func newResolver(ctx context.Context, endBy string) *resolver {
	return &resolver{ctx: ctx, endBy: endBy, values: map[string][]byte{}, errs: map[string]error{}}
}

// resolve is source's value, read at the first call for its key and remembered, failure included, for the rest; the
// value is held for redaction under name.
func (r *resolver) resolve(name string, source credential.Source) ([]byte, error) {
	key := source.Key()
	if err, failed := r.errs[key]; failed {
		return nil, err
	}
	value, resolved := r.values[key]
	if !resolved {
		var err error
		if r.endBy == "" {
			value, err = source.Resolve(r.ctx)
		} else {
			value, err = source.ResolveForTask(r.ctx, r.endBy)
		}
		if err != nil {
			r.errs[key] = err
			return nil, err
		}
		r.values[key] = value
	}
	hold(name, value)
	return value, nil
}

// supplied selects and resolves, through r, the credentials of agent's terms a task supplies, in terms order, and names
// the optional ones the run goes without. A required credential, and an optional one named in requested, must resolve:
// one without a source fails auth required, and one whose source fails names the cause. Every other optional credential
// is supplied when it resolves: its explicit source failing is an error, as for every credential, while its catalog
// variable unset leaves it out.
func (h *Host) supplied(agent operator.AgentTerms, requested []string, r *resolver) (supplied, unsupplied []string, err error) {
	for _, s := range h.sources(agent) {
		needed := !s.Optional() || slices.Contains(requested, s.Name)
		if !s.configured {
			if needed {
				return nil, nil, fmt.Errorf("%w: auth required: credential %s is not in this host's configuration", ErrInvalidInput, s.Name)
			}
			unsupplied = append(unsupplied, s.Name)
			continue
		}
		if _, err := r.resolve(s.Name, s.source); err != nil {
			switch {
			case !needed && !s.explicit:
				unsupplied = append(unsupplied, s.Name)
				continue
			case !s.explicit:
				return nil, nil, fmt.Errorf("%w: auth required: credential %s has no source: %s is unset in the process that submits, and this host's configuration names no source for it", ErrInvalidInput, s.Name, s.source.Env)
			}
			return nil, nil, fmt.Errorf("resolve credential %s: %w", s.Name, err)
		}
		supplied = append(supplied, s.Name)
	}
	return supplied, unsupplied, nil
}

// modelCredentials are the credentials of agent's terms whose targets are the variables of the catalog entry agent's
// name names, when that entry reads several: the model credentials pool apply derived, found by target. Nil for an
// agent whose name is no such entry, or whose terms share no variable with the host's entry.
func (h *Host) modelCredentials(agent operator.AgentTerms) []operator.AgentCredential {
	if h.catalog == nil {
		return nil
	}
	entry, ok := h.catalog.Entry(agent.Name)
	if !ok || len(entry.Env) < 2 {
		return nil
	}
	var model []operator.AgentCredential
	for _, c := range agent.Credentials {
		if c.Env != "" && slices.Contains(entry.Env, c.Env) {
			model = append(model, c)
		}
	}
	return model
}

// atLeastOne refuses a task of an agent whose model credentials are all optional when supplied holds none of them: the
// run would reach the vendor with no credential at all. Other credentials of the agent neither satisfy nor disable the
// check, and an agent with no model credential in its terms is not checked.
func (h *Host) atLeastOne(agent operator.AgentTerms, supplied []string) error {
	model := h.modelCredentials(agent)
	if len(model) == 0 {
		return nil
	}
	var names, variables []string
	for _, c := range model {
		if !c.Optional() || slices.Contains(supplied, c.Name) {
			return nil
		}
		names, variables = append(names, c.Name), append(variables, c.Env)
	}
	return fmt.Errorf("%w: auth required: agent %s needs one of its model credentials %s: set one of %s in the process that submits, or name a source for one in this host's configuration",
		ErrInvalidInput, agent.Name, strings.Join(names, ", "), strings.Join(variables, ", "))
}

// Readiness is what discovery says of one of an agent's credentials on a host: where its value would come from and
// whether it is there, without running a command.
type Readiness struct {
	Name        string `json:"name"`
	Env         string `json:"env,omitzero"`
	File        string `json:"file,omitzero"`
	Description string `json:"description,omitzero"`
	Optional    bool   `json:"optional,omitzero"`
	// Source is file, env or command for a source the configuration names, variable for the credential's own target
	// variable read in the process that submits because it is in the host's catalog, or empty for none.
	Source string `json:"source,omitzero"`
	// Variable is the environment variable an env or variable source reads.
	Variable string `json:"variable,omitzero"`
	// Status is StatusReady, StatusUnchecked, StatusUnset or StatusMissing.
	Status string `json:"status"`
	// CatalogVariable says the credential's target is a variable in the host's catalog, so the host reads it when it is
	// set and no explicit source is named; host init asks no question for such a credential.
	CatalogVariable bool `json:"catalog_variable,omitzero"`
}

// AgentReadiness is how an agent of a pool's terms stands on a host before any submit, from discovery alone.
type AgentReadiness struct {
	Agent       string
	Credentials []Readiness
	// Ready says every required credential has a source and, when the agent's name is a catalog entry reading several
	// variables, so has at least one of its model credentials; an unchecked command source counts as a source.
	Ready bool
	// Missing names the credentials that keep the agent from being ready: each required one without a source, or every
	// model credential when none has one. A not-ready agent of a catalog entry adds the entry's Doc.
	Missing []Readiness
	Doc     string
}

// readiness is agent's readiness on a host whose configuration names configured, under catalog c.
func readiness(configured map[string]credential.Source, c *catalog.Catalog, agent operator.AgentTerms) AgentReadiness {
	result := AgentReadiness{Agent: agent.Name, Ready: true}
	byName := map[string]Readiness{}
	for _, s := range sources(configured, c, agent) {
		r := Readiness{Name: s.Name, Env: s.Env, File: s.File, Description: s.Description, Optional: s.Optional(), Source: s.kind(), Variable: s.variable(), Status: s.status(),
			CatalogVariable: s.Env != "" && c != nil && c.Reads(s.Env)}
		result.Credentials = append(result.Credentials, r)
		byName[s.Name] = r
		if !s.Optional() && (r.Status == StatusMissing || r.Status == StatusUnset) {
			result.Ready = false
			result.Missing = append(result.Missing, r)
		}
	}
	if c != nil {
		if entry, ok := c.Entry(agent.Name); ok {
			result.Doc = entry.Doc
			if len(entry.Env) > 1 {
				var model []Readiness
				satisfied := false
				for _, s := range agent.Credentials {
					if s.Env == "" || !slices.Contains(entry.Env, s.Env) {
						continue
					}
					r := byName[s.Name]
					model = append(model, r)
					if !s.Optional() || r.Status == StatusReady || r.Status == StatusUnchecked {
						satisfied = true
					}
				}
				if len(model) > 0 && !satisfied {
					result.Ready = false
					result.Missing = append(result.Missing, model...)
				}
			}
		}
	}
	return result
}

// Discover reports how each agent of terms stands on the host config describes, from its configuration and the catalog
// this binary carries, running no command. The result follows terms' agent order.
func Discover(config Config, terms operator.Terms) ([]AgentReadiness, error) {
	c, err := catalog.Embedded()
	if err != nil {
		return nil, err
	}
	result := make([]AgentReadiness, 0, len(terms.Agents))
	for _, agent := range terms.Agents {
		result = append(result, readiness(config.Credentials, c, agent))
	}
	return result, nil
}
