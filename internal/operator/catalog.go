package operator

import (
	"fmt"
	"slices"

	"github.com/benemon/wherry/internal/catalog"
)

// Expand gives each agent whose name is a usable catalog id the credentials the entry derives, before the agent's own,
// each agent on its own: an agent whose name is no usable id keeps its list as written. The operator writes no model
// credential for a catalog agent, and may list further credentials; one that repeats a derived credential's name or
// target is an error, so a pool file written before the agent's name was in the catalog is refused rather than
// doubled.
func Expand(terms Terms, c *catalog.Catalog) (Terms, error) {
	terms.Agents = slices.Clone(terms.Agents)
	for i, agent := range terms.Agents {
		derived, ok := c.Expand(agent.Name)
		if !ok {
			continue
		}
		credentials := make([]AgentCredential, 0, len(derived)+len(agent.Credentials))
		for _, credential := range derived {
			// A required credential leaves Required unset, as an operator writes one.
			var required *bool
			if !credential.Required {
				optional := false
				required = &optional
			}
			credentials = append(credentials, AgentCredential{TaskCredential: credential.TaskCredential(), Description: credential.Description, Required: required})
		}
		for _, credential := range agent.Credentials {
			for _, other := range derived {
				if credential.Name == other.Name || credential.Env == other.Env {
					return terms, fmt.Errorf("agent %s: credential %s is one the credential catalog derives for the agent; remove it from the agent's credentials", agent.Name, credential.Name)
				}
			}
			credentials = append(credentials, credential)
		}
		terms.Agents[i].Credentials = credentials
	}
	return terms, nil
}

// expand is Expand with the embedded catalog.
func expand(terms Terms) (Terms, error) {
	c, err := catalog.Embedded()
	if err != nil {
		return terms, err
	}
	return Expand(terms, c)
}
