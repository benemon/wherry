// Package catalog is the credential catalog: which environment variables each model provider and agent harness reads,
// so that an agent named by a catalog id needs no credential entry in its pool's terms. The data is a committed snapshot
// of models.dev's providers, models.dev.json, and wherry's own harness entries, harness.json, both embedded; nothing is
// fetched at build or run time. go run ./internal/catalog/update refreshes the snapshot.
package catalog

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/benemon/wherry/internal/wire"
)

//go:embed models.dev.json harness.json
var files embed.FS

// Entry is one catalog entry, in the shape of a models.dev provider without its models: the id that names it, its
// display name, the environment variables it reads and the page documenting them. The files keep other fields, which
// wherry does not read.
type Entry struct {
	ID   string   `json:"id"`
	Name string   `json:"name"`
	Env  []string `json:"env"`
	Doc  string   `json:"doc"`
}

// Credential is a terms credential an entry derives: one per variable the entry reads, named after the variable, with
// the entry's name as its description. An entry reading one variable derives a required credential; one reading
// several derives an optional credential per variable, since wherry never picks one of them.
type Credential struct {
	Name, Env, Description string
	Required               bool
}

// TaskCredential is the credential as a task carries it.
func (c Credential) TaskCredential() wire.TaskCredential {
	return wire.TaskCredential{Name: c.Name, Env: c.Env}
}

// Catalog is the union of the catalog files, each id in exactly one of them.
type Catalog struct {
	entries map[string]Entry
}

// Parse reads catalog files, each a JSON object from id to entry, into their union. An id in two files, or an entry
// whose id is not its key, is an error.
func Parse(files ...[]byte) (*Catalog, error) {
	catalog := &Catalog{entries: map[string]Entry{}}
	for _, file := range files {
		var entries map[string]Entry
		if err := json.Unmarshal(file, &entries); err != nil {
			return nil, fmt.Errorf("catalog: %w", err)
		}
		for id, entry := range entries {
			if entry.ID != id {
				return nil, fmt.Errorf("catalog: entry %q has id %q", id, entry.ID)
			}
			if _, duplicate := catalog.entries[id]; duplicate {
				return nil, fmt.Errorf("catalog: id %s is in two files", id)
			}
			catalog.entries[id] = entry
		}
	}
	return catalog, nil
}

var embedded = sync.OnceValues(func() (*Catalog, error) {
	var contents [][]byte
	for _, name := range []string{"models.dev.json", "harness.json"} {
		data, err := files.ReadFile(name)
		if err != nil {
			return nil, err
		}
		contents = append(contents, data)
	}
	return Parse(contents...)
})

// Embedded is the catalog this binary carries.
func Embedded() (*Catalog, error) { return embedded() }

// Entry is the entry id names.
func (catalog *Catalog) Entry(id string) (Entry, bool) {
	entry, ok := catalog.entries[id]
	return entry, ok
}

// IDs are the catalog's ids, sorted.
func (catalog *Catalog) IDs() []string {
	return slices.Sorted(func(yield func(string) bool) {
		for id := range catalog.entries {
			if !yield(id) {
				return
			}
		}
	})
}

// Reads reports whether any entry reads the variable.
func (catalog *Catalog) Reads(variable string) bool {
	for _, entry := range catalog.entries {
		if slices.Contains(entry.Env, variable) {
			return true
		}
	}
	return false
}

// Expand is the credentials the entry id derives, and whether id is usable: an agent named by a usable id gets them in
// place of the credential entries an operator writes. An id that names no entry, or one that is not usable, expands
// to nothing.
func (catalog *Catalog) Expand(id string) ([]Credential, bool) {
	entry, ok := catalog.entries[id]
	if !ok {
		return nil, false
	}
	credentials := entry.Credentials()
	if !wire.ValidName(id) || len(credentials) == 0 {
		return nil, false
	}
	targets := make([]wire.TaskCredential, 0, len(credentials))
	for _, credential := range credentials {
		targets = append(targets, credential.TaskCredential())
	}
	if wire.ValidateCredentials(targets) != nil {
		return nil, false
	}
	return credentials, true
}

// Usable reports whether id can name an agent: it is in the catalog, follows the grammar of the names wherry operator
// issues, and every credential it derives is one a pool's terms may carry.
func (catalog *Catalog) Usable(id string) bool {
	_, ok := catalog.Expand(id)
	return ok
}

// Credentials are the credentials the entry derives, before the checks Expand makes.
func (entry Entry) Credentials() []Credential {
	credentials := make([]Credential, 0, len(entry.Env))
	for _, variable := range entry.Env {
		credentials = append(credentials, Credential{Name: DerivedName(variable), Env: variable, Description: entry.Name, Required: len(entry.Env) == 1})
	}
	return credentials
}

// DerivedName is the name of the credential derived from an environment variable: the variable in lower case with
// each _ replaced by -, so CLAUDE_CODE_OAUTH_TOKEN gives claude-code-oauth-token.
func DerivedName(variable string) string {
	return strings.ToLower(strings.ReplaceAll(variable, "_", "-"))
}

// Encode writes entries as a catalog file: the ids sorted, each entry's keys sorted, indented by two spaces, with no
// HTML escaping and a final line feed, so that two writes of the same data are the same bytes. Entries are the raw
// objects, so fields wherry does not read are kept.
func Encode(entries map[string]map[string]any) ([]byte, error) {
	if entries == nil {
		return nil, errors.New("catalog: no entries")
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(entries); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Strip decodes a models.dev api.json, or a catalog file, into its entries with each one's models removed. Numbers
// keep their written form.
func Strip(data []byte) (map[string]map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var entries map[string]map[string]any
	if err := decoder.Decode(&entries); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, errors.New("trailing data after the JSON object")
	}
	for _, entry := range entries {
		delete(entry, "models")
	}
	return entries, nil
}
