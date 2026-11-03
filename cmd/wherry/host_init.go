package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

// credentialUse is a pool's delivery of a credential to its agents.
type credentialUse struct {
	pool     string
	target   wire.TaskCredential
	optional bool
}

func (use credentialUse) as() string {
	if use.target.File != "" {
		return "file " + use.target.File
	}
	return use.target.Env
}

// hostInit places bundle, when given, at the default bundle path, creates and registers the host key, and gives the
// config at path what the tenant's entitled pools need and it lacks: on a terminal by asking, otherwise by listing it.
// The config holds references to sources, never a value; an entry it has is kept.
func hostInit(ctx context.Context, path, bundle string, stdout, prompts io.Writer) error {
	dir, err := busclient.ConfigDir()
	if err != nil {
		return err
	}
	if path, err = busclient.ExpandHome(path); err != nil {
		return err
	}
	var file busclient.Config
	_, statErr := os.Stat(path)
	changed := errors.Is(statErr, os.ErrNotExist)
	if !changed {
		if err := readYAML(path, &file); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	if bundle != "" {
		contents, err := operator.ReadBundle(bundle)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return err
		}
		// Bundles hold a password: ReplaceBundle writes mode 0600 and renames over the old file.
		if err := operator.ReplaceBundle(filepath.Join(dir, "bundle.yaml"), contents); err != nil {
			return err
		}
		changed = changed || file.Bundle != filepath.Join(dir, "bundle.yaml")
		file.Bundle = filepath.Join(dir, "bundle.yaml")
	} else if file.Bundle == "" {
		return fmt.Errorf("%s names no bundle; run wherry host init BUNDLE", path)
	}
	onTerminal := terminal()
	p := newPrompter(prompts)
	if _, err := os.Stat(filepath.Join(dir, "ca.pem")); file.CAFile == "" && errors.Is(err, os.ErrNotExist) && onTerminal {
		answer, err := p.ask("CA file for the broker's certificate (blank for the system roots)", "")
		if err != nil {
			return err
		}
		if answer != "" {
			if file.CAFile, err = localPath(answer); err != nil {
				return err
			}
			changed = true
		}
	}
	if changed {
		if err := writeHostConfig(path, file, prompts); err != nil {
			return err
		}
	}
	config, err := busclient.LoadConfig(path)
	if err != nil {
		return err
	}
	keyID, pools, err := busclient.Init(ctx, config)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, keyID)
	if len(pools) == 0 {
		fmt.Fprintln(prompts, "this host's tenant is entitled to no pool yet; run wherry host init again once the operator entitles it to one")
		return nil
	}
	changed = false
	var missing []string
	setSource := func(name string, source credential.Source) {
		if file.Credentials == nil {
			file.Credentials = map[string]credential.Source{}
		}
		file.Credentials[name], changed = source, true
	}
	// configured asks for name's source unless the config has one, and reports whether it has one now.
	configured := func(name string, uses []credentialUse) (bool, error) {
		if _, ok := file.Credentials[name]; ok {
			return true, nil
		}
		source, err := askSource(p, name, uses)
		if err != nil || source.Validate() != nil {
			return false, err
		}
		setSource(name, source)
		return true, nil
	}
	poolNames := slices.Sorted(maps.Keys(pools))
	uses := map[string][]credentialUse{}
	var vcsHosts []string
	for _, pool := range poolNames {
		for _, agent := range pools[pool].Agents {
			for _, agentCredential := range agent.Credentials {
				use := credentialUse{pool, agentCredential.TaskCredential, agentCredential.Optional()}
				if !slices.Contains(uses[agentCredential.Name], use) {
					uses[agentCredential.Name] = append(uses[agentCredential.Name], use)
				}
			}
		}
		vcsHosts = append(vcsHosts, pools[pool].VCS...)
	}
	for _, name := range slices.Sorted(maps.Keys(uses)) {
		if _, ok := file.Credentials[name]; ok {
			continue
		}
		if onTerminal {
			if ok, err := configured(name, uses[name]); err != nil {
				return err
			} else if ok {
				continue
			}
		}
		var delivered []string
		for _, use := range uses[name] {
			required := "required"
			if use.optional {
				required = "optional"
			}
			delivered = append(delivered, fmt.Sprintf("pool %s as %s, %s", use.pool, use.as(), required))
		}
		missing = append(missing, fmt.Sprintf("credential %s (%s)", name, strings.Join(delivered, "; ")))
	}
	slices.Sort(vcsHosts)
	for _, vcsHost := range slices.Compact(vcsHosts) {
		if slices.ContainsFunc(file.Git, func(entry busclient.GitCredential) bool {
			return (wire.GitInput{Remote: entry.URL}).VCSHost() == vcsHost
		}) {
			continue
		}
		if onTerminal {
			entry, err := askGit(p, vcsHost, file.Git)
			if err != nil {
				return err
			}
			ok := entry.Credential == ""
			if !ok {
				if ok, err = configured(entry.Credential, nil); err != nil {
					return err
				}
			}
			if ok {
				file.Git, changed = append(file.Git, entry), true
				continue
			}
		}
		missing = append(missing, "git host "+vcsHost)
	}
	for _, pool := range poolNames {
		if _, ok := file.Storage[pool]; ok || pools[pool].Storage == nil {
			continue
		}
		var names []string
		for _, key := range []string{"access key", "secret key"} {
			if !onTerminal {
				break
			}
			name, err := p.ask(fmt.Sprintf("Credential holding pool %s's S3 %s (blank skips the pool's storage)", pool, key), "")
			if err != nil {
				return err
			}
			ok := name != ""
			if ok {
				if ok, err = configured(name, nil); err != nil {
					return err
				}
			}
			if !ok {
				break
			}
			names = append(names, name)
		}
		if len(names) < 2 {
			missing = append(missing, "storage of pool "+pool)
			continue
		}
		if file.Storage == nil {
			file.Storage = map[string]busclient.StorageConfig{}
		}
		file.Storage[pool], changed = busclient.StorageConfig{AccessKey: names[0], SecretKey: names[1]}, true
	}
	if changed {
		if err := writeHostConfig(path, file, prompts); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		fmt.Fprintf(prompts, "%s has no source for:\n", path)
		for _, item := range missing {
			fmt.Fprintf(prompts, "  %s\n", item)
		}
	}
	return nil
}

// askSource asks for the source of credential name, which uses deliver. An env target set in this environment is
// suggested first, defaulting to No: an env source sends whatever the variable holds to every pool naming name.
func askSource(p *prompter, name string, uses []credentialUse) (credential.Source, error) {
	optional := len(uses) > 0
	var suggested []string
	for _, use := range uses {
		fmt.Fprintf(p.out, "pool %s delivers credential %s to its agents as %s.\n", use.pool, name, use.as())
		if use.target.Env != "" && os.Getenv(use.target.Env) != "" && !slices.Contains(suggested, use.target.Env) {
			suggested = append(suggested, use.target.Env)
		}
		optional = optional && use.optional
	}
	question := "Source for credential " + name
	if optional {
		question = "Source for optional credential " + name
	}
	for _, variable := range suggested {
		yes, err := p.yes(fmt.Sprintf("found %s in your environment. Use it for %s? It will be sent to agents in every pool, now or later, that names %s. [y/N]\n"+
			"an environment source is read where submits run, including the MCP server your harness starts.\n", variable, name, name))
		if err != nil || yes {
			return credential.Source{Env: variable}, err
		}
	}
	for {
		answer, err := p.ask(question+" (file:PATH, env:VAR or command:ARGV; blank skips)", "")
		if err != nil || answer == "" {
			return credential.Source{}, err
		}
		var source credential.Source
		switch kind, value, _ := strings.Cut(answer, ":"); {
		case kind == "file" && value != "":
			if source.File, err = localPath(value); err != nil {
				return credential.Source{}, err
			}
		case kind == "env":
			source.Env = value
		case kind == "command":
			source.Command = strings.Fields(value)
		}
		if source.Validate() == nil {
			return source, nil
		}
		fmt.Fprintln(p.out, "a source is file:PATH, env:VAR or command:ARGV")
	}
}

// askGit asks for the URL prefix and credential name of a git entry for vcsHost that git lacks.
func askGit(p *prompter, vcsHost string, git []busclient.GitCredential) (busclient.GitCredential, error) {
	for {
		prefix, err := p.ask("Git URL prefix for "+vcsHost, "https://"+vcsHost+"/")
		if err != nil {
			return busclient.GitCredential{}, err
		}
		if (strings.HasPrefix(prefix, "https://") || strings.HasPrefix(prefix, "ssh://")) &&
			!slices.ContainsFunc(git, func(entry busclient.GitCredential) bool { return entry.URL == prefix }) {
			name, err := p.ask("Credential for "+prefix+" (blank: anonymous)", "")
			return busclient.GitCredential{URL: prefix, Credential: name}, err
		}
		fmt.Fprintln(p.out, "a git URL prefix starts with https:// or ssh:// and is not already in the config")
	}
}

// localPath is path, typed at a prompt, with ~ expanded and made absolute.
func localPath(path string) (string, error) {
	path, err := busclient.ExpandHome(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(path)
}

func writeHostConfig(path string, file busclient.Config, prompts io.Writer) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if err := writeYAML(path, file); err != nil {
		return err
	}
	fmt.Fprintf(prompts, "wrote %s\n", path)
	return nil
}
