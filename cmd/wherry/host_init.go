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
	"github.com/charmbracelet/huh"
)

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

// hostInit registers a host and configures references to the sources its entitled pools need.
func hostInit(ctx context.Context, path, bundle string, stdout, prompts io.Writer) (result error) {
	defer func() {
		if result != nil {
			result = presentationError{result}
		}
	}()
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

	q := &questions{ctx, stdin, &promptLatch{out: prompts}}
	interactive := promptsEligible(prompts)
	wrote := false
	write := func() error {
		if err := writeHostConfig(path, file); err != nil {
			return err
		}
		wrote = true
		return nil
	}
	if _, err := os.Stat(filepath.Join(dir, "ca.pem")); file.CAFile == "" && errors.Is(err, os.ErrNotExist) && interactive {
		if err = q.line("\nBroker\n"); err != nil {
			return err
		}
		ca, err := q.ca()
		if err != nil {
			return err
		}
		if ca != "" {
			file.CAFile, changed = ca, true
		}
	}
	if changed {
		if err := write(); err != nil {
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
	if !interactive || !sameTerminal(stdout, prompts) {
		if _, err = fmt.Fprintln(stdout, keyID); err != nil {
			return err
		}
	}
	changed = false
	if file.Credentials == nil {
		file.Credentials = map[string]credential.Source{}
	}
	uses := map[string][]credentialUse{}
	descriptions := map[string]string{}
	var hosts, storage []string
	for _, pool := range slices.Sorted(maps.Keys(pools)) {
		terms := pools[pool]
		for _, agent := range terms.Agents {
			for _, c := range agent.Credentials {
				if descriptions[c.Name] == "" {
					descriptions[c.Name] = c.Description
				}
				use := credentialUse{pool, c.TaskCredential, c.Optional()}
				if !slices.Contains(uses[c.Name], use) {
					uses[c.Name] = append(uses[c.Name], use)
				}
			}
		}
		hosts = append(hosts, terms.VCS...)
		if _, ok := file.Storage[pool]; !ok && terms.Storage != nil {
			storage = append(storage, pool)
		}
	}
	slices.Sort(hosts)
	hosts = slices.Compact(hosts)
	hasGit := func(host string) bool {
		return slices.ContainsFunc(file.Git, func(e busclient.GitCredential) bool { return (wire.GitInput{Remote: e.URL}).VCSHost() == host })
	}
	missing := func() []string {
		var lines []string
		for _, name := range slices.Sorted(maps.Keys(uses)) {
			if _, ok := file.Credentials[name]; ok {
				continue
			}
			var delivered []string
			for _, use := range uses[name] {
				required := "required"
				if use.optional {
					required = "optional"
				}
				delivered = append(delivered, fmt.Sprintf("pool %s as %s, %s", use.pool, use.as(), required))
			}
			line := fmt.Sprintf("credential %s (%s)", name, strings.Join(delivered, "; "))
			if descriptions[name] != "" {
				line = descriptions[name] + ", " + line
			}
			lines = append(lines, line)
		}
		for _, host := range hosts {
			if !hasGit(host) {
				lines = append(lines, "git host "+host)
			}
		}
		for _, pool := range storage {
			if _, ok := file.Storage[pool]; !ok {
				lines = append(lines, "storage of pool "+pool)
			}
		}
		return lines
	}
	topics := []string{}
	for name := range uses {
		if _, ok := file.Credentials[name]; !ok {
			topics = append(topics, "Credentials")
			break
		}
	}
	for _, host := range hosts {
		if !hasGit(host) {
			topics = append(topics, "Git")
			break
		}
	}
	if len(storage) > 0 {
		topics = append(topics, "Storage")
	}
	topics = append(topics, "Summary")
	asked := map[string]bool{}
	names := newGitNames(file.Git)
	for _, host := range hosts {
		names.allocate(host, "https")
		names.allocate(host, "ssh")
	}
	source := func(name, label string, u []credentialUse, nested bool) (sourceResult, error) {
		if s, ok := file.Credentials[name]; ok {
			return sourceResult{name, s, true}, nil
		}
		if asked[name] {
			return sourceResult{}, nil
		}
		return q.source(name, label, u, file.Credentials, nested)
	}
	commit := func(s sourceResult) {
		if _, ok := file.Credentials[s.name]; !ok {
			file.Credentials[s.name] = s.source
			changed = true
		}
	}
	if interactive {
		for i, topic := range topics {
			if err = q.line("\n%s  %d of %d\n", topic, i+1, len(topics)); err != nil {
				return err
			}
			switch topic {
			case "Credentials":
				askedAny := false
				for _, name := range slices.Sorted(maps.Keys(uses)) {
					if _, ok := file.Credentials[name]; ok || asked[name] {
						continue
					}
					askedAny = true
					asked[name] = true
					s, err := q.source(name, sourceLabel(name, descriptions[name], uses[name]), uses[name], file.Credentials, false)
					if err != nil {
						return err
					}
					if s.ok {
						commit(s)
					}
				}
				if !askedAny {
					if err = q.line("Already configured.\n"); err != nil {
						return err
					}
				}
			case "Git":
				askedAny := false
				for _, host := range hosts {
					if hasGit(host) {
						continue
					}
					askedAny = true
					prev := "anonymous"
				route:
					for {
						description := "Generated names reuse an unassociated existing source; Configured credential uses an existing credential instead."
						if name := names.allocate(host, "https"); asked[name] {
							description += "\nCredential " + display(name) + " was skipped earlier; HTTPS is unavailable."
						}
						choice, err := q.pick(host, description, []huh.Option[string]{option("Anonymous over https", "anonymous"), option("Credential over https...", "https"), option("SSH...", "ssh"), option("Skip", "skip")}, &prev)
						if err != nil {
							return err
						}
						if choice == "skip" {
							if err = q.answer(host, "Skipped"); err != nil {
								return err
							}
							break
						}
						entry := busclient.GitCredential{URL: "https://" + host + "/"}
						if choice == "anonymous" {
							if err = q.answer(host, "Anonymous over https"); err != nil {
								return err
							}
							file.Git = append(file.Git, entry)
							changed = true
							break
						}
						if choice == "ssh" {
							defaultPrefix := "ssh://git@" + host + "/"
							prefix, err := q.input("SSH URL prefix (blank: "+defaultPrefix+")", "", "", func(s string) error {
								if s == "" {
									s = defaultPrefix
								}
								return sshPrefix(s, host, file.Git)
							})
							if err != nil {
								return err
							}
							if prefix == "" {
								prefix = defaultPrefix
							}
							entry.URL = prefix
						}
						name := names.allocate(host, choice)
						s, err := source(name, name, nil, true)
						if err != nil {
							return err
						}
						if !s.ok {
							continue route
						}
						entry.Credential = s.name
						if choice == "ssh" {
							path, err := q.input("known_hosts file (blank returns)", "", "path", func(s string) error {
								if s == "" {
									return nil
								}
								_, err := readablePath(s)
								return err
							})
							if err != nil {
								return err
							}
							if path == "" {
								continue route
							}
							entry.HostKey, err = readablePath(path)
							if err != nil {
								return err
							}
						}
						answer := "HTTPS " + entry.URL
						if choice == "ssh" {
							answer = "SSH " + entry.URL
						}
						if err = q.answer(host, answer); err != nil {
							return err
						}
						commit(s)
						file.Git = append(file.Git, entry)
						changed = true
						break
					}
				}
				if !askedAny {
					if err = q.line("Already configured.\n"); err != nil {
						return err
					}
				}
			case "Storage":
				prev := "keys"
				storageLabel := "S3 storage for pools " + strings.Join(storage, ", ")
				for {
					choice, err := q.pick(storageLabel, "Set per-pool keys by editing the config.", []huh.Option[string]{option("Key sources...", "keys"), option("Skip", "skip")}, &prev)
					if err != nil {
						return err
					}
					if choice == "skip" {
						if err = q.answer(storageLabel, "Skipped"); err != nil {
							return err
						}
						break
					}
					a, err := source(freeName("s3-access-key", file.Credentials), "S3 access key", nil, true)
					if err != nil {
						return err
					}
					if !a.ok {
						continue
					}
					b, err := source(freeName("s3-secret-key", file.Credentials), "S3 secret key", nil, true)
					if err != nil {
						return err
					}
					if !b.ok {
						continue
					}
					commit(a)
					commit(b)
					if file.Storage == nil {
						file.Storage = map[string]busclient.StorageConfig{}
					}
					for _, pool := range storage {
						if _, ok := file.Storage[pool]; !ok {
							file.Storage[pool] = busclient.StorageConfig{AccessKey: a.name, SecretKey: b.name}
						}
					}
					changed = true
					break
				}
			case "Summary":
				if changed {
					if err = write(); err != nil {
						return err
					}
					changed = false
				}
				if err = q.line("Host key %s registered\n", display(keyID)); err != nil {
					return err
				}
				if wrote {
					if err = q.line("Wrote %s\n", display(path)); err != nil {
						return err
					}
				}
			}
		}
	}
	if changed {
		if err = write(); err != nil {
			return err
		}
	}
	if !interactive {
		if wrote {
			if err = q.line("wrote %s\n", display(path)); err != nil {
				return err
			}
		}
	}
	if len(pools) == 0 {
		if err = q.line("this host's tenant is entitled to no pool yet; run wherry host init again once the operator runs tenant pool add\n"); err != nil {
			return err
		}
	}
	if lines := missing(); len(lines) > 0 {
		if interactive {
			err = q.line("Not configured:\n")
		} else {
			err = q.line("%s has no source for:\n", display(path))
		}
		if err != nil {
			return err
		}
		for _, line := range lines {
			if err = q.line("  %s\n", display(line)); err != nil {
				return err
			}
		}
		if interactive {
			return q.line("run wherry host init again to add them\n")
		}
	}
	return nil
}

func writeHostConfig(path string, file busclient.Config) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if err := writeYAML(path, file); err != nil {
		return err
	}
	// writeYAML's mode applies only to a new file, and the config names credential sources.
	if err := os.Chmod(path, 0o600); err != nil {
		return err
	}
	return nil
}
