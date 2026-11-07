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

	busclient "github.com/benemon/wherry/internal/client"
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

// hostInit registers the host and collects missing source references.
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
	onTerminal := pagesEligible(prompts)
	p := hostPickerFor(prompts)
	var notices []string
	write := func() error {
		if err := writeHostConfig(path, file, io.Discard); err != nil {
			return err
		}
		notices = append(notices, "Wrote "+display(path))
		return nil
	}
	if _, err := os.Stat(filepath.Join(dir, "ca.pem")); file.CAFile == "" && errors.Is(err, os.ErrNotExist) && onTerminal {
		if err := p.write("\r\nwherry host init\r\nBroker\r\n"); err != nil {
			return err
		}
		ca, err := askCA(p)
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
	if _, err := fmt.Fprintln(stdout, keyID); err != nil {
		return err
	}
	changed = false
	uses, descriptions, hosts, storage := hostNeeds(pools, file)
	requests := &sourceRequests{p: p, file: &file, asked: map[string]sourceResult{}}
	names := newGitNames(hosts, file)
	topics := []string{}
	if slices.ContainsFunc(slices.Sorted(maps.Keys(uses)), func(name string) bool { _, ok := file.Credentials[name]; return !ok }) {
		topics = append(topics, "Credentials")
	}
	if len(hosts) > 0 {
		topics = append(topics, "Git")
	}
	if len(storage) > 0 {
		topics = append(topics, "Storage")
	}
	topics = append(topics, "Summary")
	if onTerminal {
		if err := p.write("wherry host init\r\n"); err != nil {
			return err
		}
		for i, topic := range topics {
			if err := p.write(fmt.Sprintf("\r\n%s  %d of %d\r\n", topic, i+1, len(topics))); err != nil {
				return err
			}
			switch topic {
			case "Credentials":
				ok, err := askCredentials(requests, uses, descriptions)
				if err != nil {
					return err
				}
				changed = changed || ok
			case "Git":
				for _, host := range hosts {
					if hasGit(file, host) {
						if err := p.notice("Already configured."); err != nil {
							return err
						}
						continue
					}
					entry, result, err := askGitPage(requests, host, names)
					if err != nil {
						return err
					}
					if entry.URL != "" {
						if result.ok {
							requests.commit(result)
						}
						file.Git = append(file.Git, entry)
						changed = true
					}
				}
			case "Storage":
				ok, err := askStorage(requests, storage)
				if err != nil {
					return err
				}
				changed = changed || ok
			case "Summary":
				if changed {
					if err := write(); err != nil {
						return err
					}
				}
				if err := p.write("Host key " + display(keyID) + " registered\r\n"); err != nil {
					return err
				}
			}
		}
	}
	for _, notice := range notices {
		if _, err := fmt.Fprintln(prompts, notice); err != nil {
			return err
		}
	}
	missing := missingHost(file, uses, descriptions, hosts, storage)
	if len(missing) > 0 {
		if _, err := fmt.Fprintln(prompts, "Not configured:"); err != nil {
			return err
		}
		for _, item := range missing {
			if _, err := fmt.Fprintln(prompts, "  "+display(item)); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(prompts, "run wherry host init again to add them"); err != nil {
			return err
		}
	}
	if len(pools) == 0 {
		_, err = fmt.Fprintln(prompts, "this host's tenant is entitled to no pool yet; run wherry host init again once the operator runs tenant pool add")
	}
	return err
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
	// writeYAML's mode applies only to a new file, and the config names credential sources.
	if err := os.Chmod(path, 0o600); err != nil {
		return err
	}
	fmt.Fprintf(prompts, "wrote %s\n", path)
	return nil
}
