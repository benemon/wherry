package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
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

// hostInit registers a host and configures references to the sources its entitled pools need. caCert, when given, is
// the PEM --ca-cert resolved to, written as the configuration's ca_cert.
func hostInit(ctx context.Context, path, bundle string, caCert *string, stdout, prompts io.Writer) (result error) {
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
	// Each configuration's bundle and host key go with it, so two configurations never come to share a host identity
	// (agb-d1hh); the default configuration keeps the configuration directory's.
	paths, err := busclient.DefaultPaths(path)
	if err != nil {
		return err
	}
	var file busclient.Config
	_, statErr := os.Stat(path)
	changed := errors.Is(statErr, os.ErrNotExist)
	if !changed {
		encoded, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := operator.RefuseCAFile(path, encoded); err != nil {
			return err
		}
		if err := operator.DecodeYAML(encoded, &file); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	// The default configuration's copy is the configuration directory's bundle.yaml, whatever bundle it names.
	destination, explicit := paths.Bundle, !paths.Default && file.Bundle != ""
	if explicit {
		if destination, err = busclient.ExpandHome(file.Bundle); err != nil {
			return err
		}
	}
	// Explicit host_key and state_dir paths are the user's choice, and the configuration directory is wherry's own, so
	// neither is checked for another host's files.
	if !paths.Default {
		copying := ""
		if bundle != "" {
			copying = destination
		}
		if err := refuseForeignFiles(path, paths, file, !changed, copying); err != nil {
			return err
		}
	}
	newEnrolment := false
	if bundle != "" {
		contents, err := operator.ReadBundle(bundle)
		if err != nil {
			return err
		}
		// The configuration directory is wherry's own and is hardened to 0700; a configuration elsewhere sits in a
		// directory of the user's, which keeps its mode.
		if paths.Default {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return err
			}
			if err := os.Chmod(dir, 0o700); err != nil {
				return err
			}
		} else if err := makePrivateDir(filepath.Dir(destination)); err != nil {
			return err
		}
		// T-H5 and T-H6 under the host lock, so a promotion by another process is not undone by the password read here.
		release, err := busclient.LockFile(ctx, busclient.ScramLockPath(destination), nil)
		if err != nil {
			return err
		}
		newEnrolment, err = installBundle(destination, contents)
		release()
		if err != nil {
			return err
		}
		if !explicit {
			changed = changed || file.Bundle != paths.Bundle
			file.Bundle = paths.Bundle
		}
	} else if file.Bundle == "" {
		return fmt.Errorf("%s names no bundle; run wherry host init BUNDLE", path)
	}
	if caCert != nil && file.CACert != *caCert {
		file.CACert = *caCert
		changed = true
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
	if changed {
		if err := write(); err != nil {
			return err
		}
		// Writing through a link to the default path can make this file the default one; naming the host key and state
		// directory chosen above keeps every later load on them.
		if !paths.Default {
			after, err := busclient.DefaultPaths(path)
			if err != nil {
				return err
			}
			if after.Default && (file.HostKey == "" || file.StateDir == "") {
				if file.HostKey == "" {
					file.HostKey = paths.HostKey
				}
				if file.StateDir == "" {
					file.StateDir = paths.StateDir
				}
				if err := write(); err != nil {
					return err
				}
			}
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
	if newEnrolment {
		// The bundle's password is for enrolment only.
		host, err := busclient.Connect(config, log.New(prompts, "", 0))
		if err != nil {
			return err
		}
		err = host.RotateSCRAM(ctx, prompts)
		host.Close()
		if err != nil {
			return err
		}
	}
	// Bundled storage keys serve every entitled pool with storage, so those pools get no storage question; a pool the
	// configuration maps to credentials of its own keeps them.
	installed, err := operator.ReadBundle(config.Bundle)
	if err != nil {
		return err
	}
	bundled := installed.Storage != (operator.StorageKeys{})
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
		if _, ok := file.Storage[pool]; !ok && terms.Storage != nil && !bundled {
			storage = append(storage, pool)
		}
	}
	slices.Sort(hosts)
	hosts = slices.Compact(hosts)
	// A credential whose every use targets a catalog variable gets no question and no line; any other target needs a
	// source.
	catalogOnly := map[string]bool{}
	for _, pool := range slices.Sorted(maps.Keys(pools)) {
		agents, err := busclient.Discover(busclient.Config{Credentials: file.Credentials}, pools[pool])
		if err != nil {
			return err
		}
		for _, agent := range agents {
			for _, c := range agent.Credentials {
				if seen, ok := catalogOnly[c.Name]; !ok {
					catalogOnly[c.Name] = c.CatalogVariable
				} else {
					catalogOnly[c.Name] = seen && c.CatalogVariable
				}
			}
		}
	}
	hasGit := func(host string) bool {
		return slices.ContainsFunc(file.Git, func(e busclient.GitCredential) bool { return (wire.GitInput{Remote: e.URL}).VCSHost() == host })
	}
	missing := func() []string {
		var lines []string
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
		if _, ok := file.Credentials[name]; !ok && !catalogOnly[name] {
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
					if _, ok := file.Credentials[name]; ok || asked[name] || catalogOnly[name] {
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
							prefix, err := q.input("SSH URL prefix (blank: "+defaultPrefix+")", "", "", "", func(s string) error {
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
							path, err := q.input("known_hosts file (blank returns)", "", "", "path", func(s string) error {
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
		if err = q.line("this host's tenant is entitled to no pool yet; run wherry host init again once the operator runs allow --tenant TENANT --pool POOL\n"); err != nil {
			return err
		}
	}
	lines, err := agentLines(path, file, pools)
	if err != nil {
		return err
	}
	for _, line := range lines {
		if err = q.line("%s\n", display(line)); err != nil {
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

// exampleCommand is the command source a not-ready line shows: a placeholder for whatever helper, keychain or vault
// client prints the credential, naming no vendor.
const exampleCommand = "your-secret-tool, get, NAME"

// agentLines is one line per entitled agent, pool by pool, ready or not, from discovery, which runs no command; a
// not-ready line names the configuration at path.
func agentLines(path string, file busclient.Config, pools map[string]operator.Terms) ([]string, error) {
	var lines []string
	for _, pool := range slices.Sorted(maps.Keys(pools)) {
		agents, err := busclient.Discover(busclient.Config{Credentials: file.Credentials}, pools[pool])
		if err != nil {
			return nil, err
		}
		for _, agent := range agents {
			prefix := fmt.Sprintf("pool %s agent %s: ", pool, agent.Agent)
			if agent.Ready {
				var from []string
				for _, c := range agent.Credentials {
					from = append(from, credentialFrom(c))
				}
				lines = append(lines, prefix+"ready; "+strings.Join(from, "; "))
				continue
			}
			var variables, needs []string
			example := ""
			for _, c := range agent.Missing {
				if example == "" {
					example = c.Name
				}
				target := c.Env
				if c.File != "" {
					target = "file " + c.File
				}
				if c.Description != "" {
					target = c.Description + ", " + target
				}
				// The configured source comes first: an explicit env source wins over the credential's catalog variable, so
				// setting the catalog variable cannot help while the source's own variable is unset.
				switch {
				case c.Source == busclient.SourceEnv && c.Status == busclient.StatusUnset:
					needs = append(needs, fmt.Sprintf("credential %s (%s): its env source %s is unset", c.Name, target, c.Variable))
				case c.CatalogVariable:
					variables = append(variables, c.Env)
				default:
					needs = append(needs, fmt.Sprintf("credential %s (%s) has no source", c.Name, target))
				}
			}
			if len(variables) > 0 {
				set := "set " + variables[0]
				if len(variables) > 1 {
					set = "set one of " + strings.Join(variables, ", ")
				}
				set += " where submits run"
				if agent.Doc != "" {
					set += " (" + agent.Doc + ")"
				}
				needs = append([]string{set}, needs...)
			}
			lines = append(lines, prefix+"not ready; "+strings.Join(needs, "; ")+fmt.Sprintf("; or add to %s: credentials: {%s: {command: [%s]}}", path, example, exampleCommand))
		}
	}
	return lines, nil
}

// credentialFrom says where a credential comes from, as discovery found it.
func credentialFrom(c busclient.Readiness) string {
	name := c.Name
	if c.Optional {
		name += " (optional)"
	}
	switch {
	case c.Source == busclient.SourceVariable && c.Status == busclient.StatusReady:
		return name + " from " + c.Variable
	case c.Source == busclient.SourceVariable:
		return name + ", " + c.Variable + " unset"
	case c.Source == busclient.SourceEnv && c.Status == busclient.StatusReady:
		return name + " from env " + c.Variable
	case c.Source == busclient.SourceEnv:
		return name + " from env " + c.Variable + ", unset"
	case c.Source == busclient.SourceFile:
		return name + " from file"
	case c.Source == busclient.SourceCommand:
		return name + " from command, unchecked"
	}
	return name + ", no source"
}

// refuseForeignFiles fails when a derived destination of the configuration at path, or bundle, where BUNDLE is copied
// (empty without one), exists and is not the configuration's own; a configuration not yet written (exists false) owns
// nothing.
func refuseForeignFiles(path string, paths busclient.Paths, file busclient.Config, exists bool, bundle string) error {
	type destination struct {
		path, setting, what string
		named               bool // the configuration names it already
		dir                 bool // it is a directory
	}
	var destinations []destination
	if bundle != "" {
		destinations = append(destinations, destination{bundle, "bundle", "bundle", exists && sameFile(file.Bundle, bundle), false})
	}
	if file.HostKey == "" {
		destinations = append(destinations, destination{paths.HostKey, "host_key", "host key", exists, false})
	}
	if file.StateDir == "" {
		destinations = append(destinations, destination{paths.StateDir, "state_dir", "state directory", exists, true})
	}
	for _, d := range destinations {
		// The entry itself decides whether the destination is occupied, so a dangling symlink there is refused rather
		// than written through; the link is followed only to establish what it is and whether it is this configuration's.
		if _, err := os.Lstat(d.path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		if info, err := os.Stat(d.path); err == nil && d.named && info.IsDir() == d.dir {
			continue
		}
		return fmt.Errorf("%s exists and is not the %s of configuration %s; set %s in %s to a path of its own", d.path, d.what, path, d.setting, path)
	}
	return nil
}

// sameFile reports whether named, a path from a configuration, and path are one existing file.
func sameFile(named, path string) bool {
	named, err := busclient.ExpandHome(named)
	if err != nil || named == "" {
		return false
	}
	a, errA := os.Stat(named)
	b, errB := os.Stat(path)
	return errA == nil && errB == nil && os.SameFile(a, b)
}

// makePrivateDir creates path's missing directories with mode 0700, whatever the umask, and leaves an existing one as
// it is: a configuration placed in a user's own directory does not take the directory over.
func makePrivateDir(path string) error {
	var created []string
	for dir := path; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(dir); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		created = append(created, dir)
		if dir == filepath.Dir(dir) {
			break
		}
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	for _, dir := range created {
		if err := os.Chmod(dir, 0o700); err != nil {
			return err
		}
	}
	return nil
}

func writeHostConfig(path string, file busclient.Config) error {
	if err := makePrivateDir(filepath.Dir(path)); err != nil {
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

// installBundle installs contents at destination and reports whether it is a new enrolment: one whose principal or
// enrolment differs from the installed bundle's, or with none installed, which replaces the bundle whole and removes
// scram.next. The same enrolment keeps the installed password and scram.next.
func installBundle(destination string, contents operator.Bundle) (bool, error) {
	installed, err := operator.ReadBundle(destination)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err == nil && installed.Principal == contents.Principal && installed.Enrolment == contents.Enrolment {
		contents.Password = installed.Password
		return false, operator.ReplaceBundle(destination, contents)
	}
	if err := operator.ReplaceBundle(destination, contents); err != nil {
		return true, err
	}
	return true, busclient.RemoveSCRAMNext(destination)
}
