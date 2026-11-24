package main

import (
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/wire"
	"github.com/charmbracelet/huh"
)

func sourceLabel(name, description string, uses []credentialUse) string {
	label := description
	if label == "" {
		label = name
	}
	optional := len(uses) > 0
	for _, use := range uses {
		optional = optional && use.optional
	}
	if optional {
		label += " (optional)"
	}
	return label
}
func sourceDescription(name string, uses []credentialUse) string {
	lines := []string{display("credential " + name)}
	for _, use := range uses {
		required := "required"
		if use.optional {
			required = "optional"
		}
		lines = append(lines, display(fmt.Sprintf("pool %s: %s, %s", use.pool, use.as(), required)))
	}
	lines = append(lines, display("An environment source sends what the variable holds to every pool that names "+name+", now or later. Where it is optional, a run gets it whenever the variable is set, without being asked for. It is read where submits run, including the MCP server your harness starts."))
	return strings.Join(lines, "\n")
}
func targetVariables(uses []credentialUse) []string {
	var result []string
	for _, use := range uses {
		if v := use.target.Env; v != "" && os.Getenv(v) != "" && !slices.Contains(result, v) {
			result = append(result, v)
		}
	}
	return result
}

type sourceResult struct {
	name   string
	source credential.Source
	ok     bool
}

func (q *questions) source(name, label string, uses []credentialUse, configured map[string]credential.Source, nested bool) (sourceResult, error) {
	finish := func(result sourceResult, answer string) (sourceResult, error) {
		return result, q.answer(label, answer)
	}
	prev := "env"
	if nested {
		prev = "configured"
	}
	envPrev, configuredPrev := "", ""
	description := sourceDescription(name, uses)
	for {
		opts := []huh.Option[string]{}
		if nested {
			opts = append(opts, option("Configured credential...", "configured"))
		}
		opts = append(opts, option("Environment variable", "env"), option("File", "file"), option("Command", "command"), option("Skip", "skip"))
		choice, err := q.pick(label, description, opts, &prev)
		if err != nil {
			return sourceResult{}, err
		}
		if choice == "skip" {
			return finish(sourceResult{}, "Skipped")
		}
		if choice == "configured" {
			options := []huh.Option[string]{}
			for _, key := range slices.Sorted(maps.Keys(configured)) {
				options = append(options, option(key, "credential:"+key))
			}
			if len(options) == 0 {
				options = append(options, option("(none)", "none"))
			}
			options = append(options, option("Back", "back"), option("Skip", "skip"))
			value, err := q.pick("Configured credential", "", options, &configuredPrev)
			if err != nil {
				return sourceResult{}, err
			}
			if value == "skip" {
				return finish(sourceResult{}, "Skipped")
			}
			if value == "back" || value == "none" {
				continue
			}
			value = strings.TrimPrefix(value, "credential:")
			return finish(sourceResult{name: value, source: configured[value], ok: true}, "Configured credential "+value)
		}
		// A blank Variable returns to the variable list; Back and other blanks return to the source list.
		for {
			value := ""
			if choice == "env" {
				options := []huh.Option[string]{}
				for _, v := range targetVariables(uses) {
					options = append(options, option(v, "variable:"+v))
				}
				options = append(options, option("New variable...", "new"), option("Back", "back"), option("Skip", "skip"))
				value, err = q.pick("Environment variable", description, options, &envPrev)
				if err != nil {
					return sourceResult{}, err
				}
				if value == "skip" {
					return finish(sourceResult{}, "Skipped")
				}
				if value == "back" {
					break
				}
				if value != "new" {
					value = strings.TrimPrefix(value, "variable:")
					return finish(sourceResult{name, credential.Source{Env: value}, true}, "Environment variable "+value)
				}
			}
			kind, title := "path", "File (blank returns)"
			if choice == "env" {
				kind, title = "variable", "Variable (blank returns)"
			}
			if choice == "command" {
				kind, title = "command", "Command (blank returns)"
			}
			var source credential.Source
			value, err = q.input(title, "", "", kind, func(s string) error {
				if s == "" {
					return nil
				}
				var err error
				source = credential.Source{}
				switch choice {
				case "env":
					source.Env = s
				case "file":
					source.File, err = localPath(s)
				case "command":
					source.Command = strings.Fields(s)
				}
				if err != nil {
					return err
				}
				return source.Validate()
			})
			if err != nil {
				return sourceResult{}, err
			}
			if value == "" {
				if choice == "env" {
					continue
				}
				break
			}
			answer := "File " + value
			if choice == "env" {
				answer = "Environment variable " + value
			}
			if choice == "command" {
				answer = "Command " + value
			}
			return finish(sourceResult{name, source, true}, answer)
		}
	}
}

var openFile = os.OpenFile

type gitNames struct {
	pairs  map[string]string
	owners map[string]string
}

func gitPair(host, transport string) string { return strings.ToLower(host) + "|" + transport }

var newGitNames = func(entries []busclient.GitCredential) *gitNames {
	n := &gitNames{map[string]string{}, map[string]string{}}
	for _, entry := range entries {
		transport := "https"
		if strings.HasPrefix(entry.URL, "ssh://") {
			transport = "ssh"
		}
		if entry.Credential != "" {
			pair := gitPair((wire.GitInput{Remote: entry.URL}).VCSHost(), transport)
			if owner := n.owners[entry.Credential]; owner != "" && owner != pair {
				n.owners[entry.Credential] = "shared"
			} else {
				n.owners[entry.Credential] = pair
			}
		}
	}
	return n
}

func (n *gitNames) allocate(host, transport string) string {
	pair := gitPair(host, transport)
	if name := n.pairs[pair]; name != "" {
		return name
	}
	component := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		return '-'
	}, strings.ToLower(host))
	component = strings.TrimRight(component, "-")
	if component == "" {
		component = "host"
	}
	for i := 1; ; i++ {
		suffix := ""
		if transport == "ssh" {
			suffix = "-ssh"
		}
		if i > 1 {
			suffix += "-" + strconv.Itoa(i)
		}
		part := component
		if maxComponent := 32 - len("git-") - len(suffix); len(part) > maxComponent {
			part = part[:maxComponent]
		}
		part = strings.TrimRight(part, "-")
		name := "git-" + part + suffix
		if !wire.ValidName(name) {
			continue
		}
		if owner := n.owners[name]; owner != "" && owner != pair {
			continue
		}
		n.pairs[pair], n.owners[name] = name, pair
		return name
	}
}
func sshPrefix(prefix, host string, entries []busclient.GitCredential) error {
	if !strings.HasPrefix(prefix, "ssh://") || (wire.GitInput{Remote: prefix}).VCSHost() != host || slices.ContainsFunc(entries, func(e busclient.GitCredential) bool { return e.URL == prefix }) {
		return fmt.Errorf("an SSH URL prefix must start with ssh://, name %s and be new", host)
	}
	return nil
}
func readablePath(s string) (string, error) {
	p, err := localPath(s)
	if err != nil {
		return "", err
	}
	if info, err := os.Stat(p); err == nil && !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular file", p)
	}
	f, err := openFile(p, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return "", err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return "", fmt.Errorf("%s is not a regular file", p)
	}
	return p, f.Close()
}
func freeName(base string, sources map[string]credential.Source) string {
	for i := 1; ; i++ {
		name := base
		if i > 1 {
			name += "-" + strconv.Itoa(i)
		}
		if _, ok := sources[name]; !ok {
			return name
		}
	}
}
func localPath(path string) (string, error) {
	path, err := busclient.ExpandHome(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(path)
}

func loadPassword(path string) ([]byte, error) {
	if info, err := os.Stat(path); err == nil && !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	file, err := openFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	return io.ReadAll(file)
}
