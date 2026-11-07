package main

import (
	"crypto/x509"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/wire"
	"github.com/charmbracelet/huh"
	"golang.org/x/sys/unix"
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
	lines = append(lines, display("An environment source sends what the variable holds to every pool that names "+name+", now or later. It is read where submits run, including the MCP server your harness starts."))
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

func (q *hostQuestions) source(name, label string, uses []credentialUse, configured map[string]credential.Source, nested bool) (sourceResult, error) {
	prev := "env"
	if nested {
		prev = "configured"
	}
	envPrev, configuredPrev := "", ""
	for {
		opts := []huh.Option[string]{}
		if nested {
			opts = append(opts, option("Configured credential...", "configured"))
		}
		opts = append(opts, option("Environment variable", "env"), option("File", "file"), option("Command", "command"), option("Skip", "skip"))
		choice, err := q.pick(label, sourceDescription(name, uses), opts, &prev)
		if err != nil {
			return sourceResult{}, err
		}
		if choice == "skip" {
			return sourceResult{}, nil
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
				return sourceResult{}, nil
			}
			if value == "back" || value == "none" {
				continue
			}
			value = strings.TrimPrefix(value, "credential:")
			return sourceResult{name: value, source: configured[value], ok: true}, nil
		}
		value := ""
	environment:
		if choice == "env" {
			options := []huh.Option[string]{}
			for _, v := range targetVariables(uses) {
				options = append(options, option(v, "variable:"+v))
			}
			options = append(options, option("New variable...", "new"), option("Back", "back"), option("Skip", "skip"))
			value, err = q.pick("Environment variable", sourceDescription(name, uses), options, &envPrev)
			if err != nil {
				return sourceResult{}, err
			}
			if value == "skip" {
				return sourceResult{}, nil
			}
			if value == "back" {
				continue
			}
			if value != "new" {
				return sourceResult{name, credential.Source{Env: strings.TrimPrefix(value, "variable:")}, true}, nil
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
		value, err = q.input(title, "", kind, func(s string) error {
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
				goto environment
			}
			continue
		}
		return sourceResult{name, source, true}, nil
	}
}

var openCAFile = unix.Open
var readCAData = func(file io.Reader) ([]byte, error) { return io.ReadAll(io.LimitReader(file, (1<<20)+1)) }

// readCA accepts symlinks to regular files and bounds reads even if the file changes after listing.
func readCA(path string) error {
	fd, err := openCAFile(path, unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return fmt.Errorf("%s holds no PEM certificate", path)
	}
	pem, err := readCAData(file)
	if err != nil {
		return err
	}
	if len(pem) > 1<<20 || !x509.NewCertPool().AppendCertsFromPEM(pem) {
		return fmt.Errorf("%s holds no PEM certificate", path)
	}
	return nil
}
func caOptions() []huh.Option[string] {
	var result []huh.Option[string]
	info, err := os.Stat(".")
	if err != nil || !info.IsDir() {
		return []huh.Option[string]{option("System roots", "system"), option("Other file...", "other")}
	}
	entries, _ := os.ReadDir(".")
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		path, err := localPath(e.Name())
		if err == nil && readCA(path) == nil {
			result = append(result, option(e.Name(), path))
			if len(result) == 8 {
				break
			}
		}
	}
	return append(result, option("System roots", "system"), option("Other file...", "other"))
}
func (q *hostQuestions) ca() (string, error) {
	prev := ""
	for {
		value, err := q.pick("CA certificate", "", caOptions(), &prev)
		if err != nil {
			return "", err
		}
		if value == "system" {
			return "", nil
		}
		if value == "other" {
			value, err = q.input("CA file (blank returns)", "", "path", func(s string) error {
				if s == "" {
					return nil
				}
				p, err := localPath(s)
				if err != nil {
					return err
				}
				return readCA(p)
			})
			if err != nil {
				return "", err
			}
			if value == "" {
				continue
			}
			value, err = localPath(value)
			if err != nil {
				return "", err
			}
		}
		if err = readCA(value); err != nil {
			if err = q.line("%s\n", display(err.Error())); err != nil {
				return "", err
			}
			continue
		}
		return value, nil
	}
}

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
		if max := 32 - len("git-") - len(suffix); len(part) > max {
			part = part[:max]
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
	f, err := os.Open(p)
	if err != nil {
		return "", err
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
