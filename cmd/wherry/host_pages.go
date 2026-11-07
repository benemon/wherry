package main

import (
	"crypto/x509"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

var hostPickerFor = newHostPicker

func hasGit(file busclient.Config, host string) bool {
	return slices.ContainsFunc(file.Git, func(entry busclient.GitCredential) bool { return (wire.GitInput{Remote: entry.URL}).VCSHost() == host })
}
func hostNeeds(pools map[string]operator.Terms, file busclient.Config) (map[string][]credentialUse, map[string]string, []string, []string) {
	uses := map[string][]credentialUse{}
	descriptions := map[string]string{}
	var hosts, storage []string
	for _, pool := range slices.Sorted(maps.Keys(pools)) {
		for _, agent := range pools[pool].Agents {
			for _, c := range agent.Credentials {
				use := credentialUse{pool, c.TaskCredential, c.Optional()}
				if !slices.Contains(uses[c.Name], use) {
					uses[c.Name] = append(uses[c.Name], use)
				}
				if descriptions[c.Name] == "" {
					descriptions[c.Name] = c.Description
				}
			}
		}
		for _, host := range pools[pool].VCS {
			if !hasGit(file, host) {
				hosts = append(hosts, host)
			}
		}
		if _, ok := file.Storage[pool]; !ok && pools[pool].Storage != nil {
			storage = append(storage, pool)
		}
	}
	slices.Sort(hosts)
	return uses, descriptions, slices.Compact(hosts), storage
}
func missingHost(file busclient.Config, uses map[string][]credentialUse, descriptions map[string]string, hosts, storage []string) []string {
	var missing []string
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
		label := "credential " + name
		if descriptions[name] != "" {
			label = descriptions[name] + ", " + label
		}
		missing = append(missing, label+" ("+strings.Join(delivered, "; ")+")")
	}
	for _, host := range hosts {
		if !hasGit(file, host) {
			missing = append(missing, "git host "+host)
		}
	}
	for _, pool := range storage {
		if _, ok := file.Storage[pool]; !ok {
			missing = append(missing, "storage of pool "+pool)
		}
	}
	return missing
}

func validCA(path string) bool {
	pem, err := os.ReadFile(path)
	return err == nil && x509.NewCertPool().AppendCertsFromPEM(pem)
}
func caOptions() []string {
	entries, _ := os.ReadDir(".")
	var paths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path, err := localPath(entry.Name())
		if err == nil && validCA(path) {
			paths = append(paths, path)
			if len(paths) == 8 {
				break
			}
		}
	}
	return append(paths, "System roots", "Other file...")
}
func askCA(p *hostPicker) (string, error) {
	options := caOptions()
	selected := 0
	mark := p.position
	for {
		choice, err := p.pick("CA certificate", options, nil, &selected, false)
		if err != nil || choice < 0 || choice == len(options)-2 {
			return "", err
		}
		path := options[choice]
		if choice == len(options)-1 {
			path, err = p.line("File", "")
			if err != nil {
				return "", err
			}
			if path == "" {
				if err := p.rewind(mark); err != nil {
					return "", err
				}
				continue
			}
		}
		path, err = localPath(path)
		if err != nil {
			return "", err
		}
		if validCA(path) {
			return path, nil
		}
		if err := p.notice(path + " holds no PEM certificate"); err != nil {
			return "", err
		}
	}
}

type sourceResult struct {
	name      string
	source    credential.Source
	ok        bool
	cancelled bool
}
type sourceRequests struct {
	p     *hostPicker
	file  *busclient.Config
	asked map[string]sourceResult
}

func (r *sourceRequests) commit(result sourceResult) {
	if _, exists := r.file.Credentials[result.name]; exists {
		return
	}
	if r.file.Credentials == nil {
		r.file.Credentials = map[string]credential.Source{}
	}
	r.file.Credentials[result.name] = result.source
}
func (r *sourceRequests) request(name, label string, uses []credentialUse, nested bool) (sourceResult, error) {
	if source, ok := r.file.Credentials[name]; ok {
		if err := r.p.notice("Reusing configured credential " + name + "."); err != nil {
			return sourceResult{}, err
		}
		return sourceResult{name: name, source: source, ok: true}, nil
	}
	if result, ok := r.asked[name]; ok {
		return result, nil
	}
	result, err := r.source(name, label, uses, nested)
	if err == nil && !result.cancelled {
		r.asked[name] = result
	}
	return result, err
}
func (r *sourceRequests) source(name, label string, uses []credentialUse, nested bool) (sourceResult, error) {
	p := r.p
	mark := p.position
	options := []string{"Environment variable", "File", "Command"}
	if nested {
		options = append([]string{"Configured credential..."}, options...)
	}
	help := []string{"credential " + name}
	var variables []string
	for _, use := range uses {
		required := "required"
		if use.optional {
			required = "optional"
		}
		help = append(help, fmt.Sprintf("pool %s: %s, %s", use.pool, use.as(), required))
		if use.target.Env != "" && os.Getenv(use.target.Env) != "" && !slices.Contains(variables, use.target.Env) {
			variables = append(variables, use.target.Env)
		}
	}
	help = append(help, "An environment source sends what the variable holds to every pool", "that names "+name+", now or later.", "It is read where submits run, including the MCP server your harness starts.")
	selected, envSelected, configSelected := 0, 0, 0
	for {
		choice, err := p.pick(label, options, help, &selected, nested)
		if err != nil {
			return sourceResult{}, err
		}
		if choice < 0 {
			return sourceResult{name: name, cancelled: nested}, nil
		}
		var source credential.Source
		switch options[choice] {
		case "Configured credential...":
			keys := slices.Sorted(maps.Keys(r.file.Credentials))
			choice, err := p.pick(label, keys, help, &configSelected, true)
			if err != nil {
				return sourceResult{}, err
			}
			if choice < 0 {
				if err := p.rewind(mark); err != nil {
					return sourceResult{}, err
				}
				continue
			}
			if err := p.rewind(mark); err != nil {
				return sourceResult{}, err
			}
			if err := p.collapse(label, "Configured credential "+keys[choice], p.size()); err != nil {
				return sourceResult{}, err
			}
			return sourceResult{name: keys[choice], source: r.file.Credentials[keys[choice]], ok: true}, nil
		case "Environment variable":
			for {
				envMark := p.position
				list := append(slices.Clone(variables), "New variable...")
				choice, err := p.pick(label, list, help, &envSelected, true)
				if err != nil {
					return sourceResult{}, err
				}
				if choice < 0 {
					if err := p.rewind(mark); err != nil {
						return sourceResult{}, err
					}
					break
				}
				if choice < len(variables) {
					source.Env = variables[choice]
				} else {
					source.Env, err = p.line("Variable", "")
					if err != nil {
						return sourceResult{}, err
					}
					if source.Env == "" {
						if err := p.rewind(envMark); err != nil {
							return sourceResult{}, err
						}
						continue
					}
				}
				if source.Validate() == nil {
					break
				}
				if err := p.notice("a source is file:PATH, env:VAR or command:ARGV"); err != nil {
					return sourceResult{}, err
				}
			}
			if source.Env == "" {
				continue
			}
		case "File", "Command":
			value, err := p.line(options[choice], "")
			if err != nil {
				return sourceResult{}, err
			}
			if value == "" {
				if err := p.rewind(mark); err != nil {
					return sourceResult{}, err
				}
				continue
			}
			if options[choice] == "File" {
				source.File, err = localPath(value)
				if err != nil {
					return sourceResult{}, err
				}
			} else {
				source.Command = strings.Fields(value)
			}
		}
		if source.Validate() == nil {
			if err := p.rewind(mark); err != nil {
				return sourceResult{}, err
			}
			if err := p.collapse(label, sourceAnswer(source), p.size()); err != nil {
				return sourceResult{}, err
			}
			return sourceResult{name: name, source: source, ok: true}, nil
		}
		if err := p.notice("a source is file:PATH, env:VAR or command:ARGV"); err != nil {
			return sourceResult{}, err
		}
	}
}
func sourceAnswer(source credential.Source) string {
	if source.Env != "" {
		return "Environment variable " + source.Env
	}
	if source.File != "" {
		return "File " + source.File
	}
	return "Command " + strings.Join(source.Command, " ")
}

type gitNames struct {
	pairs  map[string]string
	owners map[string]string
}

func gitPair(host string, ssh bool) string {
	if ssh {
		return strings.ToLower(host) + " ssh"
	}
	return strings.ToLower(host) + " https"
}
func newGitNames(hosts []string, file busclient.Config) *gitNames {
	n := &gitNames{map[string]string{}, map[string]string{}}
	for _, entry := range file.Git {
		if entry.Credential != "" {
			pair := gitPair((wire.GitInput{Remote: entry.URL}).VCSHost(), strings.HasPrefix(entry.URL, "ssh://"))
			if owner, ok := n.owners[entry.Credential]; ok && owner != pair {
				pair = "shared"
			}
			n.owners[entry.Credential] = pair
		}
	}
	for _, host := range slices.Sorted(slices.Values(hosts)) {
		n.allocate(host, false)
		n.allocate(host, true)
	}
	return n
}
func (n *gitNames) allocate(host string, ssh bool) string {
	pair := gitPair(host, ssh)
	if name := n.pairs[pair]; name != "" {
		return name
	}
	mapped := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		return '-'
	}, strings.ToLower(host))
	for i := 1; ; i++ {
		suffix := ""
		if ssh {
			suffix = "-ssh"
		}
		if i > 1 {
			suffix += "-" + strconv.Itoa(i)
		}
		component := strings.TrimRight(mapped[:min(len(mapped), 32-len("git-")-len(suffix))], "-")
		if component == "" {
			component = "host"
		}
		name := "git-" + component + suffix
		if !wire.ValidName(name) {
			continue
		}
		if owner, ok := n.owners[name]; ok && owner != pair {
			continue
		}
		n.pairs[pair], n.owners[name] = name, pair
		return name
	}
}
func validGitPrefix(prefix, host string, entries []busclient.GitCredential) bool {
	return (strings.HasPrefix(prefix, "https://") || strings.HasPrefix(prefix, "ssh://")) && (wire.GitInput{Remote: prefix}).VCSHost() == host && !slices.ContainsFunc(entries, func(entry busclient.GitCredential) bool { return entry.URL == prefix })
}
func askGitPage(r *sourceRequests, host string, names *gitNames) (busclient.GitCredential, sourceResult, error) {
	selected := 0
	mark := r.p.position
	for {
		choice, err := r.p.pick(host, []string{"Anonymous over https", "Credential over https...", "SSH..."}, []string{"An unassociated existing generated credential name is presumed to belong", "to this host and transport. Configured credential allows deliberate sharing.", "That source is reused without asking.", "A blank SSH prefix uses ssh://git@" + host + "/."}, &selected, false)
		if err != nil || choice < 0 {
			return busclient.GitCredential{}, sourceResult{}, err
		}
		entry := busclient.GitCredential{URL: "https://" + host + "/"}
		if choice == 0 {
			if validGitPrefix(entry.URL, host, r.file.Git) {
				return entry, sourceResult{}, nil
			}
			continue
		}
		if choice == 2 {
			for {
				prefix, err := r.p.line("SSH URL prefix", "ssh://git@"+host+"/")
				if err != nil {
					return busclient.GitCredential{}, sourceResult{}, err
				}
				if validGitPrefix(prefix, host, r.file.Git) {
					entry.URL = prefix
					break
				}
				if err := r.p.notice("a git URL prefix for " + host + " starts with https://" + host + " or ssh://" + host + " and is not already in the config"); err != nil {
					return busclient.GitCredential{}, sourceResult{}, err
				}
			}
		}
		name := names.allocate(host, choice == 2)
		result, err := r.request(name, name, nil, true)
		if err != nil {
			return busclient.GitCredential{}, sourceResult{}, err
		}
		if !result.ok {
			if result.cancelled {
				if err := r.p.rewind(mark); err != nil {
					return busclient.GitCredential{}, sourceResult{}, err
				}
				continue
			}
			return busclient.GitCredential{}, sourceResult{}, nil
		}
		entry.Credential = result.name
		if choice == 2 {
			for {
				value, err := r.p.line("known_hosts file", "")
				if err != nil {
					return busclient.GitCredential{}, sourceResult{}, err
				}
				if value == "" {
					break
				}
				path, err := localPath(value)
				if err != nil {
					return busclient.GitCredential{}, sourceResult{}, err
				}
				if _, err := os.ReadFile(path); err == nil {
					entry.HostKey = path
					break
				}
				if err := r.p.notice(path + " is not readable"); err != nil {
					return busclient.GitCredential{}, sourceResult{}, err
				}
			}
			if entry.HostKey == "" {
				if err := r.p.rewind(mark); err != nil {
					return busclient.GitCredential{}, sourceResult{}, err
				}
				continue
			}
		}
		if validGitPrefix(entry.URL, host, r.file.Git) {
			return entry, result, nil
		}
	}
}
func storageName(file busclient.Config, base string, staged []sourceResult) string {
	for i := 1; ; i++ {
		name := base
		if i > 1 {
			name += "-" + strconv.Itoa(i)
		}
		_, exists := file.Credentials[name]
		if !exists && !slices.ContainsFunc(staged, func(result sourceResult) bool { return result.name == name }) {
			return name
		}
	}
}
func askStorage(r *sourceRequests, pools []string) (bool, error) {
	selected := 0
	mark := r.p.position
	for {
		choice, err := r.p.pick("S3 storage for pools "+poolList(pools), []string{"Key sources..."}, []string{"Set per-pool keys by editing the config."}, &selected, false)
		if err != nil || choice < 0 {
			return false, err
		}
		var results []sourceResult
		for i, base := range []string{"s3-access-key", "s3-secret-key"} {
			name := storageName(*r.file, base, results)
			label := []string{"S3 access key", "S3 secret key"}[i]
			result, err := r.request(name, label, nil, true)
			if err != nil {
				return false, err
			}
			if !result.ok {
				break
			}
			if existing, ok := r.file.Credentials[base]; ok && existing.Env == result.source.Env && existing.File == result.source.File && slices.Equal(existing.Command, result.source.Command) && result.name == name {
				result.name = base
			}
			results = append(results, result)
		}
		if len(results) != 2 {
			if err := r.p.rewind(mark); err != nil {
				return false, err
			}
			continue
		}
		for _, result := range results {
			r.commit(result)
		}
		if r.file.Storage == nil {
			r.file.Storage = map[string]busclient.StorageConfig{}
		}
		for _, pool := range pools {
			if _, exists := r.file.Storage[pool]; !exists {
				r.file.Storage[pool] = busclient.StorageConfig{AccessKey: results[0].name, SecretKey: results[1].name}
			}
		}
		return true, nil
	}
}

func poolList(pools []string) string {
	if len(pools) < 2 {
		return strings.Join(pools, "")
	}
	return strings.Join(pools[:len(pools)-1], ", ") + " and " + pools[len(pools)-1]
}

func askCredentials(r *sourceRequests, uses map[string][]credentialUse, descriptions map[string]string) (bool, error) {
	pending, changed := false, false
	for _, name := range slices.Sorted(maps.Keys(uses)) {
		if _, ok := r.file.Credentials[name]; ok {
			continue
		}
		pending = true
		label := descriptions[name]
		if label == "" {
			label = name
		}
		optional := true
		for _, use := range uses[name] {
			optional = optional && use.optional
		}
		if optional {
			label += " (optional)"
		}
		result, err := r.request(name, label, uses[name], false)
		if err != nil {
			return false, err
		}
		if result.ok {
			r.commit(result)
			changed = true
		}
	}
	if !pending {
		return false, r.p.notice("Already configured.")
	}
	return changed, nil
}
