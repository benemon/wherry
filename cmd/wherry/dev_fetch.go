package main

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/sigstore/sigstore-go/pkg/bundle"
	"github.com/sigstore/sigstore-go/pkg/root"
	"github.com/sigstore/sigstore-go/pkg/tuf"
	"github.com/sigstore/sigstore-go/pkg/verify"
)

// hashicorpKey is HashiCorp's release signing key, from https://www.hashicorp.com/.well-known/pgp-key.txt.
//
//go:embed hashicorp.asc
var hashicorpKey []byte

const (
	hashicorpFingerprint = "C874011F0AB405110D02105534365D9472D7468F"
	hashicorpReleases    = "https://releases.hashicorp.com"
	pluginReleases       = "https://api.github.com/repos/benemon/wherry-plugins/releases?per_page=100"
	// pluginIdentity is the release workflow's certificate identity, as docs/operations.md gives it for cosign.
	pluginIdentity = `^https://github\.com/benemon/wherry-plugins/\.github/workflows/release\.yml@refs/tags/nomad/v`
	pluginIssuer   = "https://token.actions.githubusercontent.com"
)

// hashicorpPins are the releases init takes in place of the newest: Nomad at the version the nomad plugin's CI tests,
// whose linux_amd64 zip SHA-256 (wherry-plugins .github/workflows/e2e.yml NOMAD_SHA256) the signed SHA256SUMS must
// list. Bumping a pin is a deliberate change of both values.
var hashicorpPins = map[string]struct{ version, linuxAMD64 string }{
	"nomad": {"2.0.7", "4c9b8a0850d6fd9caadbbab09b3e6fdf8b77aa777729543c70c61b85acca68c1"},
}

// devHTTP fetches init's downloads; a release archive takes minutes on a slow link.
var devHTTP = &http.Client{Timeout: 10 * time.Minute}

// releaseVersion is a release version without a qualifier: no +ent, -beta or -rc.
var releaseVersion = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)$`)

// newestRelease is the highest version among versions that releaseVersion matches.
func newestRelease(versions []string) (string, bool) {
	best, bestParts := "", []int(nil)
	for _, version := range versions {
		match := releaseVersion.FindStringSubmatch(version)
		if match == nil {
			continue
		}
		parts := make([]int, 3)
		for i := range parts {
			parts[i], _ = strconv.Atoi(match[i+1])
		}
		if bestParts == nil || slices.Compare(parts, bestParts) > 0 {
			best, bestParts = version, parts
		}
	}
	return best, bestParts != nil
}

// acquireHashiCorp fetches product's pinned or newest release binary for this OS and architecture into file, verifying
// SHA256SUMS against HashiCorp's key and the zip against SHA256SUMS. It returns prev when that version is in file.
func acquireHashiCorp(ctx context.Context, product, file string, prev *devArtefact, progress io.Writer) (*devArtefact, error) {
	var index struct {
		Versions map[string]struct {
			Shasums          string `json:"shasums"`
			ShasumsSignature string `json:"shasums_signature"`
		} `json:"versions"`
	}
	encoded, err := get(ctx, hashicorpReleases+"/"+product+"/index.json")
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(encoded, &index); err != nil {
		return nil, fmt.Errorf("%s index.json: %w", product, err)
	}
	var versions []string
	for version := range index.Versions {
		versions = append(versions, version)
	}
	version, ok := newestRelease(versions)
	if !ok {
		return nil, fmt.Errorf("%s index.json lists no release", product)
	}
	pin, pinned := hashicorpPins[product]
	if pinned {
		version = pin.version
		if _, ok := index.Versions[version]; !ok {
			return nil, fmt.Errorf("%s index.json lists no release %s", product, version)
		}
	}
	if prev != nil && prev.Version == version {
		if sum, err := fileSHA256(file); err == nil && sum == prev.SHA256 {
			return prev, nil
		}
	}
	base := hashicorpReleases + "/" + product + "/" + version + "/"
	release := index.Versions[version]
	sums, err := get(ctx, base+release.Shasums)
	if err != nil {
		return nil, err
	}
	signature, err := get(ctx, base+release.ShasumsSignature)
	if err != nil {
		return nil, err
	}
	if err := verifyHashiCorp(sums, signature); err != nil {
		return nil, err
	}
	if pinned {
		pinnedZip := fmt.Sprintf("%s_%s_linux_amd64.zip", product, version)
		if listed, err := listedSHA256(sums, pinnedZip); err != nil || listed != pin.linuxAMD64 {
			return nil, fmt.Errorf("SHA256SUMS does not list %s with the pinned SHA-256 %s", pinnedZip, pin.linuxAMD64)
		}
	}
	archive := fmt.Sprintf("%s_%s_%s_%s.zip", product, version, runtime.GOOS, runtime.GOARCH)
	want, err := listedSHA256(sums, archive)
	if err != nil {
		return nil, err
	}
	zipped, err := download(ctx, base+archive, filepath.Dir(file), product, progress)
	if err != nil {
		return nil, err
	}
	defer os.Remove(zipped.Name())
	defer zipped.Close()
	if zipped.sum != want {
		return nil, fmt.Errorf("%s has SHA-256 %s; SHA256SUMS lists %s", archive, zipped.sum, want)
	}
	reader, err := zip.NewReader(zipped, zipped.size)
	if err != nil {
		return nil, err
	}
	member, err := reader.Open(product)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", archive, err)
	}
	defer member.Close()
	sum, err := install(member, file)
	if err != nil {
		return nil, err
	}
	return &devArtefact{Version: version, File: relativeToCache(file), SHA256: sum, Verified: "SHA256SUMS+pgp"}, nil
}

// verifyHashiCorp checks signature, a detached binary OpenPGP signature, over sums against HashiCorp's key.
// go-crypto enforces key and signature expiry against the clock; the primary key and signing subkey expire 2030-03-01.
func verifyHashiCorp(sums, signature []byte) error {
	keyring, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(hashicorpKey))
	if err != nil {
		return err
	}
	signer, err := openpgp.CheckDetachedSignature(keyring, bytes.NewReader(sums), bytes.NewReader(signature), nil)
	if err != nil {
		return fmt.Errorf("SHA256SUMS signature: %w", err)
	}
	if fingerprint := strings.ToUpper(hex.EncodeToString(signer.PrimaryKey.Fingerprint)); fingerprint != hashicorpFingerprint {
		return fmt.Errorf("SHA256SUMS signed by %s, not HashiCorp's release key", fingerprint)
	}
	return nil
}

// listedSHA256 is name's SHA-256 in a sha256sum listing.
func listedSHA256(sums []byte, name string) (string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(sums))
	for scanner.Scan() {
		if sum, listed, ok := strings.Cut(scanner.Text(), "  "); ok && listed == name {
			return sum, nil
		}
	}
	return "", fmt.Errorf("the checksums list no %s", name)
}

type githubRelease struct {
	Tag    string `json:"tag_name"`
	Draft  bool   `json:"draft"`
	Assets []struct {
		Name string `json:"name"`
		URL  string `json:"browser_download_url"`
	} `json:"assets"`
}

func (release githubRelease) asset(name string) string {
	for _, asset := range release.Assets {
		if asset.Name == name {
			return asset.URL
		}
	}
	return ""
}

// acquirePlugin fetches the newest nomad plugin release whose protocol this wherry speaks into file. It returns prev
// when the newest release is in file.
func acquirePlugin(ctx context.Context, file string, prev *devPlugin, progress io.Writer) (*devPlugin, error) {
	encoded, err := get(ctx, pluginReleases)
	if err != nil {
		return nil, err
	}
	var releases []githubRelease
	if err := json.Unmarshal(encoded, &releases); err != nil {
		return nil, fmt.Errorf("plugin releases: %w", err)
	}
	// The API lists releases newest first. Every nomad release so far is a prerelease, so prereleases count.
	// The 1.0 cut switches this to GA releases only.
	releases = slices.DeleteFunc(releases, func(release githubRelease) bool {
		return release.Draft || !strings.HasPrefix(release.Tag, "nomad/v")
	})
	if len(releases) == 0 {
		return nil, errors.New("benemon/wherry-plugins has no nomad/v* release")
	}
	if prev != nil && prev.Tag == releases[0].Tag {
		if sum, err := fileSHA256(file); err == nil && sum == prev.SHA256 {
			return prev, nil
		}
	}
	tufCache := filepath.Join(filepath.Dir(filepath.Dir(file)), "tuf")
	for _, release := range releases {
		archive := fmt.Sprintf("wherry-plugin-nomad_%s_%s_%s.tar.gz", strings.TrimPrefix(release.Tag, "nomad/"), runtime.GOOS, runtime.GOARCH)
		urls := []string{release.asset("checksums.txt"), release.asset("checksums.txt.sigstore.json"), release.asset(archive)}
		if slices.Contains(urls, "") {
			fmt.Fprintf(progress, "plugin: %s has no %s with checksums.txt and its sigstore bundle; trying the next older release\n", release.Tag, archive)
			continue
		}
		checksums, err := get(ctx, urls[0])
		if err != nil {
			return nil, err
		}
		signature, err := get(ctx, urls[1])
		if err != nil {
			return nil, err
		}
		if err := verifySigstore(checksums, signature, tufCache); err != nil {
			return nil, fmt.Errorf("%s checksums.txt: %w", release.Tag, err)
		}
		want, err := listedSHA256(checksums, archive)
		if err != nil {
			return nil, err
		}
		compressed, err := download(ctx, urls[2], filepath.Dir(file), release.Tag, progress)
		if err != nil {
			return nil, err
		}
		defer os.Remove(compressed.Name())
		defer compressed.Close()
		if compressed.sum != want {
			return nil, fmt.Errorf("%s has SHA-256 %s; checksums.txt lists %s", archive, compressed.sum, want)
		}
		binary := file + ".candidate"
		sum, err := extractTarMember(compressed, strings.TrimSuffix(archive, ".tar.gz")+"/wherry-plugin-nomad", binary)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", archive, err)
		}
		protocol, err := handshake(binary)
		if err != nil && strings.Contains(strings.ToLower(err.Error()), "incompatible api version") {
			os.Remove(binary)
			fmt.Fprintf(progress, "plugin: %s speaks another protocol; trying the next older release\n", release.Tag)
			continue
		}
		if err != nil {
			os.Remove(binary)
			return nil, fmt.Errorf("%s handshake: %w", release.Tag, err)
		}
		if err := os.Rename(binary, file); err != nil {
			return nil, err
		}
		return &devPlugin{Tag: release.Tag, Protocol: protocol, File: relativeToCache(file), SHA256: sum, Verified: "checksums.txt+sigstore"}, nil
	}
	return nil, errors.New("no nomad plugin release speaks this wherry's plugin protocol; a plugin release must follow the protocol change")
}

// handshake starts the plugin at path and stops it, returning the protocol version it negotiated.
func handshake(path string) (int, error) {
	process, err := pluginsdk.Launch(func() (string, error) { return path, nil }, io.Discard)
	if err != nil {
		return 0, err
	}
	defer process.Close()
	_, protocol := process.Version()
	return protocol, nil
}

// verifySigstore checks a keyless sigstore bundle over artifact against the plugin release workflow's identity, with
// the public-good trusted root fetched through TUF into tufCache.
func verifySigstore(artifact, bundleJSON []byte, tufCache string) error {
	trusted, err := root.FetchTrustedRootWithOptions(tuf.DefaultOptions().WithCachePath(tufCache))
	if err != nil {
		return fmt.Errorf("sigstore trusted root: %w", err)
	}
	return verifyBundle(trusted, artifact, bundleJSON)
}

func verifyBundle(trusted root.TrustedMaterial, artifact, bundleJSON []byte) error {
	var signed bundle.Bundle
	if err := signed.UnmarshalJSON(bundleJSON); err != nil {
		return fmt.Errorf("sigstore bundle: %w", err)
	}
	verifier, err := verify.NewVerifier(trusted, verify.WithSignedCertificateTimestamps(1), verify.WithTransparencyLog(1), verify.WithObserverTimestamps(1))
	if err != nil {
		return err
	}
	identity, err := verify.NewShortCertificateIdentity(pluginIssuer, "", "", pluginIdentity)
	if err != nil {
		return err
	}
	if _, err := verifier.Verify(&signed, verify.NewPolicy(verify.WithArtifact(bytes.NewReader(artifact)), verify.WithCertificateIdentity(identity))); err != nil {
		return fmt.Errorf("sigstore verification: %w", err)
	}
	return nil
}

// acquireImage pulls reference with runtime, showing the runtime's output, and tags the pulled image
// repository:<id12> in the runtime's store. It returns prev when the pulled digest is the one prev records.
func acquireImage(ctx context.Context, runtime, reference, repository string, prev *devImage, output io.Writer) (*devImage, error) {
	pull := exec.CommandContext(ctx, runtime, "pull", reference)
	pull.Stdout, pull.Stderr = output, output
	if err := pull.Run(); err != nil {
		return nil, fmt.Errorf("%s pull %s: %w", runtime, reference, err)
	}
	inspected, err := runtimeOutput(ctx, runtime, "image", "inspect", "--format", "{{.Id}} {{index .RepoDigests 0}}", reference)
	if err != nil {
		return nil, err
	}
	id, repoDigest, _ := strings.Cut(strings.TrimSpace(inspected), " ")
	_, digest, ok := strings.Cut(repoDigest, "@")
	if !ok || id == "" {
		return nil, fmt.Errorf("%s image inspect %s gave %q, not an ID and a repository digest", runtime, reference, inspected)
	}
	id = "sha256:" + strings.TrimPrefix(id, "sha256:")
	// Podman stores an unqualified repository under localhost/, the name its driver resolves locally.
	if runtime == "podman" {
		repository = "localhost/" + repository
	}
	tag := repository + ":" + strings.TrimPrefix(id, "sha256:")[:12]
	// Tagging again restores a tag a prune removed.
	if _, err := runtimeOutput(ctx, runtime, "tag", id, tag); err != nil {
		return nil, err
	}
	if prev != nil && prev.Digest == digest && prev.Tag == tag && prev.ID == id {
		return prev, nil
	}
	return &devImage{Reference: reference, Digest: digest, ID: id, Tag: tag}, nil
}

// runtimeOutput runs the container runtime and returns its stdout; an error carries its stderr.
func runtimeOutput(ctx context.Context, runtime string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, runtime, args...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w: %s", runtime, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(output), nil
}

// get fetches url, sending GH_TOKEN, else GITHUB_TOKEN, to GitHub's API when set.
func get(ctx context.Context, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if token := cmp.Or(os.Getenv("GH_TOKEN"), os.Getenv("GITHUB_TOKEN")); token != "" && request.URL.Host == "api.github.com" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := devHTTP.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusForbidden && response.Header.Get("X-RateLimit-Remaining") == "0" {
		reset, _ := strconv.ParseInt(response.Header.Get("X-RateLimit-Reset"), 10, 64)
		return nil, fmt.Errorf("GET %s: GitHub's API rate limit is spent until %s; GH_TOKEN or GITHUB_TOKEN raises it", url, time.Unix(reset, 0).UTC().Format(time.RFC3339))
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, response.Status)
	}
	return io.ReadAll(io.LimitReader(response.Body, 64<<20))
}

// downloaded is a downloaded file, open at its start, with its size and SHA-256.
type downloaded struct {
	*os.File
	size int64
	sum  string
}

// download writes url to a temporary file in dir, counting bytes on progress when it is a terminal.
func download(ctx context.Context, url, dir, label string, progress io.Writer) (downloaded, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return downloaded{}, err
	}
	response, err := devHTTP.Do(request)
	if err != nil {
		return downloaded{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return downloaded{}, fmt.Errorf("GET %s: %s", url, response.Status)
	}
	file, err := os.CreateTemp(dir, ".download-*")
	if err != nil {
		return downloaded{}, err
	}
	sum := sha256.New()
	var body io.Reader = response.Body
	if terminal, ok := progress.(*os.File); ok && isTerminal(terminal) {
		counter := &byteCounter{out: progress, label: label, total: response.ContentLength}
		defer counter.finish()
		body = io.TeeReader(body, counter)
	}
	size, err := io.Copy(io.MultiWriter(file, sum), body)
	if err == nil {
		_, err = file.Seek(0, io.SeekStart)
	}
	if err != nil {
		file.Close()
		os.Remove(file.Name())
		return downloaded{}, err
	}
	return downloaded{file, size, hex.EncodeToString(sum.Sum(nil))}, nil
}

// byteCounter rewrites one terminal line with the bytes written so far and the Content-Length.
type byteCounter struct {
	out          io.Writer
	label        string
	total, count int64
	shown        time.Time
}

func (c *byteCounter) Write(p []byte) (int, error) {
	c.count += int64(len(p))
	if time.Since(c.shown) > 100*time.Millisecond {
		c.show()
	}
	return len(p), nil
}

func (c *byteCounter) show() {
	c.shown = time.Now()
	if c.total > 0 {
		fmt.Fprintf(c.out, "\r%s: %.1f of %.1f MB", c.label, float64(c.count)/1e6, float64(c.total)/1e6)
		return
	}
	fmt.Fprintf(c.out, "\r%s: %.1f MB", c.label, float64(c.count)/1e6)
}

func (c *byteCounter) finish() {
	c.show()
	fmt.Fprintln(c.out)
}

// install writes source to file through a temporary file and a rename, mode 0755, returning its SHA-256.
func install(source io.Reader, file string) (string, error) {
	data, err := io.ReadAll(source)
	if err != nil {
		return "", err
	}
	if err := writeFileAtomic(file, data, 0o755); err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// extractTarMember installs the regular file name from a gzipped tar archive as file.
func extractTarMember(archive io.Reader, name, file string) (string, error) {
	decompressed, err := gzip.NewReader(archive)
	if err != nil {
		return "", err
	}
	reader := tar.NewReader(decompressed)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			return "", fmt.Errorf("no %s in the archive", name)
		}
		if err != nil {
			return "", err
		}
		if header.Name == name && header.Typeflag == tar.TypeReg {
			return install(reader, file)
		}
	}
}

// relativeToCache is file's path under the instance's cache directory.
func relativeToCache(file string) string {
	if filepath.Base(filepath.Dir(file)) == "cache" {
		return filepath.Base(file)
	}
	return filepath.Join(filepath.Base(filepath.Dir(file)), filepath.Base(file))
}
