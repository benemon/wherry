package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/catalog"
)

// fixtureServer serves testdata/api.json at /api.json, as handler changes it.
func fixtureServer(t *testing.T, handler func(w http.ResponseWriter, data []byte)) (*httptest.Server, []byte) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "api.json"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api.json" {
			http.NotFound(w, r)
			return
		}
		handler(w, data)
	}))
	t.Cleanup(server.Close)
	return server, data
}

func serve(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// committed writes the catalog directory as a commit leaves it and returns a check that both files are unchanged.
func committed(t *testing.T) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{"models.dev.json": "{\n  \"old\": {\n    \"id\": \"old\"\n  }\n}\n", "SOURCE": "url: old\nfetched: old\nsha256: old\n"}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir, func() {
		t.Helper()
		for name, want := range files {
			got, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil || string(got) != want {
				t.Errorf("%s changed: %q, %v", name, got, err)
			}
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 2 {
			t.Errorf("directory holds %d entries after a failed update; want the two committed files", len(entries))
		}
	}
}

// SOURCE records the URL, the fetch time in UTC and the SHA-256 of the fixture bytes as served, not of the stripped
// JSON; models.dev.json holds the providers without their models, ids and keys sorted, numbers and non-ASCII kept.
func TestUpdateWritesTheSnapshotAndItsSource(t *testing.T) {
	server, data := fixtureServer(t, serve)
	dir, _ := committed(t)
	now := time.Date(2026, 11, 20, 9, 30, 15, 0, time.FixedZone("CET", 3600))
	url := server.URL + "/api.json"
	if err := update(context.Background(), server.Client(), url, dir, now); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(dir, "SOURCE"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	want := fmt.Sprintf("url: %s\nfetched: 2026-11-20T08:30:15Z\nsha256: %s\n", url, hex.EncodeToString(sum[:]))
	if string(source) != want {
		t.Errorf("SOURCE\n%s\nwant\n%s", source, want)
	}
	snapshot, err := os.ReadFile(filepath.Join(dir, "models.dev.json"))
	if err != nil {
		t.Fatal(err)
	}
	wantSnapshot := `{
  "alpha": {
    "api": "https://api.alpha.example/v1",
    "doc": "https://alpha.example/docs",
    "env": [
      "ALPHA_API_KEY",
      "ALPHA_TOKEN"
    ],
    "id": "alpha",
    "name": "Alpha",
    "npm": "@ai-sdk/alpha"
  },
  "zeta": {
    "doc": "https://zeta.example/docs?a=1&b=2",
    "env": [
      "ZETA_API_KEY"
    ],
    "id": "zeta",
    "name": "Zeta & Co",
    "npm": "@ai-sdk/openai-compatible"
  }
}
`
	if string(snapshot) != wantSnapshot {
		t.Errorf("models.dev.json\n%s\nwant\n%s", snapshot, wantSnapshot)
	}
	if info, err := os.Stat(filepath.Join(dir, "models.dev.json")); err != nil || info.Mode().Perm() != 0o644 {
		t.Errorf("models.dev.json mode %v, %v; want 0644", info.Mode(), err)
	}
	parsed, err := catalog.Parse(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if entry, ok := parsed.Entry("zeta"); !ok || entry.Name != "Zeta & Co" || entry.Doc != "https://zeta.example/docs?a=1&b=2" {
		t.Errorf("zeta = %+v, %t", entry, ok)
	}
}

// Stripping is deterministic: the same bytes, served twice, and the fixture with its keys in another order give the same
// snapshot.
func TestUpdateIsDeterministic(t *testing.T) {
	server, data := fixtureServer(t, serve)
	reordered := []byte(`{"alpha":{"api":"https://api.alpha.example/v1","npm":"@ai-sdk/alpha","doc":"https://alpha.example/docs","name":"Alpha","env":["ALPHA_API_KEY","ALPHA_TOKEN"],"id":"alpha","models":{}},` +
		`"zeta":{"doc":"https://zeta.example/docs?a=1&b=2","id":"zeta","env":["ZETA_API_KEY"],"name":"Zeta & Co","npm":"@ai-sdk/openai-compatible","models":{"z-1":{"id":"z-1"}}}}`)
	var snapshots []string
	for i, body := range [][]byte{data, data, reordered} {
		dir := t.TempDir()
		served := body
		server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serve(w, served) })
		if err := update(context.Background(), server.Client(), server.URL+"/api.json", dir, time.Now()); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
		snapshot, err := os.ReadFile(filepath.Join(dir, "models.dev.json"))
		if err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, string(snapshot))
	}
	if snapshots[0] != snapshots[1] || snapshots[0] != snapshots[2] {
		t.Errorf("snapshots differ:\n%s\n%s\n%s", snapshots[0], snapshots[1], snapshots[2])
	}
	if strings.Contains(snapshots[0], "models") {
		t.Errorf("snapshot keeps models:\n%s", snapshots[0])
	}
}

// A failed read leaves the committed files unchanged: a server error, a response cut short, and a body that is not a
// models.dev document.
func TestUpdateKeepsTheCommittedFilesOnAFailedRead(t *testing.T) {
	for _, c := range []struct {
		name    string
		handler func(w http.ResponseWriter, data []byte)
		want    string
	}{
		{"server error", func(w http.ResponseWriter, _ []byte) { http.Error(w, "down", http.StatusServiceUnavailable) }, "503"},
		{"cut short", func(w http.ResponseWriter, data []byte) {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			w.Write(data[:len(data)/2])
			w.(http.Flusher).Flush()
			// Closing the connection with bytes outstanding fails the client's read.
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				conn.Close()
			}
		}, "unexpected EOF"},
		{"not an object", func(w http.ResponseWriter, _ []byte) { w.Write([]byte(`["alpha"]`)) }, "cannot unmarshal"},
		{"trailing data", func(w http.ResponseWriter, data []byte) { w.Write(data); w.Write([]byte("{}")) }, "trailing data"},
	} {
		t.Run(c.name, func(t *testing.T) {
			server, _ := fixtureServer(t, c.handler)
			dir, unchanged := committed(t)
			err := update(context.Background(), server.Client(), server.URL+"/api.json", dir, time.Now())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("update = %v; want an error naming %q", err, c.want)
			}
			unchanged()
		})
	}
}

// A failed write leaves the committed files unchanged: a directory that cannot be written to keeps both files, and
// no temporary file is left in it.
func TestUpdateKeepsTheCommittedFilesOnAFailedWrite(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes to a read-only directory")
	}
	server, _ := fixtureServer(t, serve)
	dir, unchanged := committed(t)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	err := update(context.Background(), server.Client(), server.URL+"/api.json", dir, time.Now())
	if !errors.Is(err, os.ErrPermission) {
		t.Errorf("update = %v; want a permission error", err)
	}
	unchanged()
}

// The commit is all or nothing: when the second rename fails after the first succeeded, the file renamed first is put
// back, both committed files read as before, and no temporary or backup file is left.
func TestUpdateRestoresTheFirstFileWhenTheSecondRenameFails(t *testing.T) {
	server, _ := fixtureServer(t, serve)
	dir, unchanged := committed(t)
	original := rename
	t.Cleanup(func() { rename = original })
	var renamed []string
	rename = func(from, to string) error {
		if len(renamed) == 1 {
			return errors.New("injected rename failure")
		}
		renamed = append(renamed, filepath.Base(to))
		return original(from, to)
	}
	err := update(context.Background(), server.Client(), server.URL+"/api.json", dir, time.Now())
	if err == nil || !strings.Contains(err.Error(), "injected rename failure") {
		t.Fatalf("update = %v; want the injected failure", err)
	}
	// SOURCE sorts before models.dev.json, so SOURCE was committed and then put back.
	if !slices.Equal(renamed, []string{"SOURCE"}) {
		t.Errorf("renamed %v before the failure; want SOURCE alone", renamed)
	}
	unchanged()
}

// A target that is not a regular file, as a directory left at SOURCE is, refuses the commit before any rename, so the
// snapshot keeps its bytes and the directory is left.
func TestUpdateRefusesADirectoryAtATarget(t *testing.T) {
	server, _ := fixtureServer(t, serve)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "models.dev.json"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "SOURCE"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SOURCE", "keep"), []byte("old-source"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := update(context.Background(), server.Client(), server.URL+"/api.json", dir, time.Now())
	if err == nil || !strings.Contains(err.Error(), "is not a regular file") {
		t.Fatalf("update = %v; want the directory refused", err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "models.dev.json")); err != nil || string(got) != "old" {
		t.Errorf("snapshot changed to %q, %v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "SOURCE", "keep")); err != nil || string(got) != "old-source" {
		t.Errorf("the directory at SOURCE changed: %q, %v", got, err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 2 {
		t.Errorf("directory holds %d entries after the refusal; want the two it had", len(entries))
	}
}

// The updater builds from the standard library and the catalog's snapshot package alone: the runtime catalog, with its
// wire rules, signing and plugin dependencies, is not among its dependencies.
func TestUpdateDependsOnTheStandardLibraryAndTheSnapshotPackageAlone(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".")
	out, err := command.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	var deps []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			deps = append(deps, line)
		}
	}
	want := []string{"github.com/benemon/wherry/internal/catalog/snapshot", "github.com/benemon/wherry/internal/catalog/update"}
	if !slices.Equal(deps, want) {
		t.Errorf("dependencies outside the standard library:\n%s\nwant %v", out, want)
	}
}
