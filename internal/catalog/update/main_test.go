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
	"path/filepath"
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
