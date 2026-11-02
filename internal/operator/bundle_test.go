package operator

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A bundle written without replace leaves a file created at its path after createBundle checked it.
func TestBundleWithoutReplaceKeepsAFileCreatedAfterTheCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "host.bundle")
	bundle, err := createBundle(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func(link, rename func(string, string) error) { linkFile, renameFile = link, rename }(linkFile, renameFile)
	inject := func(link func(string, string) error) func(string, string) error {
		return func(from, to string) error {
			if err := os.WriteFile(to, []byte("other"), 0o600); err != nil {
				return err
			}
			return link(from, to)
		}
	}
	linkFile, renameFile = inject(linkFile), inject(renameFile)
	err = writeBundle(bundle, Bundle{Broker: "broker:9093", Principal: "tenant.tenant-a.host-a", Password: "password"})
	closeBundle(bundle, &err)
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("closeBundle = %v; want the file created since the check refused", err)
	}
	if contents, err := os.ReadFile(path); err != nil || string(contents) != "other" {
		t.Fatalf("%s holds %q, %v; want the file created since the check", path, contents, err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
		t.Fatalf("bundle directory holds %v, %v; want the other file alone", entries, err)
	}
}
