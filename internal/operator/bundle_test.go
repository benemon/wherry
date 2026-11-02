package operator

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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

// A reissue that fails writing, closing or renaming its bundle after the password changed removes its temporary file,
// leaves the old bundle, and says the old bundle no longer works and that --force again recovers.
func TestReissueFailingAfterThePasswordChanges(t *testing.T) {
	injected := errors.New("injected")
	for _, step := range []struct {
		name   string
		inject func() func()
	}{
		{"write", func() func() {
			saved := writeFile
			writeFile = func(*os.File, []byte) (int, error) { return 0, injected }
			return func() { writeFile = saved }
		}},
		{"close", func() func() {
			saved := closeFile
			closeFile = func(file *os.File) error { file.Close(); return injected }
			return func() { closeFile = saved }
		}},
		{"rename", func() func() {
			saved := renameFile
			renameFile = func(string, string) error { return injected }
			return func() { renameFile = saved }
		}},
	} {
		for _, identity := range []struct {
			name    string
			reissue func(*Service, string) error
		}{
			{"host", func(service *Service, path string) error {
				return service.HostAdd(t.Context(), "tenant-a", "host-a", path, true)
			}},
			{"receiver", func(service *Service, path string) error {
				return service.ReceiverReissue(t.Context(), "receiver-a", path)
			}},
		} {
			t.Run(step.name+"/"+identity.name, func(t *testing.T) {
				_, service, _ := compromiseFixture(t)
				dir := t.TempDir()
				path := filepath.Join(dir, "old.bundle")
				if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
					t.Fatal(err)
				}
				restore := step.inject()
				err := identity.reissue(service, path)
				restore()
				if !errors.Is(err, injected) || !strings.Contains(err.Error(), "old bundle no longer works") || !strings.Contains(err.Error(), "--force again") {
					t.Fatalf("reissue = %v; want the injected failure, that the old bundle no longer works and that --force again recovers", err)
				}
				if contents, err := os.ReadFile(path); err != nil || string(contents) != "old" {
					t.Fatalf("%s holds %q, %v; want the old bundle", path, contents, err)
				}
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
					t.Fatalf("bundle directory holds %v, %v; want the old bundle alone", entries, err)
				}
			})
		}
	}
}
