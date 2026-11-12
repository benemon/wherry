package pluginsdk

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// workingRoot is a run directory whose outputs directory holds .wherry.
func workingRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "outputs", ".wherry"), 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

const workingName = "outputs/" + WorkingFile

func TestReadWorkingFileReadsARegularFile(t *testing.T) {
	root := workingRoot(t)
	for _, content := range []string{"step 1\n", strings.Repeat("x", MaxWorking), ""} {
		if err := os.WriteFile(filepath.Join(root, workingName), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if data, err := ReadWorkingFile(root, workingName); err != nil || string(data) != content {
			t.Fatalf("read %d bytes = %d bytes, %v; want them all", len(content), len(data), err)
		}
	}
}

// A missing file, directory or root, as after a purge, reads as nothing, without an error.
func TestReadWorkingFileMissingIsNothing(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{workingName, "missing/" + WorkingFile} {
		if data, err := ReadWorkingFile(root, name); err != nil || data != nil {
			t.Fatalf("read missing %s = %q, %v; want nothing", name, data, err)
		}
	}
	if data, err := ReadWorkingFile(filepath.Join(root, "purged"), workingName); err != nil || data != nil {
		t.Fatalf("read under a missing root = %q, %v; want nothing", data, err)
	}
}

// A file over MaxWorking reads as nothing, never as a cut prefix.
func TestReadWorkingFileDiscardsAnOversizedFile(t *testing.T) {
	root := workingRoot(t)
	for _, size := range []int{MaxWorking + 1, 1 << 20} {
		if err := os.WriteFile(filepath.Join(root, workingName), bytes.Repeat([]byte("x"), size), 0o600); err != nil {
			t.Fatal(err)
		}
		if data, err := ReadWorkingFile(root, workingName); err != nil || data != nil {
			t.Fatalf("read %d bytes = %d bytes, %v; want nothing", size, len(data), err)
		}
	}
}

// A file that grows as it is read yields at most MaxWorking bytes or nothing.
func TestReadWorkingFileBoundsAGrowingFile(t *testing.T) {
	root := workingRoot(t)
	path := filepath.Join(root, workingName)
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = file.Write([]byte("growing "))
			}
		}
	}()
	for range 200 {
		if data, err := ReadWorkingFile(root, workingName); err != nil || len(data) > MaxWorking {
			t.Fatalf("read a growing file = %d bytes, %v; want at most %d", len(data), err, MaxWorking)
		}
	}
}

// A symbolic link in the file's place or in any directory above it beneath the root is refused, even to a file
// inside the root; so is a path that leaves the root.
func TestReadWorkingFileRefusesLinksAndEscapes(t *testing.T) {
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("outside the run\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, plant := range map[string]func(root string) error{
		"final link outside": func(root string) error { return os.Symlink(outside, filepath.Join(root, workingName)) },
		"hard link outside":  func(root string) error { return os.Link(outside, filepath.Join(root, workingName)) },
		"final link inside": func(root string) error {
			if err := os.WriteFile(filepath.Join(root, "outputs", "other"), []byte("inside\n"), 0o600); err != nil {
				return err
			}
			return os.Symlink("../other", filepath.Join(root, workingName))
		},
		"parent link": func(root string) error {
			dir := filepath.Join(filepath.Dir(outside), ".wherry")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(dir, "working"), []byte("outside the run\n"), 0o600); err != nil {
				return err
			}
			if err := os.RemoveAll(filepath.Join(root, "outputs", ".wherry")); err != nil {
				return err
			}
			return os.Symlink(dir, filepath.Join(root, "outputs", ".wherry"))
		},
		"outputs link": func(root string) error {
			if err := os.RemoveAll(filepath.Join(root, "outputs")); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Join(filepath.Dir(outside), "outputs", ".wherry"), 0o700); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(filepath.Dir(outside), workingName), []byte("outside the run\n"), 0o600); err != nil {
				return err
			}
			return os.Symlink(filepath.Join(filepath.Dir(outside), "outputs"), filepath.Join(root, "outputs"))
		},
	} {
		root := workingRoot(t)
		if err := plant(root); err != nil {
			t.Fatal(err)
		}
		if data, err := ReadWorkingFile(root, workingName); !errors.Is(err, ErrRefused) || data != nil {
			t.Fatalf("%s: read = %q, %v; want refused", name, data, err)
		}
	}
	for _, name := range []string{"../secret", "outputs/../../secret", "/secret", "outputs//.wherry/working", "./outputs/.wherry/working"} {
		if data, err := ReadWorkingFile(workingRoot(t), name); err == nil || data != nil {
			t.Fatalf("read %s = %q, %v; want refused", name, data, err)
		}
	}
}

// A FIFO is refused without blocking on its open, and a device is refused.
func TestReadWorkingFileRefusesSpecialFiles(t *testing.T) {
	root := workingRoot(t)
	if err := syscall.Mkfifo(filepath.Join(root, workingName), 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := ReadWorkingFile(root, workingName)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrRefused) {
			t.Fatalf("read a FIFO: %v, want refused", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read of a FIFO with no writer blocked")
	}
	if data, err := ReadWorkingFile("/dev", "null"); !errors.Is(err, ErrRefused) || data != nil {
		t.Fatalf("read a device = %q, %v; want refused", data, err)
	}
}
