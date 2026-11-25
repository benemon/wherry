package client

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// writeTree writes files, each at its relative path beneath dir with the path as its content, and returns dir.
func writeTree(t *testing.T, dir string, files ...string) string {
	t.Helper()
	for _, file := range files {
		path := filepath.Join(dir, filepath.FromSlash(file))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// walked walks dir as the input base and counts the files its opens then see.
func walked(t *testing.T, dir, base string) ([]InputFile, int, *atomic.Int64, error) {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	var opened atomic.Int64
	files, skipped, err := DirInputs(root, base, func(string, fs.FileInfo) error { opened.Add(1); return nil })
	return files, skipped, &opened, err
}

// A directory's files are delivered as base/<relative path> in lexical order with their bytes; entries named with a
// leading dot, files and directories alike, are skipped and counted, and nothing opens until a file is opened.
func TestDirInputsDeliversNestedFilesSkippingHiddenEntries(t *testing.T) {
	dir := writeTree(t, t.TempDir(), "readme.md", "a/b.md", "a/c/d.txt", ".hidden", ".git/config", "a/.dot", "a/.cache/x")
	files, skipped, opened, err := walked(t, dir, "docs")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, file := range files {
		names = append(names, file.Name)
	}
	if want := []string{"docs/a/b.md", "docs/a/c/d.txt", "docs/readme.md"}; !slices.Equal(names, want) || skipped != 4 {
		t.Fatalf("walked %v with %d hidden entries skipped, want %v and 4", names, skipped, want)
	}
	if opened.Load() != 0 {
		t.Fatalf("the walk opened %d files", opened.Load())
	}
	for _, file := range files {
		body, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(body)
		body.Close()
		if err != nil || string(data) != strings.TrimPrefix(file.Name, "docs/") {
			t.Fatalf("%s holds %q, %v", file.Name, data, err)
		}
	}
	if opened.Load() != 3 {
		t.Fatalf("the opens saw %d files, want 3", opened.Load())
	}
}

// A symbolic link, into or out of the directory, a FIFO and a directory with no file to send refuse the walk naming the
// entry, before any file is opened.
func TestDirInputsRefusesWhatIsNotARegularFile(t *testing.T) {
	outside := writeTree(t, t.TempDir(), "elsewhere")
	for name, test := range map[string]struct {
		setup func(t *testing.T, dir string)
		want  string
	}{
		"a link out": {func(t *testing.T, dir string) {
			if err := os.Symlink(filepath.Join(outside, "elsewhere"), filepath.Join(dir, "a", "link")); err != nil {
				t.Fatal(err)
			}
		}, "docs/a/link is not a regular file"},
		"a link in": {func(t *testing.T, dir string) {
			if err := os.Symlink("b.md", filepath.Join(dir, "a", "link")); err != nil {
				t.Fatal(err)
			}
		}, "docs/a/link is not a regular file"},
		"a link to a directory in": {func(t *testing.T, dir string) {
			if err := os.Symlink("a", filepath.Join(dir, "link")); err != nil {
				t.Fatal(err)
			}
		}, "docs/link is not a regular file"},
		"a FIFO": {func(t *testing.T, dir string) {
			if err := syscall.Mkfifo(filepath.Join(dir, "fifo"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, "docs/fifo is not a regular file"},
		"a socket": {func(t *testing.T, dir string) {
			if err := syscall.Mknod(filepath.Join(dir, "sock"), syscall.S_IFSOCK|0o600, 0); err != nil {
				t.Fatal(err)
			}
		}, "docs/sock is not a regular file"},
		"no file": {func(t *testing.T, dir string) {
			if err := os.RemoveAll(filepath.Join(dir, "a")); err != nil {
				t.Fatal(err)
			}
			writeTree(t, dir, ".hidden/x")
		}, "no file to send beneath it"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := writeTree(t, t.TempDir(), "a/b.md")
			test.setup(t, dir)
			_, _, opened, err := walked(t, dir, "docs")
			if err == nil || err.Error() != test.want {
				t.Fatalf("walk = %v, want %q", err, test.want)
			}
			if opened.Load() != 0 {
				t.Fatalf("the walk opened %d files", opened.Load())
			}
		})
	}
}

// A file swapped for a FIFO between the walk and its open is refused, without the open blocking for a writer.
func TestDirInputsOpenRefusesAFIFOSwappedInAfterTheWalk(t *testing.T) {
	dir := writeTree(t, t.TempDir(), "a.txt")
	files, _, _, err := walked(t, dir, "docs")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "a.txt")); err != nil || syscall.Mkfifo(filepath.Join(dir, "a.txt"), 0o600) != nil {
		t.Fatal(err)
	}
	refused := make(chan error, 1)
	go func() { _, err := files[0].Open(); refused <- err }()
	select {
	case err := <-refused:
		if err == nil || err.Error() != "not a regular file" {
			t.Fatalf("open = %v, want the FIFO refused", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("opening the FIFO blocked")
	}
}

// The base is the directory's own name, a trailing slash removed, the working directory's for ".", and the root
// directory has none.
func TestDirBase(t *testing.T) {
	work := t.TempDir()
	t.Chdir(work)
	for dir, want := range map[string]string{"docs/": "docs", "docs": "docs", "./docs/": "docs", filepath.Join(work, "docs") + "/": "docs", ".": filepath.Base(work), "..": filepath.Base(filepath.Dir(work))} {
		if base, err := DirBase(dir); err != nil || base != want {
			t.Errorf("DirBase(%q) = %q, %v; want %q", dir, base, err, want)
		}
	}
	if base, err := DirBase("/"); err == nil {
		t.Fatalf("DirBase(/) = %q, want it refused", base)
	}
}

// counted is an InputFile over data whose opens are counted.
func counted(name string, data []byte, opens *atomic.Int64) InputFile {
	return InputFile{Name: name, Open: func() (io.ReadSeekCloser, error) {
		opens.Add(1)
		return readSeekCloser{strings.NewReader(string(data))}, nil
	}}
}

type readSeekCloser struct{ *strings.Reader }

func (readSeekCloser) Close() error { return nil }

// Submit checks every input's name before it opens any file: a name repeated across two inputs, one that is a
// directory of another, one under previous/, one that is not a portable path, and more parts than a run carries are
// refused with no file opened.
func TestSubmitChecksInputNamesBeforeOpeningAFile(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	var opens atomic.Int64
	many := make([]InputFile, wire.MaxParts)
	for i := range many {
		many[i] = counted("docs/"+strings.Repeat("a", i+1), []byte("x"), &opens)
	}
	for name, test := range map[string]struct {
		files []InputFile
		want  string
	}{
		"a name repeated across two flags": {[]InputFile{counted("docs/a.md", []byte("1"), &opens), counted("docs/a.md", []byte("2"), &opens)}, "input docs/a.md is named twice"},
		"a parent of another name":         {[]InputFile{counted("docs", []byte("1"), &opens), counted("docs/a.md", []byte("2"), &opens)}, "input docs/a.md lies under input docs"},
		"a name under previous/":           {[]InputFile{counted("previous/a.md", []byte("1"), &opens)}, "input previous/a.md: names under previous/ are reserved for a continued run's outputs"},
		"a hidden name":                    {[]InputFile{counted(".docs/a.md", []byte("1"), &opens)}, "input .docs/a.md is not a relative path of portable file names"},
		"the prompt counted toward 256":    {many, "a run carries at most 256 parts and S3 inputs, the prompt included, not 257"},
	} {
		t.Run(name, func(t *testing.T) {
			opens.Store(0)
			_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Files: test.files, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("submit = %v, want %q", err, test.want)
			}
			if opens.Load() != 0 {
				t.Fatalf("submit opened %d files before refusing the names", opens.Load())
			}
		})
	}
	t.Run("a run's worth of inputs", func(t *testing.T) {
		opens.Store(0)
		result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Files: many[:len(many)-1], EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		if task := receiver.claim(t, f.ctx, result.RunID)[0].submission; len(task.Message.Parts) != wire.MaxParts || opens.Load() != int64(len(many)-1) {
			t.Fatalf("%d parts and %d opens, want %d and %d", len(task.Message.Parts), opens.Load(), wire.MaxParts, len(many)-1)
		}
	})
}

// An input's open failing refuses the submission naming the input, as an invalid input, with the opener's reason.
func TestSubmitRefusesAnInputItCannotOpen(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	file := InputFile{Name: "docs/a.md", Open: func() (io.ReadSeekCloser, error) { return nil, errors.New("not a regular file") }}
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Files: []InputFile{file}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "input docs/a.md: not a regular file") {
		t.Fatalf("submit = %v, want the input named with its reason", err)
	}
}
