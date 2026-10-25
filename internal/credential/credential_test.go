package credential

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Each failure is its cause alone: no value, no output of the command and no path.
func TestFailuresNameNoPathOrValue(t *testing.T) {
	previous := commandTimeout
	commandTimeout = 300 * time.Millisecond
	t.Cleanup(func() { commandTimeout = previous })
	const secret = "source-output-never-named"
	t.Setenv("WHERRY_TEST_UNSET_TOKEN", "")
	for name, test := range map[string]struct {
		source Source
		want   string
	}{
		"env unset": {Source{Env: "WHERRY_TEST_UNSET_TOKEN"}, "its environment variable is unset or empty"},
		"command fails": {Source{Command: []string{"sh", "-c", "printf " + secret + "; echo " + secret + " >&2; exit 3"}},
			"its command exited with status 3"},
		"command times out": {Source{Command: []string{"sh", "-c", "printf " + secret + "; echo " + secret + " >&2; sleep 5"}},
			"its command did not finish within 300ms"},
		"command over size":      {Source{Command: []string{"sh", "-c", "yes " + secret + " | head -c 70000"}}, "its command wrote over 65536 bytes"},
		"command writes nothing": {Source{Command: []string{"sh", "-c", "echo " + secret + " >&2"}}, "its command wrote nothing"},
		"command missing":        {Source{Command: []string{filepath.Join(t.TempDir(), secret)}}, "its command did not start"},
		"file missing":           {Source{File: filepath.Join(t.TempDir(), secret)}, "no such file or directory"},
	} {
		got, err := test.source.Resolve(context.Background())
		if err == nil || err.Error() != test.want {
			t.Errorf("%s: resolve = %q, %v; want %q", name, got, err, test.want)
		}
	}
}

func TestEachSourceKindResolvesItsValueAsItIs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(path, []byte("file-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WHERRY_TEST_TOKEN", "env-value")
	for source, want := range map[*Source]string{
		{File: path}:               "file-value\n",
		{Env: "WHERRY_TEST_TOKEN"}: "env-value",
		{Command: []string{"sh", "-c", "printf 'command-value\\n'; echo command-stderr >&2"}}: "command-value\n",
	} {
		if got, err := source.Resolve(context.Background()); err != nil || string(got) != want {
			t.Errorf("resolve %+v = %q, %v; want %q", *source, got, err, want)
		}
	}
}

func TestValidateWantsExactlyOneKind(t *testing.T) {
	for name, test := range map[string]struct {
		source Source
		want   string
	}{
		"two kinds":        {Source{File: "x", Env: "A"}, "needs exactly one of file, env and command"},
		"no kind":          {Source{}, "needs exactly one of file, env and command"},
		"an empty command": {Source{Command: []string{""}}, "command names no program"},
	} {
		if err := test.source.Validate(); err == nil || err.Error() != test.want {
			t.Errorf("%s: validate = %v; want %q", name, err, test.want)
		}
	}
	for _, source := range []Source{{File: "x"}, {Env: "A"}, {Command: []string{"vault", "read"}}} {
		if err := source.Validate(); err != nil {
			t.Errorf("validate %+v: %v", source, err)
		}
	}
}

// A file source must be a regular file its group cannot write and others cannot access; the refusal names neither the
// path nor the value.
func TestFileSourcePermissions(t *testing.T) {
	const value = "file-source-value"
	path := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []os.FileMode{0o644, 0o660, 0o602, 0o620, 0o604, 0o601} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		got, err := Source{File: path}.Resolve(context.Background())
		if err == nil || err.Error() != "its file is writable by its group or open to others; chmod 600 it" {
			t.Fatalf("resolve a file of mode %o = %q, %v; want it refused", mode, got, err)
		}
		if strings.Contains(err.Error(), path) || strings.Contains(err.Error(), value) {
			t.Fatalf("resolve a file of mode %o: error %q holds the path or the value", mode, err)
		}
	}
	for _, mode := range []os.FileMode{0o600, 0o400, 0o640} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if got, err := (Source{File: path}).Resolve(context.Background()); err != nil || string(got) != value {
			t.Fatalf("resolve a file of mode %o = %q, %v", mode, got, err)
		}
	}
	dir := filepath.Join(t.TempDir(), value)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := (Source{File: dir}).Resolve(context.Background()); err == nil || err.Error() != "its file is not a regular file" {
		t.Fatalf("resolve a directory: %v", err)
	}
}
