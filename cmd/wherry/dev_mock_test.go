package main

import (
	"bytes"
	"crypto/sha256"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// mockTask makes a task directory as the nomad plugin lays one out under raw_exec, with prompt.txt holding a preamble
// and prompt, an input and a credential file, and changes to it.
func mockTask(t *testing.T, prompt string) string {
	t.Helper()
	dir := t.TempDir()
	for path, data := range map[string]string{
		"prompt.txt":                "Your input directory is ...\nWhen you finish, ...\n\n" + prompt,
		"inputs/data.txt":           "input bytes",
		"secrets/credentials/token": "secret",
	} {
		if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(path)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, path), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("WHERRY_INPUT_DIR", filepath.Join(dir, "inputs"))
	t.Setenv("WHERRY_OUTPUT_DIR", filepath.Join(dir, "outputs"))
	t.Setenv("WHERRY_CREDENTIALS_DIR", filepath.Join(dir, "secrets", "credentials"))
	t.Chdir(dir)
	return dir
}

func outcome(t *testing.T, dir string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "outputs", ".wherry", "outcome.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestDevMock(t *testing.T) {
	t.Run("echo", func(t *testing.T) {
		dir := mockTask(t, "summarise this\nsecond line")
		t.Setenv("MOCK_TOKEN", "the-token-value")
		var stdout bytes.Buffer
		if err := run([]string{"dev", "mock"}, &stdout, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		if echoed, err := os.ReadFile(filepath.Join(dir, "outputs", "echo.txt")); err != nil || string(echoed) != "summarise this\nsecond line" {
			t.Fatalf("echo.txt %q, %v; want the task's prompt", echoed, err)
		}
		if lines := strings.Split(strings.TrimSpace(stdout.String()), "\n"); len(lines) != 4 || lines[0] != `{"step":"read-prompt"}` ||
			lines[2] != `{"count":1,"step":"inputs"}` || lines[3] != `{"count":1,"step":"credentials"}` {
			t.Fatalf("stdout %q", stdout.String())
		}
		got := outcome(t, dir)
		text := got["message"].(map[string]any)["parts"].([]any)[0].(map[string]any)["text"].(string)
		home, _ := os.UserHomeDir()
		for _, want := range []string{
			fmt.Sprintf("uid %d", os.Getuid()), "HOME " + home, "cwd " + dir,
			fmt.Sprintf("input data.txt 600 %d %x", os.Getuid(), sha256.Sum256([]byte("input bytes"))),
			fmt.Sprintf("credential token 600 %d %x", os.Getuid(), sha256.Sum256([]byte("secret"))), "MOCK_TOKEN set",
		} {
			if !strings.Contains(text, want) {
				t.Errorf("report lacks %q:\n%s", want, text)
			}
		}
		if got["state"] != "TASK_STATE_COMPLETED" || strings.Contains(text, "the-token-value") || strings.Contains(text, "secret\n") {
			t.Fatalf("outcome %v", got)
		}
	})
	t.Run("fail", func(t *testing.T) {
		dir := mockTask(t, "mock: fail")
		if err := run([]string{"dev", "mock"}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		got := outcome(t, dir)
		if got["state"] != "TASK_STATE_FAILED" || got["message"].(map[string]any)["parts"].([]any)[0].(map[string]any)["text"] != "mock failed on request" {
			t.Fatalf("outcome %v", got)
		}
	})
	t.Run("exit", func(t *testing.T) {
		dir := mockTask(t, "mock: exit 3")
		var code exitCode
		if err := run([]string{"dev", "mock"}, &bytes.Buffer{}, &bytes.Buffer{}); !errors.As(err, &code) || code != 3 {
			t.Fatalf("err %v; want exit code 3", err)
		}
		if got := outcome(t, dir); got != nil {
			t.Fatalf("outcome %v; want none", got)
		}
	})
	t.Run("auth-required", func(t *testing.T) {
		dir := mockTask(t, "mock: auth-required github-token")
		if err := run([]string{"dev", "mock"}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		got := outcome(t, dir)
		parts := got["message"].(map[string]any)["parts"].([]any)
		if got["state"] != "TASK_STATE_AUTH_REQUIRED" || len(parts) != 2 || parts[1].(map[string]any)["data"].(map[string]any)["credential"] != "github-token" {
			t.Fatalf("outcome %v", got)
		}
	})
	t.Run("sleep", func(t *testing.T) {
		dir := mockTask(t, "mock: sleep")
		stdout := &lockedBuffer{}
		done := make(chan error)
		go func() { done <- run([]string{"dev", "mock"}, stdout, &bytes.Buffer{}) }()
		for !strings.Contains(stdout.String(), `{"step":"sleep"}`) {
			time.Sleep(10 * time.Millisecond)
		}
		select {
		case err := <-done:
			t.Fatalf("returned %v before SIGTERM", err)
		case <-time.After(100 * time.Millisecond):
		}
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if got := outcome(t, dir); got != nil {
			t.Fatalf("outcome %v; want none", got)
		}
	})
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
