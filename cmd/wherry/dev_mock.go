package main

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// runDevMock is the dev environment's agent, run by the mock profile under raw_exec. It reads prompt.txt in its
// working directory, which holds the receiver's preamble, a blank line and the task's prompt; the prompt's first line
// chooses what it does.
func runDevMock(args []string, stdout io.Writer) error {
	if len(args) != 0 {
		return errors.New("usage: wherry dev mock")
	}
	step := func(fields map[string]any) {
		encoded, _ := json.Marshal(fields, json.Deterministic(true))
		fmt.Fprintf(stdout, "%s\n", encoded)
	}
	data, err := os.ReadFile("prompt.txt")
	if err != nil {
		return err
	}
	prompt := string(data)
	if _, task, ok := strings.Cut(prompt, "\n\n"); ok {
		prompt = task
	}
	step(map[string]any{"step": "read-prompt"})
	first, _, _ := strings.Cut(prompt, "\n")
	output := os.Getenv("WHERRY_OUTPUT_DIR")
	switch {
	case first == "mock: sleep" || strings.HasPrefix(first, "mock: sleep "):
		// A duration makes the run complete after it; without one it sleeps until SIGTERM, with no outcome.
		var wake <-chan time.Time
		if value := strings.TrimPrefix(first, "mock: sleep "); value != first {
			duration, err := time.ParseDuration(value)
			if err != nil || duration < 0 {
				return fmt.Errorf("mock: sleep takes a duration such as 30s, not %q", value)
			}
			wake = time.After(duration)
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		step(map[string]any{"step": "sleep"})
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		}
	case first == "mock: fail":
		return mockOutcome(output, wire.TaskStateFailed, "mock failed on request", "")
	case strings.HasPrefix(first, "mock: exit "):
		code, err := strconv.Atoi(strings.TrimPrefix(first, "mock: exit "))
		if err != nil || code < 0 || code > 255 {
			return fmt.Errorf("mock: exit takes a code of 0 to 255, not %q", strings.TrimPrefix(first, "mock: exit "))
		}
		return quietExit(code)
	case strings.HasPrefix(first, "mock: auth-required "):
		name := strings.TrimPrefix(first, "mock: auth-required ")
		return mockOutcome(output, wire.TaskStateAuthRequired, "mock needs credential "+name, name)
	}
	if err := os.MkdirAll(output, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(output, "echo.txt"), []byte(prompt), 0o600); err != nil {
		return err
	}
	step(map[string]any{"step": "echo"})
	home, _ := os.UserHomeDir()
	cwd, _ := os.Getwd()
	report := []string{fmt.Sprintf("uid %d", os.Getuid()), "HOME " + home, "cwd " + cwd}
	for _, dir := range [][2]string{{"input", os.Getenv("WHERRY_INPUT_DIR")}, {"credential", os.Getenv("WHERRY_CREDENTIALS_DIR")}} {
		lines, err := describeFiles(dir[0], dir[1])
		if err != nil {
			return err
		}
		step(map[string]any{"step": dir[0] + "s", "count": len(lines)})
		report = append(report, lines...)
	}
	if _, ok := os.LookupEnv("MOCK_TOKEN"); ok {
		report = append(report, "MOCK_TOKEN set")
	}
	return mockOutcome(output, wire.TaskStateCompleted, strings.Join(report, "\n"), "")
}

// describeFiles is a line per regular file under dir: kind, its path in dir, mode, owner and SHA-256.
func describeFiles(kind, dir string) ([]string, error) {
	var lines []string
	if dir == "" {
		return nil, nil
	}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) && path == dir {
			return filepath.SkipDir
		}
		if err != nil || !entry.Type().IsRegular() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		sum, err := fileSHA256(path)
		if err != nil {
			return err
		}
		owner := -1
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			owner = int(stat.Uid)
		}
		name, _ := filepath.Rel(dir, path)
		lines = append(lines, fmt.Sprintf("%s %s %o %d %s", kind, name, info.Mode().Perm(), owner, sum))
		return nil
	})
	return lines, err
}

// mockOutcome writes the outcome file with state and text, and a credential data part naming credential when set.
func mockOutcome(output string, state wire.TaskState, text, credential string) error {
	parts := []map[string]any{{"text": text}}
	if credential != "" {
		parts = append(parts, map[string]any{"data": map[string]string{"credential": credential}})
	}
	encoded, err := json.Marshal(map[string]any{"state": state, "message": map[string]any{"role": "ROLE_AGENT", "parts": parts}}, json.Deterministic(true))
	if err != nil {
		return err
	}
	path := filepath.Join(output, wire.OutcomeFile)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), 0o600)
}
