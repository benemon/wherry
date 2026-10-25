// Package credential resolves the sources of credential values that the host's and the receiver's local
// configurations name.
package credential

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/signed"
)

// Source is where a credential's value comes from, exactly one of: File, a regular file its group cannot write and
// others cannot access; Env, an environment variable of the resolving process; Command, an argv run without a shell
// whose stdout is the value.
type Source struct {
	File    string   `yaml:"file"`
	Env     string   `yaml:"env"`
	Command []string `yaml:"command"`
}

// commandTimeout bounds a command source, long enough for an interactive unlock such as a password manager's; tests
// shorten it.
var commandTimeout = 60 * time.Second

// Validate checks that the source sets exactly one of File, Env and Command, and that a Command names a program.
func (source Source) Validate() error {
	if kinds := len(slices.DeleteFunc([]bool{source.File != "", source.Env != "", len(source.Command) > 0}, func(set bool) bool { return !set })); kinds != 1 {
		return errors.New("needs exactly one of file, env and command")
	}
	if len(source.Command) > 0 && source.Command[0] == "" {
		return errors.New("command names no program")
	}
	return nil
}

// Resolve reads the source's value. Its errors name no value or path, and quote nothing the command writes; its
// stderr is discarded.
func (source Source) Resolve(ctx context.Context) ([]byte, error) {
	switch {
	case source.Env != "":
		value := os.Getenv(source.Env)
		if value == "" {
			return nil, errors.New("its environment variable is unset or empty")
		}
		return []byte(value), nil
	case len(source.Command) > 0:
		ctx, cancel := context.WithTimeout(ctx, commandTimeout)
		defer cancel()
		command := exec.CommandContext(ctx, source.Command[0], source.Command[1:]...)
		stdout := &boundedBuffer{limit: signed.MaxSeal}
		// WaitDelay stops a child that keeps the output open from holding the answer past the timeout.
		command.Stdout, command.WaitDelay = stdout, time.Second
		err := command.Run()
		var exit *exec.ExitError
		switch {
		case stdout.over:
			return nil, fmt.Errorf("its command wrote over %d bytes", signed.MaxSeal)
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			return nil, fmt.Errorf("its command did not finish within %s", commandTimeout)
		case errors.As(err, &exit):
			return nil, fmt.Errorf("its command exited with status %d", exit.ExitCode())
		case err != nil:
			return nil, errors.New("its command did not start")
		case len(stdout.data) == 0:
			return nil, errors.New("its command wrote nothing")
		}
		return stdout.data, nil
	}
	file, err := os.Open(source.File)
	if err != nil {
		// The cause without the path.
		return nil, errors.Unwrap(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Unwrap(err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("its file is not a regular file")
	}
	// Group read is allowed so a secrets agent can template the file for a service group.
	if info.Mode().Perm()&0o027 != 0 {
		return nil, errors.New("its file is writable by its group or open to others; chmod 600 it")
	}
	value, err := io.ReadAll(file)
	if err != nil {
		return nil, errors.Unwrap(err)
	}
	return value, nil
}

// boundedBuffer fails the write that would take it past limit bytes.
type boundedBuffer struct {
	data  []byte
	limit int
	over  bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(b.data)+len(p) > b.limit {
		b.over = true
		return 0, errors.New("output over its bound")
	}
	b.data = append(b.data, p...)
	return len(p), nil
}
