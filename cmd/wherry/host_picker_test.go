package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

func TestPickerSequences(t *testing.T) {
	for _, input := range []string{"\x1b[B\n", "\x1bOB\r", "\x1b[9~\x1b[B\n", "\x1bOQ\x1b[B\n"} {
		var out bytes.Buffer
		p := testPicker(input, &out)
		selected := 0
		choice, err := p.pick("label", []string{"one", "two"}, nil, &selected, false)
		if err != nil || choice != 1 {
			t.Fatalf("%q: %d, %v", input, choice, err)
		}
		if !strings.Contains(out.String(), "? label  two\r\n") || !strings.Contains(out.String(), "\x1b[4A") {
			t.Fatal(out.String())
		}
	}
	for _, input := range []string{"\x1b[", "\x1bO", "\x1b[12", ""} {
		p := testPicker(input, io.Discard)
		selected := 0
		if _, err := p.pick("label", []string{"one"}, nil, &selected, false); err == nil {
			t.Fatalf("%q accepted", input)
		}
	}
	p := testPicker("\x1bx\n", io.Discard)
	if b, err := p.key(); err != nil || b != 27 {
		t.Fatal(b, err)
	}
	if b, err := p.in.ReadByte(); err != nil || b != 'x' {
		t.Fatal(b, err)
	}
}
func TestPickerContinuationAndTypeahead(t *testing.T) {
	p := testPicker("\x1b", io.Discard)
	continuation := []byte{'[', 'B'}
	calls := 0
	p.timed = func() (byte, error) { b := continuation[calls]; calls++; return b, nil }
	if b, err := p.key(); err != nil || b != 257 || calls != 2 {
		t.Fatal(b, err, calls)
	}
	p = testPicker("\nhello\n", io.Discard)
	selected := 0
	p.timed = func() (byte, error) { t.Fatal("buffered input used timed read"); return 0, nil }
	if _, err := p.pick("label", []string{"one"}, nil, &selected, false); err != nil {
		t.Fatal(err)
	}
	if value, err := p.line("text", ""); err != nil || value != "hello" {
		t.Fatal(value, err)
	}
}
func TestPickerHelpAndScroll(t *testing.T) {
	var options []string
	for i := range 25 {
		options = append(options, fmt.Sprint(i))
	}
	var out bytes.Buffer
	p := testPicker(strings.Repeat("\x1b[B", 24)+"??\n", &out)
	selected := 0
	choice, err := p.pick("label", options, []string{"HELP"}, &selected, false)
	if err != nil || choice != 24 || strings.Count(out.String(), "HELP") != 1 {
		t.Fatal(choice, err)
	}
	last := out.String()[strings.LastIndex(out.String(), "? label\r\n"):]
	if strings.Contains(last, "> 14\r\n") || !strings.Contains(last, "  15\r\n") {
		t.Fatal(last)
	}
	if strings.Contains(last, "HELP") {
		t.Fatal("help not hidden")
	}
}
func TestPickerDisplayWidth(t *testing.T) {
	var out bytes.Buffer
	p := testPicker("\n", &out)
	p.size = func() int { return 20 }
	selected := 0
	_, err := p.pick("abc\x1b\n\u0085\u2029"+strings.Repeat("z", 60), []string{"yes"}, nil, &selected, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(out.String(), "\r\n") {
		if strings.HasPrefix(line, "? ") && (len(line) > 20 || !strings.HasSuffix(line, "...")) {
			t.Fatal(line)
		}
	}
	if display("a\xff\x1b\n\u0085\u2029") != "a\\xff\\x1b\\x0a\\x85\\u2029" {
		t.Fatal("unsafe display")
	}
}

type pickerBadWriter struct{}

func (pickerBadWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func TestPickerRawLifecycle(t *testing.T) {
	for _, mode := range []string{"success", "read", "interrupt", "write", "panic", "make", "restore"} {
		t.Run(mode, func(t *testing.T) {
			p := testPicker("\n", io.Discard)
			restores := 0
			p.makeRaw = func() (*term.State, error) {
				if mode == "make" {
					return nil, errors.New("make")
				}
				return nil, nil
			}
			p.restore = func(*term.State) error {
				restores++
				if mode == "restore" {
					return errors.New("restore")
				}
				return nil
			}
			if mode == "read" {
				p.in = bufio.NewReader(strings.NewReader(""))
			}
			if mode == "interrupt" {
				p.in = bufio.NewReader(strings.NewReader("\x03"))
			}
			if mode == "write" {
				p.out = pickerBadWriter{}
			}
			if mode == "panic" {
				p.size = func() int { panic("size") }
			}
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil && mode != "panic" {
						panic(r)
					}
				}()
				selected := 0
				_, err = p.pick("label", []string{"one"}, nil, &selected, false)
			}()
			want := 1
			if mode == "make" {
				want = 0
			}
			if restores != want {
				t.Fatalf("restored %d, want %d", restores, want)
			}
			if mode != "success" && mode != "panic" && err == nil {
				t.Fatal("missing error")
			}
		})
	}
}
func TestHostInputTimedContract(t *testing.T) {
	for _, mode := range []string{"timeout", "hangup", "read", "get", "setup", "restore", "success"} {
		t.Run(mode, func(t *testing.T) {
			state := unix.Termios{}
			state.Cc[unix.VMIN] = 1
			sets := 0
			input := &hostInput{timed: true,
				get: func() (*unix.Termios, error) {
					if mode == "get" {
						return nil, errors.New("get")
					}
					return &state, nil
				},
				set: func(next *unix.Termios) error {
					sets++
					if sets == 1 && (next.Cc[unix.VMIN] != 0 || next.Cc[unix.VTIME] != 1) {
						t.Fatal("bad timing")
					}
					if sets == 2 && *next != state {
						t.Fatal("not restored")
					}
					if mode == "setup" && sets == 1 || mode == "restore" && sets == 2 {
						return errors.New(mode)
					}
					return nil
				},
				read: func(b []byte) (int, error) {
					if mode == "read" {
						return -1, errors.New("read")
					}
					if mode == "success" {
						b[0] = 'x'
						return 1, nil
					}
					return 0, nil
				},
				hungup: func() (bool, error) { return mode == "hangup", nil },
			}
			n, err := input.Read(make([]byte, 10))
			if n < 0 {
				t.Fatal("negative reader count", n)
			}
			if mode == "success" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("missing error")
			}
			if mode == "timeout" && !errors.Is(err, errPickTimeout) {
				t.Fatal(err)
			}
			if mode == "hangup" && !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if mode != "get" && sets != 2 {
				t.Fatal("restore missing", sets)
			}
		})
	}
	// A timeout consumed by bufio does not poison the next pick or cooked read.
	reads := 0
	input := &hostInput{timed: true, get: func() (*unix.Termios, error) { return &unix.Termios{}, nil }, set: func(*unix.Termios) error { return nil }, hungup: func() (bool, error) { return false, nil }, read: func(b []byte) (int, error) {
		reads++
		if reads == 1 {
			return 0, nil
		}
		return copy(b, "\nhello\n"), nil
	}}
	p := testPicker("", io.Discard)
	p.in = bufio.NewReader(input)
	if _, err := p.in.ReadByte(); !errors.Is(err, errPickTimeout) {
		t.Fatal(err)
	}
	selected := 0
	if _, err := p.pick("label", []string{"one"}, nil, &selected, false); err != nil {
		t.Fatal(err)
	}
	if value, err := p.line("text", ""); err != nil || value != "hello" {
		t.Fatal(value, err)
	}
}

func TestPagesEligibility(t *testing.T) {
	original := hostIsTerminal
	defer func() { hostIsTerminal = original }()
	for _, tc := range []struct {
		name          string
		input, output bool
		dumb          bool
	}{
		{"terminal prompts redirected stdout", true, true, false},
		{"nonterminal input", false, true, false},
		{"redirected prompts", true, false, false},
		{"dumb", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TERM", "xterm")
			if tc.dumb {
				t.Setenv("TERM", "dumb")
			}
			hostIsTerminal = func(fd int) bool {
				if fd == int(os.Stdin.Fd()) {
					return tc.input
				}
				if fd == int(os.Stderr.Fd()) {
					return tc.output
				}
				t.Fatalf("unexpected descriptor %d", fd)
				return false
			}
			if got := pagesEligible(os.Stderr); got != (tc.input && tc.output && !tc.dumb) {
				t.Fatal(got)
			}
			if pagesEligible(&bytes.Buffer{}) {
				t.Fatal("buffer eligible")
			}
		})
	}
	hostIsTerminal = original
	file, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if pagesEligible(file) {
		t.Fatal("/dev/null eligible")
	}
}

func TestPickerTimeoutRestoreFailure(t *testing.T) {
	sets := 0
	input := &hostInput{timed: true, get: func() (*unix.Termios, error) { return &unix.Termios{}, nil }, set: func(*unix.Termios) error {
		sets++
		if sets%2 == 0 {
			return errors.New("restore")
		}
		return nil
	}, read: func([]byte) (int, error) { return 0, nil }, hungup: func() (bool, error) { return false, nil }}
	p := testPicker("\x1b", io.Discard)
	p.timed = func() (byte, error) { return bufio.NewReader(input).ReadByte() }
	if _, err := p.key(); err == nil || !strings.Contains(err.Error(), "restore") {
		t.Fatal("restore failure consumed as timeout", err)
	}
}

func TestPickerTerminalDescriptors(t *testing.T) {
	raw, restore, size := hostMakeRaw, hostRestore, hostGetSize
	defer func() { hostMakeRaw, hostRestore, hostGetSize = raw, restore, size }()
	hostMakeRaw = func(fd int) (*term.State, error) {
		if fd != int(os.Stdin.Fd()) {
			t.Fatal("raw uses prompt descriptor")
		}
		return nil, nil
	}
	hostRestore = func(fd int, _ *term.State) error {
		if fd != int(os.Stdin.Fd()) {
			t.Fatal("restore uses prompt descriptor")
		}
		return nil
	}
	hostGetSize = func(fd int) (int, int, error) {
		if fd != int(os.Stderr.Fd()) {
			t.Fatal("size uses input descriptor")
		}
		return 72, 24, nil
	}
	p := newHostPicker(os.Stderr)
	state, err := p.makeRaw()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.restore(state); err != nil {
		t.Fatal(err)
	}
	if p.size() != 71 {
		t.Fatal("incorrect width")
	}
	hostGetSize = func(int) (int, int, error) { return 0, 0, errors.New("size") }
	if p.size() != 79 {
		t.Fatal("incorrect fallback")
	}
}

func TestPickerEOFInSequence(t *testing.T) {
	p := testPicker("\x1b[", io.Discard)
	p.timed = func() (byte, error) { return 0, io.EOF }
	if _, err := p.key(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}
