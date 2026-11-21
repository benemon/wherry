package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	"github.com/creack/pty"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/term"
)

// typedPassword is the password the prompt tests type: spaces at either end are part of it.
const typedPassword = "  unique-secret-marker  "

func operatorAnswers(t *testing.T) map[string]string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(p, []byte("unique-secret-marker\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return map[string]string{"broker": "host", "ca-file": "", "super-user": "root", "super-user-password-file": p}
}

func stopOperatorConnection(t *testing.T, check func(string, []byte, string, string)) error {
	t.Helper()
	old := superUserKafka
	t.Cleanup(func() { superUserKafka = old })
	stop := errors.New("connection seam")
	superUserKafka = func(b string, ca []byte, u, p, _ string) (*kgo.Client, error) { check(b, ca, u, p); return nil, stop }
	return stop
}

func TestOperatorBrokerGrammar(t *testing.T) {
	for _, s := range []string{"", ":9092", "host:", "[::1]:", "host:abc", "host:65536", "host:0", "host:+9092", "[::1]x9092", "[::1]x0", "[::1]x+9092"} {
		t.Run(s, func(t *testing.T) {
			if checkOperatorBroker(s) == nil {
				t.Fatal("invalid broker accepted", s)
			}
			opts := operatorAnswers(t)
			opts["broker"] = s
			stopOperatorConnection(t, func(string, []byte, string, string) { t.Error("invalid broker reached connection") })
			if err := operatorInit(context.Background(), filepath.Join(t.TempDir(), "new"), opts, io.Discard); err == nil {
				t.Fatal("invalid flag accepted")
			}
			if s == "" {
				return
			}
			delete(opts, "broker")
			scriptForms(t, []string{s, "\r", "\x15", "host\r"})
			stop := stopOperatorConnection(t, func(b string, _ []byte, _, _ string) {
				if b != "host" {
					t.Error("correction lost", b)
				}
			})
			var out bytes.Buffer
			if err := operatorInit(context.Background(), "unused", opts, &out); !errors.Is(err, stop) {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "not a valid Kafka broker address") {
				t.Fatal("missing broker validation", out.String())
			}
		})
	}
}

func TestOperatorFormsAndFlags(t *testing.T) {
	for _, given := range []string{"none", "broker", "ca-file", "super-user", "super-user-password-file", "all"} {
		t.Run(given, func(t *testing.T) {
			opts := operatorAnswers(t)
			full := maps.Clone(opts)
			t.Chdir(t.TempDir())
			var steps [][]string
			var want []string
			for _, name := range []string{"broker", "ca-file", "super-user", "super-user-password-file"} {
				if given == "all" || given == name {
					continue
				}
				delete(opts, name)
				value := full[name]
				if name == "super-user-password-file" {
					// The prompt takes the password itself, as typed, and prints no answer line.
					value = typedPassword
				} else if name != "ca-file" {
					value = "  " + value + "  "
				}
				steps = append(steps, keys(value+"\r"))
				if name == "super-user-password-file" {
					continue
				}
				label := map[string]string{"broker": "Kafka broker address", "ca-file": "CA certificate", "super-user": "Kafka super user"}[name]
				answer := full[name]
				if name == "ca-file" {
					answer = "System roots"
				}
				want = append(want, "? "+label+"  "+answer)
			}
			scriptForms(t, steps...)
			password := "unique-secret-marker\n"
			if _, fromFile := opts["super-user-password-file"]; !fromFile {
				password = typedPassword
			}
			stop := stopOperatorConnection(t, func(b string, ca []byte, u, p string) {
				if b != "host" || ca != nil || u != "root" || p != password {
					t.Fatal("connection answers", b, ca, u)
				}
			})
			var out bytes.Buffer
			dir := filepath.Join(t.TempDir(), "new")
			if err := operatorInit(context.Background(), dir, opts, &out); !errors.Is(err, stop) {
				t.Fatal(err)
			}
			lines := collapsedLines(out.String())
			if !slices.Equal(lines, want) {
				t.Fatal("collapse lines", lines, "want", want)
			}
			if strings.Contains(out.String(), "unique-secret-marker") {
				t.Fatal("password leaked")
			}
			if strings.Count(out.String(), "\nBroker\n") != min(1, len(steps)) {
				t.Fatal("heading count")
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatal("wrote before connection")
			}
		})
	}
}

func TestOperatorMissingFlags(t *testing.T) {
	required := []string{"broker", "super-user", "super-user-password-file"}
	for mask := 1; mask < 8; mask++ {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			opts := operatorAnswers(t)
			stopOperatorConnection(t, func(string, []byte, string, string) { t.Error("missing required flag reached connection") })
			var missing []string
			for i, name := range required {
				if mask&(1<<i) != 0 {
					opts[name] = ""
					missing = append(missing, "--"+name)
				}
			}
			err := operatorInit(context.Background(), "unused", opts, io.Discard)
			want := "operator init has no terminal to prompt on; give " + strings.Join(missing, ", ")
			if err == nil || err.Error() != want {
				t.Fatal(err, want)
			}
			scriptForms(t)
			err = operatorInit(context.Background(), "unused", opts, io.Discard)
			if err == nil || err.Error() != "operator init needs "+missing[0] {
				t.Fatal(err)
			}
		})
	}
}

func TestOperatorInterruptPreservesFiles(t *testing.T) {
	for _, key := range []string{"\x03", ""} {
		t.Run(fmt.Sprintf("%q", key), func(t *testing.T) {
			opts := operatorAnswers(t)
			delete(opts, "super-user")
			dir := t.TempDir()
			for _, name := range []string{"operator.password", "operator.yaml", ".gitignore"} {
				os.WriteFile(filepath.Join(dir, name), []byte("original"), 0600)
			}
			if key != "" {
				scriptForms(t, keys(key))
			} else {
				old, eligible := stdin, promptsEligible
				t.Cleanup(func() { stdin, promptsEligible = old, eligible })
				stdin = strings.NewReader("")
				promptsEligible = func(io.Writer) bool { return true }
			}
			stopOperatorConnection(t, func(string, []byte, string, string) { t.Error("interrupt delegated") })
			if err := operatorInit(context.Background(), dir, opts, io.Discard); !errors.Is(err, errInterrupted) {
				t.Fatal("interrupt accepted", err)
			}
			for _, name := range []string{"operator.password", "operator.yaml", ".gitignore"} {
				b, _ := os.ReadFile(filepath.Join(dir, name))
				if string(b) != "original" {
					t.Fatal("changed", name)
				}
			}
			entries, _ := os.ReadDir(dir)
			if len(entries) != 3 {
				t.Fatal("temporary file written")
			}
		})
	}
}

func TestOperatorSafePassword(t *testing.T) {
	for _, kind := range []string{"normal", "large", "symlink", "directory", "fifo", "missing", "unreadable", "replacement", "descriptor"} {
		t.Run(kind, func(t *testing.T) {
			opts := operatorAnswers(t)
			path := opts["super-user-password-file"]
			data := []byte("unique-secret-marker\n\n")
			if kind == "large" {
				data = bytes.Repeat([]byte("x"), (1<<20)+1)
			}
			os.WriteFile(path, data, 0600)
			switch kind {
			case "symlink":
				os.Rename(path, path+".real")
				os.Symlink(path+".real", path)
			case "directory":
				os.Remove(path)
				os.Mkdir(path, 0700)
			case "fifo":
				os.Remove(path)
				syscall.Mkfifo(path, 0600)
			case "missing":
				os.Remove(path)
			}
			old := openFile
			t.Cleanup(func() { openFile = old })
			openFile = func(p string, flags int, mode os.FileMode) (*os.File, error) {
				if flags&syscall.O_NONBLOCK == 0 {
					t.Error("password open can block")
				}
				if kind == "unreadable" {
					return nil, syscall.EACCES
				}
				if kind == "replacement" {
					os.WriteFile(path+".replacement", []byte("replaced\n"), 0600)
					os.Rename(path+".replacement", path)
					data = []byte("replaced\n")
				}
				if kind == "descriptor" {
					return old(filepath.Dir(path), flags, mode)
				}
				return old(p, flags, mode)
			}
			bad := kind == "directory" || kind == "fifo" || kind == "missing" || kind == "unreadable" || kind == "descriptor"
			stop := stopOperatorConnection(t, func(_ string, _ []byte, _ string, p string) {
				if bad {
					t.Error("special password reached connection")
				}
				if p != string(data) {
					t.Error("password bytes changed")
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var out bytes.Buffer
			err := operatorInit(ctx, "unused", opts, &out)
			if kind == "descriptor" && (err == nil || !strings.Contains(err.Error(), "not a regular file")) {
				t.Fatal("password descriptor was not checked by Fstat", err)
			}
			if bad {
				if err == nil || errors.Is(err, stop) {
					t.Fatal("unsafe password accepted", err)
				}
			} else if !errors.Is(err, stop) {
				t.Fatal(err)
			}
			if strings.Contains(out.String(), "unique-secret-marker") || strings.Contains(fmt.Sprint(err), "unique-secret-marker") {
				t.Fatal("secret leaked")
			}
		})
	}
}

// `-` reads the super user's password from standard input, exactly as piped; a terminal there is refused before any
// question, naming standard input and no path; and the operator directory gains no file either way.
func TestOperatorPasswordFromStandardInput(t *testing.T) {
	saved := stdin
	t.Cleanup(func() { stdin = saved })
	const piped = "  piped-secret-marker  \n"
	noPath := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("error expected")
		}
		if !strings.Contains(err.Error(), "standard input") {
			t.Fatal("error does not name standard input:", err)
		}
		if strings.Contains(strings.ReplaceAll(err.Error(), "--super-user-password-file", ""), "-") {
			t.Fatal("error treats - as a value:", err)
		}
	}
	noFile := func(t *testing.T, dir string) {
		t.Helper()
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatal("a password from standard input wrote the operator directory")
		}
	}
	t.Run("piped", func(t *testing.T) {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		go func() { io.WriteString(w, piped); w.Close() }()
		stdin = r
		opts := operatorAnswers(t)
		opts["super-user-password-file"] = "-"
		reached := false
		stop := stopOperatorConnection(t, func(_ string, _ []byte, _, p string) {
			reached = true
			if p != piped {
				t.Error("piped password changed")
			}
		})
		dir := filepath.Join(t.TempDir(), "new")
		var out bytes.Buffer
		if err := operatorInit(context.Background(), dir, opts, &out); !errors.Is(err, stop) {
			t.Fatal(err)
		}
		if !reached {
			t.Fatal("piped password did not reach the connection")
		}
		if strings.Contains(out.String(), "piped-secret-marker") {
			t.Fatal("piped password echoed")
		}
		noFile(t, dir)
	})
	t.Run("empty", func(t *testing.T) {
		stdin = strings.NewReader("\r\n")
		opts := operatorAnswers(t)
		opts["super-user-password-file"] = "-"
		stopOperatorConnection(t, func(string, []byte, string, string) { t.Error("empty standard input reached connection") })
		dir := filepath.Join(t.TempDir(), "new")
		noPath(t, operatorInit(context.Background(), dir, opts, io.Discard))
		noFile(t, dir)
	})
	for _, prompted := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminal prompted=%v", prompted), func(t *testing.T) {
			master, tty, err := pty.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer tty.Close()
			defer master.Close()
			// The terminal echoes nothing of its own: it is raw, as a masked prompt leaves it, so what the master reads
			// is what wherry wrote to the terminal, and the typed password must be none of it.
			if _, err := term.MakeRaw(int(tty.Fd())); err != nil {
				t.Fatal(err)
			}
			shown := make(chan string, 1)
			go func() {
				b, _ := io.ReadAll(master)
				shown <- string(b)
			}()
			// A password typed at the terminal is there to be read; the master stays open, so the terminal stays one, and
			// is closed later so that a read of it, which the refusal forbids, ends instead of hanging.
			io.WriteString(master, "typed-secret-marker\n")
			defer time.AfterFunc(3*time.Second, func() { master.Close() }).Stop()
			stdin = tty
			opts := operatorAnswers(t)
			opts["super-user-password-file"] = "-"
			if prompted {
				// Prompts are available and the broker would be asked for: the refusal comes before any question.
				delete(opts, "broker")
				scriptForms(t)
			}
			stopOperatorConnection(t, func(string, []byte, string, string) { t.Error("terminal standard input reached connection") })
			dir := filepath.Join(t.TempDir(), "new")
			var out bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = operatorInit(ctx, dir, opts, &out)
			noPath(t, err)
			if !strings.Contains(err.Error(), "standard input is a terminal") {
				t.Fatal("terminal accepted:", err)
			}
			tty.Close()
			terminal := <-shown
			if strings.Contains(err.Error(), "typed-secret-marker") || strings.Contains(out.String(), "typed-secret-marker") || strings.Contains(terminal, "typed-secret-marker") {
				t.Fatal("typed password read or echoed")
			}
			if out.Len() != 0 || terminal != "" {
				t.Fatalf("refusal came after a prompt: %q on the prompts, %q on the terminal", out.String(), terminal)
			}
			noFile(t, dir)
		})
	}
}

// The masked prompt never shows the password: not while it is typed, not on a validation failure, not on Ctrl-C, and
// in no answer line; the spaces at either end reach the connection.
func TestOperatorPasswordPromptNeverEchoes(t *testing.T) {
	leaked := func(t *testing.T, out string, err error) {
		t.Helper()
		if strings.Contains(out, "unique-secret-marker") || strings.Contains(fmt.Sprint(err), "unique-secret-marker") {
			t.Fatal("password echoed", out)
		}
		if lines := collapsedLines(out); len(lines) != 0 {
			t.Fatal("password prompt printed an answer line", lines)
		}
	}
	t.Run("success", func(t *testing.T) {
		opts := operatorAnswers(t)
		delete(opts, "super-user-password-file")
		scriptForms(t, keys(typedPassword+"\r"))
		reached := false
		stop := stopOperatorConnection(t, func(_ string, _ []byte, _, p string) {
			reached = true
			if p != typedPassword {
				t.Error("prompted password changed: its spaces must be kept")
			}
		})
		dir := filepath.Join(t.TempDir(), "new")
		var out bytes.Buffer
		err := operatorInit(context.Background(), dir, opts, &out)
		if !errors.Is(err, stop) || !reached {
			t.Fatal(err)
		}
		leaked(t, out.String(), err)
		if !strings.Contains(ansi.Strip(out.String()), "Kafka super user's password") {
			t.Fatal("password prompt missing", out.String())
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatal("prompted password wrote the operator directory")
		}
	})
	t.Run("validation failure", func(t *testing.T) {
		opts := operatorAnswers(t)
		delete(opts, "super-user-password-file")
		scriptForms(t, []string{"\r", typedPassword + "\r"})
		stop := stopOperatorConnection(t, func(_ string, _ []byte, _, p string) {
			if p != typedPassword {
				t.Error("corrected password changed")
			}
		})
		var out bytes.Buffer
		err := operatorInit(context.Background(), "unused", opts, &out)
		if !errors.Is(err, stop) {
			t.Fatal(err)
		}
		leaked(t, out.String(), err)
		if !strings.Contains(ansi.Strip(out.String()), "Kafka super user's password is required") {
			t.Fatal("empty password accepted", out.String())
		}
	})
	t.Run("cancel", func(t *testing.T) {
		opts := operatorAnswers(t)
		delete(opts, "super-user-password-file")
		scriptForms(t, []string{typedPassword, "\x03"})
		stopOperatorConnection(t, func(string, []byte, string, string) { t.Error("cancelled prompt reached connection") })
		var out bytes.Buffer
		err := operatorInit(context.Background(), "unused", opts, &out)
		if !errors.Is(err, errInterrupted) {
			t.Fatal(err)
		}
		leaked(t, out.String(), err)
	})
	t.Run("rejected value", func(t *testing.T) {
		// The input itself, with a validator that rejects the typed password: the failure shows the validator's message
		// and nothing of the value.
		scriptForms(t, []string{typedPassword, "\r", "\x03"})
		var out bytes.Buffer
		q := &questions{context.Background(), nil, &promptLatch{out: &out}}
		_, err := q.secret("Secret", func(string) error { return errors.New("not this one") })
		if !errors.Is(err, errInterrupted) {
			t.Fatal(err)
		}
		leaked(t, out.String(), err)
		if !strings.Contains(ansi.Strip(out.String()), "not this one") {
			t.Fatal("validation message missing", out.String())
		}
	})
}

// On a real terminal the masked prompt shows nothing of what is typed: the terminal's output, which a person at it would
// see, carries the prompt, its validation message and its masking, and the kernel echoes nothing, since the prompt
// takes the terminal raw. The secret reaches the caller as typed, spaces included.
func TestSecretPromptOnATerminalShowsNothingTyped(t *testing.T) {
	master, tty, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer tty.Close()
	if err := pty.Setsize(master, &pty.Winsize{Rows: 24, Cols: 80}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TERM", "xterm-256color")
	shown := &syncBuffer{}
	read := make(chan struct{})
	go func() {
		defer close(read)
		io.Copy(shown, master)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	q := &questions{ctx, tty, &promptLatch{out: tty}}
	type answer struct {
		text string
		err  error
	}
	answers := make(chan answer, 1)
	go func() {
		text, err := q.secret("Kafka super user's password", func(s string) error {
			if s == "" {
				return errors.New("a password is required")
			}
			return nil
		})
		answers <- answer{text, err}
	}()
	// Keys are typed once the prompt is on the terminal: an empty answer first, refused with the validator's message,
	// then the password.
	onTerminal := func(want string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !strings.Contains(ansi.Strip(shown.String()), want) {
			if time.Now().After(deadline) {
				t.Fatalf("terminal shows %q; want %q", shown.String(), want)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	onTerminal("Kafka super user's password")
	io.WriteString(master, "\r")
	onTerminal("a password is required")
	io.WriteString(master, typedPassword)
	time.Sleep(100 * time.Millisecond)
	io.WriteString(master, "\r")
	var got answer
	select {
	case got = <-answers:
	case <-ctx.Done():
		t.Fatalf("prompt never returned; terminal shows %q", shown.String())
	}
	if got.err != nil || got.text != typedPassword {
		t.Fatalf("secret = %q, %v; want the password as typed", got.text, got.err)
	}
	tty.Close()
	<-read
	terminal := ansi.Strip(shown.String())
	if strings.Contains(terminal, "unique-secret-marker") {
		t.Fatalf("the terminal showed the password:\n%s", terminal)
	}
	if !strings.Contains(terminal, "a password is required") {
		t.Fatalf("the terminal did not show the validation message:\n%s", terminal)
	}
}

func TestOperatorCAConnection(t *testing.T) {
	_, pem := tlsCluster(t)
	for _, prompt := range []bool{false, true} {
		for _, mode := range []string{"valid", "oversized replacement", "growth"} {
			t.Run(fmt.Sprint(prompt, mode), func(t *testing.T) {
				opts := operatorAnswers(t)
				dir := t.TempDir()
				t.Chdir(dir)
				path := filepath.Join(dir, "ca.pem")
				os.WriteFile(path, []byte(pem), 0600)
				opts["ca-file"] = path
				if prompt {
					delete(opts, "ca-file")
					scriptForms(t, keys("\r"))
				}
				armed := !prompt
				replace := func() {
					if err := os.WriteFile(path+".replacement", append([]byte(pem), bytes.Repeat([]byte("x"), 2<<20)...), 0600); err != nil {
						t.Error(err)
					}
					if err := os.Rename(path+".replacement", path); err != nil {
						t.Error(err)
					}
				}
				var out io.Writer = io.Discard
				if mode == "oversized replacement" {
					if prompt {
						out = answerOutput{io.Discard, "CA certificate", replace}
					} else {
						replace()
					}
				}
				if mode == "growth" {
					if prompt {
						out = answerOutput{io.Discard, "CA certificate", func() { armed = true }}
					}
					oldRead := readCAData
					t.Cleanup(func() { readCAData = oldRead })
					readCAData = func(r io.Reader) ([]byte, error) {
						if armed {
							armed = false
							os.WriteFile(path, append([]byte(pem), bytes.Repeat([]byte("x"), 2<<20)...), 0600)
						}
						b, e := oldRead(r)
						if len(b) > (1<<20)+1 {
							t.Error("CA read exceeded bound")
						}
						return b, e
					}
				}
				stop := stopOperatorConnection(t, func(_ string, ca []byte, _, _ string) {
					if mode != "valid" {
						t.Error("oversized CA reached connection")
					}
					if !bytes.Equal(ca, []byte(pem)) {
						t.Error("CA connection bytes lost")
					}
				})
				err := operatorInit(context.Background(), "unused", opts, out)
				if mode == "valid" {
					if !errors.Is(err, stop) {
						t.Fatal(err)
					}
				} else if err == nil || errors.Is(err, stop) {
					t.Fatal("unsafe CA accepted", err)
				}
			})
		}
	}
}

func TestOperatorPresentation(t *testing.T) {
	opts := operatorAnswers(t)
	opts["super-user-password-file"] = "missing\x1b\a"
	err := operatorInit(context.Background(), "unused", opts, io.Discard)
	if err == nil || strings.ContainsAny(err.Error(), "\x1b\a") || errors.Unwrap(err) == nil {
		t.Fatal("unescaped error", err)
	}
}

func TestOperatorPromptCAValidation(t *testing.T) {
	t.Chdir(t.TempDir())
	os.WriteFile("bad", []byte("not PEM"), 0600)
	scriptForms(t, keys(down+"\r"), []string{"bad", "\r", "\x03"})
	opts := operatorAnswers(t)
	delete(opts, "ca-file")
	var out bytes.Buffer
	if err := operatorInit(context.Background(), "unused", opts, &out); !errors.Is(err, errInterrupted) {
		t.Fatal("invalid CA accepted", err)
	}
	text := strings.Join(strings.Fields(strings.ReplaceAll(ansi.Strip(out.String()), "┃", "")), " ")
	if !strings.Contains(text, "holds no PEM certificate") {
		t.Fatal("missing CA validation", text)
	}
}

func TestOperatorWriterAndCancellation(t *testing.T) {
	opts := operatorAnswers(t)
	delete(opts, "broker")
	old := promptsEligible
	t.Cleanup(func() { promptsEligible = old })
	promptsEligible = func(io.Writer) bool { return true }
	stopOperatorConnection(t, func(string, []byte, string, string) { t.Error("delegated") })
	sentinel := io.ErrClosedPipe
	if err := operatorInit(context.Background(), "unused", opts, failingPrompt{}); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, w, _ := os.Pipe()
	defer r.Close()
	defer w.Close()
	saved := stdin
	t.Cleanup(func() { stdin = saved })
	stdin = r
	if err := operatorInit(ctx, "unused", opts, io.Discard); !errors.Is(err, errInterrupted) {
		t.Fatal(err)
	}
}

// A blank answer to a required question is corrected in place; the password prompt, which keeps spaces, refuses only
// the empty answer and completes nothing on Tab.
func TestOperatorRequiredCorrection(t *testing.T) {
	opts := operatorAnswers(t)
	delete(opts, "broker")
	delete(opts, "super-user")
	delete(opts, "super-user-password-file")
	scriptForms(t, []string{"   ", "\r", "\x15", "[::1]:9093\r"}, []string{" ", "\r", "\x15", "root\r"}, []string{"\r", "unique-sec", "\t", "ret-marker\r"})
	stop := stopOperatorConnection(t, func(b string, _ []byte, u, p string) {
		if b != "[::1]:9093" || u != "root" || p != "unique-secret-marker" {
			t.Fatal("corrected answers lost")
		}
	})
	if err := operatorInit(context.Background(), "unused", opts, io.Discard); !errors.Is(err, stop) {
		t.Fatal(err)
	}
}

func TestOperatorOtherCACompletionAndBlank(t *testing.T) {
	_, pem := tlsCluster(t)
	dir := t.TempDir()
	t.Chdir(dir)
	path := filepath.Join(dir, ".hidden.pem")
	os.WriteFile(path, []byte(pem), 0600)
	opts := operatorAnswers(t)
	delete(opts, "ca-file")
	scriptForms(t, keys(down+"\r"), keys("\r"), keys("\r"), []string{filepath.Join(dir, ".hid"), "\t", "\r"})
	stop := stopOperatorConnection(t, func(_ string, ca []byte, _, _ string) {
		if !bytes.Equal(ca, []byte(pem)) {
			t.Fatal("Other CA bytes lost")
		}
	})
	var out bytes.Buffer
	if err := operatorInit(context.Background(), "unused", opts, &out); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if len(collapsedLines(out.String())) != 1 {
		t.Fatal("CA collapsed more than once")
	}
}

func TestOperatorCAFileKinds(t *testing.T) {
	_, pem := tlsCluster(t)
	for _, prompt := range []bool{false, true} {
		for _, kind := range []string{"valid", "symlink", "malformed", "missing", "directory", "fifo", "oversized"} {
			t.Run(fmt.Sprint(prompt, kind), func(t *testing.T) {
				dir := t.TempDir()
				t.Chdir(dir)
				path := filepath.Join(dir, ".ca")
				os.WriteFile(path, []byte(pem), 0600)
				switch kind {
				case "symlink":
					os.Rename(path, path+"real")
					os.Symlink(path+"real", path)
				case "malformed":
					os.WriteFile(path, []byte("not PEM"), 0600)
				case "missing":
					os.Remove(path)
				case "directory":
					os.Remove(path)
					os.Mkdir(path, 0700)
				case "fifo":
					os.Remove(path)
					syscall.Mkfifo(path, 0600)
				case "oversized":
					os.WriteFile(path, append([]byte(pem), bytes.Repeat([]byte("x"), 2<<20)...), 0600)
				}
				bad := kind != "valid" && kind != "symlink"
				opts := operatorAnswers(t)
				opts["ca-file"] = path
				if prompt {
					delete(opts, "ca-file")
					last := keys(path + "\r")
					if bad {
						last = []string{path, "\r", "\x03"}
					}
					scriptForms(t, keys(down+"\r"), last)
				}
				stop := stopOperatorConnection(t, func(_ string, ca []byte, _, _ string) {
					if bad {
						t.Error("invalid CA connected")
					}
					if !bytes.Equal(ca, []byte(pem)) {
						t.Error("CA data changed")
					}
				})
				var err error
				if !prompt && kind == "fifo" {
					done := make(chan error, 1)
					go func() { done <- operatorInit(context.Background(), "unused", opts, io.Discard) }()
					select {
					case err = <-done:
					case <-time.After(2 * time.Second):
						t.Fatal("flag-path FIFO blocked")
					}
				} else {
					err = operatorInit(context.Background(), "unused", opts, io.Discard)
				}
				if bad {
					if err == nil || errors.Is(err, stop) {
						t.Fatal("invalid CA accepted")
					}
				} else if !errors.Is(err, stop) {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestOperatorIneligibleWithFlags(t *testing.T) {
	opts := operatorAnswers(t)
	delete(opts, "ca-file")
	stop := stopOperatorConnection(t, func(_ string, ca []byte, _, _ string) {
		if ca != nil {
			t.Fatal("system roots loaded a CA")
		}
	})
	var out bytes.Buffer
	if err := operatorInit(context.Background(), "unused", opts, &out); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatal("all flags produced prompts", out.String())
	}
}

type answerOutput struct {
	io.Writer
	label    string
	answered func()
}

func (w answerOutput) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if strings.HasPrefix(string(p), "? "+w.label+"  ") {
		w.answered()
	}
	return n, err
}

func TestOperatorValidBrokerPaths(t *testing.T) {
	for _, broker := range []string{"host", "host:9092", "::1", "[::1]", "[::1]:9093", "host:65535"} {
		for _, prompt := range []bool{false, true} {
			t.Run(fmt.Sprint(broker, prompt), func(t *testing.T) {
				opts := operatorAnswers(t)
				opts["broker"] = broker
				if prompt {
					delete(opts, "broker")
					scriptForms(t, keys(" "+broker+" \r"))
				}
				stop := stopOperatorConnection(t, func(b string, _ []byte, _, _ string) {
					if b != broker {
						t.Fatal("broker changed", b)
					}
				})
				if err := operatorInit(context.Background(), "unused", opts, io.Discard); !errors.Is(err, stop) {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestOperatorEmptyFlagWaitsForOtherAnswers(t *testing.T) {
	opts := operatorAnswers(t)
	opts["broker"] = ""
	delete(opts, "super-user")
	scriptForms(t, keys("root\r"))
	if err := operatorInit(context.Background(), "unused", opts, io.Discard); err == nil || err.Error() != "operator init needs --broker" {
		t.Fatal(err)
	}
}

func TestOperatorInterruptBeforeWrites(t *testing.T) {
	t.Chdir(t.TempDir())
	opts := operatorAnswers(t)
	delete(opts, "broker")
	delete(opts, "ca-file")
	delete(opts, "super-user")
	scriptForms(t, keys("host\r"), keys("\r"), keys("\x03"))
	dir := filepath.Join(t.TempDir(), "operator")
	stopOperatorConnection(t, func(string, []byte, string, string) { t.Error("interrupt delegated") })
	if err := operatorInit(context.Background(), dir, opts, io.Discard); !errors.Is(err, errInterrupted) {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("interrupt created directory")
	}
}

func TestOperatorHostileCACollapse(t *testing.T) {
	_, pem := tlsCluster(t)
	t.Chdir(t.TempDir())
	path := "evil\x1b]52;c;x\a.pem"
	os.WriteFile(path, []byte(pem), 0600)
	opts := operatorAnswers(t)
	delete(opts, "ca-file")
	scriptForms(t, keys("\r"))
	stop := stopOperatorConnection(t, func(_ string, ca []byte, _, _ string) {
		if !bytes.Equal(ca, []byte(pem)) {
			t.Error("CA bytes changed")
		}
	})
	var out bytes.Buffer
	if err := operatorInit(context.Background(), "unused", opts, &out); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	lines := strings.Join(collapsedLines(out.String()), "\n")
	if strings.ContainsAny(lines, "\x1b\a") || !strings.Contains(lines, display(path)) {
		t.Fatal("unsafe collapse", lines)
	}
}

func TestOperatorPromptEligibility(t *testing.T) {
	master, tty, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer tty.Close()
	old := stdin
	t.Cleanup(func() { stdin = old })
	stdin = tty
	for _, mode := range []string{"dumb", "redirected prompts"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("TERM", "xterm-256color")
			var out io.Writer = tty
			if mode == "dumb" {
				t.Setenv("TERM", "dumb")
			} else {
				file, err := os.CreateTemp(t.TempDir(), "prompts")
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				out = file
			}
			if promptsEligible(out) {
				t.Fatal("ineligible prompts accepted")
			}
			var stdout bytes.Buffer
			err := runOperator([]string{"init", "--dir", filepath.Join(t.TempDir(), "new")}, &stdout, out)
			if err == nil || err.Error() != "operator init has no terminal to prompt on; give --broker, --super-user, --super-user-password-file" {
				t.Fatal(err)
			}
			if stdout.Len() != 0 {
				t.Fatal("ineligible init wrote stdout")
			}
		})
	}
}
