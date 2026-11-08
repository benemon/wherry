package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/huh"
	"golang.org/x/term"
)

func display(s string) string {
	var out strings.Builder
	for len(s) > 0 {
		r, n := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && n == 1 {
			fmt.Fprintf(&out, "\\x%02x", s[0])
		} else if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '\u2028' || r == '\u2029' {
			if r <= 255 {
				fmt.Fprintf(&out, "\\x%02x", r)
			} else {
				fmt.Fprintf(&out, "\\u%04x", r)
			}
		} else {
			out.WriteRune(r)
		}
		s = s[n:]
	}
	return out.String()
}

var promptsEligible = func(out io.Writer) bool {
	in, ok := stdin.(*os.File)
	if !ok || !term.IsTerminal(int(in.Fd())) {
		return false
	}
	f, ok := out.(*os.File)
	return ok && term.IsTerminal(int(f.Fd())) && term.IsTerminal(int(os.Stderr.Fd())) && os.Getenv("TERM") != "dumb"
}

func sameTerminal(a, b io.Writer) bool {
	aFile, aOK := a.(*os.File)
	bFile, bOK := b.(*os.File)
	if !aOK || !bOK || !term.IsTerminal(int(aFile.Fd())) || !term.IsTerminal(int(bFile.Fd())) {
		return false
	}
	aInfo, aErr := aFile.Stat()
	bInfo, bErr := bFile.Stat()
	return aErr == nil && bErr == nil && os.SameFile(aInfo, bInfo)
}

var errInterrupted = errors.New("interrupted")

type presentationError struct{ cause error }

func (e presentationError) Error() string { return display(e.cause.Error()) }
func (e presentationError) Unwrap() error { return e.cause }

type promptLatch struct {
	out io.Writer
	mu  sync.Mutex
	err error
}

func (w *promptLatch) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.out.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if w.err == nil {
		w.err = err
	}
	return n, err
}
func (w *promptLatch) failure() error { w.mu.Lock(); defer w.mu.Unlock(); return w.err }

type eofInput struct {
	io.Reader
	cancel context.CancelFunc
}

func (r eofInput) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != nil {
		r.cancel()
	}
	return n, err
}

type questions struct {
	ctx context.Context
	in  io.Reader
	out *promptLatch
}

// runQuestion gives one form ownership of input; typeahead consumed before the next form starts is discarded.
var runQuestion = (*questions).runForm

func (q *questions) run(field huh.Field) error { return runQuestion(q, field) }

func (q *questions) runForm(field huh.Field) error {
	ctx, cancel := context.WithCancel(q.ctx)
	defer cancel()
	km := huh.NewDefaultKeyMap()
	km.Input.AcceptSuggestion = key.NewBinding(key.WithKeys("tab"), key.WithHelp("tab", "accept suggestion"))
	km.Select.Filter = key.NewBinding()
	submitted := false
	filter := func(_ tea.Model, msg tea.Msg) tea.Msg {
		if k, ok := msg.(tea.KeyMsg); ok && k.Type == tea.KeyCtrlC {
			return tea.Quit()
		}
		if _, ok := msg.(tea.InterruptMsg); ok {
			return tea.Quit()
		}
		if k, ok := msg.(tea.KeyMsg); ok {
			if k.Type == tea.KeyCtrlV {
				return nil
			}
			if submitted {
				if field.Error() == nil {
					return nil
				}
				submitted = false
			}
			if k.Type == tea.KeyEnter {
				submitted = true
			}
		}
		return msg
	}
	form := huh.NewForm(huh.NewGroup(quittingField{Field: field, done: ctx.Done()})).WithKeyMap(km).WithAccessible(false).
		WithProgramOptions(tea.WithReportFocus(), tea.WithFilter(filter)).WithInput(eofInput{q.in, cancel}).WithOutput(q.out)
	if f, ok := q.in.(*os.File); ok {
		form.WithInput(eofFile{File: f, cancel: cancel})
	}
	if f, ok := q.out.out.(*os.File); ok {
		form.WithOutput(promptFile{File: f, latch: q.out})
	}
	err := form.RunWithContext(context.WithoutCancel(ctx))
	if writeErr := q.out.failure(); writeErr != nil {
		return writeErr
	}
	if ctx.Err() != nil {
		return errInterrupted
	}
	if errors.Is(err, huh.ErrUserAborted) || errors.Is(err, huh.ErrTimeout) || (err == nil && form.State != huh.StateCompleted) {
		return errInterrupted
	}
	return err
}
func (q *questions) line(format string, args ...any) error {
	_, err := fmt.Fprintf(q.out, format, args...)
	return err
}
func (q *questions) pick(label, description string, options []huh.Option[string], prev *string) (string, error) {
	field := huh.NewSelect[string]().Title(display(label)).Description(description).Options(options...).Value(prev)
	if err := q.run(field); err != nil {
		return "", err
	}
	return *prev, nil
}
func option(label, value string) huh.Option[string] { return huh.NewOption(display(label), value) }
func (q *questions) answer(label, answer string) error {
	return q.line("? %s  %s\n", display(label), display(answer))
}
func (q *questions) input(label, initial, kind string, validate func(string) error) (string, error) {
	text := initial
	value := &lockedText{text: &text}
	field := huh.NewInput().Title(display(label)).Value(&text).Accessor(value).Validate(func(s string) error {
		if err := validate(s); err != nil {
			return errors.New(display(err.Error()))
		}
		return nil
	})
	field.SuggestionsFunc(func() []string { return completions(value.Get(), kind) }, &text)
	if err := q.run(field); err != nil {
		return "", err
	}
	return text, nil
}
func safeCandidates(list []string) []string {
	list = slices.DeleteFunc(list, func(s string) bool { return display(s) != s })
	slices.Sort(list)
	list = slices.Compact(list)
	if len(list) > 200 {
		list = list[:200]
	}
	return list
}

var readDirectory = os.ReadDir

func completions(text, kind string) []string {
	if kind == "" {
		return nil
	}
	var result []string
	if kind == "variable" {
		for _, entry := range os.Environ() {
			name, value, _ := strings.Cut(entry, "=")
			if value != "" && strings.HasPrefix(name, text) {
				result = append(result, name)
			}
		}
		return safeCandidates(result)
	}
	prefix, fragment := "", text
	if kind == "command" {
		if i := strings.LastIndexAny(text, " \t"); i >= 0 {
			prefix, fragment = text[:i+1], text[i+1:]
		} else {
			for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
				if !filepath.IsAbs(dir) {
					continue
				}
				info, err := os.Stat(dir)
				if err != nil || !info.IsDir() {
					continue
				}
				entries, _ := readDirectory(dir)
				for _, e := range entries {
					if !strings.HasPrefix(e.Name(), text) {
						continue
					}
					info, err := os.Stat(filepath.Join(dir, e.Name()))
					if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
						result = append(result, e.Name())
					}
				}
			}
			return safeCandidates(result)
		}
	}
	typedDir, base := filepath.Split(fragment)
	lookup := typedDir
	if lookup == "" {
		lookup = "."
	}
	expanded, err := localPath(lookup)
	if err != nil {
		return nil
	}
	info, err := os.Stat(expanded)
	if err != nil || !info.IsDir() {
		return nil
	}
	entries, err := readDirectory(expanded)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, base) || strings.HasPrefix(name, ".") && !strings.HasPrefix(base, ".") {
			continue
		}
		candidate := prefix + typedDir + name
		info, err := os.Stat(filepath.Join(expanded, name))
		if err != nil {
			continue
		}
		if info.IsDir() {
			candidate += "/"
		}
		result = append(result, candidate)
	}
	return safeCandidates(result)
}

type lockedText struct {
	mu   sync.Mutex
	text *string
}

func (v *lockedText) Get() string  { v.mu.Lock(); defer v.mu.Unlock(); return *v.text }
func (v *lockedText) Set(s string) { v.mu.Lock(); defer v.mu.Unlock(); *v.text = s }

type eofFile struct {
	*os.File
	cancel context.CancelFunc
}

func (f eofFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	if err != nil {
		f.cancel()
	}
	return n, err
}

type promptFile struct {
	*os.File
	latch *promptLatch
}

func (f promptFile) Write(p []byte) (int, error) { return f.latch.Write(p) }

// quittingField keeps cancellation on the graceful shutdown path so the input loop joins before its reader closes.
type quittingField struct {
	huh.Field
	done <-chan struct{}
}

func (f quittingField) Focus() tea.Cmd {
	return tea.Batch(f.Field.Focus(), func() tea.Msg { <-f.done; return tea.Quit() })
}
