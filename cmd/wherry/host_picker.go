package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/term"
)

var hostIsTerminal = term.IsTerminal
var hostMakeRaw = term.MakeRaw
var hostRestore = term.Restore
var hostGetSize = term.GetSize

var pagesEligible = func(out io.Writer) bool {
	file, ok := out.(*os.File)
	return ok && hostIsTerminal(int(os.Stdin.Fd())) && hostIsTerminal(int(file.Fd())) && os.Getenv("TERM") != "dumb"
}

var errPickTimeout = errors.New("continuation timeout")
var errPickInterrupt = errors.New("interrupted")

// display escapes terminal controls without changing stored references.
func display(value string) string {
	var out strings.Builder
	for len(value) > 0 {
		r, n := utf8.DecodeRuneInString(value)
		if r == utf8.RuneError && n == 1 {
			fmt.Fprintf(&out, "\\x%02x", value[0])
		} else if unicode.IsControl(r) || r == '\u2028' || r == '\u2029' {
			if r <= 255 {
				fmt.Fprintf(&out, "\\x%02x", r)
			} else {
				fmt.Fprintf(&out, "\\u%04x", r)
			}
		} else {
			out.WriteRune(r)
		}
		value = value[n:]
	}
	return out.String()
}

func cutLine(value string, width int) string {
	// Keep non-ASCII wide and combining characters within the same conservative column bound.
	columns := 0
	for _, r := range value {
		if r > 127 {
			columns += 2
		} else {
			columns++
		}
	}
	if columns <= width {
		return value
	}
	if width < 3 {
		return strings.Repeat(".", max(0, width))
	}
	var out strings.Builder
	columns = 0
	for _, r := range value {
		n := 1
		if r > 127 {
			n = 2
		}
		if columns+n > width-3 {
			break
		}
		out.WriteRune(r)
		columns += n
	}
	return out.String() + "..."
}

type hostPicker struct {
	in       *bufio.Reader
	out      io.Writer
	timed    func() (byte, error)
	makeRaw  func() (*term.State, error)
	restore  func(*term.State) error
	size     func() int
	rows     int
	position int
}

func newHostPicker(out io.Writer) *hostPicker {
	input := newHostInput(os.Stdin)
	p := &hostPicker{in: bufio.NewReader(input), out: out}
	p.timed = func() (byte, error) {
		input.timed = true
		defer func() { input.timed = false }()
		return p.in.ReadByte()
	}
	p.makeRaw = func() (*term.State, error) { return hostMakeRaw(int(os.Stdin.Fd())) }
	p.restore = func(state *term.State) error { return hostRestore(int(os.Stdin.Fd()), state) }
	p.size = func() int {
		if file, ok := out.(*os.File); ok {
			if width, _, err := hostGetSize(int(file.Fd())); err == nil {
				return width - 1
			}
		}
		return 79
	}
	return p
}

func (p *hostPicker) write(value string) error {
	n, err := io.WriteString(p.out, value)
	p.position += strings.Count(value[:n], "\n")
	return err
}
func (p *hostPicker) erase() error {
	if p.rows == 0 {
		return nil
	}
	if err := p.write(fmt.Sprintf("\r\x1b[%dA\x1b[J", p.rows)); err != nil {
		return err
	}
	p.position -= p.rows
	p.rows = 0
	return nil
}
func (p *hostPicker) draw(lines []string, width int) error {
	if err := p.erase(); err != nil {
		return err
	}
	for _, line := range lines {
		if err := p.write(cutLine(line, width) + "\r\n"); err != nil {
			return err
		}
		p.rows++
	}
	return nil
}
func (p *hostPicker) collapse(label, answer string, width int) error {
	if err := p.draw([]string{"? " + display(label) + "  " + display(answer)}, width); err != nil {
		return err
	}
	p.rows = 0
	return nil
}
func (p *hostPicker) continuation() (byte, error) {
	if p.in.Buffered() > 0 {
		return p.in.ReadByte()
	}
	return p.timed()
}
func (p *hostPicker) key() (int, error) {
	b, err := p.in.ReadByte()
	if err != nil || b != 27 {
		return int(b), err
	}
	b, err = p.continuation()
	if err == errPickTimeout {
		return 27, nil
	}
	if err != nil {
		return 0, err
	}
	if b != '[' && b != 'O' {
		return 27, p.in.UnreadByte()
	}
	var sequence []byte
	for {
		b, err = p.continuation()
		if err != nil {
			return 0, fmt.Errorf("incomplete terminal sequence: %w", err)
		}
		sequence = append(sequence, b)
		if b >= 0x40 && b <= 0x7e {
			break
		}
		if b < 0x20 || b > 0x3f {
			return 0, errors.New("invalid terminal sequence")
		}
	}
	if len(sequence) == 1 && (b == 'A' || b == 'B') {
		if b == 'A' {
			return 256, nil
		}
		return 257, nil
	}
	return 0, nil
}

// pick restores stdin before returning to cooked prompts.
func (p *hostPicker) pick(label string, options, help []string, selected *int, back bool) (choice int, err error) {
	state, err := p.makeRaw()
	if err != nil {
		return -1, err
	}
	defer func() { err = errors.Join(err, p.restore(state)) }()
	width := p.size()
	shown := false
	start := 0
	for {
		if *selected < start {
			start = *selected
		}
		if *selected >= start+10 {
			start = *selected - 9
		}
		lines := []string{"? " + display(label)}
		if len(options) == 0 {
			lines = append(lines, "  (none)")
		}
		for i := start; i < min(len(options), start+10); i++ {
			marker := "  "
			if i == *selected {
				marker = "> "
			}
			lines = append(lines, marker+display(options[i]))
		}
		hint := "arrows move, enter selects, ? help, esc skips"
		if back {
			hint = "arrows move, enter selects, ? help, esc back"
		}
		lines = append(lines, hint)
		if shown {
			for _, line := range help {
				lines = append(lines, display(line))
			}
		}
		if err := p.draw(lines, width); err != nil {
			return -1, err
		}
		key, err := p.key()
		if err != nil {
			return -1, err
		}
		switch key {
		case 3:
			return -1, errPickInterrupt
		case 27:
			if back {
				return -1, p.erase()
			}
			return -1, p.collapse(label, "Skipped", width)
		case 256:
			if *selected > 0 {
				*selected--
			}
		case 257:
			if *selected+1 < len(options) {
				*selected++
			}
		case '?':
			shown = !shown
		case '\r', '\n':
			if len(options) > 0 {
				return *selected, p.collapse(label, options[*selected], width)
			}
		}
	}
}
func (p *hostPicker) line(label, valueDefault string) (string, error) {
	width := p.size()
	suffix := " (blank returns)"
	if valueDefault != "" {
		suffix = ""
	}
	if err := p.write(cutLine("? "+display(label)+suffix+": ", width)); err != nil {
		return "", err
	}
	value, err := p.in.ReadString('\n')
	if err != nil {
		return "", err
	}
	value = strings.TrimSpace(value)
	if value == "" {
		value = valueDefault
	}
	// The cooked terminal has echoed one answer row.
	if err := p.write("\r\x1b[1A\x1b[J"); err != nil {
		return "", err
	}
	if value != "" {
		if err := p.collapse(label, value, width); err != nil {
			return "", err
		}
	}
	return value, nil
}

func (p *hostPicker) rewind(position int) error {
	rows := p.position - position
	if rows > 0 {
		if err := p.write(fmt.Sprintf("\r\x1b[%dA\x1b[J", rows)); err != nil {
			return err
		}
	}
	p.position, p.rows = position, 0
	return nil
}

func (p *hostPicker) notice(value string) error {
	return p.write(cutLine(display(value), p.size()) + "\r\n")
}
