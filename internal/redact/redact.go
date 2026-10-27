// Package redact replaces the forms in which credential values are published with markers that name no value.
package redact

import (
	"bytes"
	"encoding/json/jsontext"
	"io"
	"slices"

	"github.com/benemon/wherry/internal/signed"
)

const (
	minForm = 8
	Piece   = 64 << 10
)

// Form is a form of a credential value and the marker that replaces it.
type Form struct {
	Value, Marker []byte
}

// AddForms returns forms with the published forms of each of values added, sorted longest first. A value's forms are
// the value without trailing CR and LF, as credential files written by echo or a CLI end in a newline that transcripts
// and outputs drop, and that form escaped as a JSON string, as a transcript line carries it. A value shorter than
// minForm once trimmed matches ordinary text and JSON literals, so short gets its name and it is left. Both forms take
// the marker of the value's name under key. A marker holding a form of any value would publish it, so that marker is
// [*], which no form of minForm bytes fits in.
func AddForms(forms []Form, key []byte, values map[string][]byte, short func(name string)) []Form {
	for name, value := range values {
		trimmed := bytes.TrimRight(value, "\r\n")
		if len(trimmed) < minForm {
			short(name)
			continue
		}
		escaped, _ := jsontext.AppendQuote(nil, trimmed) // invalid UTF-8 is replaced, and that form never appears in a transcript
		marker := []byte(signed.RedactionMarker(key, name))
		forms = append(forms, Form{Value: trimmed, Marker: marker})
		if escaped = escaped[1 : len(escaped)-1]; !bytes.Equal(escaped, trimmed) {
			forms = append(forms, Form{Value: escaped, Marker: marker})
		}
	}
	for i, form := range forms {
		if slices.ContainsFunc(forms, func(other Form) bool { return bytes.Contains(form.Marker, other.Value) }) {
			forms[i].Marker = []byte("[*]")
		}
	}
	slices.SortFunc(forms, func(a, b Form) int { return len(b.Value) - len(a.Value) })
	return forms
}

// To writes data to out with each of forms, longest first, replaced by its marker, and returns how many it replaced.
// The data passes through in pieces of Piece bytes, so no write, and nothing a form's replacer holds, is larger than a
// piece and the forms.
func To(out io.Writer, data []byte, forms []Form) int {
	w, replaced := out, 0
	for i := len(forms) - 1; i >= 0; i-- {
		w = &replacer{value: forms[i].Value, marker: forms[i].Marker, next: w, replaced: &replaced}
	}
	for piece := range slices.Chunk(data, Piece) {
		w.Write(piece)
	}
	if first, ok := w.(*replacer); ok {
		first.flush()
	}
	return replaced
}

// replacer writes on to next what it is written with each occurrence of value replaced by marker, holding back only
// the bytes a match could still begin in.
type replacer struct {
	value, marker []byte
	next          io.Writer
	held          []byte
	replaced      *int
}

func (w *replacer) Write(p []byte) (int, error) {
	w.held = append(w.held, p...)
	for {
		i := bytes.Index(w.held, w.value)
		if i < 0 {
			break
		}
		w.next.Write(w.held[:i])
		w.next.Write(w.marker)
		w.held = w.held[i+len(w.value):]
		*w.replaced++
	}
	cut := max(len(w.held)-len(w.value)+1, 0)
	w.next.Write(w.held[:cut])
	w.held = slices.Clone(w.held[cut:])
	return len(p), nil
}

func (w *replacer) flush() {
	w.next.Write(w.held)
	w.held = nil
	if next, ok := w.next.(*replacer); ok {
		next.flush()
	}
}
