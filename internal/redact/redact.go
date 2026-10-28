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

// To writes data to out with each of forms, longest first, replaced by its marker, and returns how many it replaced. No
// write is larger than Piece bytes.
func To(out io.Writer, data []byte, forms []Form) int {
	replaced := 0
	for _, form := range forms {
		if n := bytes.Count(data, form.Value); n > 0 {
			data, replaced = bytes.ReplaceAll(data, form.Value, form.Marker), replaced+n
		}
	}
	for piece := range slices.Chunk(data, Piece) {
		out.Write(piece)
	}
	return replaced
}
