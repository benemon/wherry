package redact

import (
	"bytes"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/signed"
)

// Overlapping and adjacent values, and a value across what was a piece boundary, are replaced leftmost first, longest
// form first, as bytes.ReplaceAll would.
func TestToReplacesOverlappingAdjacentAndSpanningValues(t *testing.T) {
	long, short := Form{Value: []byte("abcdefghijk"), Marker: []byte("[long]")}, Form{Value: []byte("ijklmnop"), Marker: []byte("[short]")}
	repeated := Form{Value: []byte("zzzzzzzz"), Marker: []byte("[z]")}
	spanning := strings.Repeat(".", Piece-5) + "abcdefghijk" + strings.Repeat(".", Piece)
	for _, test := range []struct {
		data, want string
		replaced   int
	}{
		{"abcdefghijklmnop", "[long]lmnop", 1},
		{"xxijklmnopabcdefghijk", "xx[short][long]", 2},
		{"abcdefghijkabcdefghijkijklmnop", "[long][long][short]", 3},
		{"abcdefghijklmnopijklmnop", "[long]lmnop[short]", 2},
		{strings.Repeat("z", 20), "[z][z]zzzz", 2},
		{spanning, strings.Replace(spanning, "abcdefghijk", "[long]", 1), 1},
	} {
		var out bytes.Buffer
		if replaced := To(&out, []byte(test.data), []Form{long, short, repeated}); out.String() != test.want || replaced != test.replaced {
			t.Errorf("To(%.40q) = %.40q, %d replaced; want %.40q, %d", test.data, out.String(), replaced, test.want, test.replaced)
		}
	}
}

// AddForms leaves a value shorter than eight bytes and names it; AddExact replaces a value of any length, exact and
// JSON-escaped, skips an empty one, and keeps the [*] rule for a marker that holds a value.
func TestAddExactReplacesShortValuesThatAddFormsLeaves(t *testing.T) {
	var short []string
	forms := AddForms(nil, nil, map[string][]byte{"token": []byte("a7x9key\n")}, func(name string) { short = append(short, name) })
	if len(forms) != 0 || len(short) != 1 || short[0] != "token" {
		t.Fatalf("AddForms of a seven-byte value gave %d forms and named %v; want none and the value named", len(forms), short)
	}
	forms = AddExact(nil, nil, map[string][]byte{"access": []byte("a7x9key"), "secret": []byte(`q"uote`), "empty": nil})
	marker := func(name string) string { return signed.RedactionMarker(nil, name) }
	var out strings.Builder
	replaced := To(&out, []byte(`key a7x9key, raw q"uote, json "q\"uote", other text`), forms)
	if want := "key " + marker("access") + ", raw " + marker("secret") + `, json "` + marker("secret") + `", other text`; out.String() != want || replaced != 3 {
		t.Fatalf("To = %q (%d replaced); want %q", out.String(), replaced, want)
	}
	for _, form := range forms {
		if len(form.Value) == 0 {
			t.Fatal("an empty value became a form")
		}
	}
	forms = AddExact(nil, nil, map[string][]byte{"one": []byte("r")})
	if len(forms) != 1 || string(forms[0].Marker) != "[*]" {
		t.Fatalf("the marker of a value its own marker holds is %q; want [*]", forms[0].Marker)
	}
}
