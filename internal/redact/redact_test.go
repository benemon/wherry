package redact

import (
	"bytes"
	"strings"
	"testing"
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
