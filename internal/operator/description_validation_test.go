package operator

import (
	"strings"
	"testing"
)

func TestValidateDescription(t *testing.T) {
	for _, s := range []string{"", strings.Repeat("界", 80), "hello\u202e"} {
		if err := ValidateDescription(s); err != nil {
			t.Errorf("valid description: %v", err)
		}
	}
	for _, s := range []string{strings.Repeat("界", 81), "\a", "\u2028", "\u2029", string([]byte{255})} {
		if err := ValidateDescription(s); err == nil || err.Error() != "invalid description" {
			t.Errorf("invalid description accepted: %q, %v", s, err)
		}
	}
}
