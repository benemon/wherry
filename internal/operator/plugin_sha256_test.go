package operator

import (
	"strings"
	"testing"
)

func TestValidPluginSHA256(t *testing.T) {
	valid := strings.Repeat("0123456789abcdef", 4)
	if !ValidPluginSHA256(valid) {
		t.Errorf("valid pin refused: %q", valid)
	}
	for _, sum := range []string{"", strings.ToUpper(valid), valid[:63], valid + "0", valid[:63] + "g", " " + valid[1:], valid + "\n"} {
		if ValidPluginSHA256(sum) {
			t.Errorf("invalid pin accepted: %q", sum)
		}
	}
}
