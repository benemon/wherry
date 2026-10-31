package signed

import (
	"crypto/ed25519"
	"encoding/base64"
	"testing"
)

// The vector is RFC 8037's Ed25519 key (appendix A.1) and its RFC 7638 thumbprint (appendix A.3).
func TestKeyIDIsTheRFC7638Thumbprint(t *testing.T) {
	seed, err := base64.RawURLEncoding.DecodeString("nWGxne_9WmC6hEr0kuwsxERJxWl7MmkZcDusAxyuf2A")
	if err != nil {
		t.Fatal(err)
	}
	public := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	if x := base64.RawURLEncoding.EncodeToString(public); x != "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo" {
		t.Fatalf("public key is %s, want RFC 8037's", x)
	}
	if id := KeyID(public); id != "kPrK_qmxVWaYVA9wwBF6Iuo3vVzz7TxHCTwXBygrS4k" {
		t.Fatalf("KeyID is %s, want the RFC 7638 thumbprint", id)
	}
}
