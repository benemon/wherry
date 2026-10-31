package signed

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
)

const testSecret = "envelope-canary-credential"

var testRedactionKey = []byte("0123456789abcdef0123456789abcdef")

func p256Key(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// craft encrypts plaintext under protected claims and algorithms the test chooses, as a broken or hostile writer would.
func craft(t *testing.T, alg jose.KeyAlgorithm, enc jose.ContentEncryption, key any, claims map[jose.HeaderKey]any, compression jose.CompressionAlgorithm, plaintext []byte) *jose.JSONWebEncryption {
	t.Helper()
	encrypter, err := jose.NewEncrypter(enc, jose.Recipient{Algorithm: alg, Key: key}, &jose.EncrypterOptions{ExtraHeaders: claims, Compression: compression})
	if err != nil {
		t.Fatal(err)
	}
	jwe, err := encrypter.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	return jwe
}

func compact(t *testing.T, jwe *jose.JSONWebEncryption) []byte {
	t.Helper()
	encoded, err := jwe.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return []byte(encoded)
}

// The marker is the 96-bit HMAC-SHA256 of the domain and the credential's name under the run's key, as base64url.
func TestRedactionMarker(t *testing.T) {
	oracle := func(key []byte, name string) string {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte("wherry-redaction-v1" + name))
		return "[redacted:" + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:12]) + "]"
	}
	other := []byte("fedcba9876543210fedcba9876543210")
	marker := RedactionMarker(testRedactionKey, "model")
	if marker != oracle(testRedactionKey, "model") || len(marker) != 27 || strings.Contains(marker, "model") {
		t.Fatalf("marker %q, want the 27-byte HMAC marker %q", marker, oracle(testRedactionKey, "model"))
	}
	if RedactionMarker(testRedactionKey, "model") != marker || RedactionMarker(other, "model") == marker || RedactionMarker(testRedactionKey, "deploy-key") == marker {
		t.Fatal("the marker is not fixed by the run's key and the credential's name alone")
	}
}
