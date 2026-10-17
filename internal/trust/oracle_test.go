package trust

import (
	"bytes"
	"crypto/ecdh"
	"crypto/hpke"
	"encoding/hex"
	json "encoding/json/v2"
	"os"
	"testing"
)

type hpkeVector struct {
	Mode        int64            `json:"mode"`
	KEMID       int64            `json:"kem_id"`
	KDFID       int64            `json:"kdf_id"`
	AEADID      int64            `json:"aead_id"`
	Info        string           `json:"info"`
	IKMR        string           `json:"ikmR"`
	Enc         string           `json:"enc"`
	Encryptions []hpkeEncryption `json:"encryptions"`
}

type hpkeEncryption struct {
	AAD string `json:"aad"`
	CT  string `json:"ct"`
	PT  string `json:"pt"`
}

func TestRFC9180BaseX25519SHA256AES256GCM(t *testing.T) {
	encoded, err := os.ReadFile("testdata/rfc9180-test-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []hpkeVector
	if err := json.Unmarshal(encoded, &vectors); err != nil {
		t.Fatal(err)
	}
	var selected *hpkeVector
	for i := range vectors {
		vector := &vectors[i]
		if vector.Mode == 0 && vector.KEMID == 0x20 && vector.KDFID == 1 && vector.AEADID == 2 {
			if selected != nil {
				t.Fatal("multiple matching RFC 9180 vectors")
			}
			selected = vector
		}
	}
	if selected == nil {
		t.Fatal("RFC 9180 vector for the settled suite is missing")
	}
	kem := hpke.DHKEM(ecdh.X25519())
	privateKey, err := kem.DeriveKeyPair(decodeHex(t, selected.IKMR))
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := hpke.NewRecipient(decodeHex(t, selected.Enc), privateKey, hpke.HKDFSHA256(), hpke.AES256GCM(), decodeHex(t, selected.Info))
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.Encryptions) == 0 {
		t.Fatal("RFC 9180 vector has no encryptions")
	}
	for i, encryption := range selected.Encryptions {
		plaintext, err := recipient.Open(decodeHex(t, encryption.AAD), decodeHex(t, encryption.CT))
		if err != nil {
			t.Fatalf("encryption %d: %v", i, err)
		}
		if want := decodeHex(t, encryption.PT); !bytes.Equal(plaintext, want) {
			t.Fatalf("encryption %d plaintext mismatch", i)
		}
	}
}

func decodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
