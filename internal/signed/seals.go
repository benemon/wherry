package signed

import (
	"crypto/ecdh"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"time"
)

const MaxPlaintext = 16 << 10

// sealOverhead is the X25519 encapsulated key plus the AES-GCM tag.
const sealOverhead = 32 + 16

type Seal struct {
	Version        int    `json:"v"`
	Tenant         string `json:"tenant"`
	Receiver       string `json:"receiver"`
	Pool           string `json:"pool"`
	KeyFingerprint string `json:"key_fingerprint"`
	Credential     string `json:"credential"`
	ExpiryMS       int64  `json:"expiry_ms"`
	Ciphertext     []byte `json:"ciphertext"`
}

func SealingKeyID(key *ecdh.PublicKey) string {
	sum := sha256.Sum256(key.Bytes())
	return "sha256:" + base64.RawURLEncoding.EncodeToString(sum[:])
}

func SealCredential(tenant, receiver, pool, credential string, expiry time.Time, key *ecdh.PublicKey, plaintext []byte) (Seal, error) {
	if tenant == "" || receiver == "" || pool == "" || credential == "" || expiry.IsZero() || len(plaintext) > MaxPlaintext {
		return Seal{}, errors.New("invalid seal")
	}
	hpkeKey, err := hpke.NewDHKEMPublicKey(key)
	if err != nil {
		return Seal{}, err
	}
	seal := Seal{Version: 1, Tenant: tenant, Receiver: receiver, Pool: pool, KeyFingerprint: SealingKeyID(key), Credential: credential, ExpiryMS: expiry.UnixMilli()}
	seal.Ciphertext, err = hpke.Seal(hpkeKey, hpke.HKDFSHA256(), hpke.AES256GCM(), sealInfo(seal), plaintext)
	if err != nil {
		return Seal{}, errors.New("seal credential")
	}
	return seal, nil
}

// OpenCredential opens a seal read from the host topic of tenant.
func OpenCredential(seal Seal, tenant, receiver, pool, credential string, now time.Time, allowed []string, key *ecdh.PrivateKey) ([]byte, error) {
	if seal.Version != 1 || seal.Tenant != tenant || seal.Receiver != receiver || seal.Pool != pool || seal.Credential != credential || !now.Before(time.UnixMilli(seal.ExpiryMS)) || !contains(allowed, credential) {
		return nil, errors.New("seal is not valid for receiver terms")
	}
	hpkeKey, err := hpke.NewDHKEMPrivateKey(key)
	if err != nil {
		return nil, err
	}
	plaintext, err := hpke.Open(hpkeKey, hpke.HKDFSHA256(), hpke.AES256GCM(), sealInfo(seal), seal.Ciphertext)
	if err != nil {
		return nil, errors.New("open credential")
	}
	if len(plaintext) > MaxPlaintext {
		return nil, errors.New("credential exceeds 16 KiB")
	}
	return plaintext, nil
}

func CheckPinnedKey(pins map[string]string, receiver string, key *ecdh.PublicKey) error {
	fingerprint := SealingKeyID(key)
	if pinned, ok := pins[receiver]; ok && pinned != fingerprint {
		return errors.New("receiver sealing key changed")
	}
	if _, ok := pins[receiver]; !ok {
		pins[receiver] = fingerprint
	}
	return nil
}

func sealInfo(seal Seal) []byte {
	info, _ := json.Marshal(struct {
		Version    int    `json:"v"`
		Tenant     string `json:"tenant"`
		Receiver   string `json:"receiver"`
		Pool       string `json:"pool"`
		Key        string `json:"key"`
		Credential string `json:"credential"`
		ExpiryMS   int64  `json:"expiry_ms"`
	}{seal.Version, seal.Tenant, seal.Receiver, seal.Pool, seal.KeyFingerprint, seal.Credential, seal.ExpiryMS}, json.Deterministic(true))
	return append([]byte("wherry hpke seal\x00"), info...)
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func EncodeSeal(seal Seal) ([]byte, error) { return json.Marshal(seal, json.Deterministic(true)) }

func DecodeSeal(encoded []byte) (Seal, error) {
	var seal Seal
	if err := json.Unmarshal(encoded, &seal, strict); err != nil {
		return Seal{}, fmt.Errorf("decode seal: %w", err)
	}
	if seal.Version != 1 || seal.Tenant == "" || seal.Receiver == "" || seal.Pool == "" || seal.KeyFingerprint == "" || seal.Credential == "" || seal.ExpiryMS <= 0 || len(seal.Ciphertext) == 0 || len(seal.Ciphertext) > MaxPlaintext+sealOverhead {
		return Seal{}, errors.New("invalid seal")
	}
	return seal, nil
}
