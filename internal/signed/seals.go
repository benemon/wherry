package signed

import (
	"crypto"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

const SealType = "application/vnd.wherry.seal.v2+jwe"

// maxSeal bounds a seal's compact serialisation, and with it the credentials it carries.
const maxSeal = 64 << 10

// SealBinding is the one attempt and key a seal is for. A seal carries it in its protected header and again in its
// plaintext.
type SealBinding struct {
	Tenant      string   `json:"tenant"`
	Pool        string   `json:"pool"`
	Receiver    string   `json:"receiver"`
	KeyID       string   `json:"key_id"`
	Run         string   `json:"run"`
	Turn        int64    `json:"turn"`
	Attempt     int64    `json:"attempt"`
	Credentials []string `json:"credentials"`
	ExpiryMS    int64    `json:"expiry_ms"`
}

// sealHeader is every member a seal's protected header may hold.
type sealHeader struct {
	Algorithm  string         `json:"alg"`
	Encryption string         `json:"enc"`
	Type       string         `json:"typ"`
	Ephemeral  jsontext.Value `json:"epk"`
	SealBinding
}

type sealPlaintext struct {
	Version int               `json:"v"`
	Binding SealBinding       `json:"binding"`
	Values  map[string][]byte `json:"values"`
}

// SealKeyID is the RFC 7638 SHA-256 thumbprint of a seal key, base64url.
func SealKeyID(key *ecdsa.PublicKey) string {
	thumbprint, _ := (&jose.JSONWebKey{Key: key}).Thumbprint(crypto.SHA256) // fails only for a key of another type
	return base64.RawURLEncoding.EncodeToString(thumbprint)
}

// Seal encrypts values, one per credential binding names, to key as a compact JWE.
func Seal(binding SealBinding, key *ecdsa.PublicKey, values map[string][]byte) ([]byte, error) {
	plaintext, err := json.Marshal(sealPlaintext{Version: 1, Binding: binding, Values: values}, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	options := new(jose.EncrypterOptions).WithType(SealType).
		WithHeader("tenant", binding.Tenant).WithHeader("pool", binding.Pool).WithHeader("receiver", binding.Receiver).
		WithHeader("key_id", binding.KeyID).WithHeader("run", binding.Run).WithHeader("turn", binding.Turn).
		WithHeader("attempt", binding.Attempt).WithHeader("credentials", binding.Credentials).WithHeader("expiry_ms", binding.ExpiryMS)
	encrypter, err := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.ECDH_ES_A256KW, Key: key}, options)
	if err != nil {
		return nil, err
	}
	jwe, err := encrypter.Encrypt(plaintext)
	if err != nil {
		return nil, errors.New("seal credentials")
	}
	compact, err := jwe.CompactSerialize()
	if err != nil {
		return nil, err
	}
	if len(compact) > maxSeal {
		return nil, fmt.Errorf("a seal of %v is %d bytes, over the 64 KiB limit", binding.Credentials, len(compact))
	}
	return []byte(compact), nil
}

// OpenSeal opens a seal for expected with key, the private half of expected.KeyID, and returns its values by
// credential name.
func OpenSeal(encoded []byte, expected SealBinding, key *ecdsa.PrivateKey, now time.Time) (map[string][]byte, error) {
	if expected.KeyID != SealKeyID(&key.PublicKey) {
		return nil, errors.New("seal key is not the expected key")
	}
	if len(encoded) > maxSeal {
		return nil, errors.New("seal exceeds its size limit")
	}
	jwe, err := jose.ParseEncryptedCompact(string(encoded), []jose.KeyAlgorithm{jose.ECDH_ES_A256KW}, []jose.ContentEncryption{jose.A256GCM})
	if err != nil {
		return nil, errors.New("seal is not a compact JWE of the allowed algorithms")
	}
	protected, _, _ := strings.Cut(string(encoded), ".")
	decoded, err := base64.RawURLEncoding.DecodeString(protected)
	var header sealHeader
	if err != nil || json.Unmarshal(decoded, &header, strict) != nil || header.Type != SealType {
		return nil, errors.New("seal protected header holds other members than a seal's")
	}
	if !reflect.DeepEqual(header.SealBinding, expected) {
		return nil, errors.New("seal is bound to another attempt or key")
	}
	if !now.Before(time.UnixMilli(header.ExpiryMS)) {
		return nil, errors.New("seal has expired")
	}
	plaintext, err := jwe.Decrypt(key)
	if err != nil {
		return nil, errors.New("seal does not decrypt")
	}
	var opened sealPlaintext
	if json.Unmarshal(plaintext, &opened, strict) != nil || opened.Version != 1 || !reflect.DeepEqual(opened.Binding, header.SealBinding) {
		return nil, errors.New("seal plaintext is bound otherwise than its header")
	}
	for _, name := range header.Credentials {
		if _, ok := opened.Values[name]; !ok || len(opened.Values) != len(header.Credentials) {
			return nil, errors.New("seal values are not the credentials it names")
		}
	}
	return opened.Values, nil
}
