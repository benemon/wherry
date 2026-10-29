package signed

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

const SealType = "application/vnd.wherry.seal.v2+jwe"

const redactionDomain = "wherry-redaction-v1"

// MaxSeal bounds a seal's compact serialisation, and with it the credentials it carries.
const MaxSeal = 64 << 10

// SealBinding is the one attempt and key a seal is for. A seal carries it in its protected header and again in its
// plaintext.
type SealBinding struct {
	Tenant      string   `json:"tenant"`
	Pool        string   `json:"pool"`
	Receiver    string   `json:"receiver"`
	KeyID       string   `json:"key_id"`
	Run         string   `json:"run"`
	Attempt     int64    `json:"attempt"`
	Credentials []string `json:"credentials"`
	// TaskCredentials, S3Inputs and S3Outputs are absent from the protected header when empty.
	TaskCredentials []string `json:"task_credentials,omitzero"`
	S3Inputs        []string `json:"s3_inputs,omitzero"`
	S3Outputs       []string `json:"s3_outputs,omitzero"`
	ExpiryMS        int64    `json:"expiry_at_ms"`
}

// Names are the keys of a seal's values: each credential and task credential, then S3InputValue and S3OutputValue of
// each S3 input and output, whose presigned URL the value is.
func (binding SealBinding) Names() []string {
	names := append(slices.Clone(binding.Credentials), binding.TaskCredentials...)
	for _, input := range binding.S3Inputs {
		names = append(names, S3InputValue(input))
	}
	for _, output := range binding.S3Outputs {
		names = append(names, S3OutputValue(output))
	}
	return names
}

// S3InputValue and S3OutputValue name the seal values holding the presigned URLs of an S3 input and output; the slash
// keeps them apart from credential names.
func S3InputValue(name string) string  { return "s3-inputs/" + name }
func S3OutputValue(name string) string { return "s3-outputs/" + name }

// sealHeader is every member a seal's protected header may hold.
type sealHeader struct {
	Algorithm  string         `json:"alg"`
	Encryption string         `json:"enc"`
	Type       string         `json:"typ"`
	Ephemeral  jsontext.Value `json:"epk"`
	SealBinding
}

type sealPlaintext struct {
	Version int         `json:"v"`
	Binding SealBinding `json:"binding"`
	SealValues
}

// SealValues are the confidential values a seal carries. RedactionKey is the run's 32-byte key.
type SealValues struct {
	Values       map[string][]byte `json:"values"`
	RedactionKey []byte            `json:"redaction_key"`
}

// SealKeyID is the RFC 7638 SHA-256 thumbprint of a seal key, base64url.
func SealKeyID(key *ecdsa.PublicKey) string {
	thumbprint, _ := (&jose.JSONWebKey{Key: key}).Thumbprint(crypto.SHA256) // fails only for a key of another type
	return base64.RawURLEncoding.EncodeToString(thumbprint)
}

// RedactionMarker returns the feed marker for a credential name under a run's key.
func RedactionMarker(key []byte, name string) string {
	digest := hmac.New(sha256.New, key)
	digest.Write([]byte(redactionDomain))
	digest.Write([]byte(name))
	return "[redacted:" + base64.RawURLEncoding.EncodeToString(digest.Sum(nil)[:12]) + "]"
}

// Seal encrypts values, one per name of binding.Names, and the run's redaction key to key as a compact JWE.
func Seal(binding SealBinding, key *ecdsa.PublicKey, values SealValues) ([]byte, error) {
	if len(values.RedactionKey) != 32 {
		return nil, errors.New("seal redaction key is not 32 bytes")
	}
	plaintext, err := json.Marshal(sealPlaintext{Version: 1, Binding: binding, SealValues: values}, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	options := new(jose.EncrypterOptions).WithType(SealType).
		WithHeader("tenant", binding.Tenant).WithHeader("pool", binding.Pool).WithHeader("receiver", binding.Receiver).
		WithHeader("key_id", binding.KeyID).WithHeader("run", binding.Run).
		WithHeader("attempt", binding.Attempt).WithHeader("credentials", binding.Credentials).WithHeader("expiry_at_ms", binding.ExpiryMS)
	if len(binding.TaskCredentials) > 0 {
		options = options.WithHeader("task_credentials", binding.TaskCredentials)
	}
	if len(binding.S3Inputs) > 0 {
		options = options.WithHeader("s3_inputs", binding.S3Inputs)
	}
	if len(binding.S3Outputs) > 0 {
		options = options.WithHeader("s3_outputs", binding.S3Outputs)
	}
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
	if len(compact) > MaxSeal {
		return nil, fmt.Errorf("a seal of %v is %d bytes, over the 64 KiB limit", binding.Names(), len(compact))
	}
	return []byte(compact), nil
}

// OpenSeal opens a seal for expected with key, the private half of expected.KeyID, and returns its values by the names
// of expected.Names.
func OpenSeal(encoded []byte, expected SealBinding, key *ecdsa.PrivateKey, now time.Time) (SealValues, error) {
	if expected.KeyID != SealKeyID(&key.PublicKey) {
		return SealValues{}, errors.New("seal key is not the expected key")
	}
	if len(encoded) > MaxSeal {
		return SealValues{}, errors.New("seal exceeds its size limit")
	}
	jwe, err := jose.ParseEncryptedCompact(string(encoded), []jose.KeyAlgorithm{jose.ECDH_ES_A256KW}, []jose.ContentEncryption{jose.A256GCM})
	if err != nil {
		return SealValues{}, errors.New("seal is not a compact JWE of the allowed algorithms")
	}
	protected, _, _ := strings.Cut(string(encoded), ".")
	decoded, err := base64.RawURLEncoding.DecodeString(protected)
	// An ignored binding member would widen what the seal is bound to.
	var header sealHeader
	if err != nil || json.Unmarshal(decoded, &header, Strict) != nil || header.Type != SealType {
		return SealValues{}, errors.New("seal protected header holds other members than a seal's")
	}
	if !reflect.DeepEqual(header.SealBinding, expected) {
		return SealValues{}, errors.New("seal is bound to another attempt or key")
	}
	if !now.Before(time.UnixMilli(header.ExpiryMS)) {
		return SealValues{}, errors.New("seal has expired")
	}
	plaintext, err := jwe.Decrypt(key)
	if err != nil {
		return SealValues{}, errors.New("seal does not decrypt")
	}
	var opened sealPlaintext
	if json.Unmarshal(plaintext, &opened, Strict) != nil || opened.Version != 1 || !reflect.DeepEqual(opened.Binding, header.SealBinding) {
		return SealValues{}, errors.New("seal plaintext is bound otherwise than its header")
	}
	names := header.Names()
	if len(opened.Values) != len(names) || slices.ContainsFunc(names, func(name string) bool { _, ok := opened.Values[name]; return !ok }) {
		return SealValues{}, errors.New("seal values are not the credentials and URLs it names")
	}
	if len(opened.RedactionKey) != 32 {
		return SealValues{}, errors.New("seal redaction key is not 32 bytes")
	}
	return opened.SealValues, nil
}
