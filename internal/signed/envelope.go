package signed

import (
	"crypto"
	"crypto/ecdsa"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"strings"

	jose "github.com/go-jose/go-jose/v4"
)

const EnvelopeType = "application/vnd.wherry.envelope.v1+jwe"

// MaxEnvelope bounds an envelope's compact serialisation.
const MaxEnvelope = 64 << 10

// KeyThumbprint is the RFC 7638 SHA-256 thumbprint of a P-256 key, base64url: a pool key's or an attempt key's ID.
func KeyThumbprint(key *ecdsa.PublicKey) string {
	thumbprint, _ := (&jose.JSONWebKey{Key: key}).Thumbprint(crypto.SHA256) // fails only for a key of another type
	return base64.RawURLEncoding.EncodeToString(thumbprint)
}

// EnvelopeBinding names the task an envelope travels in.
type EnvelopeBinding struct {
	Run           string `json:"run"`
	Tenant        string `json:"tenant"`
	Pool          string `json:"pool"`
	TaskStartedBy int64  `json:"task_started_by"`
	TaskEndedBy   int64  `json:"task_ended_by"`
}

// EnvelopeValues are the confidential values of a task with the binding to it, an envelope's whole plaintext.
// RedactionKey is the run's 32-byte key.
type EnvelopeValues struct {
	Binding      EnvelopeBinding   `json:"binding"`
	Credentials  map[string][]byte `json:"credentials"`
	S3Inputs     map[string]string `json:"s3_inputs"`
	S3Outputs    map[string]string `json:"s3_outputs"`
	RedactionKey []byte            `json:"redaction_key"`
}

// envelopeHeader is every member an envelope's protected header may hold.
type envelopeHeader struct {
	Algorithm  string `json:"alg"`
	Encryption string `json:"enc"`
	Type       string `json:"typ"`
}

// BuildEnvelope encrypts values under contentKey, a fresh 32-byte key, as a compact JWE.
func BuildEnvelope(values EnvelopeValues, contentKey []byte) ([]byte, error) {
	if len(contentKey) != 32 || len(values.RedactionKey) != 32 {
		return nil, errors.New("envelope content key or redaction key is not 32 bytes")
	}
	plaintext, err := json.Marshal(values, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	options := new(jose.EncrypterOptions).WithType(EnvelopeType)
	encrypter, err := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.DIRECT, Key: contentKey}, options)
	if err != nil {
		return nil, err
	}
	jwe, err := encrypter.Encrypt(plaintext)
	if err != nil {
		return nil, errors.New("encrypt envelope")
	}
	compact, err := jwe.CompactSerialize()
	if err != nil {
		return nil, err
	}
	if len(compact) > MaxEnvelope {
		return nil, fmt.Errorf("an envelope is %d bytes, over the 64 KiB limit", len(compact))
	}
	return []byte(compact), nil
}

// parseEnvelope parses a compact envelope whose protected header holds exactly an envelope's members.
func parseEnvelope(encoded []byte) (*jose.JSONWebEncryption, error) {
	if len(encoded) > MaxEnvelope {
		return nil, errors.New("envelope exceeds its size limit")
	}
	jwe, err := jose.ParseEncryptedCompact(string(encoded), []jose.KeyAlgorithm{jose.DIRECT}, []jose.ContentEncryption{jose.A256GCM})
	if err != nil {
		return nil, errors.New("envelope is not a compact JWE of the allowed algorithms")
	}
	protected, rest, _ := strings.Cut(string(encoded), ".")
	// RFC 7518 section 4.5: under dir the JWE Encrypted Key is empty.
	if !strings.HasPrefix(rest, ".") {
		return nil, errors.New("envelope carries an encrypted key")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(protected)
	var header envelopeHeader
	if err != nil || json.Unmarshal(decoded, &header, Strict) != nil || header.Type != EnvelopeType {
		return nil, errors.New("envelope protected header holds other members than an envelope's")
	}
	return jwe, nil
}

// OpenEnvelope decrypts an envelope with contentKey and returns its values; the caller checks their binding against the
// task.
func OpenEnvelope(encoded, contentKey []byte) (EnvelopeValues, error) {
	jwe, err := parseEnvelope(encoded)
	if err != nil {
		return EnvelopeValues{}, err
	}
	plaintext, err := jwe.Decrypt(contentKey)
	if err != nil {
		return EnvelopeValues{}, errors.New("envelope does not decrypt")
	}
	var values EnvelopeValues
	binding := &values.Binding
	if json.Unmarshal(plaintext, &values, Strict) != nil || values.Credentials == nil || values.S3Inputs == nil || values.S3Outputs == nil ||
		binding.Run == "" || binding.Tenant == "" || binding.Pool == "" || binding.TaskStartedBy == 0 || binding.TaskEndedBy == 0 {
		return EnvelopeValues{}, errors.New("envelope plaintext is not an envelope's")
	}
	if len(values.RedactionKey) != 32 {
		return EnvelopeValues{}, errors.New("envelope redaction key is not 32 bytes")
	}
	return values, nil
}

// CheckContentKey confirms contentKey decrypts an envelope: a wrap is valid exactly when its key does.
func CheckContentKey(envelope, contentKey []byte) error {
	jwe, err := parseEnvelope(envelope)
	if err != nil {
		return err
	}
	if _, err := jwe.Decrypt(contentKey); err != nil {
		return errors.New("content key does not decrypt the envelope")
	}
	return nil
}

// wrapHeader is every member a wrap's protected header may hold; epk is a public key only.
type wrapHeader struct {
	Algorithm  string `json:"alg"`
	Encryption string `json:"enc"`
	KeyID      string `json:"kid"`
	Ephemeral  struct {
		Type  string `json:"kty"`
		Curve string `json:"crv"`
		X     string `json:"x"`
		Y     string `json:"y"`
	} `json:"epk"`
}

// WrapContentKey encrypts a 32-byte content key to recipient as a compact JWE, for a submit, attempt or re-key wrap.
func WrapContentKey(contentKey []byte, recipient *ecdsa.PublicKey) ([]byte, error) {
	if len(contentKey) != 32 {
		return nil, errors.New("content key is not 32 bytes")
	}
	encrypter, err := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.ECDH_ES_A256KW, Key: recipient, KeyID: KeyThumbprint(recipient)}, nil)
	if err != nil {
		return nil, err
	}
	jwe, err := encrypter.Encrypt(contentKey)
	if err != nil {
		return nil, errors.New("wrap content key")
	}
	compact, err := jwe.CompactSerialize()
	return []byte(compact), err
}

// WrapKeyID is the kid of a wrap's protected header, the recipient key's thumbprint, read without decrypting the wrap.
func WrapKeyID(encoded []byte) (string, error) {
	protected, _, _ := strings.Cut(string(encoded), ".")
	decoded, err := base64.RawURLEncoding.DecodeString(protected)
	var header wrapHeader
	if err != nil || json.Unmarshal(decoded, &header, Strict) != nil {
		return "", errors.New("wrap protected header holds other members than a wrap's")
	}
	return header.KeyID, nil
}

// UnwrapContentKey decrypts a wrap to key and returns its 32-byte content key.
func UnwrapContentKey(encoded []byte, key *ecdsa.PrivateKey) ([]byte, error) {
	jwe, err := jose.ParseEncryptedCompact(string(encoded), []jose.KeyAlgorithm{jose.ECDH_ES_A256KW}, []jose.ContentEncryption{jose.A256GCM})
	if err != nil {
		return nil, errors.New("wrap is not a compact JWE of the allowed algorithms")
	}
	kid, err := WrapKeyID(encoded)
	if err != nil {
		return nil, err
	}
	if kid != KeyThumbprint(&key.PublicKey) {
		return nil, errors.New("wrap is for another key")
	}
	contentKey, err := jwe.Decrypt(key)
	if err != nil {
		return nil, errors.New("wrap does not decrypt")
	}
	if len(contentKey) != 32 {
		return nil, errors.New("wrapped content key is not 32 bytes")
	}
	return contentKey, nil
}
