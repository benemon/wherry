package signed

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	jose "github.com/go-jose/go-jose/v4"
)

const EnvelopeType = "application/vnd.wherry.envelope.v1+jwe"

// MaxEnvelope bounds an envelope's compact serialisation.
const MaxEnvelope = 64 << 10

var errSpan = errors.New("task payload breaks the envelope span rule")

// envelopeSpan tokenises payload and returns the offsets of the comma before the top-level wherry object's envelope
// member, -1 when wherry holds neither envelope nor wrap, and of wherry's closing brace. envelope and wrap, when
// present, are wherry's last two members in that order, after at least one other.
func envelopeSpan(payload []byte) (comma, closing int, err error) {
	comma, closing = -1, -1
	decoder := jsontext.NewDecoder(bytes.NewReader(payload))
	if token, err := decoder.ReadToken(); err != nil || token.Kind() != '{' {
		return 0, 0, errSpan
	}
	for decoder.PeekKind() != '}' {
		name, err := decoder.ReadToken()
		if err != nil {
			return 0, 0, errSpan
		}
		if name.String() != "wherry" {
			if decoder.SkipValue() != nil {
				return 0, 0, errSpan
			}
			continue
		}
		if token, err := decoder.ReadToken(); err != nil || token.Kind() != '{' {
			return 0, 0, errSpan
		}
		const before, envelope, wrap = 0, 1, 2
		seen, members := before, 0
		for decoder.PeekKind() != '}' {
			end := int(decoder.InputOffset())
			member, err := decoder.ReadToken()
			if err != nil {
				return 0, 0, errSpan
			}
			switch {
			case seen == before && member.String() == "envelope" && members > 0:
				// Only whitespace and the one comma lie between the previous value and this name.
				seen, comma = envelope, end+bytes.IndexByte(payload[end:], ',')
			case seen == envelope && member.String() == "wrap":
				seen = wrap
			case seen != before || member.String() == "envelope" || member.String() == "wrap":
				return 0, 0, errSpan
			}
			members++
			if decoder.SkipValue() != nil {
				return 0, 0, errSpan
			}
		}
		if _, err := decoder.ReadToken(); err != nil || seen == envelope {
			return 0, 0, errSpan
		}
		closing = int(decoder.InputOffset()) - 1
	}
	if _, err := decoder.ReadToken(); err != nil || closing < 0 {
		return 0, 0, errSpan
	}
	if _, err := decoder.ReadToken(); err != io.EOF {
		return 0, 0, errSpan
	}
	return comma, closing, nil
}

// removeSpan returns payload without the span from the comma before wherry.envelope up to wherry's closing brace.
func removeSpan(payload []byte) ([]byte, error) {
	comma, closing, err := envelopeSpan(payload)
	if err != nil {
		return nil, err
	}
	if comma < 0 {
		return nil, errors.New("task payload has no envelope and wrap")
	}
	return slices.Concat(payload[:comma], payload[closing:]), nil
}

// TaskCore is the lowercase hex SHA-256 of a signed task payload without its envelope span, the value an envelope is
// bound to. The caller verifies the payload's signature first.
func TaskCore(payload []byte) (string, error) {
	core, err := removeSpan(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(core)
	return hex.EncodeToString(digest[:]), nil
}

// InsertEnvelope returns core, a task payload without envelope and wrap, with the two inserted as the last members of
// its top-level wherry object. TaskCore of the result is the lowercase hex SHA-256 of core.
func InsertEnvelope(core, envelope, wrap []byte) ([]byte, error) {
	comma, closing, err := envelopeSpan(core)
	if err != nil {
		return nil, err
	}
	if comma >= 0 {
		return nil, errors.New("task payload already has an envelope")
	}
	span, err := jsontext.AppendQuote([]byte(`,"envelope":`), envelope)
	if err != nil {
		return nil, err
	}
	span, err = jsontext.AppendQuote(append(span, `,"wrap":`...), wrap)
	if err != nil {
		return nil, err
	}
	payload := slices.Concat(core[:closing], span, core[closing:])
	// An empty wherry object leaves no member for the span's comma to follow.
	if comma, _, err := envelopeSpan(payload); err != nil || comma != closing {
		return nil, errSpan
	}
	return payload, nil
}

// EnvelopeValues are the confidential values of a task, and an envelope's whole plaintext. RedactionKey is the run's
// 32-byte key.
type EnvelopeValues struct {
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
	TaskCore   string `json:"task_core"`
}

// BuildEnvelope encrypts values under contentKey, a fresh 32-byte key, as a compact JWE bound to taskCore.
func BuildEnvelope(values EnvelopeValues, contentKey []byte, taskCore string) ([]byte, error) {
	if len(contentKey) != 32 || len(values.RedactionKey) != 32 {
		return nil, errors.New("envelope content key or redaction key is not 32 bytes")
	}
	plaintext, err := json.Marshal(values, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	options := new(jose.EncrypterOptions).WithType(EnvelopeType).WithHeader("task_core", taskCore)
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
func parseEnvelope(encoded []byte) (*jose.JSONWebEncryption, envelopeHeader, error) {
	var header envelopeHeader
	if len(encoded) > MaxEnvelope {
		return nil, header, errors.New("envelope exceeds its size limit")
	}
	jwe, err := jose.ParseEncryptedCompact(string(encoded), []jose.KeyAlgorithm{jose.DIRECT}, []jose.ContentEncryption{jose.A256GCM})
	if err != nil {
		return nil, header, errors.New("envelope is not a compact JWE of the allowed algorithms")
	}
	protected, rest, _ := strings.Cut(string(encoded), ".")
	// RFC 7518 section 4.5: under dir the JWE Encrypted Key is empty.
	if !strings.HasPrefix(rest, ".") {
		return nil, header, errors.New("envelope carries an encrypted key")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(protected)
	if err != nil || json.Unmarshal(decoded, &header, Strict) != nil || header.Type != EnvelopeType {
		return nil, header, errors.New("envelope protected header holds other members than an envelope's")
	}
	return jwe, header, nil
}

// OpenEnvelope decrypts an envelope bound to taskCore with contentKey and returns its values.
func OpenEnvelope(encoded, contentKey []byte, taskCore string) (EnvelopeValues, error) {
	jwe, header, err := parseEnvelope(encoded)
	if err != nil {
		return EnvelopeValues{}, err
	}
	if header.TaskCore != taskCore {
		return EnvelopeValues{}, errors.New("envelope is bound to another task")
	}
	plaintext, err := jwe.Decrypt(contentKey)
	if err != nil {
		return EnvelopeValues{}, errors.New("envelope does not decrypt")
	}
	var values EnvelopeValues
	if json.Unmarshal(plaintext, &values, Strict) != nil || values.Credentials == nil || values.S3Inputs == nil || values.S3Outputs == nil {
		return EnvelopeValues{}, errors.New("envelope plaintext is not an envelope's")
	}
	if len(values.RedactionKey) != 32 {
		return EnvelopeValues{}, errors.New("envelope redaction key is not 32 bytes")
	}
	return values, nil
}

// CheckContentKey confirms contentKey decrypts an envelope: a wrap is valid exactly when its key does.
func CheckContentKey(envelope, contentKey []byte) error {
	jwe, _, err := parseEnvelope(envelope)
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
	encrypter, err := jose.NewEncrypter(jose.A256GCM, jose.Recipient{Algorithm: jose.ECDH_ES_A256KW, Key: recipient, KeyID: SealKeyID(recipient)}, nil)
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

// UnwrapContentKey decrypts a wrap to key and returns its 32-byte content key.
func UnwrapContentKey(encoded []byte, key *ecdsa.PrivateKey) ([]byte, error) {
	jwe, err := jose.ParseEncryptedCompact(string(encoded), []jose.KeyAlgorithm{jose.ECDH_ES_A256KW}, []jose.ContentEncryption{jose.A256GCM})
	if err != nil {
		return nil, errors.New("wrap is not a compact JWE of the allowed algorithms")
	}
	protected, _, _ := strings.Cut(string(encoded), ".")
	decoded, err := base64.RawURLEncoding.DecodeString(protected)
	var header wrapHeader
	if err != nil || json.Unmarshal(decoded, &header, Strict) != nil {
		return nil, errors.New("wrap protected header holds other members than a wrap's")
	}
	if header.KeyID != SealKeyID(&key.PublicKey) {
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
