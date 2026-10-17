package wire

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
)

const (
	maxEnvelopeSize = 900 << 10
	maxPayloadSize  = 900 << 10
)

var (
	errEnvelopeTooLarge = errors.New("envelope exceeds 900 KiB")
	errPayloadTooLarge  = errors.New("payload exceeds 900 KiB")
	errInvalidSignature = errors.New("invalid signature")
	errInvalidPayload   = errors.New("invalid payload")
	errUnentitledSigner = errors.New("signer kind is not entitled")
	errNotSelfSigned    = errors.New("enrolment request is not self-signed")
	strictDecodeOptions = json.JoinOptions(
		json.RejectUnknownMembers(true),
		jsontext.AllowDuplicateNames(false),
	)
	strictBase64 = base64.RawURLEncoding.Strict()
)

// Bytes is base64url-encoded without padding on the wire.
type Bytes []byte

// MarshalText encodes bytes as unpadded base64url.
func (value Bytes) MarshalText() ([]byte, error) {
	encoded := make([]byte, base64.RawURLEncoding.EncodedLen(len(value)))
	base64.RawURLEncoding.Encode(encoded, value)
	return encoded, nil
}

// UnmarshalText decodes strict unpadded base64url.
func (value *Bytes) UnmarshalText(encoded []byte) error {
	decoded := make([]byte, strictBase64.DecodedLen(len(encoded)))
	n, err := strictBase64.Decode(decoded, encoded)
	if err != nil {
		return err
	}
	*value = decoded[:n]
	return nil
}

// StdBytes is padded standard base64 on the wire, as proto3 JSON writes bytes.
type StdBytes []byte

// MarshalText encodes bytes as padded standard base64.
func (value StdBytes) MarshalText() ([]byte, error) {
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(value)))
	base64.StdEncoding.Encode(encoded, value)
	return encoded, nil
}

// UnmarshalText decodes strict base64 in the four forms proto3 JSON accepts:
// standard or URL-safe, with or without padding.
func (value *StdBytes) UnmarshalText(encoded []byte) error {
	encoding := base64.StdEncoding
	if bytes.ContainsAny(encoded, "-_") {
		encoding = base64.URLEncoding
	}
	if len(encoded)%4 != 0 {
		encoding = encoding.WithPadding(base64.NoPadding)
	}
	encoding = encoding.Strict()
	decoded := make([]byte, encoding.DecodedLen(len(encoded)))
	n, err := encoding.Decode(decoded, encoded)
	if err != nil {
		return err
	}
	*value = decoded[:n]
	return nil
}

// Envelope is the signed Kafka record value.
type Envelope struct {
	V       int64      `json:"v"`
	Type    RecordType `json:"type"`
	Signer  string     `json:"signer"`
	Payload Bytes      `json:"payload"`
	Sig     Bytes      `json:"sig"`
}

// Fingerprint returns an Ed25519 public-key fingerprint.
func Fingerprint(publicKey ed25519.PublicKey) string {
	digest := sha256.Sum256(publicKey)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// Sign signs exact payload bytes for a record type.
func Sign(recordType RecordType, payload []byte, privateKey ed25519.PrivateKey) Envelope {
	publicKey := privateKey.Public().(ed25519.PublicKey)
	return Envelope{
		V:       1,
		Type:    recordType,
		Signer:  Fingerprint(publicKey),
		Payload: append(Bytes(nil), payload...),
		Sig:     ed25519.Sign(privateKey, signatureInput(recordType, payload)),
	}
}

// Verify authenticates an entitled envelope and returns its decoded payload.
func Verify(envelope Envelope, publicKey ed25519.PublicKey, kind SignerKind) (any, error) {
	if envelope.V != 1 || !validFingerprint(envelope.Signer) || envelope.Signer != Fingerprint(publicKey) {
		return nil, errInvalidSignature
	}
	if !verifySignature(publicKey, signatureInput(envelope.Type, envelope.Payload), envelope.Sig) {
		return nil, errInvalidSignature
	}
	if !entitled(envelope.Type, kind) {
		return nil, errUnentitledSigner
	}
	decoded, err := DecodePayload(envelope.Type, envelope.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidPayload, err)
	}
	if request, ok := decoded.(*EnrolRequest); ok && !bytes.Equal(request.SigningPub, publicKey) {
		return nil, errNotSelfSigned
	}
	return decoded, nil
}

// MarshalPayload validates and deterministically encodes a payload.
func MarshalPayload(recordType RecordType, payload any) ([]byte, error) {
	if err := validatePayload(recordType, payload); err != nil {
		return nil, err
	}
	// Opaque JSON keeps its escapes; json/v2 still drops whitespace between its tokens.
	encoded, err := json.Marshal(payload, json.Deterministic(true), jsontext.PreserveRawStrings(true))
	if err != nil {
		return nil, err
	}
	if len(encoded) > maxPayloadSize {
		return nil, errPayloadTooLarge
	}
	return encoded, nil
}

// DecodePayload strictly decodes and validates a payload by record type.
func DecodePayload(recordType RecordType, encoded []byte) (any, error) {
	if len(encoded) > maxPayloadSize {
		return nil, errPayloadTooLarge
	}
	payload, err := newPayload(recordType)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(encoded, payload, strictDecodeOptions); err != nil {
		return nil, err
	}
	if err := validatePayload(recordType, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// MarshalEnvelope deterministically encodes an envelope.
func MarshalEnvelope(envelope Envelope) ([]byte, error) {
	encoded, err := json.Marshal(&envelope, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	if len(encoded) > maxEnvelopeSize {
		return nil, errEnvelopeTooLarge
	}
	return encoded, nil
}

// DecodeEnvelope strictly decodes an envelope after checking its size.
func DecodeEnvelope(encoded []byte) (Envelope, error) {
	if len(encoded) > maxEnvelopeSize {
		return Envelope{}, errEnvelopeTooLarge
	}
	var envelope Envelope
	if err := json.Unmarshal(encoded, &envelope, strictDecodeOptions); err != nil {
		return Envelope{}, err
	}
	if envelope.V != 1 || envelope.Type == "" || !validFingerprint(envelope.Signer) || envelope.Payload == nil || len(envelope.Sig) != ed25519.SignatureSize {
		return Envelope{}, errors.New("invalid envelope")
	}
	return envelope, nil
}

func signatureInput(recordType RecordType, payload []byte) []byte {
	prefix := []byte("wherry/v1/" + string(recordType) + "\x00")
	return append(prefix, payload...)
}

func verifySignature(publicKey ed25519.PublicKey, message, signature []byte) bool {
	return len(publicKey) == ed25519.PublicKeySize && ed25519.Verify(publicKey, message, signature)
}

func newPayload(recordType RecordType) (any, error) {
	switch recordType {
	case TypeTask:
		return &Task{Depth: -1, ParentHash: "\x00missing", DeadlineMS: -1, QueueDeadlineMS: -1}, nil
	case TypeCancel:
		return &Cancel{AtMS: -1}, nil
	case TypeEvent:
		return &Event{AtMS: -1}, nil
	case TypeReply:
		return &Reply{AtMS: -1}, nil
	case TypeTranscript:
		return &Transcript{Seq: -1}, nil
	case TypeCapability:
		return &Capability{Capacity: -1, InFlight: -1, Generation: -1, AtMS: -1}, nil
	case TypeEnrolRequest:
		return &EnrolRequest{AtMS: -1}, nil
	case TypeEnrolApproval:
		return &EnrolApproval{PrevStateHash: "\x00missing", Generation: -1, ExpMS: -1}, nil
	case TypeRenewRequest:
		return &RenewRequest{Generation: -1, AtMS: -1}, nil
	case TypeRevocation:
		return &Revocation{FloorGeneration: -1, AtMS: -1}, nil
	case TypeSecretVersion:
		return &SecretVersion{Version: -1, Generation: -1, ExpMS: -1}, nil
	case TypeSealedSecret:
		return &SealedSecret{Version: -1}, nil
	default:
		return nil, errors.New("unknown record type")
	}
}
