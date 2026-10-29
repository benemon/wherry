package signed

import (
	"crypto"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

const KeyType = "application/vnd.wherry.key.v1+jws"

type KeyRecord struct {
	Version       int             `json:"v"`
	Owner         string          `json:"owner"`
	Action        string          `json:"action"`
	Key           jose.JSONWebKey `json:"key"`
	PreviousKeyID string          `json:"previous_key_id,omitzero"`
}

func KeyID(key ed25519.PublicKey) string {
	sum := sha256.Sum256(key)
	return "sha256:" + base64.RawURLEncoding.EncodeToString(sum[:])
}

// RegisterKey registers an owner's first signing key.
func RegisterKey(owner string, key ed25519.PrivateKey) ([]byte, error) {
	return signKeys(KeyRecord{Version: 1, Owner: owner, Action: "register", Key: publicJWK(key)}, key)
}

// RotateKey replaces the signing key, signed by both signing keys.
func RotateKey(owner string, current, next ed25519.PrivateKey) ([]byte, error) {
	return signKeys(KeyRecord{Version: 1, Owner: owner, Action: "rotate", Key: publicJWK(next), PreviousKeyID: KeyID(current.Public().(ed25519.PublicKey))}, current, next)
}

// KeyState is a key topic folded by ApplyKeyRecord.
type KeyState struct {
	Current ed25519.PublicKey
	// Retired holds keys rotated away; they verify the owner's history,
	// never new work.
	Retired []RetiredKey
}

// RetiredKey is a key and the broker time of the rotation that retired it. It
// verifies only records the broker appended before that time.
type RetiredKey struct {
	Key ed25519.PublicKey
	At  time.Time
}

// Keys maps an owner (HostOwner, or a receiver ID) to its folded key topic.
type Keys map[string]KeyState

func HostOwner(tenant, host string) string { return tenant + "." + host }

// current returns the owner's current key. The key ID is not the guard (the
// signature is); a mismatch tells the reader to re-read the owner's key topic.
func (keys Keys) current(owner, keyID string) (ed25519.PublicKey, error) {
	state := keys[owner]
	if state.Current == nil || KeyID(state.Current) != keyID {
		return nil, errors.New("signing key is not the owner's current key")
	}
	return state.Current, nil
}

// ApplyKeyRecord folds encoded, which the broker appended at appended, into state.
func ApplyKeyRecord(state KeyState, topicOwner string, encoded []byte, appended time.Time) (KeyState, error) {
	jws, err := jose.ParseSigned(string(encoded), []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil {
		return state, fmt.Errorf("parse key JWS: %w", err)
	}
	if len(jws.Signatures) == 0 {
		return state, errors.New("key record has no signature")
	}
	for _, sig := range jws.Signatures {
		if sig.Protected.Algorithm != string(jose.EdDSA) || sig.Protected.ExtraHeaders[jose.HeaderType] != KeyType {
			return state, errors.New("invalid key JWS protected header")
		}
	}
	// Once a topic holds a key, every record must verify against it; this is
	// what rejects a second bare registration.
	var payload []byte
	if state.Current == nil {
		payload, err = jws.Verify(signatureJWK(jws))
	} else {
		_, _, payload, err = jws.VerifyMulti(state.Current)
	}
	if err != nil {
		return state, errors.New("invalid key signature")
	}
	var record KeyRecord
	if err := json.Unmarshal(payload, &record, Tolerant); err != nil {
		return state, fmt.Errorf("decode key record: %w", err)
	}
	key, ok := record.Key.Key.(ed25519.PublicKey)
	if !ok || !record.Key.IsPublic() || record.Version != 1 || record.Owner != topicOwner {
		return state, errors.New("invalid key record")
	}
	switch record.Action {
	case "register":
		if state.Current != nil || len(jws.Signatures) != 1 || !verifiedBy(jws, key) {
			return state, errors.New("registration is not first and self-signed")
		}
		return KeyState{Current: key}, nil
	case "rotate":
		if state.Current == nil || len(jws.Signatures) != 2 || record.PreviousKeyID != KeyID(state.Current) || key.Equal(state.Current) || !verifiedBy(jws, state.Current) || !verifiedBy(jws, key) {
			return state, errors.New("rotation is not signed by current and new keys")
		}
		return KeyState{Current: key, Retired: append(slices.Clone(state.Retired), RetiredKey{Key: state.Current, At: appended})}, nil
	default:
		return state, errors.New("unknown key action")
	}
}

func publicJWK(key ed25519.PrivateKey) jose.JSONWebKey {
	public := key.Public().(ed25519.PublicKey)
	return jose.JSONWebKey{Key: public, KeyID: KeyID(public), Algorithm: string(jose.EdDSA), Use: "sig"}
}

func signKeys(record KeyRecord, keys ...ed25519.PrivateKey) ([]byte, error) {
	encoded, err := json.Marshal(record, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	signing := make([]jose.SigningKey, len(keys))
	for i, key := range keys {
		signing[i] = jose.SigningKey{Algorithm: jose.EdDSA, Key: jose.JSONWebKey{Key: key, KeyID: KeyID(key.Public().(ed25519.PublicKey)), Algorithm: string(jose.EdDSA)}}
	}
	options := &jose.SignerOptions{EmbedJWK: true}
	signer, err := jose.NewMultiSigner(signing, options.WithType(jose.ContentType(KeyType)))
	if err != nil {
		return nil, err
	}
	jws, err := signer.Sign(encoded)
	if err != nil {
		return nil, err
	}
	return []byte(jws.FullSerialize()), nil
}

func signatureJWK(jws *jose.JSONWebSignature) crypto.PublicKey {
	if len(jws.Signatures) != 1 || jws.Signatures[0].Header.JSONWebKey == nil {
		return nil
	}
	return jws.Signatures[0].Header.JSONWebKey.Key
}

func verifiedBy(jws *jose.JSONWebSignature, key ed25519.PublicKey) bool {
	_, _, _, err := jws.VerifyMulti(key)
	return err == nil
}
