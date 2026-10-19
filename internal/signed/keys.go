package signed

import (
	"crypto"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"slices"

	jose "github.com/go-jose/go-jose/v4"
)

const KeyType = "application/vnd.wherry.key.v1+jws"

type KeyRecord struct {
	Version       int             `json:"v"`
	Owner         string          `json:"owner"`
	Action        string          `json:"action"`
	Key           jose.JSONWebKey `json:"key"`
	PreviousKeyID string          `json:"previous_key_id,omitzero"`
	// SealingKey is a receiver's raw X25519 public key, base64url; go-jose
	// JWKs carry only Ed25519 among the OKP curves.
	SealingKey string `json:"sealing_key,omitzero"`
}

func KeyID(key ed25519.PublicKey) string {
	sum := sha256.Sum256(key)
	return "sha256:" + base64.RawURLEncoding.EncodeToString(sum[:])
}

// RegisterKey registers an owner's first signing key and, for a receiver, its
// sealing key; sealing is nil for a host.
func RegisterKey(owner string, key ed25519.PrivateKey, sealing *ecdh.PublicKey) ([]byte, error) {
	return signKeys(KeyRecord{Version: 1, Owner: owner, Action: "register", Key: publicJWK(key), SealingKey: encodeSealing(sealing)}, key)
}

// RotateKey replaces the signing key and sets the sealing key, signed by both
// signing keys.
func RotateKey(owner string, current, next ed25519.PrivateKey, sealing *ecdh.PublicKey) ([]byte, error) {
	return signKeys(KeyRecord{Version: 1, Owner: owner, Action: "rotate", Key: publicJWK(next), PreviousKeyID: KeyID(current.Public().(ed25519.PublicKey)), SealingKey: encodeSealing(sealing)}, current, next)
}

func encodeSealing(key *ecdh.PublicKey) string {
	if key == nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(key.Bytes())
}

func decodeSealing(encoded string) (*ecdh.PublicKey, error) {
	if encoded == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return nil, errors.New("invalid sealing key encoding")
	}
	return ecdh.X25519().NewPublicKey(raw)
}

func RevokeKey(owner string, current ed25519.PrivateKey) ([]byte, error) {
	return signKeys(KeyRecord{Version: 1, Owner: owner, Action: "revoke", Key: publicJWK(current)}, current)
}

// KeyState is a key topic folded by ApplyKeyRecord. A revoked topic is closed:
// its owner is re-provisioned under a new principal.
type KeyState struct {
	Current ed25519.PublicKey
	// Retired holds keys rotated away; they verify the owner's history,
	// never new work. A revocation drops them with the topic.
	Retired []ed25519.PublicKey
	Sealing *ecdh.PublicKey
	Revoked bool
}

// Keys maps an owner (HostOwner, or a receiver ID) to its folded key topic.
type Keys map[string]KeyState

func HostOwner(tenant, host string) string { return tenant + "." + host }

// current returns the owner's current key. The key ID is not the guard (the
// signature is); a mismatch tells the reader to re-read the owner's key topic.
func (keys Keys) current(owner, keyID string) (ed25519.PublicKey, error) {
	state := keys[owner]
	if state.Revoked || state.Current == nil || KeyID(state.Current) != keyID {
		return nil, errors.New("signing key is not the owner's current key")
	}
	return state.Current, nil
}

// signedBy returns the owner's current or retired key with keyID.
func (keys Keys) signedBy(owner, keyID string) (ed25519.PublicKey, error) {
	if current, err := keys.current(owner, keyID); err == nil {
		return current, nil
	}
	state := keys[owner]
	if index := slices.IndexFunc(state.Retired, func(key ed25519.PublicKey) bool { return KeyID(key) == keyID }); index >= 0 {
		return state.Retired[index], nil
	}
	return nil, errors.New("signing key is not one of the owner's keys")
}

func ApplyKeyRecord(state KeyState, topicOwner string, encoded []byte) (KeyState, error) {
	if state.Revoked {
		return state, errors.New("key topic is closed by revocation")
	}
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
	if err := json.Unmarshal(payload, &record, strict); err != nil {
		return state, fmt.Errorf("decode key record: %w", err)
	}
	key, ok := record.Key.Key.(ed25519.PublicKey)
	if !ok || !record.Key.IsPublic() || record.Version != 1 || record.Owner != topicOwner {
		return state, errors.New("invalid key record")
	}
	sealing, err := decodeSealing(record.SealingKey)
	if err != nil {
		return state, err
	}
	switch record.Action {
	case "register":
		if len(jws.Signatures) != 1 || !verifiedBy(jws, key) {
			return state, errors.New("registration is not first and self-signed")
		}
		return KeyState{Current: key, Sealing: sealing}, nil
	case "rotate":
		if state.Current == nil || len(jws.Signatures) != 2 || record.PreviousKeyID != KeyID(state.Current) || key.Equal(state.Current) || !verifiedBy(jws, state.Current) || !verifiedBy(jws, key) {
			return state, errors.New("rotation is not signed by current and new keys")
		}
		return KeyState{Current: key, Retired: append(slices.Clone(state.Retired), state.Current), Sealing: sealing}, nil
	case "revoke":
		if state.Current == nil || len(jws.Signatures) != 1 || !key.Equal(state.Current) || !verifiedBy(jws, key) {
			return state, errors.New("revocation is not signed by retiring key")
		}
		return KeyState{Revoked: true}, nil
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
