package trust

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

const (
	sealedSecretTopic = "receivers.secrets"
	sealedSecretInfo  = "wherry/v1/sealed_secret"
	sealedSecretAAD   = "wherry/v1/sealed_secret_aad\x00"
)

type secretAAD struct {
	Topic        string `json:"topic"`
	ReceiverID   string `json:"receiver_id"`
	Name         string `json:"name"`
	SealingKeyFP string `json:"sealing_key_fp"`
	Generation   int64  `json:"generation"`
	ApprovalHash string `json:"approval_hash"`
	Version      int64  `json:"version"`
	ExpMS        int64  `json:"exp_ms"`
}

// Seal encrypts a secret to an enrolled receiver under its accepted approval and signs its version and ciphertext records.
func Seal(enrolled Enrolled, receiverID, name string, version int64, value []byte, exp time.Time, userKey ed25519.PrivateKey) ([]byte, []byte, error) {
	if enrolled.SealingPub == nil {
		return nil, nil, errors.New("enrolled sealing key is required")
	}
	sealingKeyFP := sealingFingerprint(enrolled.SealingPub.Bytes())
	header := secretAAD{
		Topic: sealedSecretTopic, ReceiverID: receiverID, Name: name,
		SealingKeyFP: sealingKeyFP, Generation: enrolled.Generation, ApprovalHash: enrolled.ApprovalHash,
		Version: version, ExpMS: exp.UnixMilli(),
	}
	aad, err := marshalSecretAAD(header)
	if err != nil {
		return nil, nil, err
	}
	publicKey, err := hpke.NewDHKEMPublicKey(enrolled.SealingPub)
	if err != nil {
		return nil, nil, err
	}
	enc, sender, err := hpke.NewSender(publicKey, hpke.HKDFSHA256(), hpke.AES256GCM(), []byte(sealedSecretInfo))
	if err != nil {
		return nil, nil, err
	}
	ciphertext, err := sender.Seal(aad, value)
	if err != nil {
		return nil, nil, err
	}
	secretVersion := &wire.SecretVersion{
		ReceiverID: receiverID, Name: name, Version: version,
		SealingKeyFP: sealingKeyFP, Generation: enrolled.Generation, ApprovalHash: enrolled.ApprovalHash,
		CiphertextHash: ciphertextHash(enc, ciphertext), ExpMS: exp.UnixMilli(),
	}
	secretVersionEnvelope, err := marshalSigned(wire.TypeSecretVersion, secretVersion, userKey)
	if err != nil {
		return nil, nil, err
	}
	sealedSecret := &wire.SealedSecret{
		ReceiverID: receiverID, Name: name, Version: version,
		Enc: wire.Bytes(enc), CT: wire.Bytes(ciphertext),
	}
	sealedSecretEnvelope, err := marshalSigned(wire.TypeSealedSecret, sealedSecret, userKey)
	if err != nil {
		return nil, nil, err
	}
	return secretVersionEnvelope, sealedSecretEnvelope, nil
}

// SecretCurrent reports whether a version of a named secret is the current one and is still sealed under the receiver's current approval and unexpired.
func (state *State) SecretCurrent(receiverID, name string, version int64, now time.Time) bool {
	id := secretID{receiverID: receiverID, name: name}
	header, ok := state.secretVersions[id][version]
	if !ok || version != state.maximumVersions[id] {
		return false
	}
	enrolled, err := state.Receiver(receiverID, now)
	return err == nil && header.Generation == enrolled.Generation && header.ApprovalHash == enrolled.ApprovalHash && now.UnixMilli() < header.ExpMS
}

// Open verifies and decrypts the current version of a named sealed secret and returns its plaintext, version and seal expiry.
func Open(sealedEnvelopeBytes []byte, state *State, receiverID, name string, sealingKey *ecdh.PrivateKey, userPublicKey ed25519.PublicKey, now time.Time) ([]byte, int64, time.Time, error) {
	if state == nil || sealingKey == nil {
		return nil, 0, time.Time{}, errors.New("state and sealing key are required")
	}
	envelope, err := wire.DecodeEnvelope(sealedEnvelopeBytes)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	verified, err := wire.Verify(envelope, userPublicKey, wire.SignerUser)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	sealed, ok := verified.(*wire.SealedSecret)
	if !ok {
		return nil, 0, time.Time{}, errors.New("not a sealed secret")
	}
	if sealed.ReceiverID != receiverID {
		return nil, 0, time.Time{}, errors.New("sealed secret receiver does not match opener")
	}
	if sealed.Name != name {
		return nil, 0, time.Time{}, errors.New("sealed secret name does not match opener")
	}
	enrolled, err := state.Receiver(receiverID, now)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	id := secretID{receiverID: sealed.ReceiverID, name: sealed.Name}
	versions := state.secretVersions[id]
	header, ok := versions[sealed.Version]
	if !ok {
		return nil, 0, time.Time{}, errors.New("matching secret version is not in state")
	}
	if sealed.Version != state.maximumVersions[id] {
		return nil, 0, time.Time{}, errors.New("sealed secret version is not current")
	}
	if header.ReceiverID != sealed.ReceiverID || header.Name != sealed.Name {
		return nil, 0, time.Time{}, errors.New("sealed secret header does not match ciphertext")
	}
	if header.CiphertextHash != ciphertextHash(sealed.Enc, sealed.CT) {
		return nil, 0, time.Time{}, errors.New("sealed secret ciphertext hash does not match")
	}
	if header.SealingKeyFP != sealingFingerprint(sealingKey.PublicKey().Bytes()) {
		return nil, 0, time.Time{}, errors.New("sealed secret sealing key does not match opener")
	}
	if header.Generation != enrolled.Generation {
		return nil, 0, time.Time{}, errors.New("sealed secret generation is not current")
	}
	if header.ApprovalHash != enrolled.ApprovalHash {
		return nil, 0, time.Time{}, errors.New("sealed secret approval is not current")
	}
	if now.UnixMilli() >= header.ExpMS {
		return nil, 0, time.Time{}, errors.New("sealed secret is expired")
	}
	aad, err := marshalSecretAAD(secretAAD{
		Topic: sealedSecretTopic, ReceiverID: header.ReceiverID, Name: header.Name,
		SealingKeyFP: header.SealingKeyFP, Generation: header.Generation, ApprovalHash: header.ApprovalHash,
		Version: header.Version, ExpMS: header.ExpMS,
	})
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	privateKey, err := hpke.NewDHKEMPrivateKey(sealingKey)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	recipient, err := hpke.NewRecipient(sealed.Enc, privateKey, hpke.HKDFSHA256(), hpke.AES256GCM(), []byte(sealedSecretInfo))
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	value, err := recipient.Open(aad, sealed.CT)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	return value, sealed.Version, time.UnixMilli(header.ExpMS), nil
}

func marshalSecretAAD(header secretAAD) ([]byte, error) {
	encoded, err := json.Marshal(&header, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	return append([]byte(sealedSecretAAD), encoded...), nil
}

func sealingFingerprint(publicKey []byte) string {
	digest := sha256.Sum256(publicKey)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func ciphertextHash(enc, ciphertext []byte) string {
	hash := sha256.New()
	hash.Write(enc)
	hash.Write(ciphertext)
	return hex.EncodeToString(hash.Sum(nil))
}
