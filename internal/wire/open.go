package wire

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/signed"
	jose "github.com/go-jose/go-jose/v4"
)

// The JWS types of the records that open a task's envelope, on the pool's events.
const (
	OpenRequestType   = "application/vnd.wherry.open-request.v1+jws"
	AttemptWrapType   = "application/vnd.wherry.attempt-wrap.v1+jws"
	RekeyWrapType     = "application/vnd.wherry.rekey-wrap.v1+jws"
	OpenRefusalType   = "application/vnd.wherry.open-refusal.v1+jws"
	ClaimDecisionType = "application/vnd.wherry.claim-decision.v1+jws"
)

// The reasons of an open refusal.
const (
	RefusedTwoClaimants    = "two live claimants"
	RefusedNotClaimant     = "not the current claimant"
	RefusedTerminal        = "terminal"
	RefusedPastBound       = "past its bound"
	RefusedKeyUnavailable  = "key unavailable"
	RefusedEnvelopeInvalid = "envelope invalid"
	RefusedPoolSuspended   = "pool suspended"
)

var refusalReasons = []string{RefusedTwoClaimants, RefusedNotClaimant, RefusedTerminal, RefusedPastBound, RefusedKeyUnavailable, RefusedEnvelopeInvalid, RefusedPoolSuspended}

// The reasons of a claim decision: no change of holder, a change after the previous holder released or after its
// lease lapsed, or a refusal while the previous holder is live.
const (
	ClaimUnchanged = "unchanged"
	ClaimReleased  = "released"
	ClaimLease     = "lease"
	ClaimLive      = "live"
)

var claimReasons = []string{ClaimUnchanged, ClaimReleased, ClaimLease, ClaimLive}

// maxWrap bounds a wrap's compact serialisation; one carries a 32-byte content key.
const maxWrap = 4 << 10

// AttemptKey is an attempt's P-256 public key as a JWK of exactly kty, crv, x and y.
type AttemptKey struct {
	Type  string `json:"kty"`
	Curve string `json:"crv"`
	X     string `json:"x"`
	Y     string `json:"y"`
}

// NewAttemptKey is key as an AttemptKey.
func NewAttemptKey(key *ecdsa.PublicKey) AttemptKey {
	encoded, _ := jose.JSONWebKey{Key: key}.MarshalJSON() // fails only for a key of another type
	var jwk AttemptKey
	json.Unmarshal(encoded, &jwk)
	return jwk
}

// Public is the P-256 public key the JWK holds.
func (key AttemptKey) Public() (*ecdsa.PublicKey, error) {
	encoded, err := json.Marshal(key)
	if err != nil {
		return nil, err
	}
	var jwk jose.JSONWebKey
	if err := jwk.UnmarshalJSON(encoded); err != nil {
		return nil, errors.New("attempt key is not a JWK")
	}
	public, ok := jwk.Key.(*ecdsa.PublicKey)
	if !ok || public.Curve != elliptic.P256() {
		return nil, errors.New("attempt key is not a P-256 public key")
	}
	return public, nil
}

// OpenRequest asks the pool's custodian for a task's content key wrapped to an attempt's key.
type OpenRequest struct {
	V        int        `json:"v"`
	Run      string     `json:"run"`
	Attempt  int64      `json:"attempt"`
	Receiver string     `json:"receiver"`
	TaskHash string     `json:"task_hash"`
	Key      AttemptKey `json:"key"`
}

// AttemptWrap answers an open request, the record whose digest is Request, with the content key wrapped to its key.
type AttemptWrap struct {
	V         int    `json:"v"`
	Custodian string `json:"custodian"`
	TaskHash  string `json:"task_hash"`
	Request   string `json:"request"`
	Receiver  string `json:"receiver"`
	Attempt   int64  `json:"attempt"`
	JWE       string `json:"jwe"`
}

// RekeyWrap is a task's content key wrapped to the pool key RecipientKID by Holder, a receiver holding an older key.
type RekeyWrap struct {
	V            int    `json:"v"`
	Holder       string `json:"holder"`
	TaskHash     string `json:"task_hash"`
	RecipientKID string `json:"recipient_kid"`
	JWE          string `json:"jwe"`
}

// OpenRefusal refuses an open request, the record whose digest is Request.
type OpenRefusal struct {
	V         int    `json:"v"`
	Custodian string `json:"custodian"`
	Request   string `json:"request"`
	Reason    string `json:"reason"`
}

// ClaimDecision is the custodian's decision on a claim, the claimed status whose digest is Claim, against its previous
// holder: whether it is served, why, and the previous holder's advertisement it was decided from. Previous is the
// previous holder, the receiver of the latest earlier claim served by its decision and not since released by that
// receiver; with none, the receiver of the ownership status before the claim; empty when that is the claimant.
type ClaimDecision struct {
	V             int            `json:"v"`
	Custodian     string         `json:"custodian"`
	Claim         string         `json:"claim"`
	Previous      string         `json:"previous,omitzero"`
	Reason        string         `json:"reason"`
	Advertisement *AdvertisedRef `json:"advertisement,omitzero"`
	Served        bool           `json:"served"`
}

// AdvertisedRef names an advertisement by its offset on its pool's receivers topic and its append time.
type AdvertisedRef struct {
	Offset int64 `json:"offset"`
	AtMS   int64 `json:"at_ms"`
}

// Digest is the SHA-256, in lowercase hex, of a record's encoded value.
func Digest(encoded []byte) string { return TaskHash(encoded) }

// ParseStrict is ParseHistory for the records decoded strictly, refusing unknown members and duplicate names.
func ParseStrict(encoded []byte, keys signed.Keys, appended time.Time, value Record) error {
	return parse(strictly(func(encoded []byte, typ string, keys signed.Keys, dst any) (string, error) {
		return signed.VerifyHistory(encoded, typ, keys, appended, dst)
	}), encoded, keys, value)
}

// strictly is verify decoding the payload strictly, a failure to decode as a decodeError.
func strictly(verify func([]byte, string, signed.Keys, any) (string, error)) func([]byte, string, signed.Keys, any) (string, error) {
	return func(encoded []byte, typ string, keys signed.Keys, dst any) (string, error) {
		var payload jsontext.Value
		owner, err := verify(encoded, typ, keys, &payload)
		if err != nil {
			return "", err
		}
		if err := json.Unmarshal(payload, dst, signed.Strict); err != nil {
			return owner, decodeError{err}
		}
		return owner, nil
	}
}

// decodeError is a payload's failure to decode, worded as the decoder words it.
type decodeError struct{ err error }

func (e decodeError) Error() string { return e.err.Error() }
func (e decodeError) Unwrap() error { return e.err }

func (*OpenRequest) jwsType() string   { return OpenRequestType }
func (*AttemptWrap) jwsType() string   { return AttemptWrapType }
func (*RekeyWrap) jwsType() string     { return RekeyWrapType }
func (*OpenRefusal) jwsType() string   { return OpenRefusalType }
func (*ClaimDecision) jwsType() string { return ClaimDecisionType }

func (value *OpenRequest) owner() string   { return value.Receiver }
func (value *AttemptWrap) owner() string   { return value.Custodian }
func (value *RekeyWrap) owner() string     { return value.Holder }
func (value *OpenRefusal) owner() string   { return value.Custodian }
func (value *ClaimDecision) owner() string { return value.Custodian }

func (value *OpenRequest) version() *int   { return &value.V }
func (value *AttemptWrap) version() *int   { return &value.V }
func (value *RekeyWrap) version() *int     { return &value.V }
func (value *OpenRefusal) version() *int   { return &value.V }
func (value *ClaimDecision) version() *int { return &value.V }

func (value *OpenRequest) validate() error {
	if err := validateIDs(map[string]string{"receiver": value.Receiver, "run": value.Run}); err != nil {
		return err
	}
	if _, err := value.Key.Public(); err != nil {
		return err
	}
	if value.Attempt < 1 || !ValidHash(value.TaskHash) {
		return errors.New("invalid open request")
	}
	return nil
}

func (value *AttemptWrap) validate() error {
	if err := validateIDs(map[string]string{"custodian": value.Custodian, "receiver": value.Receiver}); err != nil {
		return err
	}
	if value.Attempt < 1 || !ValidHash(value.TaskHash) || !ValidHash(value.Request) || value.JWE == "" || len(value.JWE) > maxWrap {
		return errors.New("invalid attempt wrap")
	}
	return nil
}

func (value *RekeyWrap) validate() error {
	if err := validateID(value.Holder, "holder"); err != nil {
		return err
	}
	kid, err := base64.RawURLEncoding.DecodeString(value.RecipientKID)
	if err != nil || len(kid) != 32 || !ValidHash(value.TaskHash) || value.JWE == "" || len(value.JWE) > maxWrap {
		return errors.New("invalid re-key wrap")
	}
	return nil
}

func (value *OpenRefusal) validate() error {
	if err := validateID(value.Custodian, "custodian"); err != nil {
		return err
	}
	if !ValidHash(value.Request) || !slices.Contains(refusalReasons, value.Reason) {
		return errors.New("invalid open refusal")
	}
	return nil
}

func (value *ClaimDecision) validate() error {
	if err := validateID(value.Custodian, "custodian"); err != nil {
		return err
	}
	if value.Previous != "" {
		if err := validateID(value.Previous, "previous"); err != nil {
			return err
		}
	}
	if !ValidHash(value.Claim) || !slices.Contains(claimReasons, value.Reason) || value.Served == (value.Reason == ClaimLive) ||
		value.Advertisement != nil && (value.Advertisement.Offset < 0 || value.Advertisement.AtMS <= 0) {
		return errors.New("invalid claim decision")
	}
	return nil
}
