package signed

import (
	"crypto/ed25519"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"strings"

	jose "github.com/go-jose/go-jose/v4"
)

// Strict decodes wherry's records and files, refusing unknown members and duplicate names.
var Strict = json.JoinOptions(json.RejectUnknownMembers(true), jsontext.AllowDuplicateNames(false))

// Sign signs payload as a compact JWS of type typ; kid names the owner and
// its key so a verifier resolves the key before reading the payload.
func Sign(typ, owner string, payload any, key ed25519.PrivateKey) ([]byte, error) {
	// Raw strings keep opaque agent JSON byte for byte through the signature.
	encoded, err := json.Marshal(payload, json.Deterministic(true), jsontext.PreserveRawStrings(true))
	if err != nil {
		return nil, err
	}
	options := new(jose.SignerOptions).WithType(jose.ContentType(typ)).WithHeader(jose.HeaderKey("kid"), owner+"/"+KeyID(key.Public().(ed25519.PublicKey)))
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, options)
	if err != nil {
		return nil, err
	}
	jws, err := signer.Sign(encoded)
	if err != nil {
		return nil, err
	}
	compact, err := jws.CompactSerialize()
	return []byte(compact), err
}

// Verify checks encoded against the owner's current key named by its kid and
// only then decodes the payload strictly into dst. It returns the owner, which
// the caller must match against the payload's own claims.
func Verify(encoded []byte, typ string, keys Keys, dst any) (string, error) {
	return verify(encoded, typ, keys.current, dst)
}

func verify(encoded []byte, typ string, key func(owner, keyID string) (ed25519.PublicKey, error), dst any) (string, error) {
	jws, err := jose.ParseSignedCompact(string(encoded), []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil {
		return "", fmt.Errorf("parse JWS: %w", err)
	}
	if len(jws.Signatures) != 1 || jws.Signatures[0].Protected.Algorithm != string(jose.EdDSA) || jws.Signatures[0].Protected.ExtraHeaders[jose.HeaderType] != typ {
		return "", errors.New("invalid JWS protected header")
	}
	owner, keyID, ok := strings.Cut(jws.Signatures[0].Protected.KeyID, "/")
	if !ok {
		return "", errors.New("JWS kid does not name an owner and key")
	}
	public, err := key(owner, keyID)
	if err != nil {
		return "", err
	}
	payload, err := jws.Verify(public)
	if err != nil {
		return "", errors.New("invalid JWS signature")
	}
	if err := json.Unmarshal(payload, dst, Strict); err != nil {
		return "", fmt.Errorf("decode payload: %w", err)
	}
	return owner, nil
}
