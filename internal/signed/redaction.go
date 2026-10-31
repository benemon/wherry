package signed

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"

	jose "github.com/go-jose/go-jose/v4"
)

const redactionDomain = "wherry-redaction-v1"

// KeyThumbprint is the RFC 7638 SHA-256 thumbprint of a P-256 key, base64url: a pool key's or an attempt key's ID.
func KeyThumbprint(key *ecdsa.PublicKey) string {
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

// S3InputValue and S3OutputValue name the presigned URLs of an S3 input and output among a run's redacted values; the
// slash keeps them apart from credential names.
func S3InputValue(name string) string  { return "s3-inputs/" + name }
func S3OutputValue(name string) string { return "s3-outputs/" + name }
