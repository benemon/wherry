package signed

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
)

const redactionDomain = "wherry-redaction-v1"

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
