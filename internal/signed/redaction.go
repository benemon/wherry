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

// S3InputValue names the presigned URL of an S3 input among a run's redacted values; the slash keeps it apart from
// credential names.
func S3InputValue(name string) string { return "s3-inputs/" + name }

// S3PolicyValue names the signed values of a run's S3 POST policy, its policy, signature and credential, among the
// run's redacted values.
const S3PolicyValue = "s3-policy"
