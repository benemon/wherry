package wire

import (
	"bytes"
	"encoding/base64"
)

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
