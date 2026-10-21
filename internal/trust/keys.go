package trust

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
)

// GenerateSigningKeyFile creates an Ed25519 private-key file without replacing an existing file.
func GenerateSigningKeyFile(path string) (ed25519.PrivateKey, error) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	if err := writeKeyFile(path, privateKey); err != nil {
		return nil, err
	}
	return privateKey, nil
}

// LoadSigningKeyFile loads an Ed25519 private-key file with private permissions.
func LoadSigningKeyFile(path string) (ed25519.PrivateKey, error) {
	key, err := readKeyFile(path)
	if err != nil {
		return nil, err
	}
	if len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("invalid Ed25519 private key length")
	}
	return ed25519.PrivateKey(key), nil
}

func writeKeyFile(path string, key []byte) (returnErr error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); returnErr == nil && err != nil {
			returnErr = err
		}
	}()
	encoded := base64.RawURLEncoding.EncodeToString(key)
	if _, err := file.WriteString(encoded); err != nil {
		return err
	}
	return nil
}

// ReadOwnerOnly reads the regular file at path, refusing one its group or others can access.
func ReadOwnerOnly(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s is open to its group or others; chmod 600 it", path)
	}
	return io.ReadAll(file)
}

func readKeyFile(path string) ([]byte, error) {
	encoded, err := ReadOwnerOnly(path)
	if err != nil {
		return nil, err
	}
	key, err := base64.RawURLEncoding.Strict().DecodeString(string(encoded))
	if err != nil {
		return nil, fmt.Errorf("invalid base64url private key: %w", err)
	}
	return key, nil
}
