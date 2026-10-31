package trust

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	jose "github.com/go-jose/go-jose/v4"
)

// GenerateSigningKeyFile creates an Ed25519 private-key file without replacing an existing file, synced with its
// directory.
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
	private, ok := key.Key.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("key file is not an Ed25519 private JWK")
	}
	return private, nil
}

// writeKeyFile writes key as a JWK (RFC 7517), private members included.
func writeKeyFile(path string, key any) (returnErr error) {
	encoded, err := (&jose.JSONWebKey{Key: key}).MarshalJSON()
	if err != nil {
		return err
	}
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
	if _, err := file.Write(encoded); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// ReplaceKeyFile renames the key file from over to and syncs their directory.
func ReplaceKeyFile(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}
	return syncDir(filepath.Dir(to))
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
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

func readKeyFile(path string) (jose.JSONWebKey, error) {
	var key jose.JSONWebKey
	encoded, err := ReadOwnerOnly(path)
	if err != nil {
		return key, err
	}
	if err := key.UnmarshalJSON(encoded); err != nil {
		return key, fmt.Errorf("invalid JWK private key: %w", err)
	}
	if key.IsPublic() {
		return key, errors.New("key file holds no private key")
	}
	return key, nil
}
