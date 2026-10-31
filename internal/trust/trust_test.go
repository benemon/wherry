package trust

import (
	"bytes"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"strings"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
)

func TestKeyFileRoundTrip(t *testing.T) {
	directory := t.TempDir()
	signingPath := filepath.Join(directory, "signing.key")
	signingKey, err := GenerateSigningKeyFile(signingPath)
	if err != nil {
		t.Fatal(err)
	}
	loadedSigningKey, err := LoadSigningKeyFile(signingPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(loadedSigningKey, signingKey) {
		t.Fatal("loaded signing key differs")
	}
	if _, err := GenerateSigningKeyFile(signingPath); err == nil {
		t.Fatal("existing signing key was overwritten")
	}
	encoded, err := os.ReadFile(signingPath)
	if err != nil {
		t.Fatal(err)
	}
	var jwk jose.JSONWebKey
	if err := jwk.UnmarshalJSON(encoded); err != nil {
		t.Fatalf("key file is not a JWK: %v", err)
	}
	if private, ok := jwk.Key.(ed25519.PrivateKey); !ok || !private.Equal(signingKey) {
		t.Fatal("key file is not the signing key's private JWK")
	}

	for _, path := range []string{signingPath} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode is %o, want 600", path, info.Mode().Perm())
		}
	}
}

func TestLoadKeyFileRejectsGroupReadable(t *testing.T) {
	directory := t.TempDir()
	t.Run("signing", func(t *testing.T) {
		path := filepath.Join(directory, "signing.key")
		if _, err := GenerateSigningKeyFile(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o640); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadSigningKeyFile(path); err == nil {
			t.Fatal("group-readable signing key was loaded")
		}
	})
	t.Run("other", func(t *testing.T) {
		path := filepath.Join(directory, "other.key")
		if _, err := GenerateSigningKeyFile(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o604); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadSigningKeyFile(path); err == nil {
			t.Fatal("other-readable signing key was loaded")
		}
	})
	t.Run("not regular", func(t *testing.T) {
		path := filepath.Join(directory, "dir.key")
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadSigningKeyFile(path); err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("loaded a directory as a signing key: %v", err)
		}
	})
}
