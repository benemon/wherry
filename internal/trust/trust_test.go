package trust

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
)

// A malformed key file's error names the file and echoes nothing it holds. The canary is a private JWK whose x5u member
// also holds its private d value, as ":<d>", which no URL parser accepts: a parser error passed on would echo the key.
func TestLoadKeyFileErrorEchoesNoKeyMaterial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "signing.key")
	if _, err := GenerateSigningKeyFile(path); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var members map[string]any
	if err := json.Unmarshal(encoded, &members); err != nil {
		t.Fatal(err)
	}
	d, ok := members["d"].(string)
	if !ok || d == "" {
		t.Fatalf("key file %s has no d member", path)
	}
	members["x5u"] = ":" + d
	if encoded, err = json.Marshal(members); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = LoadSigningKeyFile(path)
	if err == nil {
		t.Fatal("loaded a key file with an invalid x5u member")
	}
	if strings.Contains(err.Error(), d) {
		t.Fatalf("error echoes the private value: %s", strings.ReplaceAll(err.Error(), d, "<d>"))
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("error %q does not name the file", err)
	}
}

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
