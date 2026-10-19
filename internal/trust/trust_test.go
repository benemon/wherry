package trust

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
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

	sealingPath := filepath.Join(directory, "sealing.key")
	sealingKey, err := GenerateSealingKeyFile(sealingPath)
	if err != nil {
		t.Fatal(err)
	}
	loadedSealingKey, err := LoadSealingKeyFile(sealingPath)
	if err != nil {
		t.Fatal(err)
	}
	if !loadedSealingKey.Equal(sealingKey) {
		t.Fatal("loaded sealing key differs")
	}
	if _, err := GenerateSealingKeyFile(sealingPath); err == nil {
		t.Fatal("existing sealing key was overwritten")
	}
	for _, path := range []string{signingPath, sealingPath} {
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
	t.Run("sealing", func(t *testing.T) {
		path := filepath.Join(directory, "sealing.key")
		if _, err := GenerateSealingKeyFile(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o604); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadSealingKeyFile(path); err == nil {
			t.Fatal("other-readable sealing key was loaded")
		}
	})
}
