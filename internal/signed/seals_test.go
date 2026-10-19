package signed

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	json "encoding/json/v2"
	"strings"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

const sealSecret = "seal-canary-credential"

func sealKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func testBinding(key *ecdsa.PrivateKey) SealBinding {
	return SealBinding{
		Tenant: "tenant-a", Pool: "pool-a", Receiver: "receiver-a", KeyID: SealKeyID(&key.PublicKey), Run: "run-a",
		Turn: 2, Attempt: 3, Credentials: []string{"model"}, ExpiryMS: time.Now().Add(time.Minute).UnixMilli(),
	}
}

func values() map[string][]byte { return map[string][]byte{"model": []byte(sealSecret)} }

func mustSeal(t *testing.T, binding SealBinding, key *ecdsa.PublicKey, values map[string][]byte) []byte {
	t.Helper()
	sealed, err := Seal(binding, key, values)
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

// headers are the protected claims Seal writes for binding.
func headers(binding SealBinding) map[jose.HeaderKey]any {
	return map[jose.HeaderKey]any{
		jose.HeaderType: SealType, "tenant": binding.Tenant, "pool": binding.Pool, "receiver": binding.Receiver, "key_id": binding.KeyID,
		"run": binding.Run, "turn": binding.Turn, "attempt": binding.Attempt, "credentials": binding.Credentials, "expiry_ms": binding.ExpiryMS,
	}
}

// craft encrypts plaintext under protected claims and algorithms the test chooses, as a broken or hostile sealer would.
func craft(t *testing.T, alg jose.KeyAlgorithm, enc jose.ContentEncryption, key any, claims map[jose.HeaderKey]any, compression jose.CompressionAlgorithm, plaintext []byte) *jose.JSONWebEncryption {
	t.Helper()
	encrypter, err := jose.NewEncrypter(enc, jose.Recipient{Algorithm: alg, Key: key}, &jose.EncrypterOptions{ExtraHeaders: claims, Compression: compression})
	if err != nil {
		t.Fatal(err)
	}
	jwe, err := encrypter.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	return jwe
}

func compact(t *testing.T, jwe *jose.JSONWebEncryption) []byte {
	t.Helper()
	encoded, err := jwe.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return []byte(encoded)
}

func plaintext(t *testing.T, value sealPlaintext) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestSealOpensForItsBinding(t *testing.T) {
	key := sealKey(t)
	binding := testBinding(key)
	opened, err := OpenSeal(mustSeal(t, binding, &key.PublicKey, values()), binding, key, time.Now())
	if err != nil || string(opened["model"]) != sealSecret || len(opened) != 1 {
		t.Fatalf("opened %q, %v", opened, err)
	}
}

func TestSealBoundToEveryHeaderField(t *testing.T) {
	key := sealKey(t)
	expected := testBinding(key)
	for name, change := range map[string]func(*SealBinding){
		"tenant":      func(b *SealBinding) { b.Tenant = "tenant-b" },
		"pool":        func(b *SealBinding) { b.Pool = "pool-b" },
		"receiver":    func(b *SealBinding) { b.Receiver = "receiver-b" },
		"key_id":      func(b *SealBinding) { b.KeyID = SealKeyID(&sealKey(t).PublicKey) },
		"run":         func(b *SealBinding) { b.Run = "run-b" },
		"turn":        func(b *SealBinding) { b.Turn = 1 },
		"attempt":     func(b *SealBinding) { b.Attempt = 2 },
		"credentials": func(b *SealBinding) { b.Credentials = []string{"other"} },
		"expiry":      func(b *SealBinding) { b.ExpiryMS += time.Hour.Milliseconds() },
	} {
		t.Run(name, func(t *testing.T) {
			binding := expected
			change(&binding)
			// Sealed by Seal itself, so the plaintext copy agrees with the header and only the header check differs.
			sealed := mustSeal(t, binding, &key.PublicKey, map[string][]byte{binding.Credentials[0]: []byte(sealSecret)})
			if _, err := OpenSeal(sealed, expected, key, time.Now()); err == nil {
				t.Fatalf("opened a seal whose %s differs from the request", name)
			}
		})
	}
}

func TestSealPlaintextMustRepeatTheHeader(t *testing.T) {
	key := sealKey(t)
	binding := testBinding(key)
	other := binding
	other.Attempt = 1
	for name, value := range map[string]sealPlaintext{
		"another binding": {Version: 1, Binding: other, Values: values()},
		"another version": {Version: 2, Binding: binding, Values: values()},
	} {
		sealed := compact(t, craft(t, jose.ECDH_ES_A256KW, jose.A256GCM, &key.PublicKey, headers(binding), "", plaintext(t, value)))
		if _, err := OpenSeal(sealed, binding, key, time.Now()); err == nil {
			t.Fatalf("opened a seal whose plaintext has %s", name)
		}
	}
}

func TestSealValuesAreTheCredentialsItNames(t *testing.T) {
	key := sealKey(t)
	binding := testBinding(key)
	for name, values := range map[string]map[string][]byte{
		"none":  {},
		"other": {"other": []byte(sealSecret)},
		"extra": {"model": []byte(sealSecret), "other": []byte(sealSecret)},
	} {
		if _, err := OpenSeal(mustSeal(t, binding, &key.PublicKey, values), binding, key, time.Now()); err == nil {
			t.Fatalf("opened a seal with %s values", name)
		}
	}
}

func TestSealDoesNotOpenUnderAnotherKey(t *testing.T) {
	runA, runB := sealKey(t), sealKey(t)
	binding := testBinding(runA)
	if _, err := OpenSeal(mustSeal(t, binding, &runA.PublicKey, values()), binding, runB, time.Now()); err == nil {
		t.Fatal("opened run A's seal with run B's key")
	}
}

func TestSealAlgorithmsAreFixed(t *testing.T) {
	key := sealKey(t)
	binding := testBinding(key)
	body := plaintext(t, sealPlaintext{Version: 1, Binding: binding, Values: values()})
	symmetric := make([]byte, 32)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for name, sealed := range map[string]*jose.JSONWebEncryption{
		"ECDH-ES":        craft(t, jose.ECDH_ES, jose.A256GCM, &key.PublicKey, headers(binding), "", body),
		"ECDH-ES+A128KW": craft(t, jose.ECDH_ES_A128KW, jose.A256GCM, &key.PublicKey, headers(binding), "", body),
		"A128GCM":        craft(t, jose.ECDH_ES_A256KW, jose.A128GCM, &key.PublicKey, headers(binding), "", body),
		"A256CBC-HS512":  craft(t, jose.ECDH_ES_A256KW, jose.A256CBC_HS512, &key.PublicKey, headers(binding), "", body),
		"dir":            craft(t, jose.DIRECT, jose.A256GCM, symmetric, headers(binding), "", body),
		"RSA-OAEP":       craft(t, jose.RSA_OAEP, jose.A256GCM, &rsaKey.PublicKey, headers(binding), "", body),
	} {
		if _, err := OpenSeal(compact(t, sealed), binding, key, time.Now()); err == nil {
			t.Fatalf("opened a seal under %s", name)
		}
	}
}

func TestSealIsCompactWithOnlyItsHeaders(t *testing.T) {
	key := sealKey(t)
	binding := testBinding(key)
	body := plaintext(t, sealPlaintext{Version: 1, Binding: binding, Values: values()})
	extra := headers(binding)
	extra["kid"] = "receiver-a"
	retyped := headers(binding)
	retyped[jose.HeaderType] = "application/vnd.wherry.seal.v1+jwe"
	for name, sealed := range map[string][]byte{
		"JSON serialisation": []byte(craft(t, jose.ECDH_ES_A256KW, jose.A256GCM, &key.PublicKey, headers(binding), "", body).FullSerialize()),
		"zip":                compact(t, craft(t, jose.ECDH_ES_A256KW, jose.A256GCM, &key.PublicKey, headers(binding), jose.DEFLATE, body)),
		"an unexpected kid":  compact(t, craft(t, jose.ECDH_ES_A256KW, jose.A256GCM, &key.PublicKey, extra, "", body)),
		"another typ":        compact(t, craft(t, jose.ECDH_ES_A256KW, jose.A256GCM, &key.PublicKey, retyped, "", body)),
	} {
		if _, err := OpenSeal(sealed, binding, key, time.Now()); err == nil {
			t.Fatalf("opened a seal with %s", name)
		}
	}
}

func TestExpiredSealDoesNotOpen(t *testing.T) {
	key := sealKey(t)
	binding := testBinding(key)
	binding.ExpiryMS = time.Now().Add(-time.Second).UnixMilli()
	if _, err := OpenSeal(mustSeal(t, binding, &key.PublicKey, values()), binding, key, time.Now()); err == nil {
		t.Fatal("opened an expired seal")
	}
}

func TestOversizedSealDoesNotOpen(t *testing.T) {
	key := sealKey(t)
	binding := testBinding(key)
	big := map[string][]byte{"model": []byte(strings.Repeat("x", 48<<10))}
	sealed := compact(t, craft(t, jose.ECDH_ES_A256KW, jose.A256GCM, &key.PublicKey, headers(binding), "", plaintext(t, sealPlaintext{Version: 1, Binding: binding, Values: big})))
	if len(sealed) <= maxSeal {
		t.Fatalf("fixture is %d bytes, within the limit", len(sealed))
	}
	if _, err := OpenSeal(sealed, binding, key, time.Now()); err == nil {
		t.Fatal("opened an oversized seal")
	}
}

func TestSealRefusesToBuildAnOversizedSeal(t *testing.T) {
	key := sealKey(t)
	secret := strings.Repeat("y", 48<<10)
	_, err := Seal(testBinding(key), &key.PublicKey, map[string][]byte{"model": []byte(secret)})
	if err == nil || !strings.Contains(err.Error(), "[model]") || strings.Contains(err.Error(), secret[:64]) {
		t.Fatalf("oversized seal: %v; want an error naming the credential", err)
	}
}

// A seal honestly encrypted to key A, but bound to key B's ID, does not open with key A for a request naming B.
func TestSealOpensOnlyWithTheExpectedKey(t *testing.T) {
	keyA, keyB := sealKey(t), sealKey(t)
	binding := testBinding(keyB)
	if _, err := OpenSeal(mustSeal(t, binding, &keyA.PublicKey, values()), binding, keyA, time.Now()); err == nil {
		t.Fatal("opened with a key other than the one the request names")
	}
}

func TestSealErrorsDoNotExposeCredential(t *testing.T) {
	key := sealKey(t)
	binding := testBinding(key)
	parts := strings.Split(string(mustSeal(t, binding, &key.PublicKey, values())), ".")
	tag, _ := base64.RawURLEncoding.DecodeString(parts[4])
	tag[0] ^= 1
	parts[4] = base64.RawURLEncoding.EncodeToString(tag)
	_, err := OpenSeal([]byte(strings.Join(parts, ".")), binding, key, time.Now())
	if err == nil || strings.Contains(err.Error(), sealSecret) {
		t.Fatalf("tampered seal: %v", err)
	}
}
