package signed

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	json "encoding/json/v2"
	"slices"
	"strings"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

const sealSecret = "seal-canary-credential"

var sealRedactionKey = []byte("0123456789abcdef0123456789abcdef")

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
		Attempt: 3, Credentials: []string{"model"}, ExpiryMS: time.Now().Add(time.Minute).UnixMilli(),
	}
}

func values() map[string][]byte { return map[string][]byte{"model": []byte(sealSecret)} }

func mustSeal(t *testing.T, binding SealBinding, key *ecdsa.PublicKey, values map[string][]byte) []byte {
	t.Helper()
	sealed, err := Seal(binding, key, SealValues{Values: values, RedactionKey: sealRedactionKey})
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

// headers are the protected claims Seal writes for binding.
func headers(binding SealBinding) map[jose.HeaderKey]any {
	return map[jose.HeaderKey]any{
		jose.HeaderType: SealType, "tenant": binding.Tenant, "pool": binding.Pool, "receiver": binding.Receiver, "key_id": binding.KeyID,
		"run": binding.Run, "attempt": binding.Attempt, "credentials": binding.Credentials, "expiry_at_ms": binding.ExpiryMS,
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
	if err != nil || string(opened.Values["model"]) != sealSecret || len(opened.Values) != 1 || !slices.Equal(opened.RedactionKey, sealRedactionKey) {
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
		"another binding": {Version: 1, Binding: other, SealValues: SealValues{Values: values(), RedactionKey: sealRedactionKey}},
		"another version": {Version: 2, Binding: binding, SealValues: SealValues{Values: values(), RedactionKey: sealRedactionKey}},
	} {
		sealed := compact(t, craft(t, jose.ECDH_ES_A256KW, jose.A256GCM, &key.PublicKey, headers(binding), "", plaintext(t, value)))
		if _, err := OpenSeal(sealed, binding, key, time.Now()); err == nil {
			t.Fatalf("opened a seal whose plaintext has %s", name)
		}
	}
}

// A seal carries the run's 32-byte redaction key; one without it neither builds nor opens.
func TestSealCarriesTheRunsRedactionKey(t *testing.T) {
	key := sealKey(t)
	binding := testBinding(key)
	if _, err := Seal(binding, &key.PublicKey, SealValues{Values: values(), RedactionKey: sealRedactionKey[:16]}); err == nil {
		t.Fatal("built a seal with a 16-byte redaction key")
	}
	for name, redactionKey := range map[string][]byte{"no redaction key": nil, "a short redaction key": sealRedactionKey[:31]} {
		body := plaintext(t, sealPlaintext{Version: 1, Binding: binding, SealValues: SealValues{Values: values(), RedactionKey: redactionKey}})
		if _, err := OpenSeal(compact(t, craft(t, jose.ECDH_ES_A256KW, jose.A256GCM, &key.PublicKey, headers(binding), "", body)), binding, key, time.Now()); err == nil {
			t.Fatalf("opened a seal with %s", name)
		}
	}
}

// The marker is the 96-bit HMAC-SHA256 of the domain and the credential's name under the run's key, as base64url.
func TestRedactionMarker(t *testing.T) {
	oracle := func(key []byte, name string) string {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte("wherry-redaction-v1" + name))
		return "[redacted:" + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:12]) + "]"
	}
	other := []byte("fedcba9876543210fedcba9876543210")
	marker := RedactionMarker(sealRedactionKey, "model")
	if marker != oracle(sealRedactionKey, "model") || len(marker) != 27 || strings.Contains(marker, "model") {
		t.Fatalf("marker %q, want the 27-byte HMAC marker %q", marker, oracle(sealRedactionKey, "model"))
	}
	if RedactionMarker(sealRedactionKey, "model") != marker || RedactionMarker(other, "model") == marker || RedactionMarker(sealRedactionKey, "deploy-key") == marker {
		t.Fatal("the marker is not fixed by the run's key and the credential's name alone")
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
	body := plaintext(t, sealPlaintext{Version: 1, Binding: binding, SealValues: SealValues{Values: values(), RedactionKey: sealRedactionKey}})
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
	body := plaintext(t, sealPlaintext{Version: 1, Binding: binding, SealValues: SealValues{Values: values(), RedactionKey: sealRedactionKey}})
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
	sealed := compact(t, craft(t, jose.ECDH_ES_A256KW, jose.A256GCM, &key.PublicKey, headers(binding), "", plaintext(t, sealPlaintext{Version: 1, Binding: binding, SealValues: SealValues{Values: big, RedactionKey: sealRedactionKey}})))
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
	_, err := Seal(testBinding(key), &key.PublicKey, SealValues{Values: map[string][]byte{"model": []byte(secret)}, RedactionKey: sealRedactionKey})
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

// A seal carries a presigned URL per S3 input and output its binding names, bound in its protected header beside the
// credentials, and opens only to exactly those values.
func TestSealCarriesS3URLs(t *testing.T) {
	key := sealKey(t)
	binding := testBinding(key)
	binding.Credentials, binding.S3Inputs, binding.S3Outputs = []string{}, []string{"data.bin"}, []string{"out/report.pdf"}
	urls := map[string][]byte{S3InputValue("data.bin"): []byte("https://store.test/b/in?X-Amz-Signature=1"), S3OutputValue("out/report.pdf"): []byte("https://store.test/b/out?X-Amz-Signature=2")}
	sealed := mustSeal(t, binding, &key.PublicKey, urls)
	opened, err := OpenSeal(sealed, binding, key, time.Now())
	if err != nil || len(opened.Values) != 2 || string(opened.Values[S3InputValue("data.bin")]) != string(urls[S3InputValue("data.bin")]) {
		t.Fatalf("opened %v, %v; want the two URLs", opened, err)
	}
	header, _, _ := strings.Cut(string(sealed), ".")
	decoded, err := base64.RawURLEncoding.DecodeString(header)
	if err != nil || !strings.Contains(string(decoded), `"s3_inputs":["data.bin"]`) || !strings.Contains(string(decoded), `"s3_outputs":["out/report.pdf"]`) {
		t.Fatalf("protected header %s; want the S3 names bound", decoded)
	}
	other := binding
	other.S3Outputs = nil
	if _, err := OpenSeal(sealed, other, key, time.Now()); err == nil {
		t.Fatal("opened a seal bound to other S3 outputs")
	}
	// Seal takes any values, as a broken sealer would write them.
	missing := mustSeal(t, binding, &key.PublicKey, map[string][]byte{S3InputValue("data.bin"): urls[S3InputValue("data.bin")]})
	extra := mustSeal(t, binding, &key.PublicKey, map[string][]byte{S3InputValue("data.bin"): nil, S3OutputValue("out/report.pdf"): nil, "model": nil})
	for name, seal := range map[string][]byte{"a missing URL": missing, "an extra value": extra} {
		if _, err := OpenSeal(seal, binding, key, time.Now()); err == nil {
			t.Errorf("opened a seal with %s", name)
		}
	}
}
