package signed

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/base64"
	json "encoding/json/v2"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
	josecipher "github.com/go-jose/go-jose/v4/cipher"
)

func contentKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	rand.Read(key)
	return key
}

func envelopeValues() EnvelopeValues {
	return EnvelopeValues{
		Credentials:  map[string][]byte{"model": []byte(testSecret), "empty-ish": {0xfb, 0xff}},
		Binding:      EnvelopeBinding{Run: "run-a", Tenant: "tenant-a", Pool: "pool-a", TaskStartedBy: 1_800_000_000, TaskEndedBy: 1_800_003_600},
		S3Inputs:     map[string]string{"repo": "https://bucket.s3.example.com/wherry/tenant-a/run-a/inputs/repo"},
		S3Outputs:    map[string]string{"report": "https://bucket.s3.example.com/wherry/tenant-a/run-a/outputs/report"},
		RedactionKey: testRedactionKey,
	}
}

// buildEnvelope builds an envelope of envelopeValues as the host does, returning it with its content key.
func buildEnvelope(t *testing.T) (envelope, key []byte) {
	t.Helper()
	key = contentKey(t)
	envelope, err := BuildEnvelope(envelopeValues(), key)
	if err != nil {
		t.Fatal(err)
	}
	return envelope, key
}

func protectedHeader(t *testing.T, compact []byte) map[string]any {
	t.Helper()
	protected, _, _ := strings.Cut(string(compact), ".")
	decoded, err := base64.RawURLEncoding.DecodeString(protected)
	if err != nil {
		t.Fatal(err)
	}
	var header map[string]any
	if err := json.Unmarshal(decoded, &header); err != nil {
		t.Fatal(err)
	}
	return header
}

func TestEnvelopeRoundTrip(t *testing.T) {
	pool := p256Key(t)
	envelope, key := buildEnvelope(t)
	wrap, err := WrapContentKey(key, &pool.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	header := protectedHeader(t, envelope)
	if want := []string{"alg", "enc", "typ"}; !slices.Equal(slices.Sorted(maps.Keys(header)), want) ||
		header["alg"] != "dir" || header["enc"] != "A256GCM" || header["typ"] != EnvelopeType {
		t.Fatalf("envelope header %v", header)
	}
	header = protectedHeader(t, wrap)
	epk, _ := header["epk"].(map[string]any)
	if !slices.Equal(slices.Sorted(maps.Keys(header)), []string{"alg", "enc", "epk", "kid"}) || header["alg"] != "ECDH-ES+A256KW" ||
		header["enc"] != "A256GCM" || header["kid"] != KeyThumbprint(&pool.PublicKey) || !slices.Equal(slices.Sorted(maps.Keys(epk)), []string{"crv", "kty", "x", "y"}) {
		t.Fatalf("wrap header %v", header)
	}
	unwrapped, err := UnwrapContentKey(wrap, pool)
	if err != nil || !bytes.Equal(unwrapped, key) {
		t.Fatalf("unwrap: %v", err)
	}
	if err := CheckContentKey(envelope, unwrapped); err != nil {
		t.Fatal(err)
	}
	opened, err := OpenEnvelope(envelope, unwrapped)
	if err != nil || !reflect.DeepEqual(opened, envelopeValues()) {
		t.Fatalf("open: %+v, %v", opened, err)
	}
}

// craftEnvelope encrypts plaintext under key with protected claims the test chooses.
func craftEnvelope(t *testing.T, key []byte, claims map[jose.HeaderKey]any, compression jose.CompressionAlgorithm, plaintext string) []byte {
	t.Helper()
	return compact(t, craft(t, jose.DIRECT, jose.A256GCM, key, claims, compression, []byte(plaintext)))
}

const validPlaintext = `{"binding":{"run":"run-a","tenant":"tenant-a","pool":"pool-a","task_started_by":1800000000,"task_ended_by":1800003600},"credentials":{"model":"c2VjcmV0"},"s3_inputs":{},"s3_outputs":{},"redaction_key":"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="}`

func TestOpenEnvelopeRefusesHeader(t *testing.T) {
	key := contentKey(t)
	claims := func(extra map[jose.HeaderKey]any) map[jose.HeaderKey]any {
		base := map[jose.HeaderKey]any{jose.HeaderType: EnvelopeType}
		maps.Copy(base, extra)
		return base
	}
	if _, err := OpenEnvelope(craftEnvelope(t, key, claims(nil), "", validPlaintext), key); err != nil {
		t.Fatalf("crafted baseline: %v", err)
	}
	withKey := craftEnvelope(t, key, claims(nil), "", validPlaintext)
	protected, rest, _ := strings.Cut(string(withKey), ".")
	for name, envelope := range map[string][]byte{
		"zip":               craftEnvelope(t, key, claims(nil), jose.DEFLATE, validPlaintext),
		"unknown parameter": craftEnvelope(t, key, claims(map[jose.HeaderKey]any{"x": "y"}), "", validPlaintext),
		"kid":               craftEnvelope(t, key, claims(map[jose.HeaderKey]any{"kid": "k"}), "", validPlaintext),
		"missing typ":       craftEnvelope(t, key, map[jose.HeaderKey]any{}, "", validPlaintext),
		"other typ":         craftEnvelope(t, key, claims(map[jose.HeaderKey]any{jose.HeaderType: "application/vnd.wherry.other.v1+jwe"}), "", validPlaintext),
		"encrypted key":     []byte(protected + ".AAAA" + rest),
		"other enc":         compact(t, craft(t, jose.DIRECT, jose.A128GCM, key[:16], claims(nil), "", []byte(validPlaintext))),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := OpenEnvelope(envelope, key); err == nil {
				t.Fatal("opened")
			}
		})
	}
}

func TestOpenEnvelopeRefusesPlaintext(t *testing.T) {
	key := contentKey(t)
	claims := map[jose.HeaderKey]any{jose.HeaderType: EnvelopeType}
	if _, err := OpenEnvelope(craftEnvelope(t, key, claims, "", validPlaintext), key); err != nil {
		t.Fatalf("crafted baseline: %v", err)
	}
	refused := map[string]string{
		"unknown member":        strings.Replace(validPlaintext, `,"credentials"`, `,"later":1,"credentials"`, 1),
		"duplicate member":      strings.Replace(validPlaintext, `,"credentials"`, `,"s3_inputs":{},"credentials"`, 1),
		"missing member":        strings.Replace(validPlaintext, `"s3_inputs":{},`, ``, 1),
		"null member":           strings.Replace(validPlaintext, `"s3_inputs":{}`, `"s3_inputs":null`, 1),
		"wrong type":            strings.Replace(validPlaintext, `"s3_inputs":{}`, `"s3_inputs":{"repo":1}`, 1),
		"unpadded credential":   strings.Replace(validPlaintext, `"c2VjcmV0"`, `"c2VjcmV0dA"`, 1),
		"url alphabet":          strings.Replace(validPlaintext, `"c2VjcmV0"`, `"c2Vj-_V0"`, 1),
		"31-byte redaction_key": strings.Replace(validPlaintext, `MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=`, `MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZQ==`, 1),
		"binding missing":       validPlaintext[:1] + validPlaintext[strings.Index(validPlaintext, `"credentials"`):],
		"binding unknown":       strings.Replace(validPlaintext, `{"run"`, `{"host":"host-a","run"`, 1),
	}
	for _, member := range []string{`"run":"run-a",`, `"tenant":"tenant-a",`, `"pool":"pool-a",`, `"task_started_by":1800000000,`, `,"task_ended_by":1800003600`} {
		refused["binding without "+member] = strings.Replace(validPlaintext, member, "", 1)
	}
	for name, plaintext := range refused {
		t.Run(name, func(t *testing.T) {
			if _, err := OpenEnvelope(craftEnvelope(t, key, claims, "", plaintext), key); err == nil {
				t.Fatal("opened")
			}
		})
	}
}

func TestEnvelopeSizeCap(t *testing.T) {
	key := contentKey(t)
	values := envelopeValues()
	values.Credentials["large"] = make([]byte, MaxEnvelope)
	if _, err := BuildEnvelope(values, key); err == nil {
		t.Fatal("built an envelope over the cap")
	}
	large := strings.Replace(validPlaintext, `"s3_inputs":{}`, `"s3_inputs":{"repo":"https://`+strings.Repeat("a", MaxEnvelope)+`"}`, 1)
	envelope := craftEnvelope(t, key, map[jose.HeaderKey]any{jose.HeaderType: EnvelopeType}, "", large)
	if _, err := OpenEnvelope(envelope, key); err == nil {
		t.Fatal("opened an envelope over the cap")
	}
	if err := CheckContentKey(envelope, key); err == nil {
		t.Fatal("checked a key against an envelope over the cap")
	}
}

// craftWrap builds a compact ECDH-ES+A256KW wrap of plaintext to recipient by hand, so its epk may carry members
// go-jose never writes.
func craftWrap(t *testing.T, recipient *ecdsa.PublicKey, claims map[string]any, epkExtra map[string]any, plaintext []byte) []byte {
	t.Helper()
	ephemeral := p256Key(t)
	epk, err := json.Marshal(jose.JSONWebKey{Key: &ephemeral.PublicKey})
	if err != nil {
		t.Fatal(err)
	}
	var epkMembers map[string]any
	if err := json.Unmarshal(epk, &epkMembers); err != nil {
		t.Fatal(err)
	}
	maps.Copy(epkMembers, epkExtra)
	header := map[string]any{"alg": "ECDH-ES+A256KW", "enc": "A256GCM", "kid": KeyThumbprint(recipient), "epk": epkMembers}
	maps.Copy(header, claims)
	encodedHeader, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	protected := base64.RawURLEncoding.EncodeToString(encodedHeader)
	kek, err := aes.NewCipher(josecipher.DeriveECDHES("ECDH-ES+A256KW", nil, nil, ephemeral, recipient, 32))
	if err != nil {
		t.Fatal(err)
	}
	cek := contentKey(t)
	encryptedKey, err := josecipher.KeyWrap(kek, cek)
	if err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(cek)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	iv := make([]byte, gcm.NonceSize())
	rand.Read(iv)
	sealed := gcm.Seal(nil, iv, plaintext, []byte(protected))
	split := len(sealed) - gcm.Overhead()
	encode := base64.RawURLEncoding.EncodeToString
	return []byte(strings.Join([]string{protected, encode(encryptedKey), encode(iv), encode(sealed[:split]), encode(sealed[split:])}, "."))
}

func TestUnwrapRefuses(t *testing.T) {
	pool, other := p256Key(t), p256Key(t)
	key := contentKey(t)
	if unwrapped, err := UnwrapContentKey(craftWrap(t, &pool.PublicKey, nil, nil, key), pool); err != nil || !bytes.Equal(unwrapped, key) {
		t.Fatalf("crafted baseline: %v", err)
	}
	for name, wrap := range map[string][]byte{
		"extra member":       craftWrap(t, &pool.PublicKey, map[string]any{"typ": "x"}, nil, key),
		"apu":                craftWrap(t, &pool.PublicKey, map[string]any{"apu": "YQ"}, nil, key),
		"epk with d":         craftWrap(t, &pool.PublicKey, nil, map[string]any{"d": base64.RawURLEncoding.EncodeToString(other.D.Bytes())}, key),
		"epk extra member":   craftWrap(t, &pool.PublicKey, nil, map[string]any{"use": "enc"}, key),
		"kid for other key":  craftWrap(t, &pool.PublicKey, map[string]any{"kid": KeyThumbprint(&other.PublicKey)}, nil, key),
		"missing kid":        craftWrap(t, &pool.PublicKey, map[string]any{"kid": nil}, nil, key),
		"31-byte plaintext":  craftWrap(t, &pool.PublicKey, nil, nil, key[:31]),
		"33-byte plaintext":  craftWrap(t, &pool.PublicKey, nil, nil, append(slices.Clone(key), 0)),
		"wrapped to another": mustWrap(t, key, &other.PublicKey),
		"epk on P-384":       craftWrap(t, &pool.PublicKey, nil, map[string]any{"crv": "P-384"}, key),
		"alg dir":            craftWrap(t, &pool.PublicKey, map[string]any{"alg": "dir"}, nil, key),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := UnwrapContentKey(wrap, pool); err == nil {
				t.Fatal("unwrapped")
			}
		})
	}
	if _, err := WrapContentKey(key[:31], &pool.PublicKey); err == nil {
		t.Fatal("wrapped a 31-byte key")
	}
}

func mustWrap(t *testing.T, key []byte, recipient *ecdsa.PublicKey) []byte {
	t.Helper()
	wrap, err := WrapContentKey(key, recipient)
	if err != nil {
		t.Fatal(err)
	}
	return wrap
}

func TestCheckContentKey(t *testing.T) {
	envelope, key := buildEnvelope(t)
	if err := CheckContentKey(envelope, key); err != nil {
		t.Fatal(err)
	}
	if err := CheckContentKey(envelope, contentKey(t)); err == nil {
		t.Fatal("a content key that does not decrypt the envelope passed")
	}
}
