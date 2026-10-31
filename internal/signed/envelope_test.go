package signed

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
	josecipher "github.com/go-jose/go-jose/v4/cipher"
)

// cores are task payloads without envelope and wrap, with envelope and wrap members outside the top-level wherry
// object and string values holding "envelope":.
var cores = map[string]string{
	"compact":        `{"message":{"metadata":{"wherry":{"run":"m","envelope":"m","wrap":"m"}},"parts":[{"text":"x\",\"envelope\":\"y"}]},"wherry":{"run":"run-a","tenant":"tenant-a"}}`,
	"multiline":      "{\n  \"message\": {\"metadata\": {\"wherry\": {\"envelope\": \"m\", \"wrap\": \"m\"}}},\n  \"wherry\": {\n    \"run\": \"run-a\" ,\n    \"tenant\" : \"tenant-a\"\n  }\n}\n",
	"unknown member": `{"wherry":{"run":"run-a","later":{"envelope":[1,2.50,"e"]},"tenant":"tenant-a"},"message":{"parts":[{"text":"\"envelope\":"}]},"later":true}`,
	"tabs":           "{\"wherry\":{\"run\":\"run-a\"\t}\t,\"message\":{}}",
}

func contentKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	rand.Read(key)
	return key
}

func envelopeValues() EnvelopeValues {
	return EnvelopeValues{
		Credentials:  map[string][]byte{"model": []byte(sealSecret), "empty-ish": {0xfb, 0xff}},
		S3Inputs:     map[string]string{"repo": "https://bucket.s3.example.com/wherry/tenant-a/run-a/inputs/repo"},
		S3Outputs:    map[string]string{"report": "https://bucket.s3.example.com/wherry/tenant-a/run-a/outputs/report"},
		RedactionKey: sealRedactionKey,
	}
}

func coreHash(core []byte) string {
	digest := sha256.Sum256(core)
	return hex.EncodeToString(digest[:])
}

// task builds a task payload from core as the host does, returning it with its envelope and content key.
func task(t *testing.T, core string) (payload, envelope, key []byte) {
	t.Helper()
	key = contentKey(t)
	envelope, err := BuildEnvelope(envelopeValues(), key, coreHash([]byte(core)))
	if err != nil {
		t.Fatal(err)
	}
	if payload, err = InsertEnvelope([]byte(core), envelope, []byte("wrap.jwe")); err != nil {
		t.Fatal(err)
	}
	return payload, envelope, key
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
	pool := sealKey(t)
	payload, envelope, key := task(t, cores["compact"])
	wrap, err := WrapContentKey(key, &pool.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	header := protectedHeader(t, envelope)
	if want := []string{"alg", "enc", "task_core", "typ"}; !slices.Equal(slices.Sorted(maps.Keys(header)), want) ||
		header["alg"] != "dir" || header["enc"] != "A256GCM" || header["typ"] != EnvelopeType {
		t.Fatalf("envelope header %v", header)
	}
	header = protectedHeader(t, wrap)
	epk, _ := header["epk"].(map[string]any)
	if !slices.Equal(slices.Sorted(maps.Keys(header)), []string{"alg", "enc", "epk", "kid"}) || header["alg"] != "ECDH-ES+A256KW" ||
		header["enc"] != "A256GCM" || header["kid"] != SealKeyID(&pool.PublicKey) || !slices.Equal(slices.Sorted(maps.Keys(epk)), []string{"crv", "kty", "x", "y"}) {
		t.Fatalf("wrap header %v", header)
	}
	unwrapped, err := UnwrapContentKey(wrap, pool)
	if err != nil || !bytes.Equal(unwrapped, key) {
		t.Fatalf("unwrap: %v", err)
	}
	if err := CheckContentKey(envelope, unwrapped); err != nil {
		t.Fatal(err)
	}
	taskCore, err := TaskCore(payload)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenEnvelope(envelope, unwrapped, taskCore)
	if err != nil || !reflect.DeepEqual(opened, envelopeValues()) {
		t.Fatalf("open: %+v, %v", opened, err)
	}
}

func TestEnvelopeSpanInsertRemove(t *testing.T) {
	for name, core := range cores {
		t.Run(name, func(t *testing.T) {
			payload, envelope, key := task(t, core)
			removed, err := removeSpan(payload)
			if err != nil || string(removed) != core {
				t.Fatalf("removed %q, %v", removed, err)
			}
			taskCore, err := TaskCore(payload)
			if err != nil || taskCore != coreHash([]byte(core)) {
				t.Fatalf("task_core %q, %v", taskCore, err)
			}
			if _, err := OpenEnvelope(envelope, key, taskCore); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTaskCoreRefusals(t *testing.T) {
	for name, payload := range map[string]string{
		"no wherry":            `{"message":{"wherry":{"run":"a","envelope":"e","wrap":"w"}}}`,
		"wherry not an object": `{"wherry":"run"}`,
		"envelope missing":     `{"wherry":{"run":"a","wrap":"w"}}`,
		"wrap missing":         `{"wherry":{"run":"a","envelope":"e"}}`,
		"envelope duplicated":  `{"wherry":{"run":"a","envelope":"e","envelope":"e","wrap":"w"}}`,
		"wrap duplicated":      `{"wherry":{"run":"a","envelope":"e","wrap":"w","wrap":"w"}}`,
		"reordered":            `{"wherry":{"run":"a","wrap":"w","envelope":"e"}}`,
		"member after wrap":    `{"wherry":{"run":"a","envelope":"e","wrap":"w","tenant":"t"}}`,
		"member between":       `{"wherry":{"run":"a","envelope":"e","tenant":"t","wrap":"w"}}`,
		"envelope first":       `{"wherry":{"envelope":"e","wrap":"w"}}`,
		"wherry duplicated":    `{"wherry":{"run":"a","envelope":"e","wrap":"w"},"wherry":{"run":"a","envelope":"e","wrap":"w"}}`,
		"duplicate elsewhere":  `{"message":{"a":1,"a":2},"wherry":{"run":"a","envelope":"e","wrap":"w"}}`,
		"truncated":            `{"wherry":{"run":"a","envelope":"e","wrap":"w"}`,
		"invalid UTF-8":        "{\"message\":\"\xff\",\"wherry\":{\"run\":\"a\",\"envelope\":\"e\",\"wrap\":\"w\"}}",
		"trailing data":        `{"wherry":{"run":"a","envelope":"e","wrap":"w"}} {}`,
		"trailing garbage":     `{"wherry":{"run":"a","envelope":"e","wrap":"w"}}x`,
		"top-level array":      `[{"wherry":{"run":"a","envelope":"e","wrap":"w"}}]`,
	} {
		t.Run(name, func(t *testing.T) {
			if taskCore, err := TaskCore([]byte(payload)); err == nil {
				t.Fatalf("task_core %s of a payload breaking the span rule", taskCore)
			}
		})
	}
	for name, core := range map[string]string{
		"has envelope": `{"wherry":{"run":"a","envelope":"e","wrap":"w"}}`,
		"empty wherry": `{"wherry":{}}`,
		"no wherry":    `{"message":{}}`,
	} {
		t.Run("insert "+name, func(t *testing.T) {
			if _, err := InsertEnvelope([]byte(core), []byte("e"), []byte("w")); err == nil {
				t.Fatal("inserted")
			}
		})
	}
}

func TestEnvelopeBindsEveryByteOutsideSpan(t *testing.T) {
	core := cores["compact"]
	payload, envelope, key := task(t, core)
	comma, closing, err := envelopeSpan(payload)
	if err != nil {
		t.Fatal(err)
	}
	for i := range payload {
		if i >= comma && i < closing {
			continue
		}
		changed := slices.Clone(payload)
		changed[i] ^= 0x01
		taskCore, err := TaskCore(changed)
		if err != nil {
			continue
		}
		if _, err := OpenEnvelope(envelope, key, taskCore); err == nil {
			t.Fatalf("envelope opens for the payload with byte %d changed: %q", i, changed)
		}
	}
}

// craftEnvelope encrypts plaintext under key with protected claims the test chooses.
func craftEnvelope(t *testing.T, key []byte, claims map[jose.HeaderKey]any, compression jose.CompressionAlgorithm, plaintext string) []byte {
	t.Helper()
	return compact(t, craft(t, jose.DIRECT, jose.A256GCM, key, claims, compression, []byte(plaintext)))
}

const validPlaintext = `{"credentials":{"model":"c2VjcmV0"},"s3_inputs":{},"s3_outputs":{},"redaction_key":"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY"}`

func TestOpenEnvelopeRefusesHeader(t *testing.T) {
	key, taskCore := contentKey(t), coreHash([]byte("core"))
	claims := func(extra map[jose.HeaderKey]any) map[jose.HeaderKey]any {
		base := map[jose.HeaderKey]any{jose.HeaderType: EnvelopeType, "task_core": taskCore}
		maps.Copy(base, extra)
		return base
	}
	if _, err := OpenEnvelope(craftEnvelope(t, key, claims(nil), "", validPlaintext), key, taskCore); err != nil {
		t.Fatalf("crafted baseline: %v", err)
	}
	withKey := craftEnvelope(t, key, claims(nil), "", validPlaintext)
	protected, rest, _ := strings.Cut(string(withKey), ".")
	for name, envelope := range map[string][]byte{
		"zip":               craftEnvelope(t, key, claims(nil), jose.DEFLATE, validPlaintext),
		"unknown parameter": craftEnvelope(t, key, claims(map[jose.HeaderKey]any{"x": "y"}), "", validPlaintext),
		"kid":               craftEnvelope(t, key, claims(map[jose.HeaderKey]any{"kid": "k"}), "", validPlaintext),
		"missing typ":       craftEnvelope(t, key, map[jose.HeaderKey]any{"task_core": taskCore}, "", validPlaintext),
		"other typ":         craftEnvelope(t, key, claims(map[jose.HeaderKey]any{jose.HeaderType: SealType}), "", validPlaintext),
		"other task_core":   craftEnvelope(t, key, claims(map[jose.HeaderKey]any{"task_core": coreHash([]byte("other"))}), "", validPlaintext),
		"missing task_core": craftEnvelope(t, key, map[jose.HeaderKey]any{jose.HeaderType: EnvelopeType}, "", validPlaintext),
		"encrypted key":     []byte(protected + ".AAAA" + rest),
		"other enc":         compact(t, craft(t, jose.DIRECT, jose.A128GCM, key[:16], claims(nil), "", []byte(validPlaintext))),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := OpenEnvelope(envelope, key, taskCore); err == nil {
				t.Fatal("opened")
			}
		})
	}
}

func TestOpenEnvelopeRefusesPlaintext(t *testing.T) {
	key, taskCore := contentKey(t), coreHash([]byte("core"))
	claims := map[jose.HeaderKey]any{jose.HeaderType: EnvelopeType, "task_core": taskCore}
	for name, plaintext := range map[string]string{
		"unknown member":        strings.Replace(validPlaintext, `{"credentials"`, `{"later":1,"credentials"`, 1),
		"duplicate member":      strings.Replace(validPlaintext, `{"credentials"`, `{"s3_inputs":{},"credentials"`, 1),
		"missing member":        strings.Replace(validPlaintext, `"s3_inputs":{},`, ``, 1),
		"null member":           strings.Replace(validPlaintext, `"s3_inputs":{}`, `"s3_inputs":null`, 1),
		"wrong type":            strings.Replace(validPlaintext, `"s3_inputs":{}`, `"s3_inputs":{"repo":1}`, 1),
		"padded credential":     strings.Replace(validPlaintext, `"c2VjcmV0"`, `"c2VjcmV0dA=="`, 1),
		"31-byte redaction_key": strings.Replace(validPlaintext, `MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY`, `MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZQ`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := OpenEnvelope(craftEnvelope(t, key, claims, "", plaintext), key, taskCore); err == nil {
				t.Fatal("opened")
			}
		})
	}
}

func TestEnvelopeSizeCap(t *testing.T) {
	key, taskCore := contentKey(t), coreHash([]byte("core"))
	values := envelopeValues()
	values.Credentials["large"] = make([]byte, MaxEnvelope)
	if _, err := BuildEnvelope(values, key, taskCore); err == nil {
		t.Fatal("built an envelope over the cap")
	}
	large := strings.Replace(validPlaintext, `"s3_inputs":{}`, `"s3_inputs":{"repo":"https://`+strings.Repeat("a", MaxEnvelope)+`"}`, 1)
	envelope := craftEnvelope(t, key, map[jose.HeaderKey]any{jose.HeaderType: EnvelopeType, "task_core": taskCore}, "", large)
	if _, err := OpenEnvelope(envelope, key, taskCore); err == nil {
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
	ephemeral := sealKey(t)
	epk, err := json.Marshal(jose.JSONWebKey{Key: &ephemeral.PublicKey})
	if err != nil {
		t.Fatal(err)
	}
	var epkMembers map[string]any
	if err := json.Unmarshal(epk, &epkMembers); err != nil {
		t.Fatal(err)
	}
	maps.Copy(epkMembers, epkExtra)
	header := map[string]any{"alg": "ECDH-ES+A256KW", "enc": "A256GCM", "kid": SealKeyID(recipient), "epk": epkMembers}
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
	pool, other := sealKey(t), sealKey(t)
	key := contentKey(t)
	if unwrapped, err := UnwrapContentKey(craftWrap(t, &pool.PublicKey, nil, nil, key), pool); err != nil || !bytes.Equal(unwrapped, key) {
		t.Fatalf("crafted baseline: %v", err)
	}
	for name, wrap := range map[string][]byte{
		"extra member":       craftWrap(t, &pool.PublicKey, map[string]any{"typ": "x"}, nil, key),
		"apu":                craftWrap(t, &pool.PublicKey, map[string]any{"apu": "YQ"}, nil, key),
		"epk with d":         craftWrap(t, &pool.PublicKey, nil, map[string]any{"d": base64.RawURLEncoding.EncodeToString(other.D.Bytes())}, key),
		"epk extra member":   craftWrap(t, &pool.PublicKey, nil, map[string]any{"use": "enc"}, key),
		"kid for other key":  craftWrap(t, &pool.PublicKey, map[string]any{"kid": SealKeyID(&other.PublicKey)}, nil, key),
		"missing kid":        craftWrap(t, &pool.PublicKey, map[string]any{"kid": nil}, nil, key),
		"31-byte plaintext":  craftWrap(t, &pool.PublicKey, nil, nil, key[:31]),
		"33-byte plaintext":  craftWrap(t, &pool.PublicKey, nil, nil, append(slices.Clone(key), 0)),
		"wrapped to another": mustWrap(t, key, &other.PublicKey),
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
	_, envelope, key := task(t, cores["compact"])
	if err := CheckContentKey(envelope, key); err != nil {
		t.Fatal(err)
	}
	if err := CheckContentKey(envelope, contentKey(t)); err == nil {
		t.Fatal("a content key that does not decrypt the envelope passed")
	}
}
