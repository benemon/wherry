package wire

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	json "encoding/json/v2"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	jose "github.com/go-jose/go-jose/v4"
)

func openRecords() map[string]Record {
	return map[string]Record{
		"open request": &OpenRequest{Run: "run-a", Attempt: 2, Receiver: "receiver-a", TaskHash: testHash, Key: NewAttemptKey(&testPoolKey.PublicKey)},
		"attempt wrap": &AttemptWrap{Custodian: "receiver-a", TaskHash: testHash, Request: testHash, Receiver: "receiver-b", Attempt: 2, JWE: "a.b.c.d.e"},
		"re-key wrap":  &RekeyWrap{Holder: "receiver-a", TaskHash: testHash, RecipientKID: signed.KeyThumbprint(&testPoolKey.PublicKey), JWE: "a.b.c.d.e"},
		"open refusal": &OpenRefusal{Custodian: "receiver-a", Request: testHash, Reason: RefusedKeyUnavailable},
		"claim decision": &ClaimDecision{Custodian: "receiver-a", Claim: testHash, Previous: "receiver-b", Reason: ClaimLease,
			Advertisement: &AdvertisedRef{Offset: 0, AtMS: 1}, Served: true},
	}
}

// signPayload signs payload as a record of value's type and owner, as a writer that builds its own JSON would.
func signPayload(t *testing.T, value Record, payload []byte, key ed25519.PrivateKey) []byte {
	t.Helper()
	options := new(jose.SignerOptions).WithType(jose.ContentType(value.jwsType())).WithHeader("kid", value.owner()+"/"+signed.KeyID(key.Public().(ed25519.PublicKey)))
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, options)
	if err != nil {
		t.Fatal(err)
	}
	jws, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := jws.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return []byte(encoded)
}

// The open records decode strictly: an unknown member, a duplicate name or a missing version refuses the record.
func TestOpenRecordsDecodeStrictly(t *testing.T) {
	keys, _, receiver := testKeys(t)
	for name, record := range openRecords() {
		encoded, err := SignRecord(record, receiver)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		decoded := openRecords()[name]
		if err := ParseStrict(encoded, keys, time.Now(), decoded); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		payload, _ := json.Marshal(record, json.Deterministic(true))
		if reencoded, _ := json.Marshal(decoded, json.Deterministic(true)); string(reencoded) != string(payload) {
			t.Errorf("%s: decoded %s, want %s", name, reencoded, payload)
		}
		for edit, payload := range map[string][]byte{
			"unknown member": append([]byte(`{"later_member":1,`), payload[1:]...),
			"duplicate name": append([]byte(`{"v":1,`), payload[1:]...),
			"no v":           withoutMember(t, payload, `"v":1`),
		} {
			if err := ParseStrict(signPayload(t, record, payload, receiver), keys, time.Now(), openRecords()[name]); err == nil {
				t.Errorf("%s, %s: decoded", name, edit)
			}
		}
	}
}

// An attempt key is a P-256 public JWK of kty, crv, x and y alone.
func TestOpenRequestKeyIsAPublicP256JWK(t *testing.T) {
	keys, _, receiver := testKeys(t)
	request := openRecords()["open request"].(*OpenRequest)
	payload, _ := json.Marshal(request, json.Deterministic(true))
	public, err := request.Key.Public()
	if err != nil || !public.Equal(&testPoolKey.PublicKey) {
		t.Fatalf("key %v, %v", public, err)
	}
	p384, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	jwk := string(must(json.Marshal(request.Key)))
	for name, replaced := range map[string]string{
		"private member d": strings.Replace(jwk, `"kty"`, `"d":"AAAA","kty"`, 1),
		"kid member":       strings.Replace(jwk, `"kty"`, `"kid":"k","kty"`, 1),
		"P-384 key":        string(must(json.Marshal(NewAttemptKey(&p384.PublicKey)))),
	} {
		edited := strings.Replace(string(payload), jwk, replaced, 1)
		if err := ParseStrict(signPayload(t, request, []byte(edited), receiver), keys, time.Now(), &OpenRequest{}); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}

func TestOpenRecordsValidate(t *testing.T) {
	for name, edit := range map[string]func(map[string]Record) Record{
		"request attempt 0": func(r map[string]Record) Record { v := r["open request"].(*OpenRequest); v.Attempt = 0; return v },
		"request bad hash":  func(r map[string]Record) Record { v := r["open request"].(*OpenRequest); v.TaskHash = "x"; return v },
		"wrap no jwe":       func(r map[string]Record) Record { v := r["attempt wrap"].(*AttemptWrap); v.JWE = ""; return v },
		"wrap bad request":  func(r map[string]Record) Record { v := r["attempt wrap"].(*AttemptWrap); v.Request = "x"; return v },
		"re-key bad kid":    func(r map[string]Record) Record { v := r["re-key wrap"].(*RekeyWrap); v.RecipientKID = "kid"; return v },
		"refusal reason":    func(r map[string]Record) Record { v := r["open refusal"].(*OpenRefusal); v.Reason = "busy"; return v },
		"decision live served": func(r map[string]Record) Record {
			v := r["claim decision"].(*ClaimDecision)
			v.Reason = ClaimLive
			return v
		},
		"decision lease refused": func(r map[string]Record) Record {
			v := r["claim decision"].(*ClaimDecision)
			v.Served = false
			return v
		},
	} {
		if err := edit(openRecords()).validate(); err == nil {
			t.Errorf("%s: validated", name)
		}
	}
}

func must(encoded []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return encoded
}
