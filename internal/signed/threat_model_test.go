package signed

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
)

func TestP04KeysOnlyFollowRegistrationRules(t *testing.T) {
	_, first, _ := ed25519.GenerateKey(rand.Reader)
	_, second, _ := ed25519.GenerateKey(rand.Reader)
	owner := HostOwner("tenant-a", "host-a")
	registration, err := RegisterKey(owner, first)
	if err != nil {
		t.Fatal(err)
	}
	state, err := ApplyKeyRecord(KeyState{}, owner, registration)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(KeyState{}, HostOwner("tenant-b", "host-a"), registration); err == nil {
		t.Fatal("accepted a registration for another owner's topic")
	}
	bareSecond, err := RegisterKey(owner, second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(state, owner, bareSecond); err == nil {
		t.Fatal("accepted a second bare registration")
	}
	singleRotation, err := signKeys(KeyRecord{Version: 1, Owner: owner, Action: "rotate", Key: publicJWK(second), PreviousKeyID: KeyID(first.Public().(ed25519.PublicKey))}, second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(state, owner, singleRotation); err == nil {
		t.Fatal("accepted a rotation signed only by the new key")
	}
	_, third, _ := ed25519.GenerateKey(rand.Reader)
	misbound, err := signKeys(KeyRecord{Version: 1, Owner: owner, Action: "rotate", Key: publicJWK(second), PreviousKeyID: KeyID(third.Public().(ed25519.PublicKey))}, first, second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(state, owner, misbound); err == nil {
		t.Fatal("accepted a rotation naming another previous key")
	}
	rotation, err := RotateKey(owner, first, second)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := ApplyKeyRecord(state, owner, rotation)
	if err != nil || !rotated.Current.Equal(second.Public()) {
		t.Fatalf("dual-signed rotation: %v", err)
	}
	reregistration, err := RegisterKey(owner, second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(rotated, owner, reregistration); err == nil {
		t.Fatal("accepted a registration signed by the current key, which would drop the retired keys")
	}
	revocation, err := RevokeKey(owner, second)
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := ApplyKeyRecord(rotated, owner, revocation)
	if err != nil || revoked.Current != nil {
		t.Fatalf("revocation left a current key: %v", err)
	}
	if _, err := ApplyKeyRecord(revoked, owner, bareSecond); err == nil {
		t.Fatal("accepted a bare registration after revocation")
	}
}

type testRecord struct {
	V      int    `json:"v"`
	Tenant string `json:"tenant"`
	Host   string `json:"host"`
	Run    string `json:"run"`
}

const testType = "application/vnd.wherry.test.v1+jws"

func TestP07RecordTypeIsExact(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	owner := HostOwner("tenant-a", "host-a")
	keys := Keys{owner: {Current: key.Public().(ed25519.PublicKey)}}
	record := testRecord{V: 1, Tenant: "tenant-a", Host: "host-a", Run: "run-a"}
	current, err := Sign(testType, owner, record, key)
	if err != nil {
		t.Fatal(err)
	}
	var decoded testRecord
	if _, err := Verify(current, testType, keys, &decoded); err != nil || decoded != record {
		t.Fatalf("v1 record: %v", err)
	}
	underV2, err := Sign("application/vnd.wherry.test.v2+jws", owner, record, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(underV2, testType, keys, &decoded); err == nil {
		t.Fatal("accepted a record under another typ")
	}
	other, err := jose.ParseSignedCompact(string(current), []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil || len(other.Signatures) != 1 {
		t.Fatalf("fixture is not a pinned EdDSA JWS: %v", err)
	}
}

func TestP01SignatureMustVerifyAgainstNamedKey(t *testing.T) {
	_, victim, _ := ed25519.GenerateKey(rand.Reader)
	_, attacker, _ := ed25519.GenerateKey(rand.Reader)
	owner := HostOwner("tenant-a", "host-a")
	keys := Keys{owner: {Current: victim.Public().(ed25519.PublicKey)}}
	kid := owner + "/" + KeyID(victim.Public().(ed25519.PublicKey))
	var decoded testRecord
	forged, err := signRaw(testType, kid, []byte(`{"v":1,"tenant":"tenant-a","host":"host-a","run":"run-a"}`), attacker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(forged, testType, keys, &decoded); err == nil {
		t.Fatal("accepted a record naming the victim's key but signed by another")
	}
	keys[HostOwner("tenant-b", "host-b")] = KeyState{Current: attacker.Public().(ed25519.PublicKey)}
	borrowed, err := Sign(testType, HostOwner("tenant-b", "host-b"), testRecord{V: 1, Tenant: "tenant-a", Host: "host-a", Run: "run-a"}, attacker)
	if err != nil {
		t.Fatal(err)
	}
	if signer, err := Verify(borrowed, testType, keys, &decoded); err != nil || signer != HostOwner("tenant-b", "host-b") {
		t.Fatalf("Verify must report the kid's owner for the caller to match: %q %v", signer, err)
	}
	malformed, err := signRaw(testType, kid, []byte(`{"v":1,"tenant":`), attacker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(malformed, testType, keys, &decoded); err == nil || err.Error() != "invalid JWS signature" {
		t.Fatalf("decoded a payload before verifying its signature: %v", err)
	}
}

func TestP07PayloadDecodeIsStrict(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	owner := HostOwner("tenant-a", "host-a")
	keys := Keys{owner: {Current: key.Public().(ed25519.PublicKey)}}
	kid := owner + "/" + KeyID(key.Public().(ed25519.PublicKey))
	base := `{"v":1,"tenant":"tenant-a","host":"host-a","run":"run-a"`
	for name, payload := range map[string]string{"unknown member": base + `,"extra":1}`, "duplicate member": base + `,"run":"run-b"}`} {
		encoded, err := signRaw(testType, kid, []byte(payload), key)
		if err != nil {
			t.Fatal(err)
		}
		var decoded testRecord
		if _, err := Verify(encoded, testType, keys, &decoded); err == nil {
			t.Fatalf("accepted a payload with a %s", name)
		}
	}
}

// signRaw signs payload bytes under an arbitrary kid, for forgeries.
func signRaw(typ, kid string, payload []byte, key ed25519.PrivateKey) ([]byte, error) {
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, new(jose.SignerOptions).WithType(jose.ContentType(typ)).WithHeader(jose.HeaderKey("kid"), kid))
	if err != nil {
		return nil, err
	}
	jws, err := signer.Sign(payload)
	if err != nil {
		return nil, err
	}
	compact, err := jws.CompactSerialize()
	return []byte(compact), err
}

func TestKeyRecordWithARetiredSealingKeyStillFolds(t *testing.T) {
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	registration, err := RegisterKey("receiver-a", signing)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(registration), "sealing_key") {
		t.Fatal("a new key record carries a sealing key")
	}
	// The payload receivers registered before seals moved to per-run keys, signed as signKeys signs.
	jwk, err := publicJWK(signing).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	options := &jose.SignerOptions{EmbedJWK: true}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: jose.JSONWebKey{Key: signing, KeyID: KeyID(signing.Public().(ed25519.PublicKey)), Algorithm: string(jose.EdDSA)}},
		options.WithType(jose.ContentType(KeyType)))
	if err != nil {
		t.Fatal(err)
	}
	jws, err := signer.Sign([]byte(`{"v":1,"owner":"receiver-a","action":"register","key":` + string(jwk) + `,"sealing_key":"gTpG8sVq3XzF4qFQ1zH4Bz0mFj1lN6p9cE2rK7tY3wA"}`))
	if err != nil {
		t.Fatal(err)
	}
	old := []byte(jws.FullSerialize())
	if state, err := ApplyKeyRecord(KeyState{}, "receiver-a", old); err != nil || !state.Current.Equal(signing.Public()) {
		t.Fatalf("a registration carrying a sealing key does not fold: %v", err)
	}
}

func TestRetiredKeysVerifyHistoryOnly(t *testing.T) {
	_, first, _ := ed25519.GenerateKey(rand.Reader)
	_, second, _ := ed25519.GenerateKey(rand.Reader)
	owner := HostOwner("tenant-a", "host-a")
	state, err := ApplyKeyRecord(KeyState{}, owner, must(RegisterKey(owner, first)))
	if err != nil {
		t.Fatal(err)
	}
	old, err := Sign(testType, owner, testRecord{V: 1, Tenant: "tenant-a", Host: "host-a", Run: "run-a"}, first)
	if err != nil {
		t.Fatal(err)
	}
	if state, err = ApplyKeyRecord(state, owner, must(RotateKey(owner, first, second))); err != nil {
		t.Fatal(err)
	}
	var decoded testRecord
	if _, err := Verify(old, testType, Keys{owner: state}, &decoded); err == nil {
		t.Fatal("admitted a record signed with a retired key")
	}
	if _, err := VerifyHistory(old, testType, Keys{owner: state}, &decoded); err != nil {
		t.Fatalf("history refused a record signed before the rotation: %v", err)
	}
	if state, err = ApplyKeyRecord(state, owner, must(RevokeKey(owner, second))); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyHistory(old, testType, Keys{owner: state}, &decoded); err == nil {
		t.Fatal("history accepted a retired key after the owner's revocation")
	}
}

func must(value []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return value
}
