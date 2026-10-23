package signed

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

func TestKeysOnlyFollowRegistrationRules(t *testing.T) {
	_, first, _ := ed25519.GenerateKey(rand.Reader)
	_, second, _ := ed25519.GenerateKey(rand.Reader)
	owner := HostOwner("tenant-a", "host-a")
	registration, err := RegisterKey(owner, first)
	if err != nil {
		t.Fatal(err)
	}
	state, err := ApplyKeyRecord(KeyState{}, owner, registration, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(KeyState{}, HostOwner("tenant-b", "host-a"), registration, time.Time{}); err == nil {
		t.Fatal("accepted a registration for another owner's topic")
	}
	bareSecond, err := RegisterKey(owner, second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(state, owner, bareSecond, time.Time{}); err == nil {
		t.Fatal("accepted a second bare registration")
	}
	singleRotation, err := signKeys(KeyRecord{Version: 1, Owner: owner, Action: "rotate", Key: publicJWK(second), PreviousKeyID: KeyID(first.Public().(ed25519.PublicKey))}, second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(state, owner, singleRotation, time.Time{}); err == nil {
		t.Fatal("accepted a rotation signed only by the new key")
	}
	_, third, _ := ed25519.GenerateKey(rand.Reader)
	misbound, err := signKeys(KeyRecord{Version: 1, Owner: owner, Action: "rotate", Key: publicJWK(second), PreviousKeyID: KeyID(third.Public().(ed25519.PublicKey))}, first, second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(state, owner, misbound, time.Time{}); err == nil {
		t.Fatal("accepted a rotation naming another previous key")
	}
	rotation, err := RotateKey(owner, first, second)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := ApplyKeyRecord(state, owner, rotation, time.Time{})
	if err != nil || !rotated.Current.Equal(second.Public()) {
		t.Fatalf("dual-signed rotation: %v", err)
	}
	reregistration, err := RegisterKey(owner, second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(rotated, owner, reregistration, time.Time{}); err == nil {
		t.Fatal("accepted a registration signed by the current key, which would drop the retired keys")
	}
	// No action closes a topic; an admin revocation of the owner does.
	revocation, err := signKeys(KeyRecord{Version: 1, Owner: owner, Action: "revoke", Key: publicJWK(second)}, second)
	if err != nil {
		t.Fatal(err)
	}
	if after, err := ApplyKeyRecord(rotated, owner, revocation, time.Time{}); err == nil || err.Error() != "unknown key action" || !after.Current.Equal(second.Public()) {
		t.Fatalf("revoke key record: %v; want it refused as an unknown key action", err)
	}
}

type testRecord struct {
	V      int    `json:"v"`
	Tenant string `json:"tenant"`
	Host   string `json:"host"`
	Run    string `json:"run"`
}

const testType = "application/vnd.wherry.test.v1+jws"

func TestRecordTypeIsExact(t *testing.T) {
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

func TestSignatureMustVerifyAgainstNamedKey(t *testing.T) {
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

func TestPayloadDecodeIsStrict(t *testing.T) {
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

func TestRetiredKeysAdmitNothing(t *testing.T) {
	_, first, _ := ed25519.GenerateKey(rand.Reader)
	_, second, _ := ed25519.GenerateKey(rand.Reader)
	owner := HostOwner("tenant-a", "host-a")
	state, err := ApplyKeyRecord(KeyState{}, owner, must(RegisterKey(owner, first)), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	old, err := Sign(testType, owner, testRecord{V: 1, Tenant: "tenant-a", Host: "host-a", Run: "run-a"}, first)
	if err != nil {
		t.Fatal(err)
	}
	if state, err = ApplyKeyRecord(state, owner, must(RotateKey(owner, first, second)), time.Time{}); err != nil {
		t.Fatal(err)
	}
	var decoded testRecord
	if _, err := Verify(old, testType, Keys{owner: state}, &decoded); err == nil {
		t.Fatal("admitted a record signed with a retired key")
	}
}

func TestRetiredKeyVerifiesOnlyHistoryBeforeItsRotation(t *testing.T) {
	_, first, _ := ed25519.GenerateKey(rand.Reader)
	_, second, _ := ed25519.GenerateKey(rand.Reader)
	owner := HostOwner("tenant-a", "host-a")
	state, err := ApplyKeyRecord(KeyState{}, owner, must(RegisterKey(owner, first)), time.UnixMilli(1000))
	if err != nil {
		t.Fatal(err)
	}
	record := testRecord{V: 1, Tenant: "tenant-a", Host: "host-a", Run: "run-a"}
	old := must(Sign(testType, owner, record, first))
	rotatedAt := time.UnixMilli(5000)
	if state, err = ApplyKeyRecord(state, owner, must(RotateKey(owner, first, second)), rotatedAt); err != nil {
		t.Fatal(err)
	}
	keys := Keys{owner: state}
	var decoded testRecord
	if _, err := VerifyHistory(old, testType, keys, rotatedAt.Add(-time.Millisecond), &decoded); err != nil || decoded != record {
		t.Fatalf("a record appended before the rotation under the retired key: %v", err)
	}
	for _, appended := range []time.Time{rotatedAt, rotatedAt.Add(time.Hour)} {
		if _, err := VerifyHistory(old, testType, keys, appended, &decoded); err == nil {
			t.Fatalf("verified a record appended at %v under a key retired at %v", appended, rotatedAt)
		}
	}
	if _, err := Verify(old, testType, keys, &decoded); err == nil {
		t.Fatal("Verify accepted the retired key")
	}
	current := must(Sign(testType, owner, record, second))
	if _, err := Verify(current, testType, keys, &decoded); err != nil {
		t.Fatalf("Verify of the current key: %v", err)
	}
	for _, appended := range []time.Time{rotatedAt.Add(-time.Millisecond), rotatedAt.Add(time.Hour)} {
		if _, err := VerifyHistory(current, testType, keys, appended, &decoded); err != nil {
			t.Fatalf("VerifyHistory of the current key at %v: %v", appended, err)
		}
	}
}

func must(value []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return value
}
