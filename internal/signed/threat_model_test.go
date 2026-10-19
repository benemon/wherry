package signed

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func TestErrorsDoNotExposeCredential(t *testing.T) {
	key, _ := ecdh.X25519().GenerateKey(rand.Reader)
	secret := "plaintext-credential-canary"
	seal, err := SealCredential("tenant-a", "receiver-a", "pool-a", "model", time.Now().Add(time.Hour), key.PublicKey(), []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	seal.Ciphertext[len(seal.Ciphertext)-1] ^= 1
	_, err = OpenCredential(seal, "tenant-a", "receiver-a", "pool-a", "model", time.Now(), []string{"model"}, key)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe credential error: %v", err)
	}
}

func TestP04KeysOnlyFollowRegistrationRules(t *testing.T) {
	_, first, _ := ed25519.GenerateKey(rand.Reader)
	_, second, _ := ed25519.GenerateKey(rand.Reader)
	owner := HostOwner("tenant-a", "host-a")
	registration, err := RegisterKey(owner, first, nil)
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
	bareSecond, err := RegisterKey(owner, second, nil)
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
	rotation, err := RotateKey(owner, first, second, nil)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := ApplyKeyRecord(state, owner, rotation)
	if err != nil || !rotated.Current.Equal(second.Public()) {
		t.Fatalf("dual-signed rotation: %v", err)
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

func TestP10SealBindingsAndFingerprintPinning(t *testing.T) {
	first, _ := ecdh.X25519().GenerateKey(rand.Reader)
	other, _ := ecdh.X25519().GenerateKey(rand.Reader)
	expiry := time.Now().Add(time.Hour)
	seal, err := SealCredential("tenant-a", "receiver-a", "pool-a", "model", expiry, first.PublicKey(), []byte("canary-secret"))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenCredential(seal, "tenant-a", "receiver-a", "pool-a", "model", time.Now(), []string{"model"}, first)
	if err != nil || string(opened) != "canary-secret" {
		t.Fatalf("open: %q %v", opened, err)
	}
	if _, err := OpenCredential(seal, "tenant-a", "receiver-b", "pool-a", "model", time.Now(), []string{"model"}, first); err == nil {
		t.Fatal("opened seal for another receiver")
	}
	if _, err := OpenCredential(seal, "tenant-a", "receiver-a", "pool-b", "model", time.Now(), []string{"model"}, first); err == nil {
		t.Fatal("opened seal for another pool")
	}
	if _, err := OpenCredential(seal, "tenant-a", "receiver-a", "pool-a", "other", time.Now(), []string{"other"}, first); err == nil {
		t.Fatal("opened a seal under another credential name")
	}
	if _, err := OpenCredential(seal, "tenant-a", "receiver-a", "pool-a", "model", time.Now(), []string{"other"}, first); err == nil {
		t.Fatal("opened a credential the terms do not allow for the agent")
	}
	if _, err := OpenCredential(seal, "tenant-b", "receiver-a", "pool-a", "model", time.Now(), []string{"model"}, first); err == nil {
		t.Fatal("opened a seal naming another tenant than its host topic")
	}
	if _, err := OpenCredential(seal, "tenant-a", "receiver-a", "pool-a", "model", time.Now(), []string{"model"}, other); err == nil {
		t.Fatal("opened a seal with another receiver key")
	}
	if _, err := OpenCredential(seal, "tenant-a", "receiver-a", "pool-a", "model", expiry, []string{"model"}, first); err == nil {
		t.Fatal("opened expired seal with leeway")
	}
	pins := map[string]string{}
	if err := CheckPinnedKey(pins, "receiver-a", first.PublicKey()); err != nil {
		t.Fatal(err)
	}
	if err := CheckPinnedKey(pins, "receiver-a", other.PublicKey()); err == nil {
		t.Fatal("host accepted a changed receiver key")
	}
}

func TestP14StatusIsAtomicallyDualWritten(t *testing.T) {
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"))
	defer cluster.Close()
	for _, topic := range []string{"wherry.pool.pool-a.events", "wherry.tenant.tenant-a"} {
		if err := cluster.CreateTopic(topic, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	opts := []kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism())}
	producer, err := kgo.NewClient(append(opts, kgo.TransactionalID("p.pool-a.run-a"))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	value := []byte("same-signed-status")
	if err := PublishStatus(context.Background(), producer, "pool-a", "tenant-a", "event-a", value, nil); err != nil {
		t.Fatal(err)
	}
	reader, err := kgo.NewClient(append(opts, kgo.ConsumeTopics("wherry.pool.pool-a.events", "wherry.tenant.tenant-a"), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var records []*kgo.Record
	var fetchErrs []error
	for len(records) < 2 && ctx.Err() == nil {
		fetches := reader.PollRecords(ctx, 2-len(records))
		for _, fetchErr := range fetches.Errors() {
			fetchErrs = append(fetchErrs, fetchErr.Err)
		}
		records = append(records, fetches.Records()...)
	}
	if len(records) != 2 {
		t.Fatalf("read_committed saw %d copies, want 2: %v", len(records), fetchErrs)
	}
	if string(records[0].Value) != string(records[1].Value) {
		t.Fatal("dual-write payloads differ")
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

func TestKeyRecordCarriesTheSealingKey(t *testing.T) {
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	sealing, _ := ecdh.X25519().GenerateKey(rand.Reader)
	registration, err := RegisterKey("receiver-a", signing, sealing.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	state, err := ApplyKeyRecord(KeyState{}, "receiver-a", registration)
	if err != nil || state.Sealing == nil || !state.Sealing.Equal(sealing.PublicKey()) {
		t.Fatalf("sealing key not registered: %v", err)
	}
	bad, err := signKeys(KeyRecord{Version: 1, Owner: "receiver-a", Action: "register", Key: publicJWK(signing), SealingKey: "AAAA"}, signing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyKeyRecord(KeyState{}, "receiver-a", bad); err == nil {
		t.Fatal("accepted a malformed sealing key")
	}
}
