package client

import (
	"context"
	"testing"

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
)

// A storage key is registered for exact replacement whatever was registered for the same name and value first. A host
// that resolved a credential for a run holds it under hold's short-value rule, which leaves a seven-byte value; when
// its configuration then maps a pool's storage keys to the same credentials, opening the bucket registers both values
// exactly, so Redact replaces the access key and the secret key, in that order, however short.
func TestStorageKeysAreRedactedWhateverWasHeldFirst(t *testing.T) {
	const accessName, secretName = "order-access", "order-secret"
	const access, secret = "z6q4k8v", "p3m7t2x"
	hold(accessName, []byte(access))
	hold(secretName, []byte(secret))
	for _, value := range []string{access, secret} {
		if got := Redact("key " + value); got != "key "+value {
			t.Fatalf("hold replaced the short value %q: %q; want it left as for any credential a run receives", value, got)
		}
	}
	t.Setenv("WHERRY_TEST_ORDER_ACCESS", access)
	t.Setenv("WHERRY_TEST_ORDER_SECRET", secret)
	h := &Host{storage: map[string]StorageConfig{"pool-s3": {AccessKey: accessName, SecretKey: secretName}}, credentials: map[string]credential.Source{
		accessName: {Env: "WHERRY_TEST_ORDER_ACCESS"},
		secretName: {Env: "WHERRY_TEST_ORDER_SECRET"},
	}}
	store, err := h.bucket(context.Background(), "pool-s3", &operator.Storage{Endpoint: "https://s3.example.test", Bucket: testBucket})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []struct{ name, value string }{{accessName, access}, {secretName, secret}} {
		marker := signed.RedactionMarker(nil, key.name)
		if got := store.redact("key " + key.value); got != "key "+marker {
			t.Fatalf("the store redacts %q to %q; want %q", key.value, got, marker)
		}
		if got := Redact("key " + key.value); got != "key "+marker {
			t.Fatalf("Redact after the storage registration of %s = %q; want the earlier short registration upgraded to %q", key.name, got, marker)
		}
	}
}
