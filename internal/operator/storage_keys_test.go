package operator

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A tenant's storage keys, as the service's Storage gives them, reach every host bundle of the tenant as plain YAML
// strings: host add writes them, --force keeps them under the new password, and refresh rewrites a kept bundle with a
// changed pair and the same password. A receiver bundle never carries them, a tenant without a pair gets none, and a
// lookup that fails refuses host add before anything changes in Kafka and refresh before it rewrites.
func TestHostBundlesCarryTheTenantsStorageKeys(t *testing.T) {
	_, service, ctx := compromiseFixture(t)
	keys := StorageKeys{AccessKey: "AKIATESTACCESSKEY", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
	recorded := map[string]StorageKeys{"tenant-a": keys}
	service.Storage = func(tenant string) (StorageKeys, error) { return recorded[tenant], nil }
	dir := t.TempDir()
	path := filepath.Join(dir, "host-b.bundle")
	if err := service.HostAdd(ctx, "tenant-a", "host-b", path, false); err != nil {
		t.Fatal(err)
	}
	added, err := ReadBundle(path)
	if err != nil || added.Storage != keys {
		t.Fatalf("bundle %+v, %v; want the tenant's storage keys", added, err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(encoded), "access_key: "+keys.AccessKey+"\n") || !strings.Contains(string(encoded), "secret_key: "+keys.SecretKey+"\n") || strings.Contains(string(encoded), "!!binary") {
		t.Fatalf("bundle holds\n%s\n%v; want the keys as plain strings", encoded, err)
	}
	if err := service.HostAdd(ctx, "tenant-a", "host-b", path, true); err != nil {
		t.Fatal(err)
	}
	reissued, err := ReadBundle(path)
	if err != nil || reissued.Storage != keys || reissued.Password == added.Password {
		t.Fatalf("bundle after --force %+v, %v; want the keys under a new password", reissued, err)
	}
	receiverPath := filepath.Join(dir, "receiver-b.bundle")
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-b", "pool-a"), service.Broker, receiverPath); err != nil {
		t.Fatal(err)
	}
	for _, step := range []string{"add", "reissue"} {
		if step == "reissue" {
			if err := service.ReceiverReissue(ctx, "receiver-b", service.Broker, receiverPath); err != nil {
				t.Fatal(err)
			}
		}
		if encoded, err := os.ReadFile(receiverPath); err != nil || strings.Contains(string(encoded), "storage") || strings.Contains(string(encoded), keys.AccessKey) {
			t.Fatalf("receiver bundle after %s holds\n%s\n%v; want no storage keys", step, encoded, err)
		}
	}
	next := StorageKeys{AccessKey: "AKIANEXTACCESSKEY", SecretKey: "next-secret-key-value"}
	recorded["tenant-a"] = next
	if changed, err := service.HostRefresh(ctx, "tenant-a", "host-b", path); err != nil || !changed {
		t.Fatalf("refresh after the pair changed: %t, %v; want the bundle rewritten", changed, err)
	}
	if refreshed, err := ReadBundle(path); err != nil || refreshed != (Bundle{Broker: reissued.Broker, Principal: reissued.Principal, Password: reissued.Password, CA: reissued.CA, Storage: next}) {
		t.Fatalf("refreshed bundle %+v, %v; want the new pair with the password of %+v", refreshed, err, reissued)
	}
	if changed, err := service.HostRefresh(ctx, "tenant-a", "host-b", path); err != nil || changed {
		t.Fatalf("refresh again: %t, %v; want the bundle left as it is", changed, err)
	}
	// A tenant the operator recorded no pair for gets a bundle without one.
	other := filepath.Join(dir, "tenant-b.bundle")
	if err := hostFixture(t, service, ctx, "tenant-b", "host-a", other, false); err != nil {
		t.Fatal(err)
	}
	if encoded, err := os.ReadFile(other); err != nil || strings.Contains(string(encoded), "storage") {
		t.Fatalf("bundle of a tenant without a pair holds\n%s\n%v; want no storage block", encoded, err)
	}
	injected := errors.New("injected")
	service.Storage = func(string) (StorageKeys, error) { return StorageKeys{}, injected }
	before := kafkaChanges(t, service)
	unwritten := filepath.Join(dir, "host-c.bundle")
	if err := service.HostAdd(ctx, "tenant-a", "host-c", unwritten, false); !errors.Is(err, injected) {
		t.Fatalf("host add with a keys lookup that fails = %v; want the failure", err)
	}
	if after := kafkaChanges(t, service); after != before {
		t.Fatalf("records and SCRAM users %v, were %v; want nothing changed", after, before)
	}
	if _, err := os.Stat(unwritten); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused host add left %s: %v", unwritten, err)
	}
	kept, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.HostRefresh(ctx, "tenant-a", "host-b", path); !errors.Is(err, injected) {
		t.Fatalf("refresh with a keys lookup that fails = %v; want the failure", err)
	}
	if again, err := os.ReadFile(path); err != nil || string(again) != string(kept) {
		t.Fatalf("a refused refresh rewrote %s", path)
	}
}

// ReplaceStorageKeys writes a pair atomically: a write, sync, close or rename that fails keeps the pair the file held,
// leaves no temporary file and names neither key. Both it and ReadStorageKeys refuse a partial pair, and ReadStorageKeys
// an unknown field.
func TestReplaceStorageKeysKeepsTheOldPairOnFailure(t *testing.T) {
	injected := errors.New("injected")
	dir := t.TempDir()
	path := filepath.Join(dir, "tenant-a.yaml")
	old := StorageKeys{AccessKey: "AKIAOLDACCESSKEY", SecretKey: "old-secret-key-value"}
	if err := ReplaceStorageKeys(path, old); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("keys file %v, %v; want mode 0600", info, err)
	}
	next := StorageKeys{AccessKey: "AKIANEWACCESSKEY", SecretKey: "new-secret-key-value"}
	for _, step := range []struct {
		name   string
		inject func() func()
	}{
		{"write", func() func() {
			saved := writeFile
			writeFile = func(*os.File, []byte) (int, error) { return 0, injected }
			return func() { writeFile = saved }
		}},
		{"sync", func() func() {
			saved := syncFile
			syncFile = func(*os.File) error { return injected }
			return func() { syncFile = saved }
		}},
		{"close", func() func() {
			saved := closeFile
			closeFile = func(file *os.File) error { file.Close(); return injected }
			return func() { closeFile = saved }
		}},
		{"rename", func() func() {
			saved := renameFile
			renameFile = func(string, string) error { return injected }
			return func() { renameFile = saved }
		}},
	} {
		t.Run(step.name, func(t *testing.T) {
			restore := step.inject()
			err := ReplaceStorageKeys(path, next)
			restore()
			if !errors.Is(err, injected) {
				t.Fatalf("replace = %v; want the injected failure", err)
			}
			for _, value := range []string{old.AccessKey, old.SecretKey, next.AccessKey, next.SecretKey} {
				if strings.Contains(err.Error(), value) {
					t.Fatalf("replace = %v; want no key in the error", err)
				}
			}
			if kept, err := ReadStorageKeys(path); err != nil || kept != old {
				t.Fatalf("after a failed %s the file holds %+v, %v; want the old pair", step.name, kept, err)
			}
			if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
				t.Fatalf("directory holds %v, %v; want the keys file alone", entries, err)
			}
		})
	}
	if err := ReplaceStorageKeys(path, next); err != nil {
		t.Fatal(err)
	}
	if kept, err := ReadStorageKeys(path); err != nil || kept != next {
		t.Fatalf("file holds %+v, %v; want the new pair", kept, err)
	}
	for _, partial := range []StorageKeys{{AccessKey: "AKIAPARTIAL"}, {SecretKey: "partial-secret"}, {}} {
		if err := ReplaceStorageKeys(path, partial); err == nil || err.Error() != "storage requires access_key and secret_key" {
			t.Fatalf("replace with %+v = %v; want the partial pair refused", partial, err)
		}
	}
	if kept, err := ReadStorageKeys(path); err != nil || kept != next {
		t.Fatalf("after a refused replace the file holds %+v, %v; want the new pair", kept, err)
	}
	for name, contents := range map[string]string{
		"access key alone": "access_key: AKIAPARTIAL\n",
		"secret key alone": "secret_key: partial-secret\n",
		"unknown field":    "access_key: AKIAPARTIAL\nsecret_key: partial-secret\ntoken: t\n",
		"not a mapping":    "[AKIAPARTIAL]\n",
	} {
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadStorageKeys(path); err == nil || strings.Contains(err.Error(), "AKIAPARTIAL") || strings.Contains(err.Error(), "partial-secret") {
			t.Fatalf("read of %s = %v; want it refused naming no value", name, err)
		}
	}
	if _, err := ReadStorageKeys(filepath.Join(dir, "absent.yaml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read of an absent file = %v; want os.ErrNotExist", err)
	}
}
