package operator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// kafkaChanges counts wherry.operator's records and the SCRAM users, which a provisioning command changes first.
func kafkaChanges(t *testing.T, service *Service) [2]int64 {
	t.Helper()
	admin := kadm.NewClient(service.Operator)
	offsets, err := admin.ListEndOffsets(t.Context(), OperatorTopic)
	if err == nil {
		err = offsets.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	users, err := admin.DescribeUserSCRAMs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	end, _ := offsets.Lookup(OperatorTopic, 0)
	return [2]int64{end.Offset, int64(len(users))}
}

var bundleAdds = []struct {
	name string
	add  func(*Service, string) error
}{
	{"host", func(service *Service, path string) error {
		return service.HostAdd(context.Background(), "tenant-a", "host-b", path, false)
	}},
	{"receiver", func(service *Service, path string) error {
		return service.ReceiverAdd(context.Background(), modelConfig("receiver-b", "pool-a"), service.Broker, path)
	}},
}

// A bundle written without --force refuses a file at its path before anything changes in Kafka, leaving the file.
func TestBundleWithoutForceRefusesAnExistingFileFirst(t *testing.T) {
	for _, identity := range bundleAdds {
		t.Run(identity.name, func(t *testing.T) {
			_, service, _ := compromiseFixture(t)
			path := filepath.Join(t.TempDir(), "other.bundle")
			if err := os.WriteFile(path, []byte("other"), 0o600); err != nil {
				t.Fatal(err)
			}
			before := kafkaChanges(t, service)
			if err := identity.add(service, path); !errors.Is(err, os.ErrExist) {
				t.Fatalf("add = %v; want the existing file refused", err)
			}
			if after := kafkaChanges(t, service); after != before {
				t.Fatalf("records and SCRAM users %v, were %v; want nothing changed", after, before)
			}
			if contents, err := os.ReadFile(path); err != nil || string(contents) != "other" {
				t.Fatalf("%s holds %q, %v; want the other file", path, contents, err)
			}
		})
	}
}

// On a fresh cluster, host add refuses an existing bundle file before it creates wherry.operator.
func TestHostAddRefusesAnExistingFileBeforeCreatingTheRecordTopic(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	responses, err := kadm.NewClient(service.Operator).DeleteTopics(ctx, OperatorTopic)
	if err == nil {
		err = responses.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "other.bundle")
	if err := os.WriteFile(path, []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := service.HostAdd(ctx, "tenant-a", "host-b", path, false); !errors.Is(err, os.ErrExist) {
		t.Fatalf("host add = %v; want the existing file refused", err)
	}
	// kadm's ListTopics answers from cached metadata; a direct request lists what exists.
	response, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, service.Operator)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Topics) != 0 {
		t.Fatalf("host add left %d topics, first %s; want none", len(response.Topics), *response.Topics[0].Topic)
	}
}

// A host whose topic creation fails is not recorded, so check does not then look for its topic.
func TestHostAddRecordsNothingWhenItsTopicIsRefused(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	cluster.ControlKey(int16(kmsg.CreateTopics), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		create := request.(*kmsg.CreateTopicsRequest)
		if !slices.ContainsFunc(create.Topics, func(topic kmsg.CreateTopicsRequestTopic) bool { return topic.Topic == HostTopic("tenant-a", "host-a") }) {
			return nil, nil, false
		}
		response := create.ResponseKind().(*kmsg.CreateTopicsResponse)
		for _, topic := range create.Topics {
			failed := kmsg.NewCreateTopicsResponseTopic()
			failed.Topic, failed.ErrorCode = topic.Topic, kerr.TopicAuthorizationFailed.Code
			response.Topics = append(response.Topics, failed)
		}
		return response, nil, true
	})
	if err := service.HostAdd(ctx, "tenant-a", "host-a", filepath.Join(t.TempDir(), "host.bundle"), false); err == nil {
		t.Fatal("host add succeeded; want its topic's creation refused")
	}
	if state, err := service.LoadState(ctx); err != nil || len(state.Hosts) != 0 {
		t.Fatalf("hosts %v, %v; want none recorded", state.Hosts, err)
	}
}

// A bundle written without --force that fails after reserving its file removes it.
func TestBundleWithoutForceRemovesItsFileOnFailure(t *testing.T) {
	injected := errors.New("injected")
	for _, identity := range bundleAdds {
		t.Run(identity.name, func(t *testing.T) {
			_, service, _ := compromiseFixture(t)
			dir := t.TempDir()
			path := filepath.Join(dir, "new.bundle")
			defer func(saved func(*os.File, []byte) (int, error)) { writeFile = saved }(writeFile)
			writeFile = func(file *os.File, _ []byte) (int, error) {
				if _, err := os.Stat(path); err != nil {
					t.Errorf("bundle file before the write: %v; want it reserved", err)
				}
				return 0, injected
			}
			if err := identity.add(service, path); !errors.Is(err, injected) {
				t.Fatalf("add = %v; want the injected failure", err)
			}
			if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
				t.Fatalf("bundle directory holds %v, %v; want nothing", entries, err)
			}
		})
	}
}

// A bundle whose file cannot be removed after a failure says so, naming the file and not the password.
func TestBundleReportsAFailedRemoval(t *testing.T) {
	injected, unremoved := errors.New("injected"), errors.New("unremoved")
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprintf("replace=%t", replace), func(t *testing.T) {
			bundle, err := createBundle(filepath.Join(t.TempDir(), "host.bundle"), replace)
			if err != nil {
				t.Fatal(err)
			}
			defer func(write func(*os.File, []byte) (int, error), remove func(string) error) {
				writeFile, removeFile = write, remove
			}(writeFile, removeFile)
			writeFile = func(file *os.File, encoded []byte) (int, error) {
				file.Write(encoded)
				return 0, injected
			}
			removeFile = func(string) error { return unremoved }
			err = writeBundle(bundle, Bundle{Broker: "broker:9093", Principal: "tenant.tenant-a.host-a", Password: "secret-password"})
			closeBundle(bundle, &err)
			if !errors.Is(err, injected) || !errors.Is(err, unremoved) || !strings.Contains(err.Error(), bundle.Name()+" could not be removed and may hold the new password") {
				t.Fatalf("closeBundle = %v; want the failure and that %s could not be removed", err, bundle.Name())
			}
			if strings.Contains(err.Error(), "secret-password") {
				t.Fatalf("closeBundle = %v; want no password", err)
			}
		})
	}
}

// A reissue that fails writing, closing or renaming its bundle after the password changed removes its temporary file,
// leaves the old bundle, and says the old bundle no longer works and that --force again recovers.
func TestReissueFailingAfterThePasswordChanges(t *testing.T) {
	injected := errors.New("injected")
	for _, step := range []struct {
		name   string
		inject func() func()
	}{
		{"write", func() func() {
			saved := writeFile
			writeFile = func(*os.File, []byte) (int, error) { return 0, injected }
			return func() { writeFile = saved }
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
		for _, identity := range []struct {
			name    string
			reissue func(*Service, string) error
		}{
			{"host", func(service *Service, path string) error {
				return service.HostAdd(t.Context(), "tenant-a", "host-a", path, true)
			}},
			{"receiver", func(service *Service, path string) error {
				return service.ReceiverReissue(t.Context(), "receiver-a", service.Broker, path)
			}},
		} {
			t.Run(step.name+"/"+identity.name, func(t *testing.T) {
				_, service, _ := compromiseFixture(t)
				dir := t.TempDir()
				path := filepath.Join(dir, "old.bundle")
				if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
					t.Fatal(err)
				}
				restore := step.inject()
				err := identity.reissue(service, path)
				restore()
				if !errors.Is(err, injected) || !strings.Contains(err.Error(), "old bundle no longer works") || !strings.Contains(err.Error(), "--force again") {
					t.Fatalf("reissue = %v; want the injected failure, that the old bundle no longer works and that --force again recovers", err)
				}
				if contents, err := os.ReadFile(path); err != nil || string(contents) != "old" {
					t.Fatalf("%s holds %q, %v; want the old bundle", path, contents, err)
				}
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
					t.Fatalf("bundle directory holds %v, %v; want the old bundle alone", entries, err)
				}
			})
		}
	}
}

// host add --force refuses a host whose membership ended, writing no bundle.
func TestHostAddForceRefusesAnEndedHost(t *testing.T) {
	_, service, ctx := compromiseFixture(t)
	if _, err := service.appendChecked(ctx, []byte("tenant-a.host-a"), Record{Kind: "end", Tenant: "tenant-a", Host: "host-a"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "host.bundle")
	if err := service.HostAdd(ctx, "tenant-a", "host-a", path, true); err == nil || err.Error() != "host cannot return after its membership end" {
		t.Fatalf("host add --force of an ended host = %v; want it refused", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused host add wrote %s (%v)", path, err)
	}
}
