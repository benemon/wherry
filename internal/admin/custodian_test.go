package admin

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
)

// A pool's snapshot topic is created compacted, with one partition, kept forever and stamped by the broker; pool update
// restores its configs, and pool create rolls the pool's task and events topics hourly.
func TestSnapshotTopicIsCompactedSinglePartitionAndKeptForever(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	topic := CustodianTopic("pool-a")
	want := map[string]string{"cleanup.policy": "compact", "retention.ms": "-1", "message.timestamp.type": "LogAppendTime"}
	configs := func(topic string, names ...string) map[string]string {
		t.Helper()
		got := map[string]string{}
		for _, name := range names {
			values, err := service.topicConfigs(ctx, name, topic)
			if err != nil {
				t.Fatal(err)
			}
			got[name] = values[topic]
		}
		return got
	}
	if got := configs(topic, "cleanup.policy", "retention.ms", "message.timestamp.type"); !reflect.DeepEqual(got, want) {
		t.Fatalf("new %s configs %v, want %v", topic, got, want)
	}
	topics, err := kadm.NewClient(service.Admin).ListTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if partitions := len(topics[topic].Partitions); partitions != 1 {
		t.Fatalf("%s has %d partitions, want 1", topic, partitions)
	}
	for _, pooled := range []string{PoolTasksTopic("pool-a"), PoolEventsTopic("pool-a")} {
		if got := configs(pooled, "segment.ms")["segment.ms"]; got != "3600000" {
			t.Fatalf("%s segment.ms %s, want 3600000", pooled, got)
		}
	}
	if err := service.Check(ctx); err != nil {
		t.Fatalf("check of a new pool: %v", err)
	}
	setTopicConfig(t, service, topic, "cleanup.policy", "delete")
	setTopicRetention(t, service, topic, 60000)
	if err := service.PoolUpdate(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if got := configs(topic, "cleanup.policy", "retention.ms", "message.timestamp.type"); !reflect.DeepEqual(got, want) {
		t.Fatalf("existing %s configs %v, want %v", topic, got, want)
	}
}

// The pool's receivers read and write its snapshot topic and read through the custodian group; hosts of tenants
// entitled to the pool read it; no one else holds either.
func TestSnapshotTopicACLs(t *testing.T) {
	terms := modelTerms()
	records := []Record{
		{Version: 1, Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}, {Version: 1, Kind: "pool", Pool: "pool-b", Terms: &terms, AtMS: 1},
		{Version: 1, Kind: "host", Tenant: "tenant-a", Host: "host-a", AtMS: 1}, {Version: 1, Kind: "host", Tenant: "tenant-b", Host: "host-b", AtMS: 1},
		{Version: 1, Kind: "entitlement", Tenant: "tenant-a", Pool: "pool-a", AtMS: 1}, {Version: 1, Kind: "entitlement", Tenant: "tenant-b", Pool: "pool-b", AtMS: 1},
	}
	for _, config := range []ReceiverConfig{modelConfig("receiver-a", "pool-a"), modelConfig("receiver-b", "pool-b")} {
		copy := config
		records = append(records, Record{Version: 1, Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &copy, AtMS: 1})
	}
	state, err := Fold(records)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]string{}
	for _, acl := range ACLs(state) {
		if acl.Resource == CustodianTopic("pool-a") || acl.Resource == CustodianGroup("pool-a") {
			got[acl.ResourceType+" "+acl.Principal] = acl.Operations
		}
	}
	want := map[string][]string{
		"topic " + ReceiverPrincipal("receiver-a"):       {"READ", "WRITE", "DESCRIBE"},
		"group " + ReceiverPrincipal("receiver-a"):       {"READ", "DESCRIBE"},
		"topic " + TenantPrincipal("tenant-a", "host-a"): {"READ", "DESCRIBE"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pool-a snapshot ACLs %v, want %v", got, want)
	}
}

// A pool peer keeps READ on an ended member's key topic, never on its progress topic.
func TestEndedMembersKeyTopicStaysReadable(t *testing.T) {
	terms := modelTerms()
	records := []Record{{Version: 1, Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}}
	for _, config := range []ReceiverConfig{modelConfig("receiver-a", "pool-a"), modelConfig("receiver-b", "pool-a")} {
		copy := config
		records = append(records, Record{Version: 1, Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &copy, AtMS: 1})
	}
	records = append(records, Record{Version: 1, Kind: "end", Receiver: "receiver-b", AtMS: 1})
	var raw []rawRecord
	for i, record := range records {
		encoded, err := EncodeRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, rawRecord{Offset: int64(i), Value: encoded})
	}
	state, err := load(raw)
	if err != nil {
		t.Fatal(err)
	}
	acls := ACLs(state)
	if !hasACL(acls, ReceiverPrincipal("receiver-a"), ReceiverTopic("receiver-b"), "READ") || hasACL(acls, ReceiverPrincipal("receiver-a"), ReceiverProgressTopic("receiver-b"), "READ") {
		t.Fatal("receiver-a's READ on ended receiver-b's topics: want its key topic only")
	}
	if topics := EndedPoolKeyTopics(state, "pool-a"); !reflect.DeepEqual(topics, map[string]string{"receiver-b": ReceiverTopic("receiver-b")}) {
		t.Fatalf("ended key topics %v, want receiver-b's", topics)
	}
}

// Check reports a missing snapshot topic, a snapshot topic that is not compacted, and a task or events topic without
// the hourly segment.ms, with more than one partition, not deleting by time or with a finite retention.bytes.
func TestCheckReportsSnapshotTopicAndSegmentDrift(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Service)
		want   string
	}{
		{"snapshot topic missing", func(t *testing.T, service *Service) {
			responses, err := kadm.NewClient(service.Admin).DeleteTopics(context.Background(), CustodianTopic("pool-a"))
			if err == nil {
				err = responses.Error()
			}
			if err != nil {
				t.Fatal(err)
			}
		}, CustodianTopic("pool-a")},
		{"snapshot topic not compacted", func(t *testing.T, service *Service) {
			setTopicConfig(t, service, CustodianTopic("pool-a"), "cleanup.policy", "delete")
		}, CustodianTopic("pool-a") + " cleanup.policy"},
		{"snapshot topic partitioned", func(t *testing.T, service *Service) {
			addPartition(t, service, CustodianTopic("pool-a"))
		}, CustodianTopic("pool-a") + " has 2 partitions"},
		{"tasks topic partitioned", func(t *testing.T, service *Service) {
			addPartition(t, service, PoolTasksTopic("pool-a"))
		}, PoolTasksTopic("pool-a") + " has 2 partitions"},
		{"events topic compacted", func(t *testing.T, service *Service) {
			setTopicConfig(t, service, PoolEventsTopic("pool-a"), "cleanup.policy", "compact")
		}, PoolEventsTopic("pool-a") + " cleanup.policy is compact, want delete"},
		{"tasks retention.bytes finite", func(t *testing.T, service *Service) {
			setTopicConfig(t, service, PoolTasksTopic("pool-a"), "retention.bytes", "1073741824")
		}, PoolTasksTopic("pool-a") + " retention.bytes is 1073741824, want -1"},
		{"tasks segment.ms unset", func(t *testing.T, service *Service) {
			deleteTopicConfig(t, service, PoolTasksTopic("pool-a"), "segment.ms")
		}, PoolTasksTopic("pool-a") + " segment.ms"},
		{"events segment.ms unset", func(t *testing.T, service *Service) {
			deleteTopicConfig(t, service, PoolEventsTopic("pool-a"), "segment.ms")
		}, PoolEventsTopic("pool-a") + " segment.ms"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, service, closeFixture := kfakeService(t)
			defer closeFixture()
			ctx := context.Background()
			if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
				t.Fatal(err)
			}
			if err := service.Check(ctx); err != nil {
				t.Fatalf("check before drift: %v", err)
			}
			test.mutate(t, service)
			if err := service.Check(ctx); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("check = %v, want drift on %s", err, test.want)
			}
		})
	}
}

func deleteTopicConfig(t *testing.T, service *Service, topic, name string) {
	t.Helper()
	responses, err := kadm.NewClient(service.Admin).AlterTopicConfigs(context.Background(), []kadm.AlterConfig{{Op: kadm.DeleteConfig, Name: name}}, topic)
	if err == nil {
		err = responses[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
}

// A tasks topic that holds a task without an envelope but not one with a maximum envelope and its submit wrap is
// refused by pool update and check.
func TestPreflightRefusesATasksTopicTooSmallForAMaximumEnvelope(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	setTopicLimit(t, service, PoolTasksTopic("pool-a"), "88000")
	for name, err := range map[string]error{"pool update": service.PoolUpdate(ctx, "pool-a", modelTerms(), false), "check": service.Check(ctx)} {
		if err == nil || !strings.Contains(err.Error(), "a task with a maximum envelope record is ") || !strings.Contains(err.Error(), "88000-byte record limit") {
			t.Fatalf("%s = %v; want the maximum envelope refused against the tasks topic's limit", name, err)
		}
	}
	setTopicLimit(t, service, PoolTasksTopic("pool-a"), "100000")
	if err := service.Check(ctx); err != nil {
		t.Fatalf("check at 100000 bytes = %v; want a maximum envelope to fit", err)
	}
}

// A change to the task's shape, the envelope bound or the wrap changes the signed size checkLimits holds as a constant.
func TestTaskRecordIsPinned(t *testing.T) {
	value, err := largestEnvelopeTask()
	if err != nil {
		t.Fatal(err)
	}
	if batch := batchSize(value); taskRecord.batch != batch {
		t.Errorf("%s is a %d-byte batch; pinned %+v", taskRecord.name, batch, taskRecord)
	}
}

// largestEnvelopeTask signs, with throwaway keys, the smallest task a host submits around an envelope at its 64 KiB
// bound and a submit wrap: identifiers at their longest, one prompt byte.
func largestEnvelopeTask() ([]byte, error) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, err
	}
	poolKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	name, nonce := strings.Repeat("n", 32), wire.RunNonce()
	run := wire.RunID(name, name, nonce)
	task := &wire.Submission{
		Message: wire.Message{MessageID: "msg-" + strings.Repeat("0", 32), ContextID: run, TaskID: run, Role: wire.RoleUser, Parts: []wire.Part{{Text: "p"}}, Extensions: []string{wire.ExtensionURI}},
		Wherry:  wire.SubmissionBinding{Tenant: name, Host: name, Pool: name, Run: run, RunNonce: nonce, Placement: wire.Placement{Agent: name, Selector: []wire.Requirement{}}},
	}
	wrap, err := signed.WrapContentKey(make([]byte, 32), &poolKey.PublicKey)
	if err != nil {
		return nil, err
	}
	task.Wherry.Envelope, task.Wherry.Wrap = strings.Repeat("A", signed.MaxEnvelope), string(wrap)
	return wire.SignRecord(task, key)
}

// CheckEnvelope, which the host runs before it builds an envelope and the receiver after it opens one, refuses
// credentials other than the agent's supplied ones and the task's, an empty value, and an S3 URL off its object's
// exact key, reading inputs under the S3 input run.
func TestCheckEnvelope(t *testing.T) {
	terms := withDefaults(modelTerms())
	terms.Storage = &Storage{Endpoint: "https://s3.example.test", Bucket: "bucket-a"}
	task := &wire.Submission{Wherry: wire.SubmissionBinding{Tenant: "tenant-a", Run: "run-b", Replaces: "run-a", S3InputRun: "run-a", Placement: wire.Placement{Agent: "agent-a"},
		Credentials: []wire.TaskCredential{{Name: "deploy-key", File: "id"}}, S3Inputs: []wire.S3Input{{Name: "data.bin"}},
		Outputs: []wire.ExpectedOutput{{Name: "out/report.pdf", Storage: wire.StorageS3}, {Name: "notes.md"}}}}
	values := func() signed.EnvelopeValues {
		return signed.EnvelopeValues{
			Credentials:  map[string][]byte{"model": []byte("m"), "deploy-key": []byte("d")},
			S3Inputs:     map[string]string{"data.bin": "https://s3.example.test/bucket-a/wherry/tenant-a/run-a/inputs/data.bin?X-Amz-Signature=s"},
			S3Outputs:    map[string]string{"out/report.pdf": "https://s3.example.test:443/bucket-a/wherry/tenant-a/run-b/outputs/out/report.pdf?X-Amz-Signature=s"},
			RedactionKey: make([]byte, 32),
		}
	}
	if err := CheckEnvelope(terms, task, values()); err != nil {
		t.Fatalf("check of a valid envelope: %v", err)
	}
	for name, test := range map[string]struct {
		mutate func(*signed.EnvelopeValues)
		want   string
	}{
		"task credential missing":  {func(v *signed.EnvelopeValues) { delete(v.Credentials, "deploy-key") }, "envelope credentials"},
		"agent credential missing": {func(v *signed.EnvelopeValues) { delete(v.Credentials, "model") }, "envelope credentials"},
		"undeclared credential":    {func(v *signed.EnvelopeValues) { v.Credentials["other"] = []byte("o") }, "envelope credentials"},
		"empty credential":         {func(v *signed.EnvelopeValues) { v.Credentials["model"] = nil }, "envelope credential model is empty"},
		"input under its own run": {func(v *signed.EnvelopeValues) {
			v.S3Inputs["data.bin"] = strings.Replace(v.S3Inputs["data.bin"], "/run-a/", "/run-b/", 1)
		}, "S3 input data.bin"},
		"input off the bucket": {func(v *signed.EnvelopeValues) {
			v.S3Inputs["data.bin"] = strings.Replace(v.S3Inputs["data.bin"], "/bucket-a/", "/bucket-b/", 1)
		}, "S3 input data.bin"},
		"input on another host": {func(v *signed.EnvelopeValues) {
			v.S3Inputs["data.bin"] = strings.Replace(v.S3Inputs["data.bin"], "s3.example.test", "evil.test", 1)
		}, "S3 input data.bin"},
		"input with a user": {func(v *signed.EnvelopeValues) {
			v.S3Inputs["data.bin"] = strings.Replace(v.S3Inputs["data.bin"], "https://", "https://u@", 1)
		}, "S3 input data.bin"},
		"input empty": {func(v *signed.EnvelopeValues) { v.S3Inputs["data.bin"] = "" }, "S3 input data.bin"},
		"output under the root run": {func(v *signed.EnvelopeValues) {
			v.S3Outputs["out/report.pdf"] = strings.Replace(v.S3Outputs["out/report.pdf"], "/run-b/", "/run-a/", 1)
		}, "S3 output out/report.pdf"},
		"output missing":      {func(v *signed.EnvelopeValues) { delete(v.S3Outputs, "out/report.pdf") }, "envelope S3 URLs"},
		"short redaction key": {func(v *signed.EnvelopeValues) { v.RedactionKey = v.RedactionKey[:31] }, "redaction key"},
	} {
		v := values()
		test.mutate(&v)
		if err := CheckEnvelope(terms, task, v); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: check = %v, want %q", name, err, test.want)
		}
	}
}

func addPartition(t *testing.T, service *Service, topic string) {
	t.Helper()
	responses, err := kadm.NewClient(service.Admin).CreatePartitions(context.Background(), 1, topic)
	if err == nil {
		err = responses.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
}
