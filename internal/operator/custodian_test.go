package operator

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
)

// A pool's snapshot topic is created compacted, with one partition, kept forever and stamped by the broker; pool apply
// restores its configs, and pool apply rolls the pool's task and events topics hourly.
func TestSnapshotTopicIsCompactedSinglePartitionAndKeptForever(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	poolWithReceiver(t, service)
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
	topics, err := kadm.NewClient(service.Operator).ListTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if partitions := len(topics[topic].Partitions); partitions != 1 {
		t.Fatalf("%s has %d partitions, want 1", topic, partitions)
	}
	for _, pooled := range []string{modelTopic("pool-a"), PoolEventsTopic("pool-a")} {
		if got := configs(pooled, "segment.ms")["segment.ms"]; got != "3600000" {
			t.Fatalf("%s segment.ms %s, want 3600000", pooled, got)
		}
	}
	if err := service.Check(ctx); err != nil {
		t.Fatalf("check of a new pool: %v", err)
	}
	setTopicConfig(t, service, topic, "cleanup.policy", "delete")
	setTopicRetention(t, service, topic, 60000)
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if got := configs(topic, "cleanup.policy", "retention.ms", "message.timestamp.type"); !reflect.DeepEqual(got, want) {
		t.Fatalf("existing %s configs %v, want %v", topic, got, want)
	}
}

// The pool's receivers read and write its snapshot topic and read through the custodian group; hosts of tenants
// entitled to the pool read it; no one else but wherry.operator holds either.
func TestSnapshotTopicACLs(t *testing.T) {
	terms := modelTerms()
	records := []Record{{Version: 1, Kind: "tenant", Tenant: "tenant-a", AtMS: 1}, {Version: 1, Kind: "tenant", Tenant: "tenant-b", AtMS: 1},
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
	for _, acl := range append(ACLs(state), OperatorACLs...) {
		if acl.Resource == CustodianTopic("pool-a") || acl.Resource == CustodianGroup("pool-a") || acl.Prefix && (strings.HasPrefix(CustodianTopic("pool-a"), acl.Resource) || strings.HasPrefix(CustodianGroup("pool-a"), acl.Resource)) {
			got[acl.ResourceType+" "+acl.Principal] = acl.Operations
		}
	}
	want := map[string][]string{
		"topic " + ReceiverPrincipal("receiver-a"):       {"READ", "WRITE", "DESCRIBE"},
		"group " + ReceiverPrincipal("receiver-a"):       {"READ", "DESCRIBE"},
		"topic " + TenantPrincipal("tenant-a", "host-a"): {"READ", "DESCRIBE"},
		"topic " + OperatorPrincipal:                     {"CREATE", "ALTER_CONFIGS", "READ", "WRITE"},
		"group " + OperatorPrincipal:                     {"ALTER_CONFIGS", "DESCRIBE"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pool-a snapshot ACLs %v, want %v", got, want)
	}
}

// Each pool's live receivers read and write its receivers topic and the hosts of tenants entitled to the pool read it;
// no receiver of another pool, ended receiver, host of an unentitled tenant or of an ended entitlement holds
// it, and no one but wherry.operator holds a grant on another receivers topic.
func TestPoolReceiversTopicACLs(t *testing.T) {
	terms := modelTerms()
	records := []Record{{Version: 1, Kind: "tenant", Tenant: "tenant-a", AtMS: 1}, {Version: 1, Kind: "tenant", Tenant: "tenant-b", AtMS: 1},
		{Version: 1, Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}, {Version: 1, Kind: "pool", Pool: "pool-b", Terms: &terms, AtMS: 1},
		{Version: 1, Kind: "host", Tenant: "tenant-a", Host: "host-a", AtMS: 1}, {Version: 1, Kind: "host", Tenant: "tenant-b", Host: "host-b", AtMS: 1},
		{Version: 1, Kind: "entitlement", Tenant: "tenant-a", Pool: "pool-a", AtMS: 1}, {Version: 1, Kind: "entitlement", Tenant: "tenant-b", Pool: "pool-b", AtMS: 1},
		{Version: 1, Kind: "entitlement", Tenant: "tenant-b", Pool: "pool-a", AtMS: 1},
	}
	for _, config := range []ReceiverConfig{modelConfig("receiver-a", "pool-a"), modelConfig("receiver-b", "pool-b"), modelConfig("receiver-c", "pool-a")} {
		copy := config
		records = append(records, Record{Version: 1, Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &copy, AtMS: 1})
	}
	records = append(records, Record{Version: 1, Kind: "end", Receiver: "receiver-c", AtMS: 1}, Record{Version: 1, Kind: "end", Tenant: "tenant-b", Pool: "pool-a", AtMS: 1})
	state, err := Fold(records)
	if err != nil {
		t.Fatal(err)
	}
	for pool, want := range map[string]map[string][]string{
		"pool-a": {
			ReceiverPrincipal("receiver-a"):       {"READ", "WRITE", "DESCRIBE"},
			TenantPrincipal("tenant-a", "host-a"): {"READ", "DESCRIBE"},
			OperatorPrincipal:                     {"CREATE", "ALTER_CONFIGS", "READ", "WRITE"},
		},
		"pool-b": {
			ReceiverPrincipal("receiver-b"):       {"READ", "WRITE", "DESCRIBE"},
			TenantPrincipal("tenant-b", "host-b"): {"READ", "DESCRIBE"},
			OperatorPrincipal:                     {"CREATE", "ALTER_CONFIGS", "READ", "WRITE"},
		},
	} {
		got := map[string][]string{}
		for _, acl := range append(ACLs(state), OperatorACLs...) {
			if acl.ResourceType == "topic" && (acl.Resource == PoolReceiversTopic(pool) || acl.Prefix && strings.HasPrefix(PoolReceiversTopic(pool), acl.Resource)) {
				got[acl.Principal] = acl.Operations
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s receivers topic ACLs %v, want %v", pool, got, want)
		}
	}
}

// A pool peer keeps READ on an ended member's key topic, never on its progress topic.
func TestEndedMembersKeyTopicStaysReadable(t *testing.T) {
	terms := modelTerms()
	records := []Record{{Version: 1, Kind: "tenant", Tenant: "tenant-a", AtMS: 1}, {Version: 1, Kind: "tenant", Tenant: "tenant-b", AtMS: 1}, {Version: 1, Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}}
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

// Check reports a missing, uncompacted or partitioned snapshot or receivers topic, a receivers topic without the hourly
// segment.ms, and a task or events topic without the hourly segment.ms, with more than one partition, not deleting by
// time or with a finite retention.bytes.
func TestCheckReportsSnapshotTopicAndSegmentDrift(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Service)
		want   string
	}{
		{"snapshot topic missing", func(t *testing.T, service *Service) {
			responses, err := kadm.NewClient(service.Operator).DeleteTopics(context.Background(), CustodianTopic("pool-a"))
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
		{"receivers topic missing", func(t *testing.T, service *Service) {
			responses, err := kadm.NewClient(service.Operator).DeleteTopics(context.Background(), PoolReceiversTopic("pool-a"))
			if err == nil {
				err = responses.Error()
			}
			if err != nil {
				t.Fatal(err)
			}
		}, PoolReceiversTopic("pool-a")},
		{"receivers topic not compacted", func(t *testing.T, service *Service) {
			setTopicConfig(t, service, PoolReceiversTopic("pool-a"), "cleanup.policy", "delete")
		}, PoolReceiversTopic("pool-a") + " cleanup.policy is delete, want compact,delete"},
		{"receivers topic segment.ms drifted", func(t *testing.T, service *Service) {
			setTopicConfig(t, service, PoolReceiversTopic("pool-a"), "segment.ms", "604800000")
		}, PoolReceiversTopic("pool-a") + " segment.ms is 604800000, want 3600000"},
		{"receivers topic partitioned", func(t *testing.T, service *Service) {
			addPartition(t, service, PoolReceiversTopic("pool-a"))
		}, PoolReceiversTopic("pool-a") + " has 2 partitions"},
		{"tasks topic partitioned", func(t *testing.T, service *Service) {
			addPartition(t, service, modelTopic("pool-a"))
		}, modelTopic("pool-a") + " has 2 partitions"},
		{"events topic compacted", func(t *testing.T, service *Service) {
			setTopicConfig(t, service, PoolEventsTopic("pool-a"), "cleanup.policy", "compact")
		}, PoolEventsTopic("pool-a") + " cleanup.policy is compact, want delete"},
		{"tasks retention.bytes finite", func(t *testing.T, service *Service) {
			setTopicConfig(t, service, modelTopic("pool-a"), "retention.bytes", "1073741824")
		}, modelTopic("pool-a") + " retention.bytes is 1073741824, want -1"},
		{"tasks segment.ms unset", func(t *testing.T, service *Service) {
			deleteTopicConfig(t, service, modelTopic("pool-a"), "segment.ms")
		}, modelTopic("pool-a") + " segment.ms"},
		{"events segment.ms unset", func(t *testing.T, service *Service) {
			deleteTopicConfig(t, service, PoolEventsTopic("pool-a"), "segment.ms")
		}, PoolEventsTopic("pool-a") + " segment.ms"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, service, closeFixture := kfakeService(t)
			defer closeFixture()
			ctx := context.Background()
			poolWithReceiver(t, service)
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
	responses, err := kadm.NewClient(service.Operator).AlterTopicConfigs(context.Background(), []kadm.AlterConfig{{Op: kadm.DeleteConfig, Name: name}}, topic)
	if err == nil {
		err = responses[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
}

// A tasks topic that holds a task without an envelope but not one with a maximum envelope and its submit wrap is
// refused by pool apply and check.
func TestPreflightRefusesATasksTopicTooSmallForAMaximumEnvelope(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	poolWithReceiver(t, service)
	setTopicLimit(t, service, modelTopic("pool-a"), "88000")
	for name, err := range map[string]error{"pool apply": service.PoolApply(ctx, "pool-a", modelTerms(), false), "check": service.Check(ctx)} {
		if err == nil || !strings.Contains(err.Error(), "a task with a maximum envelope record is ") || !strings.Contains(err.Error(), "88000-byte record limit") {
			t.Fatalf("%s = %v; want the maximum envelope refused against the tasks topic's limit", name, err)
		}
	}
	setTopicLimit(t, service, modelTopic("pool-a"), "100000")
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

// testPolicy is a POST policy as a host builds one for run's outputs in bucket at endpoint, signed at signedAt and
// expiring at expires; the signature is a stand-in, which no test store checks.
func testPolicy(endpoint, bucket, tenant, run string, signedAt, expires time.Time) signed.S3Policy {
	return policyOf(endpoint, bucket, map[string]string{
		"key": wire.S3OutputKey(tenant, run, ""), "bucket": bucket, "x-amz-algorithm": SigV4Algorithm,
		"x-amz-credential": "AKIATEST/" + signedAt.UTC().Format("20060102") + "/us-east-1/s3/aws4_request", "x-amz-date": signedAt.UTC().Format("20060102T150405Z"),
		"x-amz-signature": strings.Repeat("ab", 32),
	}, expires, nil)
}

// policyOf is a policy of fields whose document holds the conditions a host sets for them, with conditions appended,
// expiring at expires.
func policyOf(endpoint, bucket string, fields map[string]string, expires time.Time, conditions []string) signed.S3Policy {
	document := []string{
		`["eq","$bucket","` + fields["bucket"] + `"]`, `["starts-with","$key","` + fields["key"] + `"]`, `["content-length-range", 0, ` + strconv.FormatInt(wire.MaxS3Object, 10) + `]`,
		`["eq","$x-amz-algorithm","` + fields["x-amz-algorithm"] + `"]`, `["eq","$x-amz-credential","` + fields["x-amz-credential"] + `"]`, `["eq","$x-amz-date","` + fields["x-amz-date"] + `"]`,
	}
	fields["policy"] = base64.StdEncoding.EncodeToString([]byte(`{"expiration":"` + expires.UTC().Format("2006-01-02T15:04:05.000Z") + `","conditions":[` + strings.Join(append(document, conditions...), ",") + `]}`))
	return signed.S3Policy{URL: endpoint + "/" + bucket + "/", Fields: fields}
}

// CheckEnvelope, which the host runs before it builds an envelope and the receiver after it opens one, refuses
// credentials other than the agent's supplied ones and the task's, an empty value, an S3 input URL off its object's
// exact key, read under the S3 input run, and a POST policy that is not exactly one for the run's output prefix in the
// pool's bucket: another host, bucket or path, a key other than the prefix, an extra or missing field, a policy in the
// object form, a wider prefix, a later expiration, another range, a redirect or unknown condition, or malformed base64.
func TestCheckEnvelope(t *testing.T) {
	terms := withDefaults(modelTerms())
	terms.Storage = &Storage{Endpoint: "https://s3.example.test", Bucket: "bucket-a"}
	endBy := time.Now().Add(time.Hour).Truncate(time.Second)
	task := &wire.Submission{Wherry: wire.SubmissionBinding{Tenant: "tenant-a", Run: "run-b", Replaces: "run-a", S3InputRun: "run-a", Placement: wire.Placement{Agent: "agent-a"},
		Supplied: []string{"model"}, Credentials: []wire.TaskCredential{{Name: "deploy-key", File: "id"}}, S3Inputs: []wire.S3Input{{Name: "data.bin"}},
		Outputs: []wire.ExpectedOutput{{Name: "out/report.pdf"}, {Name: "notes.md"}}, TaskEndedBy: endBy.Unix()}}
	signedAt := time.Now()
	values := func() signed.EnvelopeValues {
		policy := testPolicy("https://s3.example.test:443", "bucket-a", "tenant-a", "run-b", signedAt, endBy.Add(UploadGrace))
		return signed.EnvelopeValues{
			Credentials:  map[string][]byte{"model": []byte("m"), "deploy-key": []byte("d")},
			S3Inputs:     map[string]string{"data.bin": "https://s3.example.test/bucket-a/wherry/tenant-a/run-a/inputs/data.bin?X-Amz-Signature=s"},
			S3Policy:     &policy,
			RedactionKey: make([]byte, 32),
		}
	}
	if err := CheckEnvelope(terms, task, values()); err != nil {
		t.Fatalf("check of a valid envelope: %v", err)
	}
	// A host without a storage credential for the pool issues no policy; an earlier expiration is the host's choice.
	without := values()
	without.S3Policy = nil
	if err := CheckEnvelope(terms, task, without); err != nil {
		t.Fatalf("check of an envelope without a policy: %v", err)
	}
	earlier := values()
	*earlier.S3Policy = testPolicy("https://s3.example.test", "bucket-a", "tenant-a", "run-b", signedAt, endBy)
	if err := CheckEnvelope(terms, task, earlier); err != nil {
		t.Fatalf("check of a policy expiring at task_ended_by: %v", err)
	}
	// A session token is a field and a condition together.
	token := values()
	fields := map[string]string{}
	for name, value := range token.S3Policy.Fields {
		fields[name] = value
	}
	fields[SecurityTokenField] = "session-token"
	*token.S3Policy = policyOf("https://s3.example.test", "bucket-a", fields, endBy.Add(UploadGrace), []string{`["eq","$x-amz-security-token","session-token"]`})
	if err := CheckEnvelope(terms, task, token); err != nil {
		t.Fatalf("check of a policy with a session token: %v", err)
	}
	// The binding names the agent's credentials the envelope holds, so terms that change them after the seal agree.
	changed := withDefaults(modelTerms())
	changed.Storage = terms.Storage
	changed.Agents[0].Credentials = []AgentCredential{{TaskCredential: wire.TaskCredential{Name: "vault-token", Env: "VAULT_TOKEN"}, Required: new(bool)}}
	if err := CheckEnvelope(changed, task, values()); err != nil {
		t.Fatalf("check under terms that drop model and add an optional credential: %v", err)
	}
	other := *task
	other.Wherry.Placement.Agent = "agent-z"
	if err := CheckEnvelope(terms, &other, values()); err == nil || !strings.Contains(err.Error(), "agent is outside the pool terms") {
		t.Fatalf("check of a task whose agent is outside the terms = %v", err)
	}
	noStorage := withDefaults(modelTerms())
	if err := CheckEnvelope(noStorage, task, values()); err == nil {
		t.Fatal("check of a policy under terms without storage passed")
	}
	// rebuilt is values with the policy rebuilt from edited fields, document conditions appended.
	rebuilt := func(edit func(fields map[string]string), conditions ...string) func(*signed.EnvelopeValues) {
		return func(v *signed.EnvelopeValues) {
			fields := map[string]string{}
			for name, value := range v.S3Policy.Fields {
				fields[name] = value
			}
			edit(fields)
			*v.S3Policy = policyOf("https://s3.example.test", "bucket-a", fields, endBy.Add(UploadGrace), conditions)
		}
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
		"input empty":               {func(v *signed.EnvelopeValues) { v.S3Inputs["data.bin"] = "" }, "S3 input data.bin"},
		"input undeclared":          {func(v *signed.EnvelopeValues) { v.S3Inputs["other.bin"] = v.S3Inputs["data.bin"] }, "envelope S3 URLs"},
		"short redaction key":       {func(v *signed.EnvelopeValues) { v.RedactionKey = v.RedactionKey[:31] }, "redaction key"},
		"policy on another host":    {func(v *signed.EnvelopeValues) { v.S3Policy.URL = "https://evil.test/bucket-a/" }, "the URL is not the bucket"},
		"policy on another port":    {func(v *signed.EnvelopeValues) { v.S3Policy.URL = "https://s3.example.test:8443/bucket-a/" }, "the URL is not the bucket"},
		"policy over http":          {func(v *signed.EnvelopeValues) { v.S3Policy.URL = "http://s3.example.test/bucket-a/" }, "the URL is not the bucket"},
		"policy for another bucket": {func(v *signed.EnvelopeValues) { v.S3Policy.URL = "https://s3.example.test/bucket-b/" }, "the URL is not the bucket"},
		"policy URL at the prefix": {func(v *signed.EnvelopeValues) {
			v.S3Policy.URL = "https://s3.example.test/bucket-a/wherry/tenant-a/run-b/outputs/"
		}, "the URL is not the bucket"},
		"policy URL without the slash":      {func(v *signed.EnvelopeValues) { v.S3Policy.URL = "https://s3.example.test/bucket-a" }, "the URL is not the bucket"},
		"policy URL with a query":           {func(v *signed.EnvelopeValues) { v.S3Policy.URL += "?x=1" }, "the URL is not the bucket"},
		"policy URL with a user":            {func(v *signed.EnvelopeValues) { v.S3Policy.URL = "https://u@s3.example.test/bucket-a/" }, "the URL is not the bucket"},
		"key field another prefix":          {rebuilt(func(fields map[string]string) { fields["key"] = wire.S3OutputKey("tenant-a", "run-a", "") }), "the key, bucket or algorithm field"},
		"key field an object":               {rebuilt(func(fields map[string]string) { fields["key"] = wire.S3OutputKey("tenant-a", "run-b", "report.pdf") }), "the key, bucket or algorithm field"},
		"key field the run prefix":          {rebuilt(func(fields map[string]string) { fields["key"] = wire.S3Prefix("tenant-a", "run-b") }), "the key, bucket or algorithm field"},
		"bucket field another":              {rebuilt(func(fields map[string]string) { fields["bucket"] = "bucket-b" }), "the key, bucket or algorithm field"},
		"algorithm field another":           {rebuilt(func(fields map[string]string) { fields["x-amz-algorithm"] = "AWS4-HMAC-SHA1" }), "the key, bucket or algorithm field"},
		"extra field":                       {rebuilt(func(fields map[string]string) { fields["success_action_status"] = "201" }), "not a SigV4 policy's"},
		"extra acl field":                   {rebuilt(func(fields map[string]string) { fields["acl"] = "public-read" }), "not a SigV4 policy's"},
		"missing field":                     {rebuilt(func(fields map[string]string) { delete(fields, "x-amz-date") }), "not a SigV4 policy's"},
		"missing signature":                 {rebuilt(func(fields map[string]string) { delete(fields, "x-amz-signature") }), "not a SigV4 policy's"},
		"empty field":                       {rebuilt(func(fields map[string]string) { fields["x-amz-signature"] = "" }), "not a SigV4 policy's"},
		"token field without its condition": {rebuilt(func(fields map[string]string) { fields[SecurityTokenField] = "session-token" }), "lacks a condition"},
		"malformed base64":                  {func(v *signed.EnvelopeValues) { v.S3Policy.Fields["policy"] = "%%%" }, "not base64"},
		"policy not JSON": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = base64.StdEncoding.EncodeToString([]byte("{"))
		}, "does not decode"},
		"policy with an unknown member": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2030-01-01T00:00:00.000Z","conditions":[],"x":1}`))
		}, "does not decode"},
		"policy without conditions": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2030-01-01T00:00:00.000Z"}`))
		}, "does not decode"},
		"object form condition": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = base64.StdEncoding.EncodeToString([]byte(`{"expiration":"` + endBy.UTC().Format("2006-01-02T15:04:05.000Z") + `","conditions":[{"bucket":"bucket-a"},["starts-with","$key","wherry/tenant-a/run-b/outputs/"]]}`))
		}, "not a three-element array"},
		"expiration with a zone offset": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = base64.StdEncoding.EncodeToString([]byte(`{"expiration":"` + endBy.In(time.FixedZone("", 14*3600)).Format("2006-01-02T15:04:05.000-07:00") + `","conditions":[]}`))
		}, "expires later than"},
		"later expiration": {func(v *signed.EnvelopeValues) {
			*v.S3Policy = testPolicy("https://s3.example.test", "bucket-a", "tenant-a", "run-b", signedAt, endBy.Add(UploadGrace+time.Second))
		}, "expires later"},
		"unparsable expiration": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = base64.StdEncoding.EncodeToString([]byte(`{"expiration":"soon","conditions":[]}`))
		}, "expires later"},
		"wider prefix": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = strings.NewReplacer(`["starts-with","$key","wherry/tenant-a/run-b/outputs/"]`, `["starts-with","$key","wherry/tenant-a/"]`).Replace(decodedPolicy(t, v.S3Policy.Fields["policy"]))
			v.S3Policy.Fields["policy"] = base64.StdEncoding.EncodeToString([]byte(v.S3Policy.Fields["policy"]))
		}, "other than the run's"},
		"another bucket condition": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = encodedPolicy(strings.Replace(decodedPolicy(t, v.S3Policy.Fields["policy"]), `["eq","$bucket","bucket-a"]`, `["eq","$bucket","bucket-b"]`, 1))
		}, "other than the run's"},
		"another range": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = encodedPolicy(strings.Replace(decodedPolicy(t, v.S3Policy.Fields["policy"]), `["content-length-range", 0, 5368709120]`, `["content-length-range", 0, 5368709121]`, 1))
		}, "other than the run's"},
		"range from one": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = encodedPolicy(strings.Replace(decodedPolicy(t, v.S3Policy.Fields["policy"]), `["content-length-range", 0, 5368709120]`, `["content-length-range", 1, 5368709120]`, 1))
		}, "other than the run's"},
		"range as strings": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = encodedPolicy(strings.Replace(decodedPolicy(t, v.S3Policy.Fields["policy"]), `["content-length-range", 0, 5368709120]`, `["content-length-range", "0", "5368709120"]`, 1))
		}, "not two integers"},
		"range missing": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = encodedPolicy(strings.Replace(decodedPolicy(t, v.S3Policy.Fields["policy"]), `,["content-length-range", 0, 5368709120]`, ``, 1))
		}, "lacks a condition"},
		"key condition missing": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = encodedPolicy(strings.Replace(decodedPolicy(t, v.S3Policy.Fields["policy"]), `["starts-with","$key","wherry/tenant-a/run-b/outputs/"],`, ``, 1))
		}, "lacks a condition"},
		"redirect condition":  {rebuilt(func(map[string]string) {}, `["eq","$success_action_redirect","https://evil.test/"]`), "other than the run's"},
		"status condition":    {rebuilt(func(map[string]string) {}, `["eq","$success_action_status","201"]`), "other than the run's"},
		"acl condition":       {rebuilt(func(map[string]string) {}, `["eq","$acl","public-read"]`), "other than the run's"},
		"unknown condition":   {rebuilt(func(map[string]string) {}, `["starts-with","$x-amz-meta-tag",""]`), "other than the run's"},
		"duplicate condition": {rebuilt(func(map[string]string) {}, `["eq","$bucket","bucket-a"]`), "other than the run's"},
		"credential condition for another": {func(v *signed.EnvelopeValues) {
			v.S3Policy.Fields["policy"] = encodedPolicy(strings.Replace(decodedPolicy(t, v.S3Policy.Fields["policy"]), "AKIATEST/", "AKIAOTHER/", 1))
		}, "other than the run's"},
	} {
		v := values()
		test.mutate(&v)
		if err := CheckEnvelope(terms, task, v); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: check = %v, want %q", name, err, test.want)
		}
	}
}

// decodedPolicy is a policy field's document; encodedPolicy is the field for a document.
func decodedPolicy(t *testing.T, field string) string {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(field)
	if err != nil {
		t.Fatal(err)
	}
	return string(decoded)
}

func encodedPolicy(document string) string {
	return base64.StdEncoding.EncodeToString([]byte(document))
}

func addPartition(t *testing.T, service *Service, topic string) {
	t.Helper()
	responses, err := kadm.NewClient(service.Operator).CreatePartitions(context.Background(), 1, topic)
	if err == nil {
		err = responses.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
}
