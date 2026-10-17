package main

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/receiver"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParseTerms(t *testing.T) {
	got, err := parseTerms("region=eu,arch=arm64")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"region": "eu", "arch": "arm64"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	if _, err := parseTerms("region=eu,region=us"); err == nil {
		t.Fatal("duplicate key accepted")
	}
}

func TestParseSelectorAlternatives(t *testing.T) {
	got, err := parseRequirements("runtime=exec|podman,nomad.node_pool=default")
	if err != nil {
		t.Fatal(err)
	}
	want := []wire.Requirement{
		{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec", "podman"}},
		{Key: "nomad.node_pool", Operator: wire.OpIn, Values: []string{"default"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestParseSelectorRejectsDuplicateKey(t *testing.T) {
	if _, err := parseRequirements("runtime=exec,runtime=podman"); err == nil {
		t.Fatal("duplicate key accepted")
	}
}

func TestApproveRequiresGrants(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runEnrol(context.Background(), nil, []string{"approve", "request", "--labels", "", "--selector", "region=eu"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "--grants") {
		t.Fatalf("got %v, want required grants error", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("produced output: %q", stdout.String())
	}
}

func TestNewAdaptersRejectsUnknownType(t *testing.T) {
	_, err := newAdapters([]receiver.AdapterConfig{{Name: "k8s", Type: "kubernetes", Config: jsontext.Value(`{}`)}}, "")
	if err == nil || !strings.Contains(err.Error(), `unknown type "kubernetes"`) {
		t.Fatalf("got %v, want unknown type error", err)
	}
}

func TestNewAdaptersRequiresCredentialEnv(t *testing.T) {
	config := jsontext.Value(`{"address":"http://127.0.0.1:1","namespace":"wherry","runtimes":{}}`)
	_, err := newAdapters([]receiver.AdapterConfig{{Name: "lab", Type: "nomad", Credential: "token", Config: config}}, "")
	if err == nil || !strings.Contains(err.Error(), "credential_env") {
		t.Fatalf("got %v, want credential_env required", err)
	}
	adapters, err := newAdapters([]receiver.AdapterConfig{{Name: "lab", Type: "nomad", Config: config}}, "")
	if err != nil || len(adapters) != 1 || adapters[0].Name != "lab" {
		t.Fatalf("adapters %#v, err %v", adapters, err)
	}
}

// enrolledReceiver returns a client whose bus holds an approved enrolment for receiver-a, and that receiver's signing key.
func enrolledReceiver(t *testing.T) (context.Context, *kgo.Client, *busclient.Client, ed25519.PrivateKey) {
	t.Helper()
	cluster := kfake.MustCluster(kfake.NumBrokers(1))
	t.Cleanup(cluster.Close)
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.FetchMaxWait(10*time.Millisecond),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.KeepControlRecords(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := wire.ApplyTopics(ctx, kadm.NewClient(kafka), 1); err != nil {
		t.Fatal(err)
	}
	_, userKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, receiverKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sealingKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	detected := []wire.DetectedAdapter{{Name: "lab", Platform: "nomad", Offers: []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}}}}
	request, err := trust.NewEnrolRequest("receiver-a", receiverKey, sealingKey, map[string]string{}, detected, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	grants := []wire.AdapterGrant{{Name: "lab", Platform: "nomad", Allowed: map[string][]string{"runtime": {"exec"}}, Defaults: map[string]string{}}}
	enrolRequest, err := trust.NewState(userKey.Public().(ed25519.PublicKey)).Apply(request)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := trust.Approve(request, enrolRequest.(*wire.EnrolRequest), userKey, map[string]string{}, "default", grants, []wire.Requirement{{Key: "region", Operator: wire.OpIn, Values: []string{"eu"}}}, "auto", 1, "", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := kafka.ProduceSync(ctx,
		&kgo.Record{Topic: "receivers.state", Key: []byte("receiver-a"), Value: request},
		&kgo.Record{Topic: "receivers.state", Key: []byte("receiver-a"), Value: approval},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return ctx, kafka, busclient.New(kafka, userKey, "replies.test"), receiverKey
}

func TestReceiversJSONOutputShape(t *testing.T) {
	ctx, _, client, _ := enrolledReceiver(t)
	var stdout, stderr bytes.Buffer
	if err := runReceivers(ctx, client, []string{"--json"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
	var rows []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	keys := make([]string, 0, len(rows[0]))
	for key := range rows[0] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	wantKeys := []string{"adapters", "capacity", "generation", "heartbeat_age", "in_flight", "labels", "offers", "pool", "receiver_id", "selector", "status"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("got JSON fields %v, want %v", keys, wantKeys)
	}
	if rows[0]["receiver_id"] != "receiver-a" || rows[0]["status"] != "silent" || rows[0]["pool"] != "default" {
		t.Fatalf("unexpected JSON row: %#v", rows[0])
	}
}

func TestAwaitPrintsNonTextPartPlaceholders(t *testing.T) {
	ctx, kafka, client, receiverKey := enrolledReceiver(t)
	if _, err := kadm.NewClient(kafka).CreateTopics(ctx, 1, 1, nil, "replies.test"); err != nil {
		t.Fatal(err)
	}
	reply := &wire.Reply{
		RunID: "run-a", Attempt: 1, TaskHash: strings.Repeat("a", 64), CorrelationID: "corr",
		State: wire.TaskStateCompleted, AtMS: time.Now().UnixMilli(),
		Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{
			{Text: "done"},
			{Raw: wire.StdBytes("hello"), MediaType: "text/plain"},
			{URL: "https://example.test/report.pdf"},
			{Data: jsontext.Value(`{"k":[1,2]}`)},
		}}},
	}
	payload, err := wire.MarshalPayload(wire.TypeReply, reply)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := wire.MarshalEnvelope(wire.Sign(wire.TypeReply, payload, receiverKey))
	if err != nil {
		t.Fatal(err)
	}
	if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: "replies.test", Key: []byte("run-a"), Value: envelope}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runAwait(ctx, client, []string{"run-a", "--timeout", "5s"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	want := "TASK_STATE_COMPLETED\ndone\n[raw text/plain 5 bytes]\n[url https://example.test/report.pdf]\n[data 11 bytes]\n"
	if stdout.String() != want {
		t.Fatalf("stdout %q, want %q", stdout.String(), want)
	}
}
