package client

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// noCustodian stands in a pool's key so addReceiver publishes no snapshot for it.
var noCustodian = new(ecdsa.PrivateKey)

func (f *fixture) submitErr(t *testing.T) error {
	t.Helper()
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
	return err
}

// Submit refuses with a distinct reason when the pool has no snapshot, when the latest snapshot's custodian is silent
// or revoked, even with an earlier snapshot whose custodian is live, and when the snapshot's writer is not the custodian
// it names.
func TestSubmitRefusesWithoutALiveCurrentCustodian(t *testing.T) {
	t.Run("no snapshot", func(t *testing.T) {
		f := newFixture(t)
		f.poolKeys["pool-a"] = noCustodian
		f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
		if err := f.submitErr(t); !errors.Is(err, ErrNoSnapshot) {
			t.Fatalf("submit = %v, want %v", err, ErrNoSnapshot)
		}
	})
	t.Run("latest custodian silent", func(t *testing.T) {
		f := newFixture(t)
		f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
		f.addReceiver(t, "receiver-b", "pool-a").snapshot(t, f.ctx, func(snapshot *wire.Snapshot) { snapshot.Reason = "takeover" })
		if err := f.submitErr(t); !errors.Is(err, ErrCustodianUnavailable) || !strings.Contains(err.Error(), "custodian receiver-b is not live") {
			t.Fatalf("submit = %v, want %v for receiver-b; the earlier snapshot's live receiver-a must not stand in", err, ErrCustodianUnavailable)
		}
	})
	t.Run("latest custodian revoked", func(t *testing.T) {
		f := newFixture(t)
		f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
		f.addReceiver(t, "receiver-b", "pool-a").advertise(t, f.ctx, nil)
		f.end(t, "receiver-a")
		if err := f.submitErr(t); !errors.Is(err, ErrCustodianUnavailable) {
			t.Fatalf("submit = %v, want %v", err, ErrCustodianUnavailable)
		}
	})
	t.Run("written by another receiver", func(t *testing.T) {
		f := newFixture(t)
		f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
		other := f.addReceiver(t, "receiver-b", "pool-a")
		other.advertise(t, f.ctx, nil)
		key := f.poolKeys["pool-a"]
		forged, err := signed.Sign(wire.CustodianType, other.id, &wire.Snapshot{V: 1, Pool: "pool-a", Custodian: "receiver-a", Reason: "rotation",
			Current: wire.PoolKey{KeyID: signed.KeyThumbprint(&key.PublicKey), Key: jose.JSONWebKey{Key: &key.PublicKey}}}, other.key)
		if err != nil {
			t.Fatal(err)
		}
		other.produce(t, f.ctx, admin.CustodianTopic("pool-a"), wire.CustodianKey, forged)
		if err := f.submitErr(t); !errors.Is(err, ErrCustodianUnavailable) || !strings.Contains(err.Error(), "written by another receiver") {
			t.Fatalf("submit = %v, want the snapshot refused for its writer", err)
		}
	})
}

// A suspended pool refuses a submission before any credential command runs or any input uploads; once resumed, a
// submission is published.
func TestSuspendedPoolRefusesSubmitBeforeResolvingAnything(t *testing.T) {
	o := newUploadOracle(t)
	f := newFixture(t)
	terms := testTerms()
	terms.Storage = &admin.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	if err := f.service.PoolUpdate(f.ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	f.host.storage = o.config
	maps.Copy(f.host.credentials, o.sources)
	resolved := filepath.Join(t.TempDir(), "resolved")
	f.host.credentials["model"] = credential.Source{Command: []string{"sh", "-c", `echo >> "$0"; printf model-token`, resolved}}
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	if err := f.service.PoolSuspend(f.ctx, "pool-a", true); err != nil {
		t.Fatal(err)
	}
	submit := func() (SubmitResult, error) {
		file, _ := randomFile(t, "dataset.bin", 1024)
		return f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}},
			EndBy: time.Now().Add(time.Hour), StartBy: time.Now().Add(time.Minute)})
	}
	if _, err := submit(); err == nil || !strings.Contains(err.Error(), "pool pool-a is suspended") {
		t.Fatalf("submit = %v, want pool pool-a is suspended", err)
	}
	if _, err := os.Stat(resolved); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a credential command ran for a suspended pool: %v", err)
	}
	if len(o.ops) != 0 {
		t.Fatalf("storage requests %v for a suspended pool, want none", o.ops)
	}
	if err := f.service.PoolSuspend(f.ctx, "pool-a", false); err != nil {
		t.Fatal(err)
	}
	if submitted, err := submit(); err != nil || submitted.Pool != "pool-a" {
		t.Fatalf("submit after resume = %+v, %v", submitted, err)
	}
}

// open decodes a task as a receiver reads it from its pool's task topic, unwraps its submit wrap with key, opens its
// envelope and checks its binding against the task.
func open(t *testing.T, f *fixture, runID string, key *ecdsa.PrivateKey) (wire.Submission, signed.EnvelopeValues, error) {
	t.Helper()
	local, err := f.host.localRun(runID)
	if err != nil || local == nil {
		t.Fatalf("local record %v, %v", local, err)
	}
	pool := local.Pool
	records, err := readTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic(pool))
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if string(record.Key) != runID {
			continue
		}
		state, err := f.service.LoadState(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := admin.ReadKeys(f.ctx, f.service.Admin, admin.HostKeyTopics(state, pool))
		if err != nil {
			t.Fatal(err)
		}
		task, err := admin.AuthorizeSubmission(state, keys, record.Value, pool)
		if err != nil {
			t.Fatal(err)
		}
		// The host signs the task once, as any record.
		if resigned, err := wire.SignRecord(&task, f.host.key); err != nil || !bytes.Equal(resigned, record.Value) {
			t.Fatalf("the produced task is not its submission signed as a record: %v", err)
		}
		contentKey, err := signed.UnwrapContentKey([]byte(task.Wherry.Wrap), key)
		if err != nil {
			return task, signed.EnvelopeValues{}, err
		}
		values, err := signed.OpenEnvelope([]byte(task.Wherry.Envelope), contentKey)
		if err != nil {
			return task, values, err
		}
		binding := task.Wherry
		if want := (signed.EnvelopeBinding{Run: binding.Run, Tenant: binding.Tenant, Pool: binding.Pool, TaskStartedBy: binding.TaskStartedBy, TaskEndedBy: binding.TaskEndedBy}); values.Binding != want {
			t.Fatalf("envelope binding %+v, want the signed task's %+v", values.Binding, want)
		}
		return task, values, wire.CheckBinding(&task, values.Binding)
	}
	t.Fatalf("no task for run %s", runID)
	return wire.Submission{}, signed.EnvelopeValues{}, nil
}

// payload is a compact JWS's payload bytes.
func payload(t *testing.T, compact []byte) []byte {
	t.Helper()
	parts := strings.Split(string(compact), ".")
	decoded, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

// The produced task's envelope opens with the content key its submit wrap carries to the pool's current key, after a
// rotation, and holds the run's credentials and redaction key; a command source's child gets the task's
// task_ended_by.
func TestEnvelopeOpensWithTheCurrentKeyAndBindsTheTask(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	stale := f.poolKeys["pool-a"]
	current := receiver.snapshot(t, f.ctx, func(snapshot *wire.Snapshot) { snapshot.Reason = "rotation" })
	t.Setenv(credential.TaskEndByEnv, "inherited")
	f.host.credentials["model"] = credential.Source{Command: []string{"sh", "-c", "env | grep -c '^" + credential.TaskEndByEnv + "='; printenv " + credential.TaskEndByEnv}}
	submitted := f.submit(t, defaultSelector())
	task, values, err := open(t, f, submitted.RunID, current)
	if err != nil {
		t.Fatalf("envelope does not open with the current key: %v", err)
	}
	if want := "1\n" + wire.RFC3339(task.Wherry.TaskEndedBy) + "\n"; string(values.Credentials["model"]) != want || len(values.Credentials) != 1 {
		t.Fatalf("credentials %q, want model %q", values.Credentials, want)
	}
	local, err := f.host.localRun(submitted.RunID)
	if err != nil || local == nil || string(local.RedactionKey) != string(values.RedactionKey) {
		t.Fatalf("local record %+v, %v; want the envelope's redaction key", local, err)
	}
	if local.Task == nil || local.Task.Wherry.Envelope != "" || local.Task.Message.MessageID != task.Message.MessageID {
		t.Fatalf("template %+v; want the task without its envelope", local.Task)
	}
	state, err := f.service.LoadState(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.CheckEnvelope(state.Pools["pool-a"], &task, values); err != nil {
		t.Fatalf("receiver check of the envelope: %v", err)
	}
	if _, _, err := open(t, f, submitted.RunID, stale); err == nil {
		t.Fatal("the envelope opens with the key a rotation replaced")
	}
}

// The host refuses to build an envelope the receiver's check would refuse: an empty credential.
func TestSubmitRefusesAnEnvelopeTheCheckRefuses(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	f.host.credentials["model"] = credential.Source{File: empty}
	if err := f.submitErr(t); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "envelope credential model is empty") {
		t.Fatalf("submit = %v, want the empty credential refused", err)
	}
	if records, err := readTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic("pool-a")); err != nil || len(records) != 0 {
		t.Fatalf("task topic holds %d records, %v", len(records), err)
	}
}

// A local record write is temp, fsync, rename, directory fsync: a failed file sync leaves the old record whole, and a
// failed directory sync after the rename leaves the new one whole.
func TestLocalRecordWritesAreSynced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs", "run-a.json")
	if err := writeState(path, runRecord{Run: "run-a", Pool: "old"}); err != nil {
		t.Fatal(err)
	}
	previous := fsync
	t.Cleanup(func() { fsync = previous })
	var synced []string
	fail := -1
	fsync = func(file *os.File) error {
		synced = append(synced, file.Name())
		if len(synced)-1 == fail {
			return errors.New("injected sync failure")
		}
		return previous(file)
	}
	read := func() runRecord {
		t.Helper()
		var record runRecord
		if err := readState(path, &record); err != nil {
			t.Fatalf("record torn: %v", err)
		}
		return record
	}
	fail = 0
	if err := writeState(path, runRecord{Run: "run-a", Pool: "new-" + strings.Repeat("x", 1<<16)}); err == nil {
		t.Fatal("write succeeded through a failed file sync")
	}
	if record := read(); record.Pool != "old" {
		t.Fatalf("record %q after a failed file sync; want the old one", record.Pool)
	}
	synced, fail = nil, 1
	if err := writeState(path, runRecord{Run: "run-a", Pool: "new"}); err == nil {
		t.Fatal("write succeeded through a failed directory sync")
	}
	if record := read(); record.Pool != "new" {
		t.Fatalf("record %q after the rename; want the new one whole", record.Pool)
	}
	if len(synced) != 2 || synced[1] != filepath.Dir(path) {
		t.Fatalf("synced %v; want the temp file, then the directory", synced)
	}
	if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 1 {
		t.Fatalf("directory holds %d entries; want the record alone", len(entries))
	}
}

// keyLost ends a run's first attempt FAILED with the wherry event key-lost, as a receiver whose open was refused key
// unavailable does.
func (r *testReceiver) keyLost(t *testing.T, ctx context.Context, task claimed) {
	t.Helper()
	r.feed(t, ctx, task.submission.Wherry.Run, r.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateFailed, func(status *wire.RunStatus) {
		status.Wherry.Event = wire.EventKeyLost
	}))
}

// replacements are the tasks on pool's task topic that replace runID.
func (f *fixture) replacements(t *testing.T, pool, runID string) [][]byte {
	t.Helper()
	records, err := readTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic(pool))
	if err != nil {
		t.Fatal(err)
	}
	var found [][]byte
	for _, record := range records {
		var task wire.Submission
		if err := json.Unmarshal(payload(t, record.Value), &task, signed.Tolerant); err != nil {
			t.Fatal(err)
		}
		if task.Wherry.Replaces == runID {
			found = append(found, record.Value)
		}
	}
	return found
}

// A run that ended key-lost before its task_started_by is replaced from its template: the same message content,
// placement and bounds under a new run, replacing it with its own S3 input run; status reports the replacement and wait
// follows it.
func TestAKeyLostRunIsResubmitted(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.keyLost(t, f.ctx, task)
	status, err := f.host.Status(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	replaced := f.replacements(t, "pool-a", runID)
	if len(replaced) != 1 || status.Replacement == "" {
		t.Fatalf("%d replacements, status names %q; want one", len(replaced), status.Replacement)
	}
	replacement, values, err := open(t, f, status.Replacement, f.poolKeys["pool-a"])
	if err != nil {
		t.Fatal(err)
	}
	binding, original := replacement.Wherry, task.submission.Wherry
	if binding.Run != status.Replacement || binding.Replaces != runID || binding.S3InputRun != runID || binding.TaskStartedBy != original.TaskStartedBy ||
		binding.TaskEndedBy != original.TaskEndedBy || !reflect.DeepEqual(binding.Placement.Selector, original.Placement.Selector) || binding.Placement.Agent != original.Placement.Agent {
		t.Fatalf("replacement binding %+v; original %+v", binding, original)
	}
	message := replacement.Message
	if message.TaskID != binding.Run || message.ContextID != binding.Run || message.MessageID == task.submission.Message.MessageID || message.Parts[0].Text != "do the work" {
		t.Fatalf("replacement message %+v", message)
	}
	if string(values.Credentials["model"]) != "model-token" {
		t.Fatalf("replacement credentials %q", values.Credentials)
	}
	echo := receiver.claim(t, f.ctx, binding.Run)[0]
	receiver.feed(t, f.ctx, binding.Run, receiver.echo(t, echo), receiver.result(t, echo, 1, wire.TaskStateCompleted, nil), receiver.terminal(t, echo, 1, wire.TaskStateCompleted, 0))
	awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted || !slices.Equal(awaited.Runs, []string{runID, binding.Run}) {
		t.Fatalf("await %+v, %v; want the replacement's outcome and both runs", awaited, err)
	}
}

// A run that failed without key-lost, such as one whose envelope the custodian refused as invalid, or ended key-lost
// after its task_started_by, is not resubmitted.
func TestOnlyAKeyLostRunBeforeItsStartBoundIsResubmitted(t *testing.T) {
	f := newFixture(t)
	receiver, failed := f.admitted(t)
	receiver.feed(t, f.ctx, failed.submission.Wherry.Run, receiver.status(t, failed, 1, "terminal-1", "terminal", wire.TaskStateFailed, func(status *wire.RunStatus) {
		status.StatusUpdate.Status.Message = &wire.Message{MessageID: "msg-invalid", Role: wire.RoleAgent, Parts: []wire.Part{{Text: wire.RefusedEnvelopeInvalid}}, Extensions: []string{wire.ExtensionURI}}
	}))
	late := f.anotherRun(t, receiver)
	local, err := f.host.localRun(late.submission.Wherry.Run)
	if err != nil {
		t.Fatal(err)
	}
	local.TaskStartedBy = time.Now().Add(-time.Minute).Unix()
	if err := f.host.writeRun(*local); err != nil {
		t.Fatal(err)
	}
	receiver.keyLost(t, f.ctx, late)
	for _, task := range []claimed{failed, late} {
		runID := task.submission.Wherry.Run
		if replacement, err := f.host.resubmit(f.ctx, runID, false); err != nil || replacement != "" {
			t.Fatalf("resubmit %s = %q, %v; want none", runID, replacement, err)
		}
		if local, err := f.host.localRun(runID); err != nil || local.Replacement != "" {
			t.Fatalf("run %s records replacement %q, %v", runID, local.Replacement, err)
		}
		if replaced := f.replacements(t, "pool-a", runID); len(replaced) != 0 {
			t.Fatalf("run %s has %d replacements", runID, len(replaced))
		}
	}
}

// A crash after the replacement's bytes are stored and before they are produced leaves them to the next pass, which
// produces exactly those bytes and resolves no credential again.
func TestAStoredReplacementIsProducedAsStored(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	resolved := filepath.Join(t.TempDir(), "resolved")
	f.host.credentials["model"] = credential.Source{Command: []string{"sh", "-c", `echo >> "$0"; printf model-token`, resolved}}
	receiver.keyLost(t, f.ctx, task)
	previous := produceReplacement
	t.Cleanup(func() { produceReplacement = previous })
	produceReplacement = func(context.Context, *Host, wire.SubmissionBinding, []byte) error {
		return errors.New("injected crash")
	}
	if _, err := f.host.resubmit(f.ctx, runID, false); err == nil {
		t.Fatal("resubmit succeeded through a failed write")
	}
	local, err := f.host.localRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := f.host.localRun(local.Replacement)
	if err != nil || stored == nil || len(stored.Signed) == 0 || stored.Produced {
		t.Fatalf("replacement record %+v, %v; want its bytes stored, not produced", stored, err)
	}
	if replaced := f.replacements(t, "pool-a", runID); len(replaced) != 0 {
		t.Fatalf("%d replacements produced through a failed write", len(replaced))
	}
	before, _ := os.ReadFile(resolved)
	produceReplacement = previous
	if _, err := f.host.Status(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	replaced := f.replacements(t, "pool-a", runID)
	if len(replaced) != 1 || string(replaced[0]) != string(stored.Signed) {
		t.Fatalf("%d replacements; want the stored bytes once", len(replaced))
	}
	if after, _ := os.ReadFile(resolved); len(after) != len(before) {
		t.Fatalf("credentials resolved %d times before and %d after; want no new resolution", len(before), len(after))
	}
	if produced, err := f.host.localRun(local.Replacement); err != nil || !produced.Produced {
		t.Fatalf("replacement record %+v, %v; want it marked produced", produced, err)
	}
}

// Wait refuses a chain of replacements that names a run twice.
func TestWaitRefusesAReplacementCycle(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	other := f.anotherRun(t, receiver)
	first, second := task.submission.Wherry.Run, other.submission.Wherry.Run
	for run, next := range map[string]string{first: second, second: first} {
		local, err := f.host.localRun(run)
		if err != nil {
			t.Fatal(err)
		}
		local.Replacement, local.Signed, local.Produced = next, []byte("signed"), true
		if err := f.host.writeRun(*local); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.host.Await(f.ctx, first, 2*time.Second); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("await = %v, want the cycle refused", err)
	}
}

// A replacement reads its S3 inputs from the root run's prefix, and cleanup keeps that prefix until the replacement
// has ended.
func TestAReplacementReadsTheRootRunsInputs(t *testing.T) {
	o, f, receiver, task := keyLostS3Run(t)
	root := task.submission.Wherry.Run
	replacementID, err := f.host.resubmit(f.ctx, root, false)
	if err != nil || replacementID == "" {
		t.Fatalf("resubmit = %q, %v", replacementID, err)
	}
	replacement, values, err := open(t, f, replacementID, f.poolKeys["pool-a"])
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Wherry.S3InputRun != root || !strings.Contains(values.S3Inputs["dataset.bin"], "/"+testBucket+"/"+wire.S3InputKey("tenant-a", root, "dataset.bin")+"?") ||
		!strings.Contains(values.S3Outputs["report.pdf"], "/"+testBucket+"/"+wire.S3OutputKey("tenant-a", replacementID, "report.pdf")+"?") {
		t.Fatalf("replacement inputs from %q at %q, outputs at %q", replacement.Wherry.S3InputRun, values.S3Inputs, values.S3Outputs)
	}
	input := "/" + testBucket + "/" + wire.S3InputKey("tenant-a", root, "dataset.bin")
	if err := f.host.Cleanup(f.ctx, root); !errors.Is(err, ErrInvalidInput) || o.object(input) == nil {
		t.Fatalf("cleanup of the root while its replacement runs = %v; want its inputs kept", err)
	}
	next := receiver.claim(t, f.ctx, replacementID)[0]
	receiver.feed(t, f.ctx, replacementID, receiver.echo(t, next), receiver.status(t, next, 1, "rejected-1", "rejected", wire.TaskStateRejected, nil))
	if err := f.host.Cleanup(f.ctx, replacementID); err != nil {
		t.Fatal(err)
	}
	if o.object(input) != nil {
		t.Fatal("cleanup of the ended replacement left the root's inputs")
	}
}

// keyLostS3Run submits a run with the S3 input dataset.bin and the S3 output report.pdf, and ends it key-lost after
// receiver-a claims it.
func keyLostS3Run(t *testing.T) (*uploadOracle, *fixture, *testReceiver, claimed) {
	t.Helper()
	o := newUploadOracle(t)
	f := newFixture(t)
	terms := testTerms()
	terms.Storage = &admin.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	if err := f.service.PoolUpdate(f.ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	f.host.storage = o.config
	maps.Copy(f.host.credentials, o.sources)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	file, _ := randomFile(t, "dataset.bin", 1024)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}},
		Outputs: []wire.ExpectedOutput{{Name: "report.pdf", Storage: wire.StorageS3}}, EndBy: time.Now().Add(time.Hour), StartBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	receiver.keyLost(t, f.ctx, task)
	return o, f, receiver, task
}

// Cleanup refuses a run that ended key-lost before its task_started_by, and a run whose last replacement did, and keeps
// the root run's inputs.
func TestCleanupKeepsAResubmittableRunsInputs(t *testing.T) {
	o, f, receiver, task := keyLostS3Run(t)
	root := task.submission.Wherry.Run
	input := "/" + testBucket + "/" + wire.S3InputKey("tenant-a", root, "dataset.bin")
	until := "resubmittable until " + wire.RFC3339(task.submission.Wherry.TaskStartedBy)
	if err := f.host.Cleanup(f.ctx, root); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "run "+root+" lost its pool key and is "+until) || o.object(input) == nil {
		t.Fatalf("cleanup of a key-lost run before its resubmission = %v; want it refused and its inputs kept", err)
	}
	replacementID, err := f.host.resubmit(f.ctx, root, true)
	if err != nil || replacementID == "" {
		t.Fatalf("resubmit = %q, %v", replacementID, err)
	}
	receiver.keyLost(t, f.ctx, receiver.claim(t, f.ctx, replacementID)[0])
	for _, run := range []string{root, replacementID} {
		if err := f.host.Cleanup(f.ctx, run); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "run "+replacementID+" lost its pool key and is "+until) || o.object(input) == nil {
			t.Fatalf("cleanup of %s with its last replacement key-lost = %v; want it refused and the inputs kept", run, err)
		}
	}
}

// One submit judges every pending run from one read of the tenant feed and resubmits only the run that ended key-lost.
func TestResubmissionReadsTheFeedOnce(t *testing.T) {
	f := newFixture(t)
	receiver, lost := f.admitted(t)
	pending := []claimed{lost, f.anotherRun(t, receiver), f.anotherRun(t, receiver)}
	receiver.keyLost(t, f.ctx, lost)
	reads := f.failListings(t, admin.TenantTopic("tenant-a"), startListing, 0, 0)
	if err := f.submitErr(t); err != nil {
		t.Fatal(err)
	}
	if n := reads.Load(); n != 1 {
		t.Fatalf("submit read the tenant feed %d times with %d pending runs; want once", n, len(pending))
	}
	for i, task := range pending {
		want := 0
		if i == 0 {
			want = 1
		}
		if replaced := f.replacements(t, "pool-a", task.submission.Wherry.Run); len(replaced) != want {
			t.Fatalf("pending run %d has %d replacements; want %d", i, len(replaced), want)
		}
	}
}

// Wait on a run that ended key-lost and whose resubmission fails returns the resubmission's error with the bound until
// which the run is resubmittable.
func TestWaitReportsAFailedResubmission(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	receiver.keyLost(t, f.ctx, task)
	injected := errors.New("injected write failure")
	previous := produceReplacement
	t.Cleanup(func() { produceReplacement = previous })
	produceReplacement = func(context.Context, *Host, wire.SubmissionBinding, []byte) error { return injected }
	result, err := f.host.Await(f.ctx, task.submission.Wherry.Run, 5*time.Second)
	if !errors.Is(err, injected) || !strings.Contains(err.Error(), "resubmittable until "+wire.RFC3339(task.submission.Wherry.TaskStartedBy)) || result.Status.State != wire.TaskStateFailed {
		t.Fatalf("await = %+v, %v; want the outcome with the resubmission's error and bound", result, err)
	}
}

// A resubmission's lock file is removed when it is done and never made for a run without a local record, and the
// sweep removes one no process holds.
func TestResubmitLockFiles(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	receiver.keyLost(t, f.ctx, task)
	locks := filepath.Join(f.stateDir, "resubmit")
	if replacement, err := f.host.resubmit(f.ctx, "run-unknown", true); err != nil || replacement != "" {
		t.Fatalf("resubmit of an unknown run = %q, %v", replacement, err)
	}
	if _, err := os.Stat(locks); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resubmit of an unknown run made the lock directory: %v", err)
	}
	if replacement, err := f.host.resubmit(f.ctx, task.submission.Wherry.Run, true); err != nil || replacement == "" {
		t.Fatalf("resubmit = %q, %v", replacement, err)
	}
	if entries, err := os.ReadDir(locks); err != nil || len(entries) != 0 {
		t.Fatalf("lock directory holds %d entries, %v; want none", len(entries), err)
	}
	stale := filepath.Join(locks, "run-stale.json.lock")
	if err := os.WriteFile(stale, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	f.host.sweep()
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sweep left a stale lock file: %v", err)
	}
}

// The snapshot read takes the latest record under the snapshot key, as compaction keeps it; a later record under another
// key does not stand in.
func TestTheSnapshotIsTheLatestRecordUnderItsKey(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	receiver.produce(t, f.ctx, admin.CustodianTopic("pool-a"), "other", []byte("not a snapshot"))
	if err := f.submitErr(t); err != nil {
		t.Fatalf("submit = %v; want the snapshot under its key", err)
	}
}

// helperConfig is what TestResubmitHelperProcess, a second host process, runs resubmit with.
type helperConfig struct {
	Broker, Principal, StateDir, Secret, Run, Barrier string
	Key                                               []byte
}

// Two host processes on one state directory that see a run end key-lost write one replacement: the lock is the
// state directory's, not a process's.
func TestTwoHostProcessesReplaceAKeyLostRunOnce(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.keyLost(t, f.ctx, task)
	config := helperConfig{Broker: f.service.Broker, Principal: admin.TenantPrincipal("tenant-a", "host-a"), StateDir: f.stateDir, Secret: f.secret, Run: runID,
		Barrier: t.TempDir(), Key: f.host.key}
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "helper.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	outputs := make([][]byte, 2)
	var wait sync.WaitGroup
	for i := range outputs {
		wait.Go(func() {
			command := exec.CommandContext(f.ctx, os.Args[0], "-test.run=^TestResubmitHelperProcess$", "-test.v")
			command.Env = append(os.Environ(), "WHERRY_TEST_RESUBMIT_HELPER="+path)
			outputs[i], _ = command.CombinedOutput()
		})
	}
	wait.Wait()
	var replacements []string
	for _, output := range outputs {
		_, after, ok := strings.Cut(string(output), "replacement=")
		if !ok {
			t.Fatalf("helper output:\n%s", output)
		}
		replacements = append(replacements, strings.Fields(after)[0])
	}
	if replacements[0] != replacements[1] {
		t.Fatalf("processes replaced the run with %v; want one replacement", replacements)
	}
	if replaced := f.replacements(t, "pool-a", runID); len(replaced) != 1 {
		t.Fatalf("%d replacements on the task topic; want one", len(replaced))
	}
}

// TestResubmitHelperProcess is the second host process of TestTwoHostProcessesReplaceAKeyLostRunOnce. Each process
// that reaches the point of recording a new replacement waits there for the other, so two processes inside the lock at
// once both record one.
func TestResubmitHelperProcess(t *testing.T) {
	path := os.Getenv("WHERRY_TEST_RESUBMIT_HELPER")
	if path == "" {
		t.Skip("a helper process of TestTwoHostProcessesReplaceAKeyLostRunOnce")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config helperConfig
	if err := json.Unmarshal(encoded, &config); err != nil {
		t.Fatal(err)
	}
	kafka, err := kgo.NewClient(kgo.SeedBrokers(config.Broker), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism()),
		kgo.FetchMaxWait(10*time.Millisecond), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.DisableClientMetrics())
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHost(kafka, ed25519.PrivateKey(config.Key), config.Principal, config.StateDir, map[string]credential.Source{"model": {File: config.Secret}}, nil, log.New(os.Stderr, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	var arrived atomic.Bool
	replacing = func(string) {
		os.WriteFile(filepath.Join(config.Barrier, fmt.Sprint(os.Getpid())), nil, 0o600)
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline) && !arrived.Load(); time.Sleep(10 * time.Millisecond) {
			if entries, _ := os.ReadDir(config.Barrier); len(entries) > 1 {
				arrived.Store(true)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	replacement, err := h.resubmit(ctx, config.Run, false)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("replacement=%s\n", replacement)
}
