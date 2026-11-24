package client

import (
	json "encoding/json/v2"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
)

// policyRun submits a run with one expected output to pool-s3, claims it and returns it with its envelope's policy.
func policyRun(t *testing.T, f *fixture, receiver *testReceiver) (claimed, *signed.S3Policy) {
	t.Helper()
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Outputs: []wire.ExpectedOutput{{Name: "report.pdf"}},
		EndBy: time.Now().Add(time.Hour), StartBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if submitted.Pool != "pool-s3" {
		t.Fatalf("placed in %s", submitted.Pool)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	_, values, err := open(t, f, submitted.RunID, f.poolKeys["pool-s3"])
	if err != nil {
		t.Fatal(err)
	}
	if values.S3Policy == nil {
		t.Fatal("the envelope carries no policy")
	}
	return task, values.S3Policy
}

// Every storable run's envelope carries one POST policy: the bucket's URL, a SigV4 policy's fields with key the run's
// output prefix, and a document of exactly the bucket, the prefix, the 0 to 5 GiB range and the signing fields, expiring
// at task_ended_by plus the upload grace. The host records that it issued one, redacts its values under one name and
// logs none of them.
func TestThePolicyCoversTheRunsOutputPrefix(t *testing.T) {
	f, o, receiver := fakeStorageFixture(t)
	task, policy := policyRun(t, f, receiver)
	runID := task.submission.Wherry.Run
	if policy.URL != o.endpoint+"/"+testBucket+"/" {
		t.Fatalf("policy URL %s, want the bucket's", policy.URL)
	}
	if names := slices.Sorted(maps.Keys(policy.Fields)); !slices.Equal(names, []string{"bucket", "key", "policy", "x-amz-algorithm", "x-amz-credential", "x-amz-date", "x-amz-signature"}) {
		t.Fatalf("policy fields %v", names)
	}
	prefix := wire.S3OutputKey("tenant-a", runID, "")
	if policy.Fields["key"] != prefix || policy.Fields["bucket"] != testBucket || policy.Fields["x-amz-algorithm"] != operator.SigV4Algorithm || !strings.HasPrefix(policy.Fields["x-amz-credential"], "access/") {
		t.Fatalf("policy fields %+v; want the run's prefix, the bucket, SigV4 and the host's access key", policy.Fields)
	}
	endBy := time.Unix(task.submission.Wherry.TaskEndedBy, 0)
	expiration, conditions := policyDocument(t, policy)
	if !expiration.Equal(endBy.Add(operator.UploadGrace)) {
		t.Fatalf("the policy expires at %v, want task_ended_by %v plus the grace", expiration, endBy)
	}
	want := []string{
		`["eq","$bucket","` + testBucket + `"]`, `["starts-with","$key","` + prefix + `"]`, `["content-length-range", 0, ` + strconv.FormatInt(wire.MaxS3Object, 10) + `]`,
		`["eq","$x-amz-algorithm","` + operator.SigV4Algorithm + `"]`, `["eq","$x-amz-credential","` + policy.Fields["x-amz-credential"] + `"]`, `["eq","$x-amz-date","` + policy.Fields["x-amz-date"] + `"]`,
	}
	slices.Sort(want)
	slices.Sort(conditions)
	if !slices.Equal(conditions, want) {
		t.Fatalf("policy conditions %v, want %v", conditions, want)
	}
	// What the host checks before sealing is what a receiver checks after opening.
	state, err := f.service.LoadState(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.CheckEnvelope(state.Pools["pool-s3"], &task.submission, signed.EnvelopeValues{Credentials: map[string][]byte{"model": []byte("m")}, S3Inputs: map[string]string{}, S3Policy: policy, RedactionKey: make([]byte, 32)}); err != nil {
		t.Fatalf("the sealed policy fails CheckEnvelope: %v", err)
	}
	local, err := f.host.localRun(runID)
	if err != nil || local == nil || !local.Policy {
		t.Fatalf("local record %+v, %v; want the policy recorded", local, err)
	}
	if markers := f.host.markers(local); !slices.Contains(slices.Collect(maps.Values(markers)), signed.S3PolicyValue) {
		t.Fatalf("markers %v; want the policy's name", markers)
	}
	for _, field := range []string{"policy", "x-amz-signature", "x-amz-credential"} {
		if strings.Contains(f.logs.String(), policy.Fields[field]) {
			t.Fatalf("host logs hold the policy's %s:\n%s", field, f.logs.String())
		}
	}
}

// A result that lists no S3 output still has the run's prefix cleaned: the policy let the receiver create objects there
// whether or not it listed them.
func TestCollectCleansThePrefixOfAResultListingNothing(t *testing.T) {
	f, o, receiver := fakeStorageFixture(t)
	task, _ := policyRun(t, f, receiver)
	runID := task.submission.Wherry.Run
	stray := "/" + testBucket + "/" + wire.S3OutputKey("tenant-a", runID, "unlisted.bin")
	o.mu.Lock()
	o.objects[stray] = []byte("unlisted")
	o.mu.Unlock()
	receiver.feed(t, f.ctx, runID, receiver.manifestResult(t, task, 1, nil, nil), receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	result, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	collected, err := f.host.Collect(f.ctx, runID, result, t.TempDir())
	if err != nil || len(collected.Outputs) != 0 || len(collected.Rejected) != 0 {
		t.Fatalf("collected %+v, %v", collected, err)
	}
	if o.object(stray) != nil {
		t.Fatal("an object under the run's prefix survived the collect")
	}
}

// outputLists is a result's metadata with the three output lists as given.
func (r *testReceiver) outputLists(t *testing.T, task claimed, inline, stored []wire.ReturnedOutput, unreturned []wire.UnreturnedOutput) [][]byte {
	t.Helper()
	metadata, err := json.Marshal(map[string]any{"receiver_id": r.id, "pool": r.pool, "outputs": inline, "s3_outputs": stored, "not_returned": unreturned})
	if err != nil {
		t.Fatal(err)
	}
	return [][]byte{
		r.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) { result.Task.Metadata = metadata }),
		r.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil),
	}
}

// A result whose output lists are not a set this host writes is refused whole before anything is written: nothing
// goes on disk, no object is read, and the result carries one rejection naming the reason.
func TestAnInvalidResultIsRefusedBeforeAnyWrite(t *testing.T) {
	stored := func(runID, name string) wire.ReturnedOutput {
		return wire.ReturnedOutput{Name: name, SHA256: hexSum([]byte("bytes of " + name)), Size: int64(len("bytes of " + name)), Key: wire.S3OutputKey("tenant-a", runID, name)}
	}
	for name, test := range map[string]struct {
		lists func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput)
		want  string
	}{
		"257 names": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			var unreturned []wire.UnreturnedOutput
			for i := range 255 {
				unreturned = append(unreturned, wire.UnreturnedOutput{Name: "missing-" + strconv.Itoa(i), Reason: "missing"})
			}
			return []wire.ReturnedOutput{listed("a", "a")}, []wire.ReturnedOutput{stored(runID, "b.bin")}, unreturned
		}, "more than 256 outputs"},
		"duplicate across the lists": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			return []wire.ReturnedOutput{listed("a", "a")}, []wire.ReturnedOutput{stored(runID, "a")}, nil
		}, "listed twice"},
		"duplicate S3 output": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			return nil, []wire.ReturnedOutput{stored(runID, "b.bin"), stored(runID, "b.bin")}, nil
		}, "listed twice"},
		"an ancestor of another": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			return []wire.ReturnedOutput{listed("a", "a")}, []wire.ReturnedOutput{stored(runID, "a/b")}, nil
		}, "under another's name"},
		"a descendant listed first": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			return nil, []wire.ReturnedOutput{stored(runID, "a/b/c"), stored(runID, "a")}, nil
		}, "under another's name"},
		"reserved reply file": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			return nil, []wire.ReturnedOutput{stored(runID, wire.ReplyFile)}, nil
		}, "reserved name"},
		"reserved result": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			return []wire.ReturnedOutput{listed(wire.ResultArtifact, "r")}, nil, nil
		}, "reserved name"},
		"invalid name": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			return nil, []wire.ReturnedOutput{stored(runID, "../escape")}, nil
		}, "not a relative path"},
		"key outside the prefix": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			output := stored(runID, "b.bin")
			output.Key = wire.S3OutputKey("tenant-a", "run-other", "b.bin")
			return nil, []wire.ReturnedOutput{output}, nil
		}, "outside the run's prefix"},
		"key in the inputs": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			output := stored(runID, "b.bin")
			output.Key = wire.S3InputKey("tenant-a", runID, "b.bin")
			return nil, []wire.ReturnedOutput{output}, nil
		}, "outside the run's prefix"},
		"uppercase hash": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			output := stored(runID, "b.bin")
			output.SHA256 = strings.ToUpper(output.SHA256)
			return nil, []wire.ReturnedOutput{output}, nil
		}, "lowercase hex"},
		"size over 5 GiB": {func(runID string) ([]wire.ReturnedOutput, []wire.ReturnedOutput, []wire.UnreturnedOutput) {
			output := stored(runID, "b.bin")
			output.Size = wire.MaxS3Object + 1
			return nil, []wire.ReturnedOutput{output}, nil
		}, "0 to 5 GiB"},
	} {
		t.Run(name, func(t *testing.T) {
			f, o, receiver := fakeStorageFixture(t)
			task, _ := policyRun(t, f, receiver)
			runID := task.submission.Wherry.Run
			inline, s3, unreturned := test.lists(runID)
			o.mu.Lock()
			for _, output := range s3 {
				o.objects["/"+testBucket+"/"+output.Key] = []byte("bytes of " + output.Name)
			}
			o.mu.Unlock()
			var records [][]byte
			for _, output := range inline {
				records = append(records, receiver.outputChunk(t, task, 1, output.Name, 0, true, strings.Repeat(output.Name[:1], int(output.Size))))
			}
			receiver.feed(t, f.ctx, runID, append(records, receiver.outputLists(t, task, inline, s3, unreturned)...)...)
			result, err := f.host.Await(f.ctx, runID, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Outputs) != 0 || len(result.Stored) != 0 || len(result.NotReturned) != 0 || len(result.Rejected) != 1 ||
				result.Rejected[0].Name != wire.ResultArtifact || !strings.HasPrefix(result.Rejected[0].Reason, "rejected by this host: ") || !strings.Contains(result.Rejected[0].Reason, test.want) {
				t.Fatalf("outputs %+v, stored %+v, not returned %+v, rejected %+v; want the result refused for %q", result.Outputs, result.Stored, result.NotReturned, result.Rejected, test.want)
			}
			dir := filepath.Join(t.TempDir(), "outputs")
			collected, err := f.host.Collect(f.ctx, runID, result, dir)
			if err != nil || len(collected.Outputs) != 0 || len(collected.Rejected) != 1 {
				t.Fatalf("collected %+v, %v", collected, err)
			}
			if entries, err := os.ReadDir(dir); err == nil && len(entries) != 0 {
				t.Fatalf("the outputs directory holds %v; want nothing written", entries)
			}
			o.mu.Lock()
			defer o.mu.Unlock()
			if slices.ContainsFunc(o.ops, func(op string) bool { return strings.HasPrefix(op, "GET ") }) {
				t.Fatalf("ops %q; want no object read", o.ops)
			}
			if !strings.Contains(f.logs.String(), "result refused: ") {
				t.Fatalf("host logs:\n%s\nwant the refusal logged", f.logs.String())
			}
		})
	}
}
