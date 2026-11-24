package view

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"io"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/wire"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const sentinel = "s3cr3t"

// Rule 1: a sentinel planted in every record and span field the view reads, where the wire lets one through, reaches
// no response, no stream event, no error body and no log line. Each planted record is folded, so the check is of the
// projection, not of a refusal: the names show as the placeholder. The output names planted are outside the output
// name grammar, the one text a result shows when it is within it.
func TestNoRecordStringReachesAnySurface(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-10 * time.Minute)
	stamp := at.UTC().Format(time.RFC3339Nano)
	hostS, name := sentinel+"-host", "planted"
	run := runID(tenantA, hostS, name)
	// An echo naming a host, agent and model the configuration does not.
	echo := &wire.Echo{Receiver: receiverA, Tenant: tenantA, Host: hostS, Pool: poolA, Run: run, RunNonce: nonce(name), TaskHash: hashOf(run), MessageID: sentinel + "-message",
		Placement: wire.Placement{Agent: sentinel + "-agent", Model: sentinel + " model text", Selector: []wire.Requirement{{Key: sentinel + "-key", Operator: "In", Values: []string{sentinel + "-value"}}}}}
	f.produce(operator.TenantTopic(tenantA), run, f.sign(echo, f.keys[receiverA]), at)
	// A claimed status with a free event ID, a task topic of an unconfigured agent, and metadata.
	offset := int64(3)
	claimed := &wire.RunStatus{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: run, ContextID: run, Status: wire.TaskStatus{State: wire.TaskStateWorking, Timestamp: stamp}, Metadata: jsontext.Value(`{"` + sentinel + `-meta":"` + sentinel + `-value"}`)},
		Wherry: wire.StatusBinding{ReceiverBinding: wire.ReceiverBinding{Receiver: receiverA, Tenant: tenantA, Pool: poolA, Run: run, Attempt: 1}, EventID: sentinel + "-event", Kind: "claimed", TaskHash: hashOf(run),
			TaskTopic: "wherry.pool." + poolA + ".tasks." + sentinel + "." + hashOf("topic"), TaskOffset: &offset},
	}
	claim := f.sign(claimed, f.keys[receiverA])
	f.produce(operator.PoolEventsTopic(poolA), run, claim, at.Add(time.Second))
	f.produce(operator.TenantTopic(tenantA), run, claim, at.Add(time.Second))
	f.status(at.Add(2*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "working", state: wire.TaskStateWorking, text: sentinel + "-working-line", eventID: sentinel + "-working"})
	f.decision(at.Add(2*time.Second), receiverA, poolA, run, wire.Digest(claim), sentinel+"-previous", wire.ClaimUnchanged, true)
	request := f.openRequest(at.Add(3*time.Second), receiverA, poolA, run, 1)
	f.produce(operator.PoolEventsTopic(poolA), run, f.sign(&wire.AttemptWrap{Custodian: receiverA, TaskHash: hashOf(run), Request: request, Receiver: sentinel + "-receiver", Attempt: 1, JWE: sentinel + "-jwe"}, f.keys[receiverA]), at.Add(4*time.Second))
	f.status(at.Add(5*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateFailed, text: sentinel + "-failure", eventID: sentinel + "-terminal"})
	// A result whose metadata, message, artifact and history carry the sentinel, output names included.
	metadata := wire.ResultMetadata{ReceiverID: receiverA, Pool: poolA, Profile: sentinel + "-profile", Labels: map[string]string{sentinel + "-label-key": sentinel + "-label-value"}, Job: sentinel + "-job",
		Outputs: []wire.ReturnedOutput{{Name: sentinel + "/../output.txt", SHA256: hashOf("out"), Size: 3, Key: sentinel + "-s3-key"}}, S3Outputs: []wire.ReturnedOutput{{Name: "/" + sentinel + "-big.tar", SHA256: hashOf("big"), Size: 9, Key: sentinel + "-s3-key"}},
		NotReturned: []wire.UnreturnedOutput{{Name: "<" + sentinel + ">", Reason: sentinel + "-reason"}}, Reply: &wire.ReturnedReply{SHA256: hashOf("reply"), Size: 70000}}
	encoded := must(t)(json.Marshal(metadata))
	encoded = append(encoded[:len(encoded)-1], []byte(`,"`+sentinel+`-extra":"`+sentinel+`-value"}`)...)
	message := func(id, text string) *wire.Message {
		return &wire.Message{MessageID: id, Role: wire.RoleAgent, Parts: []wire.Part{{Text: text}}, Extensions: []string{wire.ExtensionURI}}
	}
	result := &wire.Result{
		Task: wire.Task{ID: run, ContextID: run, Status: wire.TaskStatus{State: wire.TaskStateFailed, Timestamp: stamp, Message: message(sentinel+"-reply-id", sentinel+"-reply-text")},
			Artifacts: []wire.Artifact{{ArtifactID: wire.ResultArtifact, Name: sentinel + "-artifact-name", Parts: []wire.Part{{Text: sentinel + "-artifact-text"}}, Extensions: []string{wire.ExtensionURI}}},
			History:   []wire.Message{*message(sentinel+"-history-id", sentinel+"-history")}, Metadata: encoded},
		Wherry: wire.ResultBinding{ReceiverBinding: wire.ReceiverBinding{Receiver: receiverA, Tenant: tenantA, Pool: poolA, Run: run, Attempt: 1}, TaskHash: hashOf(run)},
	}
	f.produce(operator.TenantTopic(tenantA), run, f.sign(result, f.keys[receiverA]), at.Add(5*time.Second))
	f.startFailureWith(at.Add(6*time.Second), receiverB, tenantA, poolA, run, 2, "dispatch", sentinel+"-profile", sentinel+"-agent", hashOf(run))
	// An advertisement offering unconfigured profiles and agents, a progress record naming an unknown tenant, and
	// one keyed by no run ID.
	ad := &wire.Advertisement{Receiver: receiverB, Pool: poolA, Offers: []wire.Offer{{Profile: sentinel + "-offer", Agent: sentinel + "-agent", Replicas: 1}, {Profile: "profile-a", Agent: sentinel + "-agent", Replicas: 1}}, Capacity: 2, InFlight: 1, AtMS: f.now.UnixMilli()}
	f.produce(operator.PoolReceiversTopic(poolA), receiverB, f.sign(ad, f.keys[receiverB]), f.now)
	f.produce(operator.ReceiverProgressTopic(receiverB), run, must(t)(wire.EncodeProgress(wire.Progress{Tenant: sentinel + "-tenant", TaskHash: hashOf(run), Attempt: 2, LogOffset: 1})), at.Add(6*time.Second))
	f.produce(operator.ReceiverProgressTopic(receiverB), sentinel+"-run", must(t)(wire.EncodeProgress(wire.Progress{Tenant: tenantA, TaskHash: hashOf(run), Attempt: 1, LogOffset: 1})), at.Add(6*time.Second))
	// A span of the run carrying the sentinel in its name, service, every allowed attribute key, an unknown key, its
	// status, an event and a link.
	tid := id(16)
	s := span(tid, id(8), nil, sentinel+"-span-name", run, at)
	for _, key := range []string{string(telemetry.AttrHost), string(telemetry.AttrPool), string(telemetry.AttrReceiver), string(telemetry.AttrProfile), string(telemetry.AttrAgent), string(telemetry.AttrModel),
		string(telemetry.AttrCredential), string(telemetry.AttrCredentialSource), string(telemetry.AttrOutput), string(telemetry.AttrStorageEndpoint), string(telemetry.AttrStorageBucket), string(telemetry.AttrSystemName),
		"messaging.operation.type", "messaging.operation.name", "messaging.destination.name", "messaging.consumer.group.name", sentinel + ".key"} {
		s.Attributes = append(s.Attributes, str(key, sentinel+"-"+key))
	}
	s.Status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: sentinel + "-status"}
	s.Events = []*tracepb.Span_Event{{Name: sentinel + "-span-event", Attributes: []*commonpb.KeyValue{str(sentinel+".k", sentinel+"-v")}}}
	s.Links = []*tracepb.Span_Link{{TraceId: id(16), SpanId: id(8), Attributes: []*commonpb.KeyValue{str(sentinel+".k", sentinel+"-v")}}}
	f.traceRecord(at.Add(7*time.Second), sentinel+"-service", s)

	served := serveFixture(t, f, ServeOptions{})
	port := strconv.Itoa(served.server.Port())
	cookie := served.session(t)
	// Hostile requests: a free method, path, query, header, origin, host, cookie and token.
	bodies := map[string]string{}
	for _, c := range []struct {
		method, path string
		headers      map[string]string
		cookie       *http.Cookie
	}{
		{sentinel + "-method", "/", nil, nil},
		{http.MethodGet, "/" + sentinel + "-path?" + sentinel + "=1", nil, cookie},
		{http.MethodGet, "/api/runs/" + sentinel + "-run", map[string]string{"X-" + sentinel: sentinel}, cookie},
		{http.MethodGet, "/api/model", map[string]string{"Origin": "http://" + sentinel + ".example"}, cookie},
		{http.MethodGet, "/api/model", map[string]string{"Host": sentinel + ".example:" + port}, cookie},
		{http.MethodGet, "/api/model", nil, &http.Cookie{Name: cookie.Name, Value: sentinel + "-cookie"}},
		{http.MethodPost, "/session?" + sentinel, map[string]string{"Content-Type": "application/json"}, nil},
	} {
		var body io.Reader
		if c.method == http.MethodPost {
			body = strings.NewReader(`{"token":"` + sentinel + `-token"}`)
		}
		response, text := served.request(t, c.method, c.path, body, c.cookie, c.headers)
		bodies[c.method+" "+c.path] = text
		if response.StatusCode == http.StatusOK {
			t.Errorf("%s %s answered 200", c.method, c.path)
		}
	}
	// Every surface: the model, every run's detail and trace, the stream's first event and the log.
	_, model := served.request(t, http.MethodGet, "/api/model", nil, cookie, nil)
	bodies["model"] = model
	var snapshot Snapshot
	if err := json.Unmarshal([]byte(model), &snapshot); err != nil {
		t.Fatal(err)
	}
	for _, r := range snapshot.Runs {
		_, bodies["run "+r.ID] = served.request(t, http.MethodGet, "/api/runs/"+r.ID, nil, cookie, nil)
		_, bodies["trace "+r.ID] = served.request(t, http.MethodGet, "/api/runs/"+r.ID+"/trace", nil, cookie, nil)
	}
	bodies["event"] = served.firstEvent(t, "/api/stream", cookie)
	bodies["log"] = served.logs.String()
	forest, err := served.v.traces.Trace(f.ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	bodies["forest"] = forest.String()
	for surface, body := range bodies {
		if strings.Contains(body, sentinel) {
			t.Errorf("%s carries the sentinel: %.400s", surface, body[max(strings.Index(body, sentinel)-120, 0):])
		}
	}
	// The planted records were folded: each name shows as the placeholder, the event IDs as derived ones.
	r := summaryOf(t, snapshot, run)
	if r.Host != Unknown || r.Agent != Unknown || r.Model != Unknown || !r.Terminal || r.Class != ClassFailed {
		t.Errorf("planted echo and statuses: %+v", r)
	}
	d, _ := served.v.RunDetail(run)
	derived := regexp.MustCompile(`^event-[0-9a-f]{32}$`)
	if len(d.Attempts) != 2 || d.Attempts[0].TaskTopic != Unknown || len(d.Attempts[0].Events) != 3 || !derived.MatchString(d.Attempts[0].Events[0].EventID) ||
		d.Attempts[0].Decision == nil || d.Attempts[0].Decision.Previous != Unknown || d.Attempts[1].StartFailure == nil || d.Attempts[1].StartFailure.Profile != Unknown || d.Attempts[1].StartFailure.Agent != Unknown {
		t.Errorf("planted attempts: %+v", d.Attempts)
	}
	if d.Result == nil || d.Result.Profile != Unknown || len(d.Result.Outputs) != 1 || d.Result.Outputs[0].SHA256 != hashOf("out") || d.Result.Outputs[0].Name != InvalidName || len(d.Result.S3Outputs) != 1 || d.Result.S3Outputs[0].Name != InvalidName ||
		len(d.Result.NotReturned) != 1 || d.Result.NotReturned[0].Reason != "other" || d.Result.NotReturned[0].Name != InvalidName || len(d.Opens) != 1 || d.Opens[0].Answer != "wrap" || d.Opens[0].AnswerSigner != operator.ReceiverPrincipal(receiverA) {
		t.Errorf("planted result and open exchange: %+v %+v", d.Result, d.Opens)
	}
	b := receiverOf(t, snapshot, receiverB)
	if len(b.Offers) != 1 || b.Offers[0].Profile != "profile-a" || b.Offers[0].Agent != Unknown || len(b.Held) != 0 || b.Unidentified != 1 {
		t.Errorf("planted advertisement and progress: %+v", b)
	}
	served.v.mu.RLock()
	_, keyed := served.v.m.progress[receiverB][sentinel+"-run"]
	served.v.mu.RUnlock()
	if keyed {
		t.Error("a progress record keyed by no run ID was kept")
	}
	if forest.Spans != 1 || len(forest.Traces) != 1 || len(forest.Traces[0].Roots) != 1 {
		t.Fatalf("planted span: %+v", forest)
	}
	node := forest.Traces[0].Roots[0]
	if node.Name != nameOther || node.Service != nameOther || node.Status != "error: "+nameOther || len(node.Attributes) != 2 || node.Attributes[string(telemetry.AttrRun)] != run || len(node.Links) != 1 || node.Events != 1 || !slices.Contains(forest.Markers, MarkerFiltered) {
		t.Errorf("planted span node: %+v markers %v", node, forest.Markers)
	}
}

// Rule 3: an echo retracted by a rotation read late takes nothing else with it. Every fact stands on its own
// signature: the statuses, result and start failure other receivers signed after the entitlement ended stay, labelled
// with their signers, and so does the cancel.
func TestEchoRetractionLeavesOtherSigners(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-time.Minute)
	// Run A: admitted, then the entitlement ends; receiver-b's terminal, result and start failure follow.
	a := f.echo(at, receiverA, tenantA, hostA, poolA, "dependent")
	// Run B: admitted and worked while the tenant was entitled.
	b := f.echo(at, receiverA, tenantA, hostA, poolA, "independent")
	f.status(at.Add(500*time.Millisecond), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: b, kind: "claimed", state: wire.TaskStateWorking})
	f.operator(at.Add(time.Second), operator.Record{Kind: "end", Tenant: tenantA, Pool: poolA})
	f.status(at.Add(2*time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: a, kind: "terminal", state: wire.TaskStateCompleted})
	f.result(at.Add(2*time.Second), receiverB, tenantA, poolA, a, 1, wire.TaskStateCompleted, wire.ResultMetadata{Profile: "profile-a"}, "reply")
	f.startFailure(at.Add(2*time.Second), receiverB, tenantA, poolA, a, 2, "dispatch")
	f.cancel(at.Add(2*time.Second), tenantA, hostA, poolA, a, nil)
	v := f.view(time.Hour, testCaps())
	f.tick(v)
	s := v.Snapshot(0)
	if r := summaryOf(t, s, a); !r.Terminal || r.Attempt != 2 || r.Outputs != 0 || !r.Cancelled {
		t.Fatalf("setup: %+v", r)
	}
	if d, _ := v.RunDetail(a); d.Result == nil {
		t.Fatal("setup: no result")
	}
	// The rotation predates both echoes and is read only now.
	f.rotate(receiverA, operator.ReceiverTopic(receiverA), at.Add(-time.Second))
	f.tick(v)
	s = v.Snapshot(0)
	signer := operator.ReceiverPrincipal(receiverB)
	if d, ok := v.RunDetail(a); !ok || len(d.Echoes) != 0 || len(d.Attempts) != 2 || d.Result == nil || len(d.Cancels) != 1 || !d.Terminal || d.Host != "" ||
		d.Attempts[0].Events[0].Signer != signer || d.Result.Signer != signer || d.Attempts[1].StartFailure == nil || d.Attempts[1].StartFailure.Signer != signer || d.Cancels[0].Signer != operator.TenantPrincipal(tenantA, hostA) {
		t.Errorf("facts under other keys retracted with the echo, or the labels wrong: %+v %v", d, ok)
	}
	if d, ok := v.RunDetail(b); !ok || len(d.Echoes) != 0 || len(d.Attempts) != 1 || len(d.Attempts[0].Events) != 1 || d.Attempts[0].Events[0].Kind != "claimed" || d.Host != "" {
		t.Errorf("facts under another key retracted with the echo: %+v %v", d, ok)
	}
	if s.Counts["retracted"] != 2 {
		t.Errorf("counts %v; want the two echoes retracted and nothing else", s.Counts)
	}
	// A terminal after the end, with no echo at all, is taken and labelled.
	f.status(at.Add(3*time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: a, kind: "terminal", state: wire.TaskStateFailed})
	f.tick(v)
	if r := summaryOf(t, v.Snapshot(0), a); !r.Terminal || r.Class != ClassFailed {
		t.Errorf("a record of an ended entitlement refused without its echo: %+v", r)
	}
}

// Rule 2: every retained collection is bounded per run and over every run, each reported when hit: echoes and
// cancels per run, echoes, cancels, open requests and attempts over every run with the growing run dropped when it
// alone is over, event IDs one per event, sessions, and a span's links.
func TestEveryCollectionBounded(t *testing.T) {
	t.Run("run echoes and cancels", func(t *testing.T) {
		f := newFixture(t)
		at := f.now.Add(-time.Minute)
		var echoes string
		for i := range 5 {
			echoes = f.echoWith(at.Add(time.Duration(i)*time.Second), receiverA, tenantA, hostA, poolA, "echoes", hashOf(string(rune('a'+i))))
		}
		// Cancels of one host are one record; each comes from a host of its own.
		cancels := f.echo(at, receiverA, tenantA, hostA, poolA, "cancels")
		for i := range 4 {
			host := "host-" + string(rune('w'+i))
			f.addHost(tenantA, host)
			f.cancel(at.Add(time.Duration(i+1)*time.Second), tenantA, host, poolA, cancels, nil)
		}
		caps := testCaps()
		caps.RunEchoes, caps.RunCancels = 2, 2
		v := f.view(time.Hour, caps)
		s := v.Snapshot(0)
		v.mu.RLock()
		e, c := v.m.runs[echoes], v.m.runs[cancels]
		kept, joined, cancelled := len(e.Echoes), e.Hash == hashOf("a"), len(c.Cancels)
		_, newest := e.Echoes[hashOf("e")]
		v.mu.RUnlock()
		if kept != 2 || !joined || !newest || cancelled != 2 || c.Cancels[0].At != at.Add(3*time.Second) {
			t.Errorf("echoes kept %d joined %v newest %v, cancels kept %d", kept, joined, newest, cancelled)
		}
		if r := summaryOf(t, s, echoes); !slices.Equal(r.Truncated, []string{capRunEchoes}) {
			t.Errorf("echoes run: %+v", r)
		}
		if r := summaryOf(t, s, cancels); !slices.Equal(r.Truncated, []string{capRunCancels}) || !r.Cancelled {
			t.Errorf("cancels run: %+v", r)
		}
		if s.Counts[capRunEchoes] != 3 || s.Counts[capRunCancels] != 2 {
			t.Errorf("counts %v", s.Counts)
		}
	})
	t.Run("run bytes", func(t *testing.T) {
		// A hundred echoes of distinct hashes under a per-run byte cap, the model's cap far above: the run stays with
		// its joined echo alone, under the cap, marked and counted.
		f := newFixture(t)
		at := f.now.Add(-time.Minute)
		var run string
		for i := range 100 {
			run = f.echoWith(at.Add(time.Duration(i)*time.Second), receiverA, tenantA, hostA, poolA, "bytes", hashOf(string(rune('a'+i%26))+string(rune('a'+i/26))))
		}
		caps := testCaps()
		caps.RunBytes = 1024
		v := f.view(time.Hour, caps)
		s := v.Snapshot(0)
		v.mu.RLock()
		r := v.m.runs[run]
		var echoes int
		var bytes int64
		if r != nil {
			echoes, bytes = len(r.Echoes), r.me.bytes
		}
		v.mu.RUnlock()
		if r == nil || echoes != 1 || bytes > caps.RunBytes || !slices.Equal(summaryOf(t, s, run).Truncated, []string{capRunBytes}) || s.Counts[capRunBytes] != 99 {
			t.Errorf("run kept %v with %d echoes and %d bytes, counts %v", r != nil, echoes, bytes, s.Counts)
		}
	})
	t.Run("over every run", func(t *testing.T) {
		for _, c := range []struct {
			name    string
			caps    func(*Caps)
			produce func(*fixture, time.Time) []string
			cap     string
		}{
			{"echoes", func(c *Caps) { c.Echoes = 2 }, func(f *fixture, at time.Time) []string {
				var runs []string
				for i := range 3 {
					runs = append(runs, f.echo(at.Add(time.Duration(i)*time.Second), receiverA, tenantA, hostA, poolA, "e-"+string(rune('a'+i))))
				}
				return runs
			}, capEchoes},
			// The lone runs below are made by pool events alone, so no later record makes one again once dropped.
			{"opens", func(c *Caps) { c.Opens = 2 }, func(f *fixture, at time.Time) []string {
				run := runID(tenantA, hostA, "opens")
				for i := range 3 {
					f.openRequest(at.Add(time.Duration(i+1)*time.Second), receiverA, poolA, run, 1)
				}
				return []string{run}
			}, capOpens},
			{"cancels", func(c *Caps) { c.Cancels = 1 }, func(f *fixture, at time.Time) []string {
				run := runID(tenantA, hostA, "cancels")
				f.poolOnly(at, st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
				f.addHost(tenantA, hostB)
				for i, host := range []string{hostA, hostB} {
					f.cancel(at.Add(time.Duration(i+1)*time.Second), tenantA, host, poolA, run, nil)
				}
				return []string{run}
			}, capCancels},
			{"attempts", func(c *Caps) { c.Attempts = 2 }, func(f *fixture, at time.Time) []string {
				run := runID(tenantA, hostA, "attempts")
				for i := range 3 {
					f.startFailure(at.Add(time.Duration(i+1)*time.Second), receiverA, tenantA, poolA, run, int64(i+1), "dispatch")
				}
				return []string{run}
			}, capAttempts},
		} {
			t.Run(c.name, func(t *testing.T) {
				f := newFixture(t)
				runs := c.produce(f, f.now.Add(-time.Minute))
				caps := testCaps()
				c.caps(&caps)
				v := f.view(time.Hour, caps)
				s := v.Snapshot(0)
				// The oldest run goes first; a run that alone exceeds the cap goes too.
				if hasRun(s, runs[0]) || s.Counts[c.cap] == 0 {
					t.Errorf("%s: oldest run kept %v, counts %v", c.name, hasRun(s, runs[0]), s.Counts)
				}
				if len(runs) > 1 && s.RunsTotal != len(runs)-1 {
					t.Errorf("%s: %d runs kept of %d", c.name, s.RunsTotal, len(runs))
				}
				v.mu.RLock()
				total := v.m.total
				v.mu.RUnlock()
				if int64(total.echoes) > max(caps.Echoes, 0) && caps.Echoes > 0 || int64(total.opens) > caps.Opens && caps.Opens > 0 || int64(total.cancels) > caps.Cancels && caps.Cancels > 0 || int64(total.attempts) > caps.Attempts && caps.Attempts > 0 {
					t.Errorf("%s: totals over the caps: %+v", c.name, total)
				}
			})
		}
	})
	t.Run("event ids", func(t *testing.T) {
		f := newFixture(t)
		at := f.now.Add(-time.Minute)
		run := f.echo(at, receiverA, tenantA, hostA, poolA, "ids")
		for i := int64(6); i > 0; i-- {
			f.status(at.Add(time.Duration(7-i)*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, attempt: i, kind: "claimed", state: wire.TaskStateWorking})
			f.status(at.Add(time.Duration(7-i)*time.Second+500*time.Millisecond), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, attempt: i, kind: "started", state: wire.TaskStateWorking})
		}
		caps := testCaps()
		caps.RunAttempts, caps.RunEvents = 3, 5
		v := f.view(time.Hour, caps)
		v.mu.RLock()
		r := v.m.runs[run]
		seen, me, lowest := len(r.seen), r.me, slices.Min(slices.Collect(maps.Keys(r.Attempts)))
		v.mu.RUnlock()
		if seen != me.events || me.events > 5 || me.attempts > 3 || lowest != 4 {
			t.Errorf("event IDs %d, measure %+v, lowest attempt %d", seen, me, lowest)
		}
	})
	t.Run("sessions", func(t *testing.T) {
		s := newServed(t, ServeOptions{SessionRate: 100000})
		for range maxSessions + 5 {
			s.session(t)
		}
		s.server.mu.Lock()
		sessions := len(s.server.sessions)
		s.server.mu.Unlock()
		if sessions != maxSessions || s.v.Snapshot(0).Counts[capSessions] != 5 {
			t.Errorf("%d sessions kept, counts %v", sessions, s.v.Snapshot(0).Counts)
		}
	})
	t.Run("span links", func(t *testing.T) {
		f := newFixture(t)
		at := f.now.Add(-time.Minute)
		run := f.echo(at, receiverA, tenantA, hostA, poolA, "links")
		s := span(id(16), id(8), nil, "submit", run, at)
		for range maxLinks + 4 {
			s.Links = append(s.Links, &tracepb.Span_Link{TraceId: id(16), SpanId: id(8)})
		}
		f.traceRecord(at.Add(time.Second), "wherry-host."+tenantA+"."+hostA, s)
		v := f.view(time.Hour, testCaps())
		forest, err := v.traces.Trace(f.ctx, run)
		if err != nil || forest.Spans != 1 || len(forest.Traces[0].Roots[0].Links) != maxLinks || !slices.Contains(forest.Markers, markerTruncated+capTraceLinks) || v.Snapshot(0).Counts[capTraceLinks] == 0 {
			t.Errorf("links: %+v %v counts %v", forest, err, v.Snapshot(0).Counts)
		}
	})
}
