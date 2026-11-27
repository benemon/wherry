package operator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json/jsontext"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// requestFixture is kfake with tenant-a's hosts host-a and host-b, their keys registered, and wherry.requests, which
// the fixture creates without LogAppendTime so that a test can give a copy the append time of a clock hook.
type requestFixture struct {
	t        *testing.T
	cluster  *kfake.Cluster
	service  *Service
	ctx      context.Context
	keys     map[string]ed25519.PrivateKey
	upserts  *kfake.FaultHandle
	bundles  map[string]string
	produced map[string][]byte
}

func newRequestFixture(t *testing.T) *requestFixture {
	t.Helper()
	cluster, service, closeFixture := kfakeService(t)
	t.Cleanup(closeFixture)
	f := &requestFixture{t: t, cluster: cluster, service: service, ctx: context.Background(), keys: map[string]ed25519.PrivateKey{}, bundles: map[string]string{}, produced: map[string][]byte{}}
	for _, host := range []string{"host-a", "host-b"} {
		f.bundles[host] = filepath.Join(t.TempDir(), host+".bundle")
		if err := hostFixture(t, service, f.ctx, "tenant-a", host, f.bundles[host]); err != nil {
			t.Fatal(err)
		}
		_, key, _ := ed25519.GenerateKey(rand.Reader)
		f.keys[host] = key
		f.produceTo(HostTopic("tenant-a", host), must(t)(signed.RegisterKey(signed.HostOwner("tenant-a", host), key)), time.Time{})
	}
	// The clock hook: CreateTime keeps the append time a test gives a copy.
	altered, err := kadm.NewClient(service.Operator).AlterTopicConfigs(f.ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "message.timestamp.type", Value: kadm.StringPtr("CreateTime")}}, RequestsTopic)
	if err == nil {
		err = altered[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	f.upserts = cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.AlterUserSCRAMCredentials}, Observe: true, Count: -1})
	return f
}

func must(t *testing.T) func([]byte, error) []byte {
	return func(value []byte, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
}

func (f *requestFixture) produceTo(topic string, value []byte, at time.Time) int64 {
	f.t.Helper()
	produced, err := f.service.Operator.ProduceSync(f.ctx, &kgo.Record{Topic: topic, Value: value, Timestamp: at}).First()
	if err != nil {
		f.t.Fatal(err)
	}
	return produced.Offset
}

func (f *requestFixture) state() State {
	f.t.Helper()
	state, err := f.service.LoadState(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return state
}

func principal(host string) string { return TenantPrincipal("tenant-a", host) }

// request is a host's request as T-H1 builds it, replacing the host's cur, and its password; mutate may change it
// before it is signed with the host's key.
func (f *requestFixture) request(host string, mutate func(*CredentialRequest)) (CredentialRequest, string) {
	f.t.Helper()
	password, _ := Password()
	id, _ := CredentialID()
	salt := make([]byte, 32)
	rand.Read(salt)
	salted, err := pbkdf2.Key(sha512.New, password, salt, 4096, 64)
	if err != nil {
		f.t.Fatal(err)
	}
	request := CredentialRequest{Version: 1, ID: id, Tenant: "tenant-a", Host: host, Kind: SCRAMRequestKind, Replaces: f.state().Credentials[principal(host)],
		SCRAM: SCRAMCredential{Iterations: 4096, Salt: salt, SaltedPassword: salted}}
	if mutate != nil {
		mutate(&request)
	}
	return request, password
}

func (f *requestFixture) sign(request CredentialRequest, signer string) []byte {
	f.t.Helper()
	return must(f.t)(signed.Sign(CredentialRequestType, signed.HostOwner("tenant-a", signer), request, f.keys[signer]))
}

// file signs request with its host's key and produces it, returning the copy's offset.
func (f *requestFixture) file(request CredentialRequest) int64 {
	f.t.Helper()
	encoded := f.sign(request, request.Host)
	f.produced[request.ID] = encoded
	return f.produceTo(RequestsTopic, encoded, time.Time{})
}

func (f *requestFixture) approve(id string, now time.Time) (string, error) {
	f.t.Helper()
	var out bytes.Buffer
	err := f.service.ApproveRequests(f.ctx, id, id == "", now, &out)
	return out.String(), err
}

func (f *requestFixture) logsIn(host, password string) bool {
	f.t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(f.cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: principal(host), Pass: password}.AsSha512Mechanism()))
	if err != nil {
		f.t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
	defer cancel()
	return client.Ping(ctx) == nil
}

func (f *requestFixture) logStart() int64 {
	f.t.Helper()
	listed, err := kadm.NewClient(f.service.Operator).ListStartOffsets(f.ctx, RequestsTopic)
	if err != nil {
		f.t.Fatal(err)
	}
	start, _ := listed.Lookup(RequestsTopic, 0)
	return start.Offset
}

func (f *requestFixture) record(kind string, request CredentialRequest, digest string) {
	f.t.Helper()
	record := decisionRecord(kind, request)
	record.Digest = digest
	if kind == "rejected" {
		record.Reason = "test"
	}
	if _, err := f.service.appendChecked(f.ctx, []byte(kind), record); err != nil {
		f.t.Fatal(err)
	}
}

// upsertDirectly is the upsert a pass makes at T-O4, for a test whose pass stops after it.
func (f *requestFixture) upsertDirectly(request CredentialRequest) {
	f.t.Helper()
	results, err := kadm.NewClient(f.service.Operator).AlterUserSCRAMs(f.ctx, nil, []kadm.UpsertSCRAM{{User: principal(request.Host), Mechanism: kadm.ScramSha512,
		Iterations: request.SCRAM.Iterations, Salt: request.SCRAM.Salt, SaltedPassword: request.SCRAM.SaltedPassword}})
	if err == nil {
		err = results.Error()
	}
	if err != nil {
		f.t.Fatal(err)
	}
}

func payloadOf(t *testing.T, encoded []byte) []byte {
	t.Helper()
	_, payload, err := DecodeCredentialRequest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func digestOf(t *testing.T, encoded []byte) string { return payloadDigest(payloadOf(t, encoded)) }

// Test 1 (operator half): an approved request's salted password logs in and the enrolment password is refused.
func TestApproveUpsertsTheSaltedPassword(t *testing.T) {
	f := newRequestFixture(t)
	enrolment, err := ReadBundle(f.bundles["host-a"])
	if err != nil {
		t.Fatal(err)
	}
	request, password := f.request("host-a", nil)
	f.file(request)
	if out, err := f.approve(request.ID, time.Now()); err != nil || out != "approved "+request.ID+"\n" {
		t.Fatalf("approve = %q, %v", out, err)
	}
	if !f.logsIn("host-a", password) || f.logsIn("host-a", enrolment.Password) {
		t.Fatal("after approval the requested password must log in and the enrolment password must not")
	}
	if state := f.state(); state.Credentials[principal("host-a")] != request.ID || state.Decision(principal("host-a"), request.ID) != "approved" {
		t.Fatalf("cur = %s, decision %s", state.Credentials[principal("host-a")], state.Decision(principal("host-a"), request.ID))
	}
}

// Test 2 (operator half): no salt or salted password reaches a wherry.operator record, the pass's output, or a decode
// error.
func TestRequestSecretsReachNoRecordOrError(t *testing.T) {
	f := newRequestFixture(t)
	request, password := f.request("host-a", nil)
	f.file(request)
	out, err := f.approve(request.ID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	reader := NewReader(f.service.Operator)
	defer reader.Close()
	records, err := reader.ReadFrom(f.ctx, OperatorTopic, 0)
	if err != nil {
		t.Fatal(err)
	}
	var seen []byte
	for _, record := range records {
		seen = append(seen, record.Value...)
	}
	seen = append(seen, out...)
	wrongType := map[string]any{"v": "one", "id": request.ID, "tenant": "tenant-a", "host": "host-a", "kind": request.Kind, "replaces": request.Replaces, "scram": request.SCRAM}
	_, _, decodeErr := DecodeCredentialRequest(must(t)(signed.Sign(CredentialRequestType, "tenant-a.host-a", wrongType, f.keys["host-a"])))
	_, _, strictErr := DecodeCredentialRequest(must(t)(signed.Sign(CredentialRequestType, "tenant-a.host-a", struct {
		CredentialRequest
		Extra string `json:"extra"`
	}{request, "x"}, f.keys["host-a"])))
	seen = append(seen, fmtErr(decodeErr)+fmtErr(strictErr)...)
	for _, secret := range [][]byte{[]byte(password), request.SCRAM.Salt, request.SCRAM.SaltedPassword} {
		for _, form := range [][]byte{secret, []byte(base64.RawURLEncoding.EncodeToString(secret)), []byte(base64.StdEncoding.EncodeToString(secret))} {
			if bytes.Contains(seen, form) {
				t.Fatalf("a secret appears in a record, the output or a decode error")
			}
		}
	}
	if decodeErr == nil || strictErr == nil {
		t.Fatal("a corrupt or extended request decoded")
	}
}

func fmtErr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Test 3: a copy of a rejected, an expired and an approved request, produced again, is not approved.
func TestDecidedRequestProducedAgainIsNotApproved(t *testing.T) {
	f := newRequestFixture(t)
	rejected, _ := f.request("host-a", nil)
	f.file(rejected)
	if err := f.service.RejectRequest(f.ctx, rejected.ID, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	expired, _ := f.request("host-b", nil)
	f.produceTo(RequestsTopic, f.sign(expired, "host-b"), time.Now().Add(-8*24*time.Hour))
	if _, err := f.approve("", time.Now()); err != nil {
		t.Fatal(err)
	}
	if decision := f.state().Decision(principal("host-b"), expired.ID); decision != "expired" {
		t.Fatalf("an 8-day-old request is %q", decision)
	}
	approved, _ := f.request("host-a", nil)
	f.file(approved)
	if _, err := f.approve(approved.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	before := f.upserts.Hits()
	for _, request := range []CredentialRequest{rejected, expired, approved} {
		f.produceTo(RequestsTopic, f.sign(request, request.Host), time.Time{})
		if _, err := f.approve(request.ID, time.Now()); err == nil {
			t.Fatalf("a %s request produced again was approved", f.state().Decision(principal(request.Host), request.ID))
		}
	}
	if f.upserts.Hits() != before {
		t.Fatal("a decided request produced again was upserted")
	}
}

// Test 4: a request with one copy older than 7 days and one newer is expired, not approved.
func TestRequestAgeRunsFromItsLowestKeptCopy(t *testing.T) {
	f := newRequestFixture(t)
	request, _ := f.request("host-a", nil)
	encoded := f.sign(request, "host-a")
	f.produceTo(RequestsTopic, encoded, time.Now().Add(-8*24*time.Hour))
	f.produceTo(RequestsTopic, encoded, time.Time{})
	if _, err := f.approve(request.ID, time.Now()); err == nil {
		t.Fatal("an 8-day-old request was approved")
	}
	if decision := f.state().Decision(principal("host-a"), request.ID); decision != "expired" || f.upserts.Hits() != 0 {
		t.Fatalf("decision %q, %d upserts", decision, f.upserts.Hits())
	}
}

// Test 5: a request whose replaces is not current is refused, including one produced again after a revoke.
func TestStaleRequestIsRefused(t *testing.T) {
	f := newRequestFixture(t)
	other, _ := CredentialID()
	stale, _ := f.request("host-a", func(request *CredentialRequest) { request.Replaces = other })
	f.file(stale)
	earlier, _ := f.request("host-a", nil)
	f.file(earlier)
	f.record("resetting", earlier, "")
	enrolment, _ := CredentialID()
	if _, err := f.service.appendChecked(f.ctx, []byte("enrolled"), Record{Kind: "enrolled", Tenant: "tenant-a", Host: "host-a", Enrolment: enrolment}); err != nil {
		t.Fatal(err)
	}
	for _, request := range []CredentialRequest{stale, earlier} {
		f.produceTo(RequestsTopic, f.sign(request, "host-a"), time.Time{})
		if out, err := f.approve(request.ID, time.Now()); err == nil || !strings.Contains(out, "is stale") {
			t.Fatalf("approve of a request replacing an old credential = %q, %v", out, err)
		}
	}
	if f.upserts.Hits() != 0 {
		t.Fatal("a stale request was upserted")
	}
}

// Test 6: a copy signed by another host, naming another host, or signed with a retired key is dropped.
func TestCopiesFailingTheirOwnChecksAreDropped(t *testing.T) {
	f := newRequestFixture(t)
	byOther, _ := f.request("host-a", nil)
	f.produceTo(RequestsTopic, f.sign(byOther, "host-b"), time.Time{})
	namingOther, _ := f.request("host-b", nil)
	f.produceTo(RequestsTopic, f.sign(namingOther, "host-a"), time.Time{})
	retired, _ := f.request("host-a", nil)
	f.produceTo(RequestsTopic, f.sign(retired, "host-a"), time.Time{})
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produceTo(HostTopic("tenant-a", "host-a"), must(t)(signed.RotateKey(signed.HostOwner("tenant-a", "host-a"), f.keys["host-a"], next)), time.Time{})
	var out bytes.Buffer
	if err := f.service.ListRequests(f.ctx, time.Now(), &out); err != nil || !strings.Contains(out.String(), "bad-signature=3") {
		t.Fatalf("list requests = %q, %v", out.String(), err)
	}
	if _, err := f.approve("", time.Now()); err != nil {
		t.Fatal(err)
	}
	if f.upserts.Hits() != 0 {
		t.Fatal("a dropped copy was upserted")
	}
}

// Test 7: SHA-256, wrong lengths and iterations outside 4096 to 16384 are refused by wherry before any upsert is sent.
func TestRequestParametersAreCheckedBeforeTheUpsert(t *testing.T) {
	for name, mutate := range map[string]func(*CredentialRequest){
		"sha-256":            func(r *CredentialRequest) { r.Kind = "scram-sha-256" },
		"short salt":         func(r *CredentialRequest) { r.SCRAM.Salt = r.SCRAM.Salt[:8] },
		"long salt":          func(r *CredentialRequest) { r.SCRAM.Salt = append(r.SCRAM.Salt, make([]byte, 40)...) },
		"short salted":       func(r *CredentialRequest) { r.SCRAM.SaltedPassword = r.SCRAM.SaltedPassword[:32] },
		"zero iterations":    func(r *CredentialRequest) { r.SCRAM.Iterations = 0 },
		"too few iterations": func(r *CredentialRequest) { r.SCRAM.Iterations = 4095 },
		"too many":           func(r *CredentialRequest) { r.SCRAM.Iterations = 16385 },
	} {
		t.Run(name, func(t *testing.T) {
			f := newRequestFixture(t)
			request, _ := f.request("host-a", mutate)
			f.file(request)
			if _, err := f.approve(request.ID, time.Now()); err == nil {
				t.Fatal("approve succeeded")
			}
			if f.upserts.Hits() != 0 || f.state().Approvals[principal("host-a")].Request != "" {
				t.Fatal("a refused request was recorded approving or upserted")
			}
		})
	}
}

// Test 8: after a revoke stopped at resetting nothing is approved; two requests naming the same replaces approve one.
func TestApprovalNeedsTheCurrentCredential(t *testing.T) {
	f := newRequestFixture(t)
	request, _ := f.request("host-a", nil)
	f.file(request)
	f.record("resetting", request, "")
	if _, err := f.approve(request.ID, time.Now()); err == nil || f.upserts.Hits() != 0 || f.state().Approvals[principal("host-a")].Request != "" {
		t.Fatal("a request of a host being revoked was approved")
	}
	first, _ := f.request("host-b", nil)
	second, _ := f.request("host-b", nil)
	f.file(first)
	f.file(second)
	if _, err := f.approve("", time.Now()); err != nil {
		t.Fatal(err)
	}
	state := f.state()
	decided := state.Decision(principal("host-b"), first.ID) + "," + state.Decision(principal("host-b"), second.ID)
	if decided != "approved," && decided != ",approved" || f.upserts.Hits() != 1 {
		t.Fatalf("two requests naming one replaces: decisions %s, %d upserts", decided, f.upserts.Hits())
	}
}

// Test 10 (pass half): a pass stopped after its upsert, or before it, is settled by the next pass; reject refuses it;
// resetting voids it, after which no approve upserts it; apply refuses a credential record whose approval is not open.
func TestOpenApprovalIsSettledByTheNextPass(t *testing.T) {
	for _, upserted := range []bool{true, false} {
		f := newRequestFixture(t)
		request, password := f.request("host-a", nil)
		f.file(request)
		f.record("approving", request, digestOf(t, f.produced[request.ID]))
		if upserted {
			f.upsertDirectly(request)
		}
		if err := f.service.RejectRequest(f.ctx, request.ID, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "has begun; run approve to finish it") {
			t.Fatalf("reject of an open approval = %v", err)
		}
		if f.state().Decision(principal("host-a"), request.ID) != "" {
			t.Fatal("reject settled an open approval")
		}
		if _, err := f.approve("", time.Now()); err != nil {
			t.Fatal(err)
		}
		if f.state().Decision(principal("host-a"), request.ID) != "approved" || !f.logsIn("host-a", password) {
			t.Fatalf("upserted=%t: the next pass did not settle the approval", upserted)
		}
	}
	f := newRequestFixture(t)
	request, _ := f.request("host-a", nil)
	f.file(request)
	f.record("approving", request, digestOf(t, f.produced[request.ID]))
	f.record("resetting", request, "")
	if decision := f.state().Decision(principal("host-a"), request.ID); decision != "void" {
		t.Fatalf("resetting left the approval %q", decision)
	}
	f.approve("", time.Now())
	if f.upserts.Hits() != 0 {
		t.Fatal("an approve after resetting upserted the voided request")
	}
	state := f.state()
	if err := apply(&state, decisionRecord("credential", request)); !errors.Is(err, errApprovalNotOpen) {
		t.Fatalf("apply of a credential record for a closed approval = %v", err)
	}
}

// Test 16: after a pass, decided copies are below the log start and undecided ones, an open approval's and one
// produced after the load stay; a copy signed with a key registered after the end offset is not read or deleted.
func TestPassDeletesOnlyDecidedRequests(t *testing.T) {
	f := newRequestFixture(t)
	decided, _ := f.request("host-a", nil)
	f.file(decided)
	open, _ := f.request("host-b", nil)
	openAt := f.file(open)
	f.record("approving", open, digestOf(t, f.produced[open.ID]))
	f.cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.AlterUserSCRAMCredentials}, Resource: principal("host-b"), Err: kerr.UnacceptableCredential, Count: -1})
	if _, err := f.approve(decided.ID, time.Now()); err == nil {
		t.Fatal("a pass whose resumed approval failed exited zero")
	}
	if start := f.logStart(); start != openAt {
		t.Fatalf("log start %d after the pass; want %d, the open approval's copy", start, openAt)
	}
	f = newRequestFixture(t)
	decided, _ = f.request("host-a", nil)
	f.file(decided)
	undecided, _ := f.request("host-b", func(r *CredentialRequest) { r.Kind = "scram-sha-256" })
	undecidedAt := f.file(undecided)
	if _, err := f.approve(decided.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if start := f.logStart(); start != undecidedAt {
		t.Fatalf("log start %d; want %d, the undecided request's copy", start, undecidedAt)
	}
	// A reject pass records no expiry, so an undecided request older than 7 days stays for the next approve pass.
	f = newRequestFixture(t)
	aged, _ := f.request("host-a", nil)
	agedAt := f.produceTo(RequestsTopic, f.sign(aged, "host-a"), time.Now().Add(-8*24*time.Hour))
	other, _ := f.request("host-a", func(r *CredentialRequest) { r.Kind = "scram-sha-256" })
	f.file(other)
	if err := f.service.RejectRequest(f.ctx, other.ID, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if start := f.logStart(); start > agedAt {
		t.Fatalf("a reject pass deleted an undecided request older than 7 days: log start %d, its copy at %d", start, agedAt)
	}
	f = newRequestFixture(t)
	decided, _ = f.request("host-b", nil)
	f.file(decided)
	late, _ := f.request("host-a", nil)
	var lateAt int64
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	afterRequestsEnd = func() {
		afterRequestsEnd = func() {}
		f.produceTo(HostTopic("tenant-a", "host-a"), must(t)(signed.RotateKey(signed.HostOwner("tenant-a", "host-a"), f.keys["host-a"], next)), time.Time{})
		f.keys["host-a"] = next
		lateAt = f.file(late)
	}
	t.Cleanup(func() { afterRequestsEnd = func() {} })
	if out, err := f.approve(decided.ID, time.Now()); err != nil {
		t.Fatalf("approve = %v: %s", err, out)
	}
	if start := f.logStart(); start > lateAt {
		t.Fatalf("log start %d passed the copy sent after the end offset, at %d", start, lateAt)
	}
	var out bytes.Buffer
	if err := f.service.ListRequests(f.ctx, time.Now(), &out); err != nil || strings.Contains(out.String(), "bad-signature") || !strings.Contains(out.String(), "request="+late.ID) {
		t.Fatalf("the copy sent after the end offset: %q, %v", out.String(), err)
	}
}

// Test 17 (operator half): a request of another kind stays through passes and is recorded expired after 7 days.
func TestOtherKindExpiresAfterSevenDays(t *testing.T) {
	f := newRequestFixture(t)
	request, _ := f.request("host-a", func(r *CredentialRequest) { r.Kind = "scram-sha-256" })
	at := f.file(request)
	for range 2 {
		f.approve("", time.Now())
	}
	if f.state().Decision(principal("host-a"), request.ID) != "" || f.logStart() != at {
		t.Fatal("a fresh request of another kind was decided or deleted")
	}
	f.approve("", time.Now().Add(8*24*time.Hour))
	if f.state().Decision(principal("host-a"), request.ID) != "expired" || f.logStart() <= at {
		t.Fatal("an 8-day-old request of another kind was not recorded expired and deleted")
	}
}

// Test 22 (operator half): removal meeting an open approval, before and after its upsert, completes; no credential
// remains after end; a later approve sends no upsert.
func TestRemovalClosesAnOpenApproval(t *testing.T) {
	for _, upserted := range []bool{false, true} {
		f := newRequestFixture(t)
		revocationBounds(t, f.service, "1", "1", "1")
		request, _ := f.request("host-a", nil)
		f.file(request)
		f.record("approving", request, digestOf(t, f.produced[request.ID]))
		if upserted {
			f.upsertDirectly(request)
		}
		hits := f.upserts.Hits()
		result := f.service.RevokeHost(f.ctx, "tenant-a", "host-a", 0, denied, nil)
		if !result.Complete || len(result.Steps) != 6 {
			t.Fatalf("removal = %+v", result)
		}
		if decision := f.state().Decision(principal("host-a"), request.ID); decision != "void" {
			t.Fatalf("removal left the approval %q", decision)
		}
		if ok, err := hasCredential(f.ctx, f.service, principal("host-a")); err != nil || ok {
			t.Fatalf("a credential remains after end: %t, %v", ok, err)
		}
		hits = f.upserts.Hits()
		f.approve("", time.Now())
		if f.upserts.Hits() != hits || f.state().Decision(principal("host-a"), request.ID) != "void" {
			t.Fatal("an approve after removal upserted the voided approval")
		}
	}
}

// Test 24: forged copies of a pending request's ID, and during an open approval, change nothing; another host's own
// request under the same ID is its own, and approve ID names both hosts and does nothing.
func TestForgedCopiesDoNotDisplaceTheVictimsRequest(t *testing.T) {
	for _, stage := range []string{"pending", "approving", "upserted"} {
		f := newRequestFixture(t)
		victim, password := f.request("host-a", nil)
		f.file(victim)
		if stage != "pending" {
			f.record("approving", victim, digestOf(t, f.produced[victim.ID]))
		}
		if stage == "upserted" {
			f.upsertDirectly(victim)
		}
		forged, _ := f.request("host-b", func(r *CredentialRequest) { r.ID = victim.ID })
		naming := forged
		naming.Host, naming.Replaces = "host-a", victim.Replaces
		broken := f.sign(naming, "host-a")
		broken[len(broken)-2] ^= 1
		for _, value := range [][]byte{f.sign(naming, "host-b"), broken} {
			f.produceTo(RequestsTopic, value, time.Time{})
		}
		// The other host's own request under the victim's ID, with the victim's principal as the Kafka key.
		if _, err := f.service.Operator.ProduceSync(f.ctx, &kgo.Record{Topic: RequestsTopic, Key: []byte(principal("host-a")), Value: f.sign(forged, "host-b")}).First(); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := f.service.ListRequests(f.ctx, time.Now(), &out); err != nil || !strings.Contains(out.String(), "bad-signature=2") {
			t.Fatalf("list requests = %q", out.String())
		}
		if _, err := f.approve("", time.Now()); err != nil {
			t.Fatalf("%s: approve = %v", stage, err)
		}
		if f.state().Decision(principal("host-a"), victim.ID) != "approved" || !f.logsIn("host-a", password) {
			t.Fatalf("%s: the victim's own payload was not approved", stage)
		}
	}
	f := newRequestFixture(t)
	victim, _ := f.request("host-a", nil)
	f.file(victim)
	own, _ := f.request("host-b", func(r *CredentialRequest) { r.ID = victim.ID })
	f.file(own)
	if _, err := f.approve(victim.ID, time.Now()); err == nil || !strings.Contains(err.Error(), "tenant-a/host-a, tenant-a/host-b") || f.upserts.Hits() != 0 {
		t.Fatalf("approve of an ID two hosts filed = %v", err)
	}
	f.record("rejected", own, "")
	if f.state().Decision(principal("host-a"), victim.ID) != "" {
		t.Fatal("deciding another host's request under the same ID decided the victim's")
	}
}

// Test 25: the approving record's digest is the payload's; a pass stopped after approving is finished from the copy
// under a key the host has since retired.
func TestResumeFromTheRetiredKeyCopy(t *testing.T) {
	f := newRequestFixture(t)
	request, password := f.request("host-a", nil)
	f.file(request)
	payload := payloadOf(t, f.produced[request.ID])
	if _, err := f.approve("missing-id-xxxxxxxxxxx", time.Now()); err == nil {
		t.Fatal("approve of an unknown ID exited zero")
	}
	f.record("approving", request, payloadDigest(payload))
	if f.state().Approvals[principal("host-a")].Digest != payloadDigest(payload) {
		t.Fatal("the approving record does not carry the payload's digest")
	}
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produceTo(HostTopic("tenant-a", "host-a"), must(t)(signed.RotateKey(signed.HostOwner("tenant-a", "host-a"), f.keys["host-a"], next)), time.Time{})
	if _, err := f.approve("", time.Now()); err != nil {
		t.Fatal(err)
	}
	if f.state().Decision(principal("host-a"), request.ID) != "approved" || !f.logsIn("host-a", password) {
		t.Fatal("the approval was not finished from the retired-key copy")
	}
	resigned := must(t)(signed.Sign(CredentialRequestType, "tenant-a.host-a", jsontext.Value(payload), next))
	if !bytes.Equal(payloadOf(t, resigned), payload) {
		t.Fatal("signing the payload bytes again changed them")
	}
}

// Test 26: an approval open for 8 days is finished, and no expired record is written for it.
func TestOpenApprovalOutlivesSevenDays(t *testing.T) {
	f := newRequestFixture(t)
	request, _ := f.request("host-a", nil)
	f.file(request)
	f.record("approving", request, digestOf(t, f.produced[request.ID]))
	fault := f.cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.AlterUserSCRAMCredentials}, Resource: principal("host-a"), Err: kerr.UnacceptableCredential})
	if _, err := f.approve("", time.Now().Add(8*24*time.Hour)); err == nil || f.state().Decision(principal("host-a"), request.ID) != "" {
		t.Fatal("a pass 8 days on whose upsert failed decided the open approval")
	}
	fault.Remove()
	if _, err := f.approve("", time.Now().Add(8*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if decision := f.state().Decision(principal("host-a"), request.ID); decision != "approved" {
		t.Fatalf("an approval open for 8 days is %q", decision)
	}
}

// Test 27: after host rotate --key the request is approved from the copy under the new key, the old copy counts as a
// bad signature, and its age runs from the new copy.
func TestRequestSentAgainAfterAKeyRotation(t *testing.T) {
	f := newRequestFixture(t)
	request, _ := f.request("host-a", nil)
	f.produceTo(RequestsTopic, f.sign(request, "host-a"), time.Now().Add(-8*24*time.Hour))
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produceTo(HostTopic("tenant-a", "host-a"), must(t)(signed.RotateKey(signed.HostOwner("tenant-a", "host-a"), f.keys["host-a"], next)), time.Time{})
	f.keys["host-a"] = next
	f.file(request)
	var out bytes.Buffer
	if err := f.service.ListRequests(f.ctx, time.Now(), &out); err != nil || !strings.Contains(out.String(), "bad-signature=1") || !strings.Contains(out.String(), "state=pending") {
		t.Fatalf("list requests = %q", out.String())
	}
	if _, err := f.approve(request.ID, time.Now()); err != nil || f.state().Decision(principal("host-a"), request.ID) != "approved" {
		t.Fatalf("approve from the new key's copy = %v", err)
	}
}

// Test 28: an upsert that fails leaves its approval open, prints T-O4's line for a timeout alone, and the pass goes on
// with the other host's request, exiting non-zero; the next pass finishes the approval; reject refuses it meanwhile.
func TestFailedUpsertLeavesItsApprovalOpen(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		f := newRequestFixture(t)
		first, password := f.request("host-a", nil)
		f.file(first)
		f.record("approving", first, digestOf(t, f.produced[first.ID]))
		second, _ := f.request("host-b", nil)
		f.file(second)
		release := make(chan struct{})
		if timeout {
			f.cluster.ControlKey(int16(kmsg.AlterUserSCRAMCredentials), func(request kmsg.Request) (kmsg.Response, error, bool) {
				f.cluster.KeepControl()
				if request.(*kmsg.AlterUserSCRAMCredentialsRequest).Upsertions[0].Name == principal("host-a") {
					f.cluster.SleepControl(func() { <-release })
				}
				return nil, nil, false
			})
			short, err := kgo.NewClient(append(f.service.Operator.Opts(), kgo.RequestTimeoutOverhead(200*time.Millisecond), kgo.RetryTimeout(500*time.Millisecond))...)
			if err != nil {
				t.Fatal(err)
			}
			defer short.Close()
			service := *f.service
			service.Operator = short
			f.service = &service
		} else {
			fault := f.cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.AlterUserSCRAMCredentials}, Resource: principal("host-a"), Err: kerr.UnacceptableCredential, Count: -1})
			defer fault.Remove()
		}
		out, err := f.approve("", time.Now())
		if err == nil {
			t.Fatal("a pass whose upsert failed exited zero")
		}
		if strings.Contains(out, "the upsert may still be applied") != timeout {
			t.Fatalf("timeout=%t: output %q", timeout, out)
		}
		state := f.state()
		if state.Approvals[principal("host-a")].Request != first.ID || state.Decision(principal("host-b"), second.ID) != "approved" {
			t.Fatalf("timeout=%t: the failed approval closed or the other host's request was not approved", timeout)
		}
		if err := f.service.RejectRequest(f.ctx, first.ID, &bytes.Buffer{}); err == nil {
			t.Fatal("reject took an approval whose upsert failed")
		}
		close(release)
		if !timeout {
			continue
		}
		f.cluster.DropControl()
		if _, err := f.approve("", time.Now()); err != nil || f.state().Decision(principal("host-a"), first.ID) != "approved" || !f.logsIn("host-a", password) {
			t.Fatalf("the next pass did not finish the approval: %v", err)
		}
	}
}

// Test 29: a host's two payloads under one ID are a conflict and are not approved; an approval open on the first is
// finished with the first.
func TestTwoPayloadsUnderOneIDAreAConflict(t *testing.T) {
	f := newRequestFixture(t)
	first, _ := f.request("host-a", nil)
	f.file(first)
	second, _ := f.request("host-a", func(r *CredentialRequest) { r.ID = first.ID })
	f.file(second)
	if out, err := f.approve(first.ID, time.Now()); err == nil || !strings.Contains(out, "is conflict") || f.upserts.Hits() != 0 {
		t.Fatalf("approve of a conflict = %q, %v", out, err)
	}
	f = newRequestFixture(t)
	first, password := f.request("host-a", nil)
	f.file(first)
	f.record("approving", first, digestOf(t, f.produced[first.ID]))
	second, _ = f.request("host-a", func(r *CredentialRequest) { r.ID = first.ID })
	f.file(second)
	if out, err := f.approve("", time.Now()); err != nil || !f.logsIn("host-a", password) {
		t.Fatalf("the open approval of the first payload was not finished with it: %v: %s", err, out)
	}
}

// Test 31: add --host refuses a host whose removal stopped after revoking, before anything is changed.
func TestHostAddRefusesAHostBeingRemoved(t *testing.T) {
	f := newRequestFixture(t)
	if _, err := f.service.appendChecked(f.ctx, []byte("revoking"), Record{Kind: "revoking", Tenant: "tenant-a", Host: "host-a"}); err != nil {
		t.Fatal(err)
	}
	before := len(f.records())
	path := filepath.Join(t.TempDir(), "again.bundle")
	if err := f.service.HostAdd(f.ctx, "tenant-a", "host-a", path); err == nil || err.Error() != "host is being removed; run remove --host again" {
		t.Fatalf("host add of a host being removed = %v", err)
	}
	if len(f.records()) != before || f.upserts.Hits() != 0 {
		t.Fatal("a refused host add changed something")
	}
}

func (f *requestFixture) records() [][]byte {
	f.t.Helper()
	reader := NewReader(f.service.Operator)
	defer reader.Close()
	records, err := reader.ReadFrom(f.ctx, OperatorTopic, 0)
	if err != nil {
		f.t.Fatal(err)
	}
	var values [][]byte
	for _, record := range records {
		values = append(values, record.Value)
	}
	return values
}

// Test 33: the new kinds are written at version 2, a version 1 log followed by version 2 records folds, and a record
// above this release's version is "upgrade this wherry".
func TestNewKindsAreRecordVersionTwo(t *testing.T) {
	id, _ := CredentialID()
	digest := payloadDigest([]byte("x"))
	var records []Record
	for _, record := range []Record{
		{Version: 1, Kind: "tenant", Tenant: "tenant-a"},
		{Version: 1, Kind: "host", Tenant: "tenant-a", Host: "host-a"},
		{Kind: "resetting", Tenant: "tenant-a", Host: "host-a"},
		{Kind: "enrolled", Tenant: "tenant-a", Host: "host-a", Enrolment: id},
		{Kind: "rotate", Tenant: "tenant-a", Host: "host-a"},
		{Kind: "approving", Tenant: "tenant-a", Host: "host-a", Request: digest[:22], Replaces: id, Digest: digest},
	} {
		if record.Kind == "approving" {
			record.Request, _ = CredentialID()
		}
		encoded, err := EncodeRecord(record)
		if err != nil {
			t.Fatalf("%s: %v", record.Kind, err)
		}
		decoded, err := DecodeRecord(encoded)
		if err != nil {
			t.Fatalf("%s: %v", record.Kind, err)
		}
		if record.Version == 0 && decoded.Version != 2 {
			t.Fatalf("%s written at version %d", record.Kind, decoded.Version)
		}
		records = append(records, decoded)
	}
	if state, err := Fold(records); err != nil || state.Credentials[principal("host-a")] != id {
		t.Fatalf("fold of version 1 then 2 = %v", err)
	}
	for _, kind := range []string{"resetting", "rotate"} {
		if _, err := EncodeRecord(Record{Version: 1, Kind: kind, Tenant: "tenant-a", Host: "host-a"}); err == nil {
			t.Fatalf("a %s record is valid at version 1", kind)
		}
	}
	if _, err := DecodeRecord([]byte(`{"v":3,"kind":"resetting","tenant":"tenant-a","host":"host-a","at_ms":1}`)); !errors.Is(err, ErrNewerRecord) {
		t.Fatalf("a version 3 record = %v", err)
	}
}

// Test 37: apply refuses a credential record whose approval is not open, so planRecord appends nothing, and a log
// holding approving, resetting and that record fails Fold naming its offset and two operator directories.
func TestFoldRefusesACredentialForAClosedApproval(t *testing.T) {
	f := newRequestFixture(t)
	request, _ := f.request("host-a", nil)
	f.record("approving", request, payloadDigest([]byte("x")))
	f.record("resetting", request, "")
	before := len(f.records())
	if _, err := f.service.appendChecked(f.ctx, []byte("credential"), decisionRecord("credential", request)); !errors.Is(err, errApprovalNotOpen) || len(f.records()) != before {
		t.Fatalf("planRecord of a credential record for a closed approval = %v", err)
	}
	encoded, err := EncodeRecord(decisionRecord("credential", request))
	if err != nil {
		t.Fatal(err)
	}
	at := f.produceTo(OperatorTopic, encoded, time.Time{})
	_, err = f.service.LoadState(f.ctx)
	if err == nil || !strings.Contains(err.Error(), "credential record at offset "+strconv.FormatInt(at, 10)+" is for an approval that was not open: two operator directories wrote to this deployment at once; reset the deployment") {
		t.Fatalf("load of a log with a refused credential record = %v", err)
	}
}

// Test 23: wherry.requests is created with retention.ms -1, and check reports any other value.
func TestRequestsTopicKeepsEveryRecordUntilAPass(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	t.Cleanup(closeFixture)
	f := &requestFixture{t: t, service: service, ctx: context.Background()}
	values, err := f.service.topicConfigs(f.ctx, "retention.ms", RequestsTopic)
	if err != nil || values[RequestsTopic] != "-1" {
		t.Fatalf("wherry.requests retention.ms = %q, %v; want -1", values[RequestsTopic], err)
	}
	altered, err := kadm.NewClient(f.service.Operator).AlterTopicConfigs(f.ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "retention.ms", Value: kadm.StringPtr("1209600000")}}, RequestsTopic)
	if err == nil {
		err = altered[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := f.service.checkRequestsTopic(f.ctx); err == nil || !strings.Contains(err.Error(), "retention.ms is 1209600000, want -1") {
		t.Fatalf("check of a finite retention = %v", err)
	}
}
