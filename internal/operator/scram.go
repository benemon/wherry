package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/signed"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
)

const (
	RequestsTopic         = "wherry.requests"
	CredentialRequestType = "wherry-credential-request"
	SCRAMRequestKind      = "scram-sha-512"
	requestLifetime       = 7 * 24 * time.Hour
)

var requestTopicConfigs = map[string]string{
	"cleanup.policy":         "delete",
	"message.timestamp.type": "LogAppendTime",
	"retention.ms":           "-1",
	"segment.ms":             strconv.FormatInt((24 * time.Hour).Milliseconds(), 10),
	"max.message.bytes":      strconv.Itoa(64 << 10),
}

// EnsureRequestsTopic creates wherry.requests with its fixed configuration where it is absent.
func (service *Service) EnsureRequestsTopic(ctx context.Context) error {
	return service.createTopicsWith(ctx, requestTopicConfigs, RequestsTopic)
}

func (service *Service) checkRequestsTopic(ctx context.Context) error {
	for _, name := range slices.Sorted(maps.Keys(requestTopicConfigs)) {
		values, err := service.topicConfigs(ctx, name, RequestsTopic)
		if err != nil {
			return err
		}
		if want := requestTopicConfigs[name]; values[RequestsTopic] != want {
			return fmt.Errorf("topic %s %s is %s, want %s", RequestsTopic, name, values[RequestsTopic], want)
		}
	}
	return nil
}

// SCRAMCredential is the salted form of a host's requested password, as RFC 5802 derives it.
type SCRAMCredential struct {
	Iterations     int32  `json:"iterations"`
	Salt           []byte `json:"salt"`
	SaltedPassword []byte `json:"salted_password"`
}

// CredentialRequest is the payload a host signs to ask for a new credential.
type CredentialRequest struct {
	Version  int             `json:"v"`
	ID       string          `json:"id"`
	Tenant   string          `json:"tenant"`
	Host     string          `json:"host"`
	Kind     string          `json:"kind"`
	Replaces string          `json:"replaces"`
	SCRAM    SCRAMCredential `json:"scram"`
}

// checkSCRAM is T-O2's guard on the payload: wherry checks what kadm passes through for a salted upsert.
func (request CredentialRequest) checkSCRAM() error {
	if request.Kind != SCRAMRequestKind {
		return errors.New("credential request kind must be scram-sha-512")
	}
	if len(request.SCRAM.Salt) < 16 || len(request.SCRAM.Salt) > 64 {
		return errors.New("SCRAM salt must be 16 to 64 bytes")
	}
	if len(request.SCRAM.SaltedPassword) != 64 {
		return errors.New("SCRAM salted password must be 64 bytes")
	}
	if request.SCRAM.Iterations < 4096 || request.SCRAM.Iterations > 16384 {
		return errors.New("SCRAM iterations must be 4096 to 16384")
	}
	return nil
}

// DecodeCredentialRequest strictly decodes the payload of a credential request without verifying its signature, and
// returns the payload bytes as signed. Its error never quotes the input.
func DecodeCredentialRequest(encoded []byte) (CredentialRequest, []byte, error) {
	jws, err := jose.ParseSignedCompact(string(encoded), []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil {
		return CredentialRequest{}, nil, errors.New("invalid credential request")
	}
	payload := jws.UnsafePayloadWithoutVerification()
	var request CredentialRequest
	if err := json.Unmarshal(payload, &request, signed.Strict); err != nil || !validRequest(request) {
		return CredentialRequest{}, nil, errors.New("invalid credential request")
	}
	return request, payload, nil
}

func validRequest(request CredentialRequest) bool {
	return request.Version == 1 && validCredentialID(request.ID) && validateNames(request.Tenant, request.Host) == nil && validCredentialID(request.Replaces)
}

func payloadDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// requestCopy is one record of wherry.requests below the pass's end offset. A copy that is not kept has a request and
// digest only if its payload decodes; only its digest is read.
type requestCopy struct {
	request CredentialRequest
	digest  string
	offset  int64
	kept    bool
}

// requestGroup is the kept copies of one signer and request ID.
type requestGroup struct {
	request   CredentialRequest
	digest    string
	principal string
	first     time.Time
	conflict  bool
}

type requestPass struct {
	state         State
	end           int64
	copies        []requestCopy
	groups        map[string]requestGroup
	badSignatures int
}

// afterRequestsEnd runs between the pass's read of the end offset and its load of the host keys; tests replace it.
var afterRequestsEnd = func() {}

func (service *Service) loadRequestPass(ctx context.Context) (requestPass, error) {
	state, err := service.LoadState(ctx)
	if err != nil {
		return requestPass{}, err
	}
	listed, err := ListOffsets(ctx, kadm.NewClient(service.Operator).ListEndOffsets, RequestsTopic)
	if err != nil {
		return requestPass{}, err
	}
	end, ok := listed.Lookup(RequestsTopic, 0)
	if !ok {
		return requestPass{}, errors.New("wherry.requests has no partition 0")
	}
	afterRequestsEnd()
	// Every copy below the end offset was signed with a key registered before this load.
	keys, err := ReadKeys(ctx, service.Operator, AllKeyTopics(state))
	if err != nil {
		return requestPass{}, err
	}
	reader := NewReader(service.Operator)
	defer reader.Close()
	records, _, _, err := reader.ReadRange(ctx, RequestsTopic, 0)
	if err != nil {
		return requestPass{}, err
	}
	pass := requestPass{state: state, end: end.Offset, groups: map[string]requestGroup{}}
	for _, record := range records {
		if record.Offset >= end.Offset {
			continue
		}
		copy := requestCopy{offset: record.Offset}
		request, payload, err := DecodeCredentialRequest(record.Value)
		if err == nil {
			copy.request, copy.digest = request, payloadDigest(payload)
		}
		var verified CredentialRequest
		owner, _, verifyErr := signed.VerifyStrict(record.Value, CredentialRequestType, keys, &verified)
		if err != nil || verifyErr != nil || owner != signed.HostOwner(request.Tenant, request.Host) {
			pass.badSignatures++
			pass.copies = append(pass.copies, copy)
			continue
		}
		copy.kept = true
		pass.copies = append(pass.copies, copy)
		principal := TenantPrincipal(request.Tenant, request.Host)
		key := principal + "\x00" + request.ID
		group, exists := pass.groups[key]
		if !exists {
			group = requestGroup{request: request, digest: copy.digest, principal: principal, first: record.Timestamp}
		}
		group.conflict = group.conflict || group.digest != copy.digest
		pass.groups[key] = group
	}
	return pass, nil
}

// undecided is section 3's state: kind and age play no part.
func (pass requestPass) undecided(group requestGroup) bool {
	current := pass.state.Credentials[group.principal]
	return !group.conflict && pass.state.Decision(group.principal, group.request.ID) == "" && current != "" &&
		group.request.Replaces == current && pass.state.Approvals[group.principal].Request != group.request.ID
}

// groupState is the state list requests prints.
func (pass requestPass) groupState(group requestGroup, now time.Time) string {
	switch {
	case pass.state.Decision(group.principal, group.request.ID) != "":
		return pass.state.Decision(group.principal, group.request.ID)
	case pass.state.Approvals[group.principal].Request == group.request.ID:
		return "approving"
	case group.conflict:
		return "conflict"
	case pass.state.Ended[group.principal] || pass.state.Revoking[group.principal]:
		return "host-ended"
	case !pass.undecided(group):
		return "stale"
	case group.request.Kind != SCRAMRequestKind:
		return "bad-kind"
	case now.Sub(group.first) > requestLifetime:
		return "expired"
	}
	return "pending"
}

// requestHosts is the groups of the hosts that have a kept copy under id.
func (pass requestPass) requestHosts(id string) []requestGroup {
	var groups []requestGroup
	for _, key := range slices.Sorted(maps.Keys(pass.groups)) {
		if pass.groups[key].request.ID == id {
			groups = append(groups, pass.groups[key])
		}
	}
	return groups
}

func ambiguous(id string, groups []requestGroup) error {
	var hosts []string
	for _, group := range groups {
		hosts = append(hosts, group.request.Tenant+"/"+group.request.Host)
	}
	return fmt.Errorf("request %s is filed by more than one host (%s); nothing was done", id, strings.Join(hosts, ", "))
}

func decisionRecord(kind string, request CredentialRequest) Record {
	return Record{Kind: kind, Tenant: request.Tenant, Host: request.Host, Request: request.ID, Replaces: request.Replaces}
}

func (service *Service) appendDecision(ctx context.Context, pass *requestPass, record Record) error {
	state, err := service.appendChecked(ctx, []byte(record.Kind+"."+TenantPrincipal(record.Tenant, record.Host)+"."+record.Request), record)
	if err == nil {
		pass.state = state
	}
	return err
}

// upsert is T-O4 and T-O5 for one approval, writing to out what the operator is to do when either fails.
func (service *Service) upsert(ctx context.Context, pass *requestPass, request CredentialRequest, out io.Writer) error {
	results, err := kadm.NewClient(service.Operator).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{
		User: TenantPrincipal(request.Tenant, request.Host), Mechanism: kadm.ScramSha512,
		Iterations: request.SCRAM.Iterations, Salt: request.SCRAM.Salt, SaltedPassword: request.SCRAM.SaltedPassword,
	}})
	if err != nil {
		// A request-level failure says nothing of whether the controller makes the change, then or later.
		fmt.Fprintf(out, "%v\napproval of %s is still open; the upsert may still be applied, at any time and over a later approve or revoke of this host; run approve again, or, if the host is in doubt, revoke it\n", err, request.ID)
		return err
	}
	if err := results.Error(); err != nil {
		fmt.Fprintln(out, err)
		return err
	}
	if err := service.appendDecision(ctx, pass, decisionRecord("credential", request)); err != nil {
		fmt.Fprintln(out, err)
		if errors.Is(err, errApprovalNotOpen) {
			fmt.Fprintln(out, "run revoke for this host again")
		}
		return err
	}
	fmt.Fprintf(out, "approved %s\n", request.ID)
	return nil
}

// ApproveRequests is an approve pass for request id, or for every pending request with all, at now.
func (service *Service) ApproveRequests(ctx context.Context, id string, all bool, now time.Time, out io.Writer) (err error) {
	ctx, span := service.marker(ctx, "request approve")
	defer ended(span, &err)
	pass, err := service.loadRequestPass(ctx)
	if err != nil {
		return err
	}
	if hosts := pass.requestHosts(id); !all && len(hosts) > 1 {
		return ambiguous(id, hosts)
	}
	failed := 0
	acted := false
	for _, principal := range slices.Sorted(maps.Keys(pass.state.Approvals)) {
		approval := pass.state.Approvals[principal]
		index := slices.IndexFunc(pass.copies, func(copy requestCopy) bool { return copy.digest == approval.Digest })
		if index < 0 {
			fmt.Fprintf(out, "approval of %s has no copy of its payload in %s\n", approval.Request, RequestsTopic)
			failed++
			continue
		}
		if err := service.upsert(ctx, &pass, pass.copies[index].request, out); err != nil {
			failed++
		} else if approval.Request == id {
			acted = true
		}
	}
	for _, key := range slices.Sorted(maps.Keys(pass.groups)) {
		group := pass.groups[key]
		if pass.undecided(group) && now.Sub(group.first) > requestLifetime {
			if err := service.appendDecision(ctx, &pass, decisionRecord("expired", group.request)); err != nil {
				fmt.Fprintln(out, err)
				failed++
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(pass.groups)) {
		group := pass.groups[key]
		if !all && group.request.ID != id {
			continue
		}
		if state := pass.groupState(group, now); state != "pending" {
			if !all && !acted {
				fmt.Fprintf(out, "request %s is %s\n", id, state)
				failed++
			}
			continue
		}
		if err := group.request.checkSCRAM(); err != nil {
			fmt.Fprintf(out, "request %s: %v\n", group.request.ID, err)
			failed++
			continue
		}
		record := decisionRecord("approving", group.request)
		record.Digest = group.digest
		if err := service.appendDecision(ctx, &pass, record); err != nil {
			fmt.Fprintln(out, err)
			failed++
			continue
		}
		if err := service.upsert(ctx, &pass, group.request, out); err != nil {
			failed++
		}
		acted = true
	}
	if !all && !acted && failed == 0 {
		fmt.Fprintf(out, "no request %s\n", id)
		failed++
	}
	if err := service.deleteDecidedRequests(ctx, pass); err != nil {
		return err
	}
	if failed > 0 {
		return fmt.Errorf("approve: %d did not finish", failed)
	}
	return nil
}

// RejectRequest is a reject pass for request id.
func (service *Service) RejectRequest(ctx context.Context, id string, out io.Writer) (err error) {
	ctx, span := service.marker(ctx, "request reject")
	defer ended(span, &err)
	pass, err := service.loadRequestPass(ctx)
	if err != nil {
		return err
	}
	hosts := pass.requestHosts(id)
	switch {
	case len(hosts) > 1:
		return ambiguous(id, hosts)
	case len(hosts) == 0:
		err = fmt.Errorf("no request %s", id)
	case pass.state.Decision(hosts[0].principal, id) != "":
		err = fmt.Errorf("request %s is %s", id, pass.state.Decision(hosts[0].principal, id))
	case pass.state.Approvals[hosts[0].principal].Request == id:
		err = fmt.Errorf("approval of %s has begun; run approve to finish it", id)
	default:
		record := decisionRecord("rejected", hosts[0].request)
		record.Reason = "rejected by the operator"
		if err = service.appendDecision(ctx, &pass, record); err == nil {
			fmt.Fprintf(out, "rejected %s\n", id)
		}
	}
	return errors.Join(err, service.deleteDecidedRequests(ctx, pass))
}

// deleteDecidedRequests is T-O7: it never passes the pass's end offset, a kept copy of an undecided request, or a copy
// whose payload an open approval names.
func (service *Service) deleteDecidedRequests(ctx context.Context, pass requestPass) error {
	open := map[string]bool{}
	for _, approval := range pass.state.Approvals {
		open[approval.Digest] = true
	}
	cutoff := pass.end
	for _, copy := range pass.copies {
		held := open[copy.digest] || copy.kept && pass.undecided(pass.groups[TenantPrincipal(copy.request.Tenant, copy.request.Host)+"\x00"+copy.request.ID])
		if held && copy.offset < cutoff {
			cutoff = copy.offset
		}
	}
	to := kadm.Offsets{}
	to.Add(kadm.Offset{Topic: RequestsTopic, Partition: 0, At: cutoff})
	deleted, err := kadm.NewClient(service.Operator).DeleteRecords(ctx, to)
	if err != nil {
		return err
	}
	return deleted.Error()
}

// ListRequests writes each request's state at now, the count of dropped copies, and the hosts T-O10 reports.
func (service *Service) ListRequests(ctx context.Context, now time.Time, out io.Writer) error {
	pass, err := service.loadRequestPass(ctx)
	if err != nil {
		return err
	}
	for _, key := range slices.Sorted(maps.Keys(pass.groups)) {
		group := pass.groups[key]
		fmt.Fprintf(out, "request=%s tenant=%s host=%s kind=%s state=%s\n", group.request.ID, group.request.Tenant, group.request.Host, group.request.Kind, pass.groupState(group, now))
	}
	if pass.badSignatures > 0 {
		fmt.Fprintf(out, "bad-signature=%d\n", pass.badSignatures)
	}
	for _, line := range CredentialReport(pass.state) {
		fmt.Fprintln(out, line)
	}
	return nil
}

// CredentialReport is a line for each live host with no credential ID or still on its enrolment password.
func CredentialReport(state State) []string {
	var lines []string
	for principal, host := range state.Hosts {
		if state.Ended[principal] || state.Revoking[principal] {
			continue
		}
		switch current := state.Credentials[principal]; current {
		case "":
			lines = append(lines, fmt.Sprintf("tenant=%s host=%s has no credential ID; revoke and re-enrol it", host.Tenant, host.Name))
		case state.Enrolments[principal]:
			lines = append(lines, fmt.Sprintf("tenant=%s host=%s is on its enrolment password", host.Tenant, host.Name))
		}
	}
	slices.Sort(lines)
	return lines
}
