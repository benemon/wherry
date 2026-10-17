package trust

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// Enrolled is the current accepted enrolment for a receiver.
type Enrolled struct {
	SigningPub ed25519.PublicKey
	SealingPub *ecdh.PublicKey
	Labels     map[string]string
	Pool       string
	Adapters   []wire.AdapterGrant
	Selector   []wire.Requirement
	Generation int64
	// ApprovalHash is the envelope hash of the generation's founding approval.
	ApprovalHash string
}

type acceptedApproval struct {
	// enrolled keeps the founding approval's hash, which seals bind.
	enrolled Enrolled
	// head is the latest accepted approval or renewal at that generation.
	head     *wire.EnrolApproval
	headHash string
}

type secretID struct {
	receiverID string
	name       string
}

// ErrRevoked reports that a receiver's accepted approval is at or below its revocation floor.
var ErrRevoked = errors.New("receiver is revoked")

// State computes current trust state from records applied in log order.
type State struct {
	userPublicKey   ed25519.PublicKey
	requests        map[string]*wire.EnrolRequest
	approvals       map[string]acceptedApproval
	floors          map[string]int64
	renewRequests   map[string]*wire.RenewRequest
	secretVersions  map[secretID]map[int64]*wire.SecretVersion
	maximumVersions map[secretID]int64
}

// NewState constructs empty trust state rooted in the user public key.
func NewState(userPublicKey ed25519.PublicKey) *State {
	return &State{
		userPublicKey:   append(ed25519.PublicKey(nil), userPublicKey...),
		requests:        make(map[string]*wire.EnrolRequest),
		approvals:       make(map[string]acceptedApproval),
		floors:          make(map[string]int64),
		renewRequests:   make(map[string]*wire.RenewRequest),
		secretVersions:  make(map[secretID]map[int64]*wire.SecretVersion),
		maximumVersions: make(map[secretID]int64),
	}
}

// EnvelopeHash returns the SHA-256 digest of exact envelope bytes as lowercase hex.
func EnvelopeHash(envelopeBytes []byte) string {
	digest := sha256.Sum256(envelopeBytes)
	return hex.EncodeToString(digest[:])
}

// NewEnrolRequest builds a receiver-self-signed enrolment request.
func NewEnrolRequest(receiverID string, signingKey ed25519.PrivateKey, sealingKey *ecdh.PrivateKey, labels map[string]string, adapters []wire.DetectedAdapter, now time.Time) ([]byte, error) {
	if sealingKey == nil {
		return nil, errors.New("sealing key is required")
	}
	request := &wire.EnrolRequest{
		ReceiverID: receiverID,
		SigningPub: wire.Bytes(signingKey.Public().(ed25519.PublicKey)),
		SealingPub: wire.Bytes(sealingKey.PublicKey().Bytes()),
		Labels:     labels,
		Adapters:   adapters,
		AtMS:       now.UnixMilli(),
	}
	return marshalSigned(wire.TypeEnrolRequest, request, signingKey)
}

// Approve verifies an enrolment request and signs its approval with the user key.
func Approve(requestEnvelopeBytes []byte, request *wire.EnrolRequest, userKey ed25519.PrivateKey, labels map[string]string, pool string, grants []wire.AdapterGrant, selector []wire.Requirement, renewal string, generation int64, prevStateHash string, exp time.Time) ([]byte, error) {
	envelope, err := wire.DecodeEnvelope(requestEnvelopeBytes)
	if err != nil {
		return nil, err
	}
	if envelope.Type != wire.TypeEnrolRequest {
		return nil, errors.New("not an enrolment request")
	}
	verified, err := wire.Verify(envelope, ed25519.PublicKey(request.SigningPub), wire.SignerReceiver)
	if err != nil {
		return nil, fmt.Errorf("verify enrolment request: %w", err)
	}
	request = verified.(*wire.EnrolRequest)
	approval := &wire.EnrolApproval{
		ReceiverID:    request.ReceiverID,
		RequestHash:   EnvelopeHash(requestEnvelopeBytes),
		Labels:        labels,
		Pool:          pool,
		Adapters:      grants,
		Selector:      selector,
		Renewal:       renewal,
		Generation:    generation,
		PrevStateHash: prevStateHash,
		ExpMS:         exp.UnixMilli(),
	}
	return marshalSigned(wire.TypeEnrolApproval, approval, userKey)
}

// Renew signs a renewal of the current approval: identical terms, chained to its hash, expiring at exp.
func Renew(current wire.EnrolApproval, currentHash string, userKey ed25519.PrivateKey, exp time.Time) ([]byte, error) {
	current.PrevStateHash, current.ExpMS = currentHash, exp.UnixMilli()
	return marshalSigned(wire.TypeEnrolApproval, &current, userKey)
}

// NewRevocation signs a revocation floor for a receiver with the user key.
func NewRevocation(receiverID string, floor int64, userKey ed25519.PrivateKey, now time.Time) ([]byte, error) {
	return marshalSigned(wire.TypeRevocation, &wire.Revocation{ReceiverID: receiverID, FloorGeneration: floor, AtMS: now.UnixMilli()}, userKey)
}

// Apply verifies and applies one append-only trust record and returns its verified payload, nil for other record types.
func (state *State) Apply(envelopeBytes []byte) (any, error) {
	envelope, err := wire.DecodeEnvelope(envelopeBytes)
	if err != nil {
		return nil, err
	}
	switch envelope.Type {
	case wire.TypeEnrolRequest:
		return state.applyRequest(envelope, envelopeBytes)
	case wire.TypeEnrolApproval:
		return state.applyApproval(envelope, envelopeBytes)
	case wire.TypeRenewRequest:
		return state.applyRenewRequest(envelope)
	case wire.TypeRevocation:
		return state.applyRevocation(envelope)
	case wire.TypeSecretVersion:
		return state.applySecretVersion(envelope)
	default:
		return nil, nil
	}
}

// Receiver returns the current unexpired, unrevoked enrolment for a receiver.
func (state *State) Receiver(id string, now time.Time) (Enrolled, error) {
	approval, ok := state.approvals[id]
	if !ok {
		return Enrolled{}, errors.New("receiver is not enrolled")
	}
	if approval.enrolled.Generation <= state.floors[id] {
		return Enrolled{}, ErrRevoked
	}
	if now.UnixMilli() >= approval.head.ExpMS {
		return Enrolled{}, errors.New("receiver enrolment is expired")
	}
	return cloneEnrolled(approval.enrolled), nil
}

// Approval returns a receiver's latest accepted approval or renewal and its envelope hash, whether or not it is expired or revoked.
func (state *State) Approval(id string) (wire.EnrolApproval, string, bool) {
	approval, ok := state.approvals[id]
	if !ok {
		return wire.EnrolApproval{}, "", false
	}
	return *approval.head, approval.headHash, true
}

// RenewRequest returns the receiver's unanswered renew request for its current, unrevoked generation.
func (state *State) RenewRequest(id string) (wire.RenewRequest, bool) {
	request, ok := state.renewRequests[id]
	if !ok || request.Generation != state.approvals[id].enrolled.Generation || request.Generation <= state.floors[id] {
		return wire.RenewRequest{}, false
	}
	return *request, true
}

// Floor returns the highest revocation floor applied for a receiver, 0 if none.
func (state *State) Floor(id string) int64 {
	return state.floors[id]
}

// CapabilityWithin verifies that a receiver capability stays within its enrolment and returns it.
func CapabilityWithin(capEnvelopeBytes []byte, enrolled Enrolled) (*wire.Capability, error) {
	envelope, err := wire.DecodeEnvelope(capEnvelopeBytes)
	if err != nil {
		return nil, err
	}
	verified, err := wire.Verify(envelope, enrolled.SigningPub, wire.SignerReceiver)
	if err != nil {
		return nil, err
	}
	capability, ok := verified.(*wire.Capability)
	if !ok {
		return nil, errors.New("not a capability")
	}
	if capability.Pool != enrolled.Pool {
		return nil, errors.New("capability pool exceeds enrolment")
	}
	if !maps.Equal(capability.Labels, enrolled.Labels) {
		return nil, errors.New("capability labels exceed enrolment")
	}
	if !reflect.DeepEqual(capability.Selector, enrolled.Selector) {
		return nil, errors.New("capability selector exceeds enrolment")
	}
	for _, offer := range capability.Offers {
		index := slices.IndexFunc(enrolled.Adapters, func(grant wire.AdapterGrant) bool { return grant.Name == offer.Adapter })
		if index < 0 {
			return nil, errors.New("capability offer names an ungranted adapter")
		}
		for key, value := range offer.Labels {
			if !slices.Contains(enrolled.Adapters[index].Allowed[key], value) {
				return nil, errors.New("capability offer exceeds its grant")
			}
		}
	}
	if capability.Generation != enrolled.Generation {
		return nil, errors.New("capability generation does not match enrolment")
	}
	return capability, nil
}

// SignRootSubmission signs a root submission with the user key.
func SignRootSubmission(submission wire.SubmissionRecord, userKey ed25519.PrivateKey) ([]byte, error) {
	if submission.Wherry.Lineage.Depth != 0 {
		return nil, errors.New("root submission depth must be zero")
	}
	return marshalSigned(wire.TypeSubmission, &submission, userKey)
}

// VerifyRootSubmission verifies a user-signed root submission and its root lineage fields.
func VerifyRootSubmission(envelopeBytes []byte, userPublicKey ed25519.PublicKey) (*wire.SubmissionRecord, error) {
	envelope, err := wire.DecodeEnvelope(envelopeBytes)
	if err != nil {
		return nil, err
	}
	verified, err := wire.Verify(envelope, userPublicKey, wire.SignerUser)
	if err != nil {
		return nil, err
	}
	submission, ok := verified.(*wire.SubmissionRecord)
	if !ok {
		return nil, errors.New("not a submission")
	}
	if lineage := submission.Wherry.Lineage; lineage.Depth != 0 || lineage.ParentHash != "" || len(lineage.Chain) != 0 || submission.Wherry.RunID != lineage.RootRunID {
		return nil, errors.New("submission is not a root submission")
	}
	return submission, nil
}

// NewCancel signs a request to cancel a run with the user key.
func NewCancel(runID string, userKey ed25519.PrivateKey) ([]byte, error) {
	return marshalSigned(wire.TypeCancel, &wire.CancelRecord{CancelRequest: wire.CancelTaskRequest{ID: runID}}, userKey)
}

// VerifyCancel verifies a user-signed cancel and returns the run id it cancels.
func VerifyCancel(envelopeBytes []byte, userPublicKey ed25519.PublicKey) (string, error) {
	envelope, err := wire.DecodeEnvelope(envelopeBytes)
	if err != nil {
		return "", err
	}
	verified, err := wire.Verify(envelope, userPublicKey, wire.SignerUser)
	if err != nil {
		return "", err
	}
	cancel, ok := verified.(*wire.CancelRecord)
	if !ok {
		return "", errors.New("not a cancel")
	}
	return cancel.CancelRequest.ID, nil
}

func (state *State) applyRequest(envelope wire.Envelope, envelopeBytes []byte) (any, error) {
	decoded, err := wire.DecodePayload(wire.TypeEnrolRequest, envelope.Payload)
	if err != nil {
		return nil, err
	}
	request := decoded.(*wire.EnrolRequest)
	verified, err := wire.Verify(envelope, ed25519.PublicKey(request.SigningPub), wire.SignerReceiver)
	if err != nil {
		return nil, fmt.Errorf("verify enrolment request: %w", err)
	}
	state.requests[EnvelopeHash(envelopeBytes)] = verified.(*wire.EnrolRequest)
	return verified, nil
}

func (state *State) applyApproval(envelope wire.Envelope, envelopeBytes []byte) (any, error) {
	verified, err := wire.Verify(envelope, state.userPublicKey, wire.SignerUser)
	if err != nil {
		return nil, fmt.Errorf("verify enrolment approval: %w", err)
	}
	approval := verified.(*wire.EnrolApproval)
	if approval.Generation <= state.floors[approval.ReceiverID] {
		return nil, errors.New("approval is at or below the revocation floor")
	}
	current, enrolled := state.approvals[approval.ReceiverID]
	if enrolled && approval.Generation == current.enrolled.Generation {
		if err := renews(current, approval); err != nil {
			return nil, err
		}
		current.head, current.headHash = approval, EnvelopeHash(envelopeBytes)
		state.approvals[approval.ReceiverID] = current
		delete(state.renewRequests, approval.ReceiverID)
		return approval, nil
	}
	if enrolled && approval.Generation < current.enrolled.Generation {
		return approval, nil
	}
	request, ok := state.requests[approval.RequestHash]
	if !ok || request.ReceiverID != approval.ReceiverID {
		return nil, errors.New("approval refers to an unknown enrolment request")
	}
	wantPreviousHash := ""
	if enrolled {
		wantPreviousHash = current.headHash
	}
	if approval.PrevStateHash != wantPreviousHash {
		return nil, errors.New("approval has wrong previous state hash")
	}
	sealingPublicKey, err := ecdh.X25519().NewPublicKey(request.SealingPub)
	if err != nil {
		return nil, fmt.Errorf("invalid request sealing key: %w", err)
	}
	approvalHash := EnvelopeHash(envelopeBytes)
	state.approvals[approval.ReceiverID] = acceptedApproval{
		enrolled: Enrolled{
			SigningPub:   append(ed25519.PublicKey(nil), request.SigningPub...),
			SealingPub:   sealingPublicKey,
			Labels:       maps.Clone(approval.Labels),
			Pool:         approval.Pool,
			Adapters:     cloneGrants(approval.Adapters),
			Selector:     slices.Clone(approval.Selector),
			Generation:   approval.Generation,
			ApprovalHash: approvalHash,
		},
		head:     approval,
		headHash: approvalHash,
	}
	return approval, nil
}

// renews reports why a same-generation approval is not a renewal of the current one.
func renews(current acceptedApproval, renewal *wire.EnrolApproval) error {
	if renewal.PrevStateHash != current.headHash {
		return errors.New("renewal does not chain to the current approval")
	}
	if renewal.ExpMS <= current.head.ExpMS {
		return errors.New("renewal does not extend the current approval")
	}
	terms := *renewal
	terms.PrevStateHash, terms.ExpMS = current.head.PrevStateHash, current.head.ExpMS
	if !reflect.DeepEqual(&terms, current.head) {
		return errors.New("renewal changes the approved terms")
	}
	return nil
}

func (state *State) applyRenewRequest(envelope wire.Envelope) (any, error) {
	decoded, err := wire.DecodePayload(wire.TypeRenewRequest, envelope.Payload)
	if err != nil {
		return nil, err
	}
	current, ok := state.approvals[decoded.(*wire.RenewRequest).ReceiverID]
	if !ok {
		return nil, errors.New("renew request for an unenrolled receiver")
	}
	verified, err := wire.Verify(envelope, current.enrolled.SigningPub, wire.SignerReceiver)
	if err != nil {
		return nil, fmt.Errorf("verify renew request: %w", err)
	}
	request := verified.(*wire.RenewRequest)
	if request.Generation != current.enrolled.Generation {
		return nil, errors.New("renew request is not for the current generation")
	}
	state.renewRequests[request.ReceiverID] = request
	return request, nil
}

func (state *State) applyRevocation(envelope wire.Envelope) (any, error) {
	verified, err := wire.Verify(envelope, state.userPublicKey, wire.SignerUser)
	if err != nil {
		return nil, fmt.Errorf("verify revocation: %w", err)
	}
	revocation := verified.(*wire.Revocation)
	if revocation.FloorGeneration > state.floors[revocation.ReceiverID] {
		state.floors[revocation.ReceiverID] = revocation.FloorGeneration
	}
	return revocation, nil
}

func (state *State) applySecretVersion(envelope wire.Envelope) (any, error) {
	verified, err := wire.Verify(envelope, state.userPublicKey, wire.SignerUser)
	if err != nil {
		return nil, fmt.Errorf("verify secret version: %w", err)
	}
	version := verified.(*wire.SecretVersion)
	id := secretID{receiverID: version.ReceiverID, name: version.Name}
	if state.secretVersions[id] == nil {
		state.secretVersions[id] = make(map[int64]*wire.SecretVersion)
	}
	state.secretVersions[id][version.Version] = version
	maximum, ok := state.maximumVersions[id]
	if !ok || version.Version > maximum {
		state.maximumVersions[id] = version.Version
	}
	return version, nil
}

func marshalSigned(recordType wire.RecordType, value any, key ed25519.PrivateKey) ([]byte, error) {
	payload, err := wire.MarshalPayload(recordType, value)
	if err != nil {
		return nil, err
	}
	return wire.MarshalEnvelope(wire.Sign(recordType, payload, key))
}

func cloneEnrolled(enrolled Enrolled) Enrolled {
	return Enrolled{
		SigningPub:   append(ed25519.PublicKey(nil), enrolled.SigningPub...),
		SealingPub:   enrolled.SealingPub,
		Labels:       maps.Clone(enrolled.Labels),
		Pool:         enrolled.Pool,
		Adapters:     cloneGrants(enrolled.Adapters),
		Selector:     slices.Clone(enrolled.Selector),
		Generation:   enrolled.Generation,
		ApprovalHash: enrolled.ApprovalHash,
	}
}

func cloneGrants(grants []wire.AdapterGrant) []wire.AdapterGrant {
	cloned := make([]wire.AdapterGrant, 0, len(grants))
	for _, grant := range grants {
		allowed := make(map[string][]string, len(grant.Allowed))
		for key, values := range grant.Allowed {
			allowed[key] = append([]string(nil), values...)
		}
		cloned = append(cloned, wire.AdapterGrant{Name: grant.Name, Platform: grant.Platform, Allowed: allowed, Defaults: maps.Clone(grant.Defaults)})
	}
	return cloned
}
