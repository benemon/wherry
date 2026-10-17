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

	"github.com/benemon/agentbus/internal/wire"
)

// Enrolled is the current accepted enrolment for a receiver.
type Enrolled struct {
	SigningPub ed25519.PublicKey
	SealingPub *ecdh.PublicKey
	Labels     map[string]string
	Pool       string
	Adapters   []wire.AdapterGrant
	Selector   []wire.Requirement
	Renewal    string
	Generation int64
}

type acceptedApproval struct {
	enrolled     Enrolled
	expMS        int64
	envelopeHash string
}

type secretID struct {
	receiverID string
	name       string
}

// State computes current trust state from records applied in log order.
type State struct {
	userPublicKey   ed25519.PublicKey
	requests        map[string]*wire.EnrolRequest
	approvals       map[string]acceptedApproval
	secretVersions  map[secretID]map[int64]*wire.SecretVersion
	maximumVersions map[secretID]int64
}

// NewState constructs empty trust state rooted in the user public key.
func NewState(userPublicKey ed25519.PublicKey) *State {
	return &State{
		userPublicKey:   append(ed25519.PublicKey(nil), userPublicKey...),
		requests:        make(map[string]*wire.EnrolRequest),
		approvals:       make(map[string]acceptedApproval),
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
func Approve(requestEnvelopeBytes []byte, userKey ed25519.PrivateKey, labels map[string]string, pool string, grants []wire.AdapterGrant, selector []wire.Requirement, renewal string, generation int64, prevStateHash string, exp time.Time) ([]byte, error) {
	envelope, err := wire.DecodeEnvelope(requestEnvelopeBytes)
	if err != nil {
		return nil, err
	}
	if envelope.Type != wire.TypeEnrolRequest {
		return nil, errors.New("not an enrolment request")
	}
	decoded, err := wire.DecodePayload(wire.TypeEnrolRequest, envelope.Payload)
	if err != nil {
		return nil, err
	}
	request := decoded.(*wire.EnrolRequest)
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

// Apply verifies and applies one append-only trust record.
func (state *State) Apply(envelopeBytes []byte) error {
	envelope, err := wire.DecodeEnvelope(envelopeBytes)
	if err != nil {
		return err
	}
	switch envelope.Type {
	case wire.TypeEnrolRequest:
		return state.applyRequest(envelope, envelopeBytes)
	case wire.TypeEnrolApproval:
		return state.applyApproval(envelope, envelopeBytes)
	case wire.TypeSecretVersion:
		return state.applySecretVersion(envelope)
	default:
		return nil
	}
}

// Receiver returns the current unexpired enrolment for a receiver.
func (state *State) Receiver(id string, now time.Time) (Enrolled, error) {
	approval, ok := state.approvals[id]
	if !ok {
		return Enrolled{}, errors.New("receiver is not enrolled")
	}
	if now.UnixMilli() >= approval.expMS {
		return Enrolled{}, errors.New("receiver enrolment is expired")
	}
	return cloneEnrolled(approval.enrolled), nil
}

// CapabilityWithin verifies that a receiver capability stays within its enrolment.
func CapabilityWithin(capEnvelopeBytes []byte, enrolled Enrolled) error {
	envelope, err := wire.DecodeEnvelope(capEnvelopeBytes)
	if err != nil {
		return err
	}
	verified, err := wire.Verify(envelope, enrolled.SigningPub, wire.SignerReceiver)
	if err != nil {
		return err
	}
	capability, ok := verified.(*wire.Capability)
	if !ok {
		return errors.New("not a capability")
	}
	if capability.Pool != enrolled.Pool {
		return errors.New("capability pool exceeds enrolment")
	}
	if !wire.LabelsEqual(capability.Labels, enrolled.Labels) {
		return errors.New("capability labels exceed enrolment")
	}
	if !reflect.DeepEqual(capability.Selector, enrolled.Selector) {
		return errors.New("capability selector exceeds enrolment")
	}
	for _, offer := range capability.Offers {
		index := slices.IndexFunc(enrolled.Adapters, func(grant wire.AdapterGrant) bool { return grant.Name == offer.Adapter })
		if index < 0 {
			return errors.New("capability offer names an ungranted adapter")
		}
		for key, value := range offer.Labels {
			if !slices.Contains(enrolled.Adapters[index].Allowed[key], value) {
				return errors.New("capability offer exceeds its grant")
			}
		}
	}
	if capability.Generation != enrolled.Generation {
		return errors.New("capability generation does not match enrolment")
	}
	return nil
}

// SignRootTask signs a root task with the user key.
func SignRootTask(task wire.Task, userKey ed25519.PrivateKey) ([]byte, error) {
	if task.Depth != 0 {
		return nil, errors.New("root task depth must be zero")
	}
	return marshalSigned(wire.TypeTask, &task, userKey)
}

// VerifyRootTask verifies a user-signed root task and its root lineage fields.
func VerifyRootTask(envelopeBytes []byte, userPublicKey ed25519.PublicKey) (*wire.Task, error) {
	envelope, err := wire.DecodeEnvelope(envelopeBytes)
	if err != nil {
		return nil, err
	}
	verified, err := wire.Verify(envelope, userPublicKey, wire.SignerUser)
	if err != nil {
		return nil, err
	}
	task, ok := verified.(*wire.Task)
	if !ok {
		return nil, errors.New("not a task")
	}
	if task.Depth != 0 || task.ParentHash != "" || len(task.Chain) != 0 || task.RunID != task.RootRunID {
		return nil, errors.New("task is not a root task")
	}
	return task, nil
}

func (state *State) applyRequest(envelope wire.Envelope, envelopeBytes []byte) error {
	decoded, err := wire.DecodePayload(wire.TypeEnrolRequest, envelope.Payload)
	if err != nil {
		return err
	}
	request := decoded.(*wire.EnrolRequest)
	verified, err := wire.Verify(envelope, ed25519.PublicKey(request.SigningPub), wire.SignerReceiver)
	if err != nil {
		return fmt.Errorf("verify enrolment request: %w", err)
	}
	state.requests[EnvelopeHash(envelopeBytes)] = verified.(*wire.EnrolRequest)
	return nil
}

func (state *State) applyApproval(envelope wire.Envelope, envelopeBytes []byte) error {
	verified, err := wire.Verify(envelope, state.userPublicKey, wire.SignerUser)
	if err != nil {
		return fmt.Errorf("verify enrolment approval: %w", err)
	}
	approval := verified.(*wire.EnrolApproval)
	current, enrolled := state.approvals[approval.ReceiverID]
	if enrolled && approval.Generation <= current.enrolled.Generation {
		return nil
	}
	request, ok := state.requests[approval.RequestHash]
	if !ok || request.ReceiverID != approval.ReceiverID {
		return errors.New("approval refers to an unknown enrolment request")
	}
	wantPreviousHash := ""
	if enrolled {
		wantPreviousHash = current.envelopeHash
	}
	if approval.PrevStateHash != wantPreviousHash {
		return errors.New("approval has wrong previous state hash")
	}
	sealingPublicKey, err := ecdh.X25519().NewPublicKey(request.SealingPub)
	if err != nil {
		return fmt.Errorf("invalid request sealing key: %w", err)
	}
	state.approvals[approval.ReceiverID] = acceptedApproval{
		enrolled: Enrolled{
			SigningPub: append(ed25519.PublicKey(nil), request.SigningPub...),
			SealingPub: sealingPublicKey,
			Labels:     maps.Clone(approval.Labels),
			Pool:       approval.Pool,
			Adapters:   cloneGrants(approval.Adapters),
			Selector:   slices.Clone(approval.Selector),
			Renewal:    approval.Renewal,
			Generation: approval.Generation,
		},
		expMS:        approval.ExpMS,
		envelopeHash: EnvelopeHash(envelopeBytes),
	}
	return nil
}

func (state *State) applySecretVersion(envelope wire.Envelope) error {
	verified, err := wire.Verify(envelope, state.userPublicKey, wire.SignerUser)
	if err != nil {
		return fmt.Errorf("verify secret version: %w", err)
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
	return nil
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
		SigningPub: append(ed25519.PublicKey(nil), enrolled.SigningPub...),
		SealingPub: enrolled.SealingPub,
		Labels:     maps.Clone(enrolled.Labels),
		Pool:       enrolled.Pool,
		Adapters:   cloneGrants(enrolled.Adapters),
		Selector:   slices.Clone(enrolled.Selector),
		Renewal:    enrolled.Renewal,
		Generation: enrolled.Generation,
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
