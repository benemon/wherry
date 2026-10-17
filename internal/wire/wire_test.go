package wire

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func validPayloads(t *testing.T) map[RecordType]any {
	t.Helper()
	hashA := strings.Repeat("a", 64)
	hashB := strings.Repeat("b", 64)
	signingPublicKey, _ := fixedKey(t)
	line := Bytes("line")
	logOffset := int64(42)
	reason := "done"
	return map[RecordType]any{
		TypeTask: &Task{
			RunID: "run-1", Attempt: 1, RootRunID: "run-1", Depth: 0,
			Ancestors: []string{}, ParentHash: "", Chain: []Bytes{},
			Pool: "linux", Labels: map[string]string{"lab/os": "linux"}, Selector: []Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu"}}},
			DeadlineMS: 2_000, QueueDeadlineMS: 1_000, ReplyTopic: "replies.client-1", CorrelationID: "correlation-1",
			Message: Message{MessageID: "message-1", ContextID: "context-1", TaskID: "task-1", Role: RoleUser, Parts: []Part{{Text: "hello"}}},
		},
		TypeCancel: &Cancel{RunID: "run-1", AtMS: 1_000},
		TypeEvent: &Event{
			RunID: "run-1", Attempt: 1, TaskHash: hashA, ReceiverID: "receiver-1", AtMS: 1_000,
			State: TaskStateWorking, Kind: "heartbeat", LogOffset: &logOffset, Reason: &reason,
		},
		TypeReply: &Reply{
			RunID: "run-1", Attempt: 1, TaskHash: hashA, CorrelationID: "correlation-1", State: TaskStateCompleted,
			Artifacts: []Artifact{{ArtifactID: "artifact-1", Name: "answer", Description: "result", Parts: []Part{{Text: "done"}}}}, AtMS: 1_000,
		},
		TypeTranscript: &Transcript{RunID: "run-1", Attempt: 1, Seq: 1, Line: &line},
		TypeCapability: &Capability{
			ReceiverID: "receiver-1", Labels: map[string]string{"lab/os": "linux"}, Pool: "linux",
			Offers:   []Offer{{Adapter: "nomad", Labels: map[string]string{"runtime": "exec"}}},
			Selector: []Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu"}}},
			Capacity: 4, InFlight: 1, Generation: 2, AtMS: 1_000,
		},
		TypeEnrolRequest: &EnrolRequest{
			ReceiverID: "receiver-1", SigningPub: Bytes(signingPublicKey), SealingPub: bytes.Repeat([]byte{2}, 32),
			Labels:   map[string]string{"lab/os": "linux"},
			Adapters: []DetectedAdapter{{Name: "nomad", Platform: "nomad", Offers: []Offer{{Adapter: "nomad", Labels: map[string]string{"runtime": "exec"}}}}},
			AtMS:     1_000,
		},
		TypeEnrolApproval: &EnrolApproval{
			ReceiverID: "receiver-1", RequestHash: hashA, Labels: map[string]string{"lab/os": "linux"}, Pool: "linux",
			Adapters: []AdapterGrant{{Name: "nomad", Platform: "nomad", Allowed: map[string][]string{"runtime": {"exec"}}, Defaults: map[string]string{"runtime": "exec"}}},
			Selector: []Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu"}}},
			Renewal:  "auto", Generation: 2, PrevStateHash: hashB, ExpMS: 2_000,
		},
		TypeRenewRequest: &RenewRequest{ReceiverID: "receiver-1", Generation: 2, AtMS: 1_000},
		TypeRevocation:   &Revocation{ReceiverID: "receiver-1", FloorGeneration: 2, AtMS: 1_000},
		TypeSecretVersion: &SecretVersion{
			ReceiverID: "receiver-1", Name: "token", Version: 3, SealingKeyFP: "sha256:" + hashA,
			Generation: 2, CiphertextHash: hashB, ExpMS: 2_000,
		},
		TypeSealedSecret: &SealedSecret{ReceiverID: "receiver-1", Name: "token", Version: 3, Enc: Bytes("enc"), CT: Bytes("ciphertext")},
	}
}

func fixedKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	seed, err := hex.DecodeString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	return privateKey.Public().(ed25519.PublicKey), privateKey
}

func verificationKind(recordType RecordType) SignerKind {
	switch recordType {
	case TypeTask, TypeCancel, TypeEnrolApproval, TypeRevocation, TypeSecretVersion, TypeSealedSecret:
		return SignerUser
	default:
		return SignerReceiver
	}
}

func TestSignVerifyAndStrictDecodeEveryRecordType(t *testing.T) {
	publicKey, privateKey := fixedKey(t)
	for recordType, value := range validPayloads(t) {
		t.Run(string(recordType), func(t *testing.T) {
			payload, err := MarshalPayload(recordType, value)
			if err != nil {
				t.Fatal(err)
			}
			decodedPayload, err := DecodePayload(recordType, payload)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decodedPayload, value) {
				t.Fatalf("decoded payload differs:\n got: %#v\nwant: %#v", decodedPayload, value)
			}

			envelope := Sign(recordType, payload, privateKey)
			encodedEnvelope, err := MarshalEnvelope(envelope)
			if err != nil {
				t.Fatal(err)
			}
			decodedEnvelope, err := DecodeEnvelope(encodedEnvelope)
			if err != nil {
				t.Fatal(err)
			}
			verified, err := Verify(decodedEnvelope, publicKey, verificationKind(recordType))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(verified, value) {
				t.Fatalf("verified payload differs:\n got: %#v\nwant: %#v", verified, value)
			}
		})
	}
}

func TestDecodePayloadRejectsDuplicateMember(t *testing.T) {
	_, err := DecodePayload(TypeCancel, []byte(`{"run_id":"run-1","run_id":"run-2","at_ms":1}`))
	if err == nil {
		t.Fatal("duplicate member accepted")
	}
}

func TestDecodePayloadRejectsUnknownMember(t *testing.T) {
	_, err := DecodePayload(TypeCancel, []byte(`{"run_id":"run-1","at_ms":1,"unexpected":true}`))
	if err == nil {
		t.Fatal("unknown member accepted")
	}
}

func TestDecodePayloadRejectsTrailingValue(t *testing.T) {
	_, err := DecodePayload(TypeCancel, []byte(`{"run_id":"run-1","at_ms":1} {}`))
	if err == nil {
		t.Fatal("trailing value accepted")
	}
}

func TestDecodePayloadRejectsMissingRequiredNumericMember(t *testing.T) {
	for name, testCase := range map[string]struct {
		recordType RecordType
		payload    string
	}{
		"cancel at_ms":         {TypeCancel, `{"run_id":"run-1"}`},
		"task depth":           {TypeTask, `{"run_id":"run-1","attempt":1,"root_run_id":"run-1","ancestors":[],"parent_hash":"","chain":[],"pool":"linux","labels":{},"selector":[],"deadline_ms":2,"queue_deadline_ms":1,"reply_topic":"replies.client-1","correlation_id":"correlation-1","message":{"messageId":"message-1","role":"ROLE_USER","parts":[{"text":"hello"}]}}`},
		"capability in_flight": {TypeCapability, `{"receiver_id":"receiver-1","pool":"linux","labels":{},"offers":[],"selector":[],"capacity":1,"generation":1,"at_ms":1}`},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodePayload(testCase.recordType, []byte(testCase.payload)); err == nil {
				t.Fatal("missing required numeric member accepted")
			}
		})
	}
}

func TestAttemptRequired(t *testing.T) {
	for recordType, value := range validPayloads(t) {
		if recordType != TypeTask && recordType != TypeEvent && recordType != TypeReply && recordType != TypeTranscript {
			continue
		}
		t.Run(string(recordType), func(t *testing.T) {
			payload, err := MarshalPayload(recordType, value)
			if err != nil {
				t.Fatal(err)
			}
			payload = bytes.Replace(payload, []byte(`"attempt":1,`), nil, 1)
			if _, err := DecodePayload(recordType, payload); err == nil {
				t.Fatal("missing attempt accepted")
			}
		})
	}
}

func TestAttemptZeroRejected(t *testing.T) {
	for recordType, value := range validPayloads(t) {
		if recordType != TypeTask && recordType != TypeEvent && recordType != TypeReply && recordType != TypeTranscript {
			continue
		}
		t.Run(string(recordType), func(t *testing.T) {
			payload, err := MarshalPayload(recordType, value)
			if err != nil {
				t.Fatal(err)
			}
			payload = bytes.Replace(payload, []byte(`"attempt":1`), []byte(`"attempt":0`), 1)
			if _, err := DecodePayload(recordType, payload); err == nil {
				t.Fatal("zero attempt accepted")
			}
		})
	}
}

func TestAttemptFilterDropsLowerAttempt(t *testing.T) {
	var filter AttemptFilter
	if !filter.Admit("r", 2) {
		t.Fatal("first attempt rejected")
	}
	if filter.Admit("r", 1) {
		t.Fatal("lower attempt admitted")
	}
	if !filter.Admit("r", 2) {
		t.Fatal("equal attempt rejected")
	}
	if !filter.Admit("r", 3) {
		t.Fatal("higher attempt rejected")
	}
	if !filter.Admit("other", 1) {
		t.Fatal("other run rejected")
	}
}

func TestDecodePayloadRequiresExplicitEmptyLineageFields(t *testing.T) {
	missingParentHash := `{"run_id":"run-1","attempt":1,"root_run_id":"run-1","depth":0,"ancestors":[],"chain":[],"pool":"linux","labels":{},"selector":[],"deadline_ms":2,"queue_deadline_ms":1,"reply_topic":"replies.client-1","correlation_id":"correlation-1","message":{"messageId":"message-1","role":"ROLE_USER","parts":[{"text":"hello"}]}}`
	if _, err := DecodePayload(TypeTask, []byte(missingParentHash)); err == nil {
		t.Fatal("missing parent_hash accepted")
	}
	hash := strings.Repeat("a", 64)
	missingPreviousHash := `{"receiver_id":"receiver-1","request_hash":"` + hash + `","labels":{},"pool":"linux","adapters":[],"selector":[],"renewal":"auto","generation":1,"exp_ms":2}`
	if _, err := DecodePayload(TypeEnrolApproval, []byte(missingPreviousHash)); err == nil {
		t.Fatal("missing prev_state_hash accepted")
	}
}

func TestDecodeEnvelopeIsStrict(t *testing.T) {
	for name, input := range map[string]string{
		"duplicate": `{"v":1,"v":1}`,
		"unknown":   `{"v":1,"unknown":true}`,
		"trailing":  `{"v":1} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeEnvelope([]byte(input)); err == nil {
				t.Fatal("non-strict envelope accepted")
			}
		})
	}
}

func TestVerifyRejectsCrossTypeSignature(t *testing.T) {
	publicKey, privateKey := fixedKey(t)
	payload, err := MarshalPayload(TypeEvent, validPayloads(t)[TypeEvent])
	if err != nil {
		t.Fatal(err)
	}
	envelope := Sign(TypeEvent, payload, privateKey)
	envelope.Type = TypeReply
	if _, err := Verify(envelope, publicKey, SignerReceiver); !errors.Is(err, errInvalidSignature) {
		t.Fatalf("got %v, want invalid signature", err)
	}
}

func TestVerifyRejectsAlteredPayload(t *testing.T) {
	publicKey, privateKey := fixedKey(t)
	payload, err := MarshalPayload(TypeCancel, validPayloads(t)[TypeCancel])
	if err != nil {
		t.Fatal(err)
	}
	envelope := Sign(TypeCancel, payload, privateKey)
	envelope.Payload[len(envelope.Payload)-2] ^= 1
	if _, err := Verify(envelope, publicKey, SignerUser); !errors.Is(err, errInvalidSignature) {
		t.Fatalf("got %v, want invalid signature", err)
	}
}

func TestVerifyChecksSignatureBeforePayloadDecode(t *testing.T) {
	publicKey, privateKey := fixedKey(t)
	envelope := Sign(TypeCancel, []byte(`not-json`), privateKey)
	if _, err := Verify(envelope, publicKey, SignerUser); err == nil || errors.Is(err, errInvalidSignature) {
		t.Fatalf("got %v, want payload decode error after valid signature", err)
	}
	envelope.Sig[0] ^= 1
	if _, err := Verify(envelope, publicKey, SignerUser); !errors.Is(err, errInvalidSignature) {
		t.Fatalf("got %v, want invalid signature before payload decode", err)
	}
}

func TestDecodeEnvelopeRejectsOversizeBeforeDecode(t *testing.T) {
	_, err := DecodeEnvelope(bytes.Repeat([]byte{'{'}, maxEnvelopeSize+1))
	if !errors.Is(err, errEnvelopeTooLarge) {
		t.Fatalf("got %v, want envelope too large", err)
	}
}

func TestEntitledRejectsWrongSignerKind(t *testing.T) {
	if entitled(TypeCancel, SignerReceiver) {
		t.Fatal("receiver was entitled to sign cancel")
	}
	if !entitled(TypeTask, SignerUser) || !entitled(TypeTask, SignerReceiver) {
		t.Fatal("task signer entitlements incomplete")
	}
}

func TestEntitlementTable(t *testing.T) {
	want := map[RecordType]map[SignerKind]bool{
		TypeTask:          {SignerUser: true, SignerReceiver: true},
		TypeCancel:        {SignerUser: true},
		TypeEvent:         {SignerReceiver: true},
		TypeReply:         {SignerReceiver: true},
		TypeTranscript:    {SignerReceiver: true},
		TypeCapability:    {SignerReceiver: true},
		TypeEnrolRequest:  {SignerReceiver: true},
		TypeEnrolApproval: {SignerUser: true},
		TypeRenewRequest:  {SignerReceiver: true},
		TypeRevocation:    {SignerUser: true},
		TypeSecretVersion: {SignerUser: true},
		TypeSealedSecret:  {SignerUser: true},
	}
	for recordType, kinds := range want {
		for _, kind := range []SignerKind{SignerUser, SignerReceiver} {
			if entitled(recordType, kind) != kinds[kind] {
				t.Errorf("entitled(%q, %q) = %t, want %t", recordType, kind, entitled(recordType, kind), kinds[kind])
			}
		}
	}
}

func TestVerifyRejectsWrongSignerFingerprint(t *testing.T) {
	publicKey, privateKey := fixedKey(t)
	payload, err := MarshalPayload(TypeCancel, validPayloads(t)[TypeCancel])
	if err != nil {
		t.Fatal(err)
	}
	envelope := Sign(TypeCancel, payload, privateKey)
	envelope.Signer = "sha256:" + strings.Repeat("0", 64)
	if _, err := Verify(envelope, publicKey, SignerUser); !errors.Is(err, errInvalidSignature) {
		t.Fatalf("got %v, want invalid signature", err)
	}
	if Fingerprint(publicKey) == envelope.Signer {
		t.Fatal("test signer fingerprint unexpectedly matched")
	}
}

func TestVerifyRejectsEnrolmentRequestSignedByDifferentKey(t *testing.T) {
	publicKey, privateKey := fixedKey(t)
	request := validPayloads(t)[TypeEnrolRequest].(*EnrolRequest)
	request.SigningPub = bytes.Repeat([]byte{9}, ed25519.PublicKeySize)
	payload, err := MarshalPayload(TypeEnrolRequest, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(Sign(TypeEnrolRequest, payload, privateKey), publicKey, SignerReceiver); err == nil {
		t.Fatal("enrolment request signed by a different key was accepted")
	}
}

func TestVerifyRejectsUnentitledSigner(t *testing.T) {
	publicKey, privateKey := fixedKey(t)
	payload, err := MarshalPayload(TypeCancel, validPayloads(t)[TypeCancel])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(Sign(TypeCancel, payload, privateKey), publicKey, SignerReceiver); !errors.Is(err, errUnentitledSigner) {
		t.Fatalf("got %v, want unentitled signer", err)
	}
}

func TestTransitionAllowsEveryNonterminalMove(t *testing.T) {
	nonterminal := []TaskState{TaskStateUnspecified, TaskStateSubmitted, TaskStateWorking, TaskStateInputRequired, TaskStateAuthRequired}
	all := []TaskState{
		TaskStateUnspecified, TaskStateSubmitted, TaskStateWorking, TaskStateCompleted, TaskStateFailed,
		TaskStateCanceled, TaskStateInputRequired, TaskStateRejected, TaskStateAuthRequired,
	}
	for _, previous := range nonterminal {
		for _, next := range all {
			if err := Transition(previous, next); err != nil {
				t.Errorf("%s -> %s: %v", previous, next, err)
			}
		}
	}
	for _, state := range []TaskState{TaskStateCompleted, TaskStateFailed, TaskStateCanceled, TaskStateRejected} {
		if err := Transition(state, state); err != nil {
			t.Errorf("idempotent %s transition: %v", state, err)
		}
	}
}

func TestTransitionRejectsMoveOutOfTerminalStates(t *testing.T) {
	for _, state := range []TaskState{TaskStateCompleted, TaskStateFailed, TaskStateCanceled, TaskStateRejected} {
		if err := Transition(state, TaskStateWorking); err == nil {
			t.Errorf("transition out of %s accepted", state)
		}
	}
}

func TestRequirementUnknownOperatorRejected(t *testing.T) {
	task := validPayloads(t)[TypeTask].(*Task)
	task.Selector[0].Operator = "Exists"
	if _, err := MarshalPayload(TypeTask, task); err == nil || !strings.Contains(err.Error(), "unknown operator") {
		t.Fatalf("got %v, want unknown operator", err)
	}
}

func TestKeyGrammar(t *testing.T) {
	for _, key := range []string{"platform", "runtime", "region", "zone", "nomad.node_pool", "lab/gpu"} {
		if !validKey(key) {
			t.Errorf("valid key %q rejected", key)
		}
	}
	for _, key := range []string{"team", "Nomad.pool", "/x", "a/"} {
		if validKey(key) {
			t.Errorf("invalid key %q accepted", key)
		}
	}
}

func nomadGrant(defaults map[string]string) []AdapterGrant {
	return []AdapterGrant{{Name: "nomad", Platform: "nomad", Allowed: map[string][]string{}, Defaults: defaults}}
}

func nomadOffers(offers ...map[string]string) []Offer {
	result := make([]Offer, 0, len(offers))
	for _, labels := range offers {
		result = append(result, Offer{Adapter: "nomad", Labels: labels})
	}
	return result
}

func TestMatchOfferRespectsCombinations(t *testing.T) {
	selector := []Requirement{{Key: "runtime", Operator: OpIn, Values: []string{"exec"}}, {Key: "nomad.node_pool", Operator: OpIn, Values: []string{"default"}}}
	offers := nomadOffers(map[string]string{"runtime": "exec", "nomad.node_pool": "OCPv"}, map[string]string{"runtime": "podman", "nomad.node_pool": "default"})
	if _, ok := MatchOffer(selector, map[string]string{}, nomadGrant(map[string]string{}), offers); ok {
		t.Fatal("matched a flattened combination that no offer contains")
	}
}

func TestMatchOfferAppliesDefaults(t *testing.T) {
	offers := nomadOffers(map[string]string{"runtime": "exec"}, map[string]string{"runtime": "podman"})
	matched, ok := MatchOffer([]Requirement{}, map[string]string{}, nomadGrant(map[string]string{"runtime": "podman"}), offers)
	if !ok || matched.Labels["runtime"] != "podman" {
		t.Fatalf("matched %#v, %v", matched, ok)
	}
}

func TestMatchOfferExplicitOverridesDefault(t *testing.T) {
	selector := []Requirement{{Key: "runtime", Operator: OpIn, Values: []string{"exec"}}}
	offers := nomadOffers(map[string]string{"runtime": "exec"}, map[string]string{"runtime": "podman"})
	matched, ok := MatchOffer(selector, map[string]string{}, nomadGrant(map[string]string{"runtime": "podman"}), offers)
	if !ok || matched.Labels["runtime"] != "exec" {
		t.Fatalf("matched %#v, %v", matched, ok)
	}
}

func twoAdapterGrants() []AdapterGrant {
	return []AdapterGrant{
		{Name: "lab", Platform: "nomad", Allowed: map[string][]string{"runtime": {"exec", "podman"}}, Defaults: map[string]string{}},
		{Name: "cloud", Platform: "aws", Allowed: map[string][]string{"runtime": {"lambda", "exec"}}, Defaults: map[string]string{}},
	}
}

func TestMatchOfferGrantOrderPreference(t *testing.T) {
	grants := twoAdapterGrants()
	offers := []Offer{
		{Adapter: "cloud", Labels: map[string]string{"runtime": "exec"}},
		{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}},
	}
	SortOffers(grants, offers)
	matched, ok := MatchOffer([]Requirement{{Key: "runtime", Operator: OpIn, Values: []string{"exec"}}}, map[string]string{}, grants, offers)
	if !ok || matched.Adapter != "lab" {
		t.Fatalf("matched %#v, %v; want the first grant's adapter", matched, ok)
	}
}

func TestMatchOfferPlatformFromGrant(t *testing.T) {
	grants := twoAdapterGrants()
	offers := []Offer{
		{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}},
		{Adapter: "cloud", Labels: map[string]string{"runtime": "lambda"}},
		{Adapter: "cloud", Labels: map[string]string{"runtime": "exec"}},
	}
	selector := []Requirement{{Key: "platform", Operator: OpIn, Values: []string{"aws"}}}
	matched, ok := MatchOffer(selector, map[string]string{}, grants, offers)
	if !ok || matched.Adapter != "cloud" || matched.Labels["runtime"] != "lambda" {
		t.Fatalf("matched %#v, %v; want the first aws offer", matched, ok)
	}
	selector = append(selector, Requirement{Key: "runtime", Operator: OpIn, Values: []string{"podman"}})
	if matched, ok := MatchOffer(selector, map[string]string{}, grants, offers); ok {
		t.Fatalf("matched %#v; podman is offered by no aws adapter", matched)
	}
}

func TestMatchOfferDefaultsPerAdapter(t *testing.T) {
	grants := twoAdapterGrants()
	grants[0].Defaults = map[string]string{"runtime": "podman"}
	grants[1].Defaults = map[string]string{"runtime": "lambda"}
	offers := []Offer{
		{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}},
		{Adapter: "cloud", Labels: map[string]string{"runtime": "exec"}},
		{Adapter: "cloud", Labels: map[string]string{"runtime": "lambda"}},
	}
	matched, ok := MatchOffer([]Requirement{}, map[string]string{}, grants, offers)
	if !ok || matched.Adapter != "cloud" || matched.Labels["runtime"] != "lambda" {
		t.Fatalf("matched %#v, %v; want cloud's own default, not lab's", matched, ok)
	}
	matched, ok = MatchOffer([]Requirement{{Key: "runtime", Operator: OpIn, Values: []string{"exec"}}}, map[string]string{}, grants, offers)
	if !ok || matched.Adapter != "lab" {
		t.Fatalf("matched %#v, %v; a named key must ignore defaults", matched, ok)
	}
}

func TestOfferWithoutGrantNeverMatches(t *testing.T) {
	offers := []Offer{{Adapter: "ungranted", Labels: map[string]string{"runtime": "exec"}}}
	if matched, ok := MatchOffer([]Requirement{}, map[string]string{}, twoAdapterGrants(), offers); ok {
		t.Fatalf("matched %#v from an adapter without a grant", matched)
	}
}

func TestSortOffersByGrantThenLabels(t *testing.T) {
	offers := []Offer{
		{Adapter: "ungranted", Labels: map[string]string{"runtime": "a"}},
		{Adapter: "cloud", Labels: map[string]string{"runtime": "exec"}},
		{Adapter: "lab", Labels: map[string]string{"runtime": "podman"}},
		{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}},
	}
	SortOffers(twoAdapterGrants(), offers)
	var got []string
	for _, offer := range offers {
		got = append(got, offer.Adapter+":"+TermsText(offer.Labels))
	}
	want := []string{"lab:runtime=exec", "lab:runtime=podman", "cloud:runtime=exec", "ungranted:runtime=a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestTermsTextIsDeterministic(t *testing.T) {
	if got, want := TermsText(map[string]string{"region": "eu", "lab/arch": "arm64"}), "lab/arch=arm64,region=eu"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReceiverLabelsRejectPlatform(t *testing.T) {
	payloads := validPayloads(t)
	payloads[TypeEnrolRequest].(*EnrolRequest).Labels["platform"] = "nomad"
	payloads[TypeEnrolApproval].(*EnrolApproval).Labels["platform"] = "nomad"
	payloads[TypeCapability].(*Capability).Labels["platform"] = "nomad"
	for _, recordType := range []RecordType{TypeEnrolRequest, TypeEnrolApproval, TypeCapability} {
		if _, err := MarshalPayload(recordType, payloads[recordType]); err == nil || !strings.Contains(err.Error(), "platform") {
			t.Errorf("%s: got %v, want platform label rejected", recordType, err)
		}
	}
}

func TestGrantAllowedRejectsPlatformAndLabelKeys(t *testing.T) {
	for name, key := range map[string]string{"platform": "platform", "receiver label": "lab/os"} {
		t.Run(name, func(t *testing.T) {
			approval := validPayloads(t)[TypeEnrolApproval].(*EnrolApproval)
			approval.Adapters[0].Allowed[key] = []string{"linux"}
			if _, err := MarshalPayload(TypeEnrolApproval, approval); err == nil || !strings.Contains(err.Error(), "exclude platform and receiver label keys") {
				t.Fatalf("got %v, want allowed key rejected", err)
			}
		})
	}
}

func TestAdapterNamesValidated(t *testing.T) {
	for name, mutate := range map[string]func(*EnrolApproval){
		"duplicate": func(value *EnrolApproval) { value.Adapters = append(value.Adapters, value.Adapters[0]) },
		"uppercase": func(value *EnrolApproval) { value.Adapters[0].Name = "Nomad" },
		"platform":  func(value *EnrolApproval) { value.Adapters[0].Platform = "" },
		"pool":      func(value *EnrolApproval) { value.Pool = "Linux" },
	} {
		t.Run(name, func(t *testing.T) {
			approval := validPayloads(t)[TypeEnrolApproval].(*EnrolApproval)
			mutate(approval)
			if _, err := MarshalPayload(TypeEnrolApproval, approval); err == nil {
				t.Fatal("invalid approval accepted")
			}
		})
	}
}

func TestOfferValidation(t *testing.T) {
	for name, offer := range map[string]Offer{
		"empty labels":    {Adapter: "nomad", Labels: map[string]string{}},
		"platform key":    {Adapter: "nomad", Labels: map[string]string{"platform": "nomad"}},
		"invalid key":     {Adapter: "nomad", Labels: map[string]string{"team": "a"}},
		"missing adapter": {Adapter: "", Labels: map[string]string{"runtime": "exec"}},
	} {
		t.Run(name, func(t *testing.T) {
			capability := validPayloads(t)[TypeCapability].(*Capability)
			capability.Offers = []Offer{offer}
			if _, err := MarshalPayload(TypeCapability, capability); err == nil {
				t.Fatal("invalid offer accepted")
			}
		})
	}
	request := validPayloads(t)[TypeEnrolRequest].(*EnrolRequest)
	request.Adapters[0].Offers[0].Adapter = "other"
	if _, err := MarshalPayload(TypeEnrolRequest, request); err == nil {
		t.Fatal("detected offer naming another adapter accepted")
	}
}

func TestApprovalAllowedValidation(t *testing.T) {
	for name, mutate := range map[string]func(*AdapterGrant){
		"overlap":         func(grant *AdapterGrant) { grant.Allowed = map[string][]string{"lab/os": {"linux"}} },
		"default outside": func(grant *AdapterGrant) { grant.Defaults = map[string]string{"runtime": "podman"} },
		"empty values":    func(grant *AdapterGrant) { grant.Allowed = map[string][]string{"runtime": {}} },
	} {
		t.Run(name, func(t *testing.T) {
			approval := validPayloads(t)[TypeEnrolApproval].(*EnrolApproval)
			mutate(&approval.Adapters[0])
			if _, err := MarshalPayload(TypeEnrolApproval, approval); err == nil {
				t.Fatal("invalid approval accepted")
			}
		})
	}
}

func TestDecodeTranscriptRequiresExactlyOneContent(t *testing.T) {
	hash := strings.Repeat("a", 64)
	for name, input := range map[string]string{
		"both":    `{"run_id":"run-1","attempt":1,"seq":1,"line":"bGluZQ","pointer":{"url":"https://example.test/line","sha256":"` + hash + `","size":4}}`,
		"neither": `{"run_id":"run-1","attempt":1,"seq":1}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodePayload(TypeTranscript, []byte(input)); err == nil {
				t.Fatal("invalid transcript accepted")
			}
		})
	}
}

func TestDecodePayloadRejectsFloatNumericField(t *testing.T) {
	_, err := DecodePayload(TypeCancel, []byte(`{"run_id":"run-1","at_ms":1.5}`))
	if err == nil {
		t.Fatal("float accepted for integer field")
	}
}

func TestDecodeRejectsEmptyLabelValue(t *testing.T) {
	for name, testCase := range map[string]struct {
		recordType RecordType
		payload    string
	}{
		"labels":         {TypeCapability, `{"receiver_id":"receiver-1","pool":"linux","labels":{"lab/os":""},"offers":[],"selector":[],"capacity":1,"in_flight":0,"generation":1,"at_ms":1}`},
		"selector":       {TypeCapability, `{"receiver_id":"receiver-1","pool":"linux","labels":{"lab/os":"linux"},"offers":[],"selector":[{"key":"region","operator":"In","values":[""]}],"capacity":1,"in_flight":0,"generation":1,"at_ms":1}`},
		"request labels": {TypeEnrolRequest, `{"receiver_id":"receiver-1","signing_pub":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","sealing_pub":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","labels":{"lab/os":""},"adapters":[],"at_ms":1}`},
		"offer labels":   {TypeCapability, `{"receiver_id":"receiver-1","pool":"linux","labels":{},"offers":[{"adapter":"nomad","labels":{"runtime":""}}],"selector":[],"capacity":1,"in_flight":0,"generation":1,"at_ms":1}`},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodePayload(testCase.recordType, []byte(testCase.payload)); err == nil {
				t.Fatal("empty map value accepted")
			}
		})
	}
}

func TestDecodeRejectsEmptyLabelKey(t *testing.T) {
	payload := `{"receiver_id":"receiver-1","pool":"linux","labels":{"":"linux"},"offers":[],"selector":[],"capacity":1,"in_flight":0,"generation":1,"at_ms":1}`
	if _, err := DecodePayload(TypeCapability, []byte(payload)); err == nil {
		t.Fatal("empty map key accepted")
	}
}

func TestBytesUseUnpaddedBase64URL(t *testing.T) {
	encoded, err := json.Marshal(&SealedSecret{ReceiverID: "receiver-1", Name: "token", Version: 1, Enc: Bytes{0xfb, 0xff}, CT: Bytes{0xff}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(string(encoded), "+/=") || !strings.Contains(string(encoded), "-_8") {
		t.Fatalf("not unpadded base64url: %s", encoded)
	}
	bad := `{"receiver_id":"receiver-1","name":"token","version":1,"enc":"+/8=","ct":"_w"}`
	if _, err := DecodePayload(TypeSealedSecret, []byte(bad)); err == nil {
		t.Fatal("padded standard base64 accepted")
	}
}

func TestRFC8032Vector1(t *testing.T) {
	publicKey, _ := hex.DecodeString("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
	signature, _ := hex.DecodeString("e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b")
	if !verifySignature(publicKey, nil, signature) {
		t.Fatal("RFC 8032 section 7.1 vector 1 failed")
	}
}

func TestEnvelopeAndPayloadUseRawBase64URL(t *testing.T) {
	_, privateKey := fixedKey(t)
	payload := []byte{0xfb, 0xff}
	envelope := Sign(TypeCancel, payload, privateKey)
	encoded, err := MarshalEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), base64.StdEncoding.EncodeToString(payload)) {
		t.Fatalf("envelope used standard base64: %s", encoded)
	}
}
