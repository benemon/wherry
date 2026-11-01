package admin

import (
	"errors"
	"maps"
	"net/url"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
)

// AuthorizeSnapshot accepts a snapshot of pool written by the custodian it names, a live member of the pool. Whether
// the custodian is live by its advertisement is the caller's check.
func AuthorizeSnapshot(state State, keys signed.Keys, encoded []byte, pool string) (wire.Snapshot, error) {
	snapshot, err := wire.ParseSnapshot(encoded, keys)
	if err != nil {
		return wire.Snapshot{}, err
	}
	return snapshot, state.custodianOf(snapshot, pool)
}

// AuthorizeSnapshotHistory is AuthorizeSnapshot for a snapshot the broker appended at appended, which also verifies
// against a key its writer retired after that.
func AuthorizeSnapshotHistory(state State, keys signed.Keys, encoded []byte, pool string, appended time.Time) (wire.Snapshot, error) {
	snapshot, err := wire.ParseSnapshotHistory(encoded, keys, appended)
	if err != nil {
		return wire.Snapshot{}, err
	}
	return snapshot, state.custodianOf(snapshot, pool)
}

func (state State) custodianOf(snapshot wire.Snapshot, pool string) error {
	if snapshot.Pool != pool {
		return errors.New("snapshot names another pool than its topic")
	}
	return state.liveMember(snapshot.Custodian, pool)
}

// CheckEnvelope checks an envelope's values against the task they travel in under its pool's terms: the credentials are
// exactly the agent's supplied credentials and the task's, the S3 URLs exactly the task's S3 inputs and outputs, each
// presigned for its object in the pool's storage, and no value is empty. Errors name no value.
func CheckEnvelope(terms Terms, task *wire.Submission, values signed.EnvelopeValues) error {
	binding := task.Wherry
	names, err := envelopeCredentials(terms, binding)
	if err != nil {
		return err
	}
	if !sameNames(names, values.Credentials) {
		return errors.New("envelope credentials are not the agent's supplied credentials and the task's")
	}
	for name, value := range values.Credentials {
		if len(value) == 0 {
			return errors.New("envelope credential " + name + " is empty")
		}
	}
	inputs, outputs := map[string]bool{}, map[string]bool{}
	for _, input := range binding.S3Inputs {
		inputs[input.Name] = true
	}
	for _, output := range binding.Outputs {
		if output.Storage == wire.StorageS3 {
			outputs[output.Name] = true
		}
	}
	if !sameNames(inputs, values.S3Inputs) || !sameNames(outputs, values.S3Outputs) {
		return errors.New("envelope S3 URLs are not the task's S3 inputs and outputs")
	}
	presigned := func(value, key string) bool {
		parsed, err := url.Parse(value)
		return err == nil && terms.Storage != nil && terms.Storage.Serves(parsed) && parsed.User == nil && parsed.Opaque == "" && parsed.Fragment == "" &&
			parsed.EscapedPath() == "/"+terms.Storage.Bucket+"/"+key
	}
	for name, value := range values.S3Inputs {
		if !presigned(value, wire.S3InputKey(binding.Tenant, binding.InputRun(), name)) {
			return errors.New("the URL for S3 input " + name + " is not its object in the pool's storage")
		}
	}
	for name, value := range values.S3Outputs {
		if !presigned(value, wire.S3OutputKey(binding.Tenant, binding.Run, name)) {
			return errors.New("the URL for S3 output " + name + " is not its object in the pool's storage")
		}
	}
	if len(values.RedactionKey) != 32 {
		return errors.New("envelope redaction key is not 32 bytes")
	}
	return nil
}

// envelopeCredentials names the credentials a task's envelope holds under terms: the agent's supplied credentials and the
// task's.
func envelopeCredentials(terms Terms, binding wire.SubmissionBinding) (map[string]bool, error) {
	agent := slices.IndexFunc(terms.Agents, func(agent AgentTerms) bool { return agent.Name == binding.Placement.Agent })
	if agent < 0 {
		return nil, errors.New("agent is outside the pool terms")
	}
	names := map[string]bool{}
	for _, credential := range terms.Agents[agent].Credentials {
		if !slices.Contains(binding.Unsupplied, credential.Name) {
			names[credential.Name] = true
		}
	}
	for _, credential := range binding.Credentials {
		names[credential.Name] = true
	}
	return names, nil
}

// sameNames reports whether values has exactly the keys of names.
func sameNames[V any](names map[string]bool, values map[string]V) bool {
	return len(names) == len(values) && !slices.ContainsFunc(slices.Collect(maps.Keys(values)), func(name string) bool { return !names[name] })
}
