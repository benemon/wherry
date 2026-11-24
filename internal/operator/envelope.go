package operator

import (
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
)

// AuthorizeSnapshot accepts a snapshot of pool written by the custodian it names, a live member of the pool. No caller
// reads the custodian's advertisement as liveness: custody follows the custodian group's session.
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

// SigV4Algorithm is the x-amz-algorithm of a SigV4 POST policy.
const SigV4Algorithm = "AWS4-HMAC-SHA256"

// policyFields are the form fields a POST policy carries; SecurityTokenField is present only when the host's credential
// has a session token.
var policyFields = []string{"key", "bucket", "policy", "x-amz-algorithm", "x-amz-credential", "x-amz-date", "x-amz-signature"}

const SecurityTokenField = "x-amz-security-token"

// CheckEnvelope checks an envelope's values against the task they travel in under its pool's terms: the credentials are
// exactly the agent's supplied credentials and the task's, the S3 URLs exactly the task's S3 inputs, each presigned for
// its object in the pool's storage, the POST policy, when there is one, exactly a policy for the run's output prefix in
// the pool's storage, and no value is empty. Errors name no value.
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
	inputs := map[string]bool{}
	for _, input := range binding.S3Inputs {
		inputs[input.Name] = true
	}
	if !sameNames(inputs, values.S3Inputs) {
		return errors.New("envelope S3 URLs are not the task's S3 inputs")
	}
	for name, value := range values.S3Inputs {
		parsed, err := url.Parse(value)
		if err != nil || !servedBy(terms.Storage, parsed) || parsed.EscapedPath() != "/"+terms.Storage.Bucket+"/"+wire.S3InputKey(binding.Tenant, binding.InputRun(), name) {
			return errors.New("the URL for S3 input " + name + " is not its object in the pool's storage")
		}
	}
	if values.S3Policy != nil {
		if err := checkPolicy(terms, binding, *values.S3Policy); err != nil {
			return errors.New("the S3 policy is not one for the run's outputs in the pool's storage: " + err.Error())
		}
	}
	if len(values.RedactionKey) != 32 {
		return errors.New("envelope redaction key is not 32 bytes")
	}
	return nil
}

// servedBy reports whether u is a URL of storage with no user, opaque part or fragment.
func servedBy(storage *Storage, u *url.URL) bool {
	return storage != nil && storage.Serves(u) && u.User == nil && u.Opaque == "" && u.Fragment == ""
}

// checkPolicy checks a POST policy against the run it travels with: its URL is the pool's bucket, its fields exactly a
// SigV4 policy's with key the run's output prefix, and its policy document exactly the conditions the host sets,
// expiring no later than task_ended_by plus the upload grace.
func checkPolicy(terms Terms, binding wire.SubmissionBinding, policy signed.S3Policy) error {
	parsed, err := url.Parse(policy.URL)
	if err != nil || !servedBy(terms.Storage, parsed) || parsed.EscapedPath() != "/"+terms.Storage.Bucket+"/" || parsed.RawQuery != "" || parsed.ForceQuery {
		return errors.New("the URL is not the bucket")
	}
	fields := policyFields
	if _, ok := policy.Fields[SecurityTokenField]; ok {
		fields = append(slices.Clone(fields), SecurityTokenField)
	}
	if len(policy.Fields) != len(fields) || slices.ContainsFunc(fields, func(name string) bool { return policy.Fields[name] == "" }) {
		return errors.New("the fields are not a SigV4 policy's")
	}
	prefix := wire.S3OutputKey(binding.Tenant, binding.Run, "")
	if policy.Fields["key"] != prefix || policy.Fields["bucket"] != terms.Storage.Bucket || policy.Fields["x-amz-algorithm"] != SigV4Algorithm {
		return errors.New("the key, bucket or algorithm field is not the run's")
	}
	decoded, err := base64.StdEncoding.DecodeString(policy.Fields["policy"])
	if err != nil {
		return errors.New("the policy is not base64")
	}
	var document struct {
		Expiration string           `json:"expiration"`
		Conditions []jsontext.Value `json:"conditions"`
	}
	if json.Unmarshal(decoded, &document, signed.Strict) != nil || document.Conditions == nil {
		return errors.New("the policy does not decode")
	}
	expiration, err := time.Parse(time.RFC3339Nano, document.Expiration)
	if err != nil || expiration.After(time.Unix(binding.TaskEndedBy, 0).Add(UploadGrace)) {
		return errors.New("the policy expires later than task_ended_by plus the upload grace")
	}
	// Each condition the host sets is an array: [match, "$field", value] or ["content-length-range", min, max].
	want := map[string]bool{
		condition("eq", "$bucket", terms.Storage.Bucket):                                true,
		condition("starts-with", "$key", prefix):                                        true,
		condition("content-length-range", "0", strconv.FormatInt(wire.MaxS3Object, 10)): true,
		condition("eq", "$x-amz-algorithm", SigV4Algorithm):                             true,
		condition("eq", "$x-amz-credential", policy.Fields["x-amz-credential"]):         true,
		condition("eq", "$x-amz-date", policy.Fields["x-amz-date"]):                     true,
	}
	if token, ok := policy.Fields[SecurityTokenField]; ok {
		want[condition("eq", "$"+SecurityTokenField, token)] = true
	}
	seen := map[string]bool{}
	for _, raw := range document.Conditions {
		var parts []jsontext.Value
		if json.Unmarshal(raw, &parts) != nil || len(parts) != 3 {
			return errors.New("the policy holds a condition that is not a three-element array")
		}
		var match string
		if json.Unmarshal(parts[0], &match) != nil {
			return errors.New("the policy holds a condition that is not a three-element array")
		}
		var elements [3]string
		elements[0] = match
		for i, part := range parts[1:] {
			switch match {
			case "content-length-range":
				var n int64
				if json.Unmarshal(part, &n) != nil {
					return errors.New("the policy holds a content-length-range that is not two integers")
				}
				elements[i+1] = strconv.FormatInt(n, 10)
			default:
				if json.Unmarshal(part, &elements[i+1]) != nil {
					return errors.New("the policy holds a condition that is not of strings")
				}
			}
		}
		key := condition(elements[0], elements[1], elements[2])
		if !want[key] || seen[key] {
			return errors.New("the policy holds a condition other than the run's, or one twice")
		}
		seen[key] = true
	}
	if len(seen) != len(want) {
		return errors.New("the policy lacks a condition of the run's")
	}
	return nil
}

// condition is a policy condition's elements as one comparable string.
func condition(match, field, value string) string {
	encoded, _ := json.Marshal([]string{match, field, value}) // strings always encode
	return string(encoded)
}

// envelopeCredentials names the credentials a task's envelope holds: the agent's supplied credentials and the task's.
// They come from the binding alone, so a change of the agent's credentials in terms between seal and open does not fail
// the check.
func envelopeCredentials(terms Terms, binding wire.SubmissionBinding) (map[string]bool, error) {
	if !slices.ContainsFunc(terms.Agents, func(agent AgentTerms) bool { return agent.Name == binding.Placement.Agent }) {
		return nil, errors.New("agent is outside the pool terms")
	}
	names := map[string]bool{}
	for _, name := range binding.Supplied {
		names[name] = true
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
