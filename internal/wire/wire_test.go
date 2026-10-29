package wire

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"reflect"
	"slices"
	"strings"
	"testing"

	jose "github.com/go-jose/go-jose/v4"

	"github.com/benemon/wherry/internal/signed"
)

var (
	wireKey  = ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	wireKeys = signed.Keys{
		signed.HostOwner("tenant-1", "host-1"): {Current: wireKey.Public().(ed25519.PublicKey)},
		"receiver-1":                           {Current: wireKey.Public().(ed25519.PublicKey)},
	}
	wireHash = strings.Repeat("a", 64)
)

func validSubmission() *Submission {
	return &Submission{
		V:       1,
		Message: Message{MessageID: "message-1", ContextID: "run-1", TaskID: "run-1", Role: RoleUser, Parts: []Part{{Text: "hello"}}, Extensions: []string{ExtensionURI}},
		Wherry: SubmissionBinding{
			Tenant: "tenant-1", Host: "host-1", Pool: "linux", Run: "run-1",
			Placement:  Placement{Agent: "claude", Selector: []Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu"}}}},
			DeadlineMS: 2_000, QueueDeadlineMS: 1_000,
		},
	}
}

func receiverBinding() ReceiverBinding {
	return ReceiverBinding{Receiver: "receiver-1", Tenant: "tenant-1", Pool: "linux", Run: "run-1", Attempt: 1}
}

func validStatus() *RunStatus {
	return &RunStatus{
		V: 1,
		StatusUpdate: TaskStatusUpdateEvent{TaskID: "run-1", ContextID: "run-1", Status: TaskStatus{
			State: TaskStateWorking, Timestamp: "2026-10-17T10:00:00.000Z",
			Message: &Message{MessageID: "status-1", Role: RoleAgent, Parts: []Part{{Text: "working"}}, Extensions: []string{ExtensionURI}},
		}},
		Wherry: StatusBinding{ReceiverBinding: receiverBinding(), EventID: "event-1", Kind: "claimed", TaskHash: wireHash},
	}
}

func validChunk() *Chunk {
	return &Chunk{
		V: 1,
		ArtifactUpdate: TaskArtifactUpdateEvent{
			TaskID: "run-1", ContextID: "run-1", Append: true, LastChunk: true,
			Artifact: Artifact{ArtifactID: "transcript", Parts: []Part{{Text: `{"type":"assistant"}`, MediaType: "application/x-ndjson"}}, Extensions: []string{ExtensionURI}},
		},
		Wherry: ChunkBinding{ReceiverBinding: receiverBinding(), TaskHash: wireHash, Seq: 1},
	}
}

func validResult() *Result {
	return &Result{
		V: 1,
		Task: Task{
			ID: "run-1", ContextID: "run-1",
			Status:    TaskStatus{State: TaskStateCompleted, Timestamp: "2026-10-17T10:00:00.000Z"},
			History:   []Message{validSubmission().Message},
			Artifacts: []Artifact{{ArtifactID: "artifact-1", Name: "answer", Parts: []Part{{Text: "done"}}, Extensions: []string{ExtensionURI}}},
		},
		Wherry: ResultBinding{ReceiverBinding: receiverBinding(), TaskHash: wireHash},
	}
}

func validEcho() *Echo {
	submission := validSubmission()
	return &Echo{V: 1, Receiver: "receiver-1", Tenant: "tenant-1", Pool: "linux", Run: "run-1", Placement: submission.Wherry.Placement, TaskHash: wireHash, MessageID: "message-1"}
}

// recordJSON marshals value as signed.Sign does.
func recordJSON(t *testing.T, value Record) []byte {
	t.Helper()
	encoded, err := json.Marshal(value, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// parseJSON signs payload verbatim under the fixture key as owner's record of
// into's type and parses it into into.
func parseJSON(t *testing.T, owner string, payload []byte, into Record) error {
	t.Helper()
	options := new(jose.SignerOptions).WithType(jose.ContentType(into.jwsType())).WithHeader("kid", owner+"/"+signed.KeyID(wireKey.Public().(ed25519.PublicKey)))
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: wireKey}, options)
	if err != nil {
		t.Fatal(err)
	}
	jws, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := jws.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return ParseRecord([]byte(compact), wireKeys, into)
}

func replaceOnce(t *testing.T, encoded []byte, old, replacement string) []byte {
	t.Helper()
	if bytes.Count(encoded, []byte(old)) != 1 {
		t.Fatalf("payload does not hold %s once: %s", old, encoded)
	}
	return bytes.Replace(encoded, []byte(old), []byte(replacement), 1)
}

func withoutMember(t *testing.T, encoded []byte, member string) []byte {
	t.Helper()
	if bytes.Count(encoded, []byte(member+",")) == 1 {
		return replaceOnce(t, encoded, member+",", "")
	}
	return replaceOnce(t, encoded, ","+member, "")
}

// parseSubmissionParts parses the valid submission with its message parts member replaced by parts.
func parseSubmissionParts(t *testing.T, parts string) (*Submission, error) {
	t.Helper()
	payload := replaceOnce(t, recordJSON(t, validSubmission()), `"parts":[{"text":"hello"}]`, parts)
	var submission Submission
	err := parseJSON(t, "tenant-1.host-1", payload, &submission)
	return &submission, err
}

func TestFixturesParse(t *testing.T) {
	for name, fixture := range map[string]func() Record{
		"submission": func() Record { return validSubmission() }, "status": func() Record { return validStatus() },
		"chunk": func() Record { return validChunk() }, "result": func() Record { return validResult() }, "echo": func() Record { return validEcho() },
	} {
		encoded, err := SignRecord(fixture(), wireKey)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := ParseRecord(encoded, wireKeys, fixture()); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestParseRecordIsStrict(t *testing.T) {
	encoded := recordJSON(t, validSubmission())
	for name, payload := range map[string][]byte{
		"duplicate member": replaceOnce(t, encoded, `"run":"run-1"`, `"run":"run-1","run":"run-1"`),
		"trailing value":   append(bytes.Clone(encoded), " {}"...),
	} {
		t.Run(name, func(t *testing.T) {
			if err := parseJSON(t, "tenant-1.host-1", payload, &Submission{}); err == nil {
				t.Fatal("non-strict payload accepted")
			}
		})
	}
}

func TestAttemptRequired(t *testing.T) {
	for name, test := range map[string]struct {
		value   Record
		members []string
	}{
		"status":        {validStatus(), []string{`"attempt":1`}},
		"chunk":         {validChunk(), []string{`"attempt":1`}},
		"result":        {validResult(), []string{`"attempt":1`}},
		"start failure": {&StartFailure{V: 1, Wherry: receiverBinding(), Profile: "nomad", Agent: "claude", Class: "dispatch", TaskHash: strings.Repeat("a", 64)}, []string{`"attempt":1`}},
	} {
		empty := func() Record { return reflect.New(reflect.TypeOf(test.value).Elem()).Interface().(Record) }
		encoded := recordJSON(t, test.value)
		if err := parseJSON(t, test.value.owner(), encoded, empty()); err != nil {
			t.Fatalf("%s: valid fixture rejected: %v", name, err)
		}
		for _, member := range test.members {
			zero := strings.TrimSuffix(member, "1") + "0"
			for variant, payload := range map[string][]byte{"missing": withoutMember(t, encoded, member), "zero": replaceOnce(t, encoded, member, zero)} {
				t.Run(name+" "+variant+" "+member, func(t *testing.T) {
					if err := parseJSON(t, test.value.owner(), payload, empty()); err == nil {
						t.Fatal("accepted")
					}
				})
			}
		}
	}
}

func TestAttemptFilterOrdersByAttempt(t *testing.T) {
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

func TestTerminalStates(t *testing.T) {
	terminal := map[TaskState]bool{TaskStateCompleted: true, TaskStateFailed: true, TaskStateCanceled: true, TaskStateRejected: true, TaskStateAuthRequired: true}
	for _, state := range []TaskState{
		TaskStateUnspecified, TaskStateSubmitted, TaskStateWorking, TaskStateCompleted, TaskStateFailed,
		TaskStateCanceled, TaskStateInputRequired, TaskStateRejected, TaskStateAuthRequired,
	} {
		if Terminal(state) != terminal[state] {
			t.Errorf("Terminal(%s) = %v, want %v", state, Terminal(state), terminal[state])
		}
	}
}

func TestArtifactUpdateRequiresOnePart(t *testing.T) {
	pointer := Part{URL: "unstored:oversize", Metadata: jsontext.Value(`{"sha256":"` + wireHash + `","size":4}`)}
	for name, test := range map[string]struct {
		parts []Part
		ok    bool
	}{
		"text":                {[]Part{{Text: "line"}}, true},
		"pointer":             {[]Part{pointer}, true},
		"line and pointer":    {[]Part{{Text: "line"}, pointer}, false},
		"neither":             {[]Part{}, false},
		"pointer no metadata": {[]Part{{URL: "unstored:oversize"}}, false},
		"pointer bad hash":    {[]Part{{URL: "unstored:oversize", Metadata: jsontext.Value(`{"sha256":"x","size":4}`)}}, false},
		"pointer zero size":   {[]Part{{URL: "unstored:oversize", Metadata: jsontext.Value(`{"sha256":"` + wireHash + `","size":0}`)}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			chunk := validChunk()
			chunk.ArtifactUpdate.Artifact.Parts = test.parts
			if err := chunk.validate(); (err == nil) != test.ok {
				t.Fatalf("validate error %v, want ok=%v", err, test.ok)
			}
		})
	}
}

func TestPartRequiresExactlyOneContent(t *testing.T) {
	for name, test := range map[string]struct {
		parts string
		ok    bool
	}{
		"text":         {`"parts":[{"text":"hello"}]`, true},
		"raw":          {`"parts":[{"raw":"aGk=","mediaType":"text/plain","filename":"hi.txt"}]`, true},
		"empty raw":    {`"parts":[{"raw":"","filename":"empty"}]`, true},
		"url":          {`"parts":[{"url":"https://example.test/a"}]`, true},
		"data":         {`"parts":[{"data":{"k":[1,2]},"filename":"k"}]`, true},
		"null data":    {`"parts":[{"data":null,"filename":"null"}]`, true},
		"none":         {`"parts":[{"mediaType":"text/plain"}]`, false},
		"empty text":   {`"parts":[{"text":""}]`, false},
		"empty url":    {`"parts":[{"url":""}]`, false},
		"text and url": {`"parts":[{"text":"hello","url":"https://example.test/a"}]`, false},
		"text and raw": {`"parts":[{"text":"hello","raw":"aGk="}]`, false},
		"raw and data": {`"parts":[{"raw":"aGk=","data":1}]`, false},
		"url and data": {`"parts":[{"url":"https://example.test/a","data":"x"}]`, false},
		"second part":  {`"parts":[{"text":"hello"},{"text":"hello","data":true}]`, false},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseSubmissionParts(t, test.parts); (err == nil) != test.ok {
				t.Fatalf("parse error %v, want ok=%v", err, test.ok)
			}
		})
	}
	for name, part := range map[string]Part{
		"none":          {MediaType: "text/plain"},
		"text and url":  {Text: "hello", URL: "https://example.test/a"},
		"raw and data":  {Raw: StdBytes("hi"), Data: jsontext.Value(`1`)},
		"text and data": {Text: "hello", Data: jsontext.Value(`null`)},
	} {
		t.Run("built "+name, func(t *testing.T) {
			submission := validSubmission()
			submission.Message.Parts = []Part{part}
			if err := submission.validate(); err == nil {
				t.Fatal("part without exactly one content accepted")
			}
		})
	}
}

// Proto3 JSON writes bytes as padded standard base64 and accepts standard or
// URL-safe, padded or not: https://protobuf.dev/programming-guides/json/
func TestPartRawIsStandardBase64(t *testing.T) {
	result := validResult()
	result.Task.Artifacts[0].Parts = []Part{{Raw: StdBytes{0xfb, 0xff}, MediaType: "application/octet-stream"}}
	if encoded := recordJSON(t, result); !bytes.Contains(encoded, []byte(`"raw":"+/8="`)) {
		t.Fatalf("raw is not padded standard base64: %s", encoded)
	}
	for input, ok := range map[string]bool{
		"+/8=":  true,
		"+/8":   true,
		"-_8=":  true,
		"-_8":   true,
		"+_8=":  false,
		"+/8==": false,
		"+/9=":  false, // non-zero trailing bits: wherry decodes strictly
		"*":     false,
	} {
		t.Run(input, func(t *testing.T) {
			submission, err := parseSubmissionParts(t, `"parts":[{"raw":"`+input+`","filename":"f"}]`)
			if (err == nil) != ok {
				t.Fatalf("parse error %v, want ok=%v", err, ok)
			}
			if ok && !bytes.Equal(submission.Message.Parts[0].Raw, []byte{0xfb, 0xff}) {
				t.Fatalf("decoded raw %x", submission.Message.Parts[0].Raw)
			}
		})
	}
}

func TestOpaqueJSONRelayedByteForByte(t *testing.T) {
	// Unsorted names, exponent and trailing-zero numbers, an integer beyond int64 and escapes json/v2 would rewrite.
	metadata := jsontext.Value(`{"z":1,"a":[1E2,1.50,-0,12345678901234567890,"\/A"],"n":null}`)
	data := jsontext.Value(`[{"y":"é"},1e-7]`)
	submission := validSubmission()
	submission.Message.Metadata = metadata
	submission.Message.Parts = []Part{{Data: data, Metadata: metadata, Filename: "data"}}
	encoded, err := SignRecord(submission, wireKey)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.Split(string(encoded), ".")[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(payload, []byte(`"metadata":`+string(metadata))) || !bytes.Contains(payload, []byte(`"data":`+string(data))) {
		t.Fatalf("opaque JSON re-encoded: %s", payload)
	}
	var decoded Submission
	if err := ParseRecord(encoded, wireKeys, &decoded); err != nil {
		t.Fatal(err)
	}
	message := decoded.Message
	if !bytes.Equal(message.Metadata, metadata) || !bytes.Equal(message.Parts[0].Metadata, metadata) || !bytes.Equal(message.Parts[0].Data, data) {
		t.Fatalf("decoded metadata %s, part metadata %s, data %s", message.Metadata, message.Parts[0].Metadata, message.Parts[0].Data)
	}
	spaced := `{ "z" : 1 ,` + "\n\t" + `"a" : [ ] }`
	parsed, err := parseSubmissionParts(t, `"parts":[{"text":"hello"}],"metadata":`+spaced)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(parsed.Message.Metadata); got != spaced {
		t.Fatalf("decoded metadata %q, want %q", got, spaced)
	}
}

func TestMetadataMustBeObject(t *testing.T) {
	for name, test := range map[string]struct {
		parts string
		ok    bool
	}{
		"object":               {`"parts":[{"text":"hello"}],"metadata":{}`, true},
		"part object":          {`"parts":[{"text":"hello","metadata":{"k":"v"}}]`, true},
		"array":                {`"parts":[{"text":"hello"}],"metadata":[]`, false},
		"string":               {`"parts":[{"text":"hello"}],"metadata":"x"`, false},
		"number":               {`"parts":[{"text":"hello"}],"metadata":1`, false},
		"null":                 {`"parts":[{"text":"hello"}],"metadata":null`, false},
		"part array":           {`"parts":[{"text":"hello","metadata":[1]}]`, false},
		"part data not object": {`"parts":[{"data":[1],"filename":"list"}]`, true},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseSubmissionParts(t, test.parts); (err == nil) != test.ok {
				t.Fatalf("parse error %v, want ok=%v", err, test.ok)
			}
		})
	}
	for name, metadata := range map[string]jsontext.Value{"array": jsontext.Value(`[]`), "malformed": jsontext.Value(`{"k":`)} {
		t.Run("built "+name, func(t *testing.T) {
			result := validResult()
			result.Task.Artifacts[0].Metadata = metadata
			if err := result.validate(); err == nil {
				t.Fatal("artifact metadata that is not a JSON object accepted")
			}
		})
	}
}

func TestOpaqueJSONRejectsDuplicateNames(t *testing.T) {
	for name, parts := range map[string]string{
		"metadata": `"parts":[{"text":"hello"}],"metadata":{"k":1,"k":2}`,
		"data":     `"parts":[{"data":{"x":{"k":1,"k":2}}}]`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseSubmissionParts(t, parts); err == nil {
				t.Fatal("duplicate member name inside opaque JSON accepted")
			}
		})
	}
	submission := validSubmission()
	submission.Message.Metadata = jsontext.Value(`{"k":1,"k":2}`)
	if err := submission.validate(); err == nil {
		t.Fatal("duplicate member name inside built metadata accepted")
	}
}

func TestStatusTimestampIsRFC3339(t *testing.T) {
	for timestamp, ok := range map[string]bool{
		"2026-10-17T10:00:00Z":        true,
		"2026-10-17T10:00:00.123456Z": true,
		"2026-10-17T10:00:00+01:00":   true,
		"":                            false,
		"1760695200000":               false,
		"2026-10-17 10:00:00Z":        false,
	} {
		t.Run(timestamp, func(t *testing.T) {
			status := validStatus()
			status.StatusUpdate.Status.Timestamp = timestamp
			result := validResult()
			result.Task.Status.Timestamp = timestamp
			for name, value := range map[string]Record{"status": status, "result": result} {
				if err := value.validate(); (err == nil) != ok {
					t.Errorf("%s: validate error %v, want ok=%v", name, err, ok)
				}
			}
		})
	}
}

// hostKey is a known_hosts line as ssh-keyscan prints one.
const hostKey = "git.example.test ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl\n"

func TestGitInputValidation(t *testing.T) {
	https := GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"}
	ssh := GitInput{Remote: "ssh://git@git.example.test/org/repo.git", Ref: "main", HostKey: hostKey}
	with := func(base GitInput, change func(*GitInput)) GitInput {
		change(&base)
		return base
	}
	const (
		remoteRule        = "git remote must be an https or ssh url of at most 2048 letters, digits and . _ ~ % + @ : / -"
		hostRule          = "git remote must name a host and a repository path"
		refRule           = "git ref must be a branch, tag or commit name of at most 255 bytes"
		pathRule          = "git path must be a relative path of at most 512 bytes without . or .. segments; name one when the repository's name is not a portable file name"
		keyRule           = "git host_key must be at most 8 KiB of OpenSSH known_hosts lines"
		hostRuleKey       = "git host_key entries must each name the remote's host, as host or [host]:port for a port other than 22, and no wildcard"
		gitCredentialRule = "git credential must be one of the submission's task credentials, delivered as a file"
	)
	for name, test := range map[string]struct {
		git  GitInput
		want string
	}{
		"https": {https, ""},
		"ssh":   {ssh, ""},
		"ssh port": {with(ssh, func(g *GitInput) {
			g.Remote, g.HostKey = "ssh://git@git.example.test:2222/org/repo.git", "[git.example.test]:2222"+hostKey[len("git.example.test"):]
		}), ""},
		"ssh port 22":     {with(ssh, func(g *GitInput) { g.Remote = "ssh://git@Git.Example.Test:22/org/repo.git" }), ""},
		"port key bare":   {with(ssh, func(g *GitInput) { g.Remote = "ssh://git@git.example.test:2222/org/repo.git" }), hostRuleKey},
		"other host key":  {with(ssh, func(g *GitInput) { g.HostKey = "evil.test" + hostKey[len("git.example.test"):] }), hostRuleKey},
		"one other entry": {with(ssh, func(g *GitInput) { g.HostKey = hostKey + "evil.test" + hostKey[len("git.example.test"):] }), hostRuleKey},
		"wildcard key":    {with(ssh, func(g *GitInput) { g.HostKey = "*" + hostKey[len("git.example.test"):] }), hostRuleKey},
		"wildcard beside": {with(ssh, func(g *GitInput) { g.HostKey = "git.example.test,*.evil.test" + hostKey[len("git.example.test"):] }), hostRuleKey},
		"single wildcard": {with(ssh, func(g *GitInput) {
			g.HostKey = "git.example.test,git?.example.test" + hostKey[len("git.example.test"):]
		}), hostRuleKey},
		"negated beside":    {with(ssh, func(g *GitInput) { g.HostKey = "git.example.test,!other.test" + hostKey[len("git.example.test"):] }), ""},
		"hashed key":        {with(ssh, func(g *GitInput) { g.HostKey = "|1|c2FsdA==|aGFzaA==" + hostKey[len("git.example.test"):] }), hostRuleKey},
		"host list":         {with(ssh, func(g *GitInput) { g.HostKey = "GIT.example.test,192.0.2.1" + hostKey[len("git.example.test"):] }), ""},
		"marker":            {with(ssh, func(g *GitInput) { g.HostKey = "@revoked " + hostKey + hostKey }), ""},
		"marker only":       {with(ssh, func(g *GitInput) { g.HostKey = "@revoked git.example.test ssh-ed25519\n" }), keyRule},
		"commit":            {with(https, func(g *GitInput) { g.Ref = "0123456789abcdef0123456789abcdef01234567" }), ""},
		"credential":        {with(https, func(g *GitInput) { g.Credential = "forge-token" }), ""},
		"path":              {with(https, func(g *GitInput) { g.Path = "src/repo" }), ""},
		"http":              {with(https, func(g *GitInput) { g.Remote = "http://git.example.test/org/repo.git" }), remoteRule},
		"file":              {with(https, func(g *GitInput) { g.Remote = "file:///srv/repo.git" }), remoteRule},
		"scp form":          {with(https, func(g *GitInput) { g.Remote = "git@git.example.test:org/repo.git" }), remoteRule},
		"quote":             {with(https, func(g *GitInput) { g.Remote = "https://git.example.test/org/repo'.git" }), remoteRule},
		"command":           {with(https, func(g *GitInput) { g.Remote = "https://git.example.test/$(id)" }), remoteRule},
		"space":             {with(https, func(g *GitInput) { g.Remote = "https://git.example.test/org/my repo" }), remoteRule},
		"query":             {with(https, func(g *GitInput) { g.Remote = "https://git.example.test/org/repo.git?token=x" }), remoteRule},
		"fragment":          {with(https, func(g *GitInput) { g.Remote = "https://git.example.test/org/repo.git#x" }), remoteRule},
		"long remote":       {with(https, func(g *GitInput) { g.Remote = "https://git.example.test/" + strings.Repeat("a", MaxGitRemote) }), remoteRule},
		"no host":           {with(https, func(g *GitInput) { g.Remote = "https:///org/repo.git" }), hostRule},
		"bad host":          {with(https, func(g *GitInput) { g.Remote = "https://-git.example.test/org/repo.git" }), hostRule},
		"no path":           {with(https, func(g *GitInput) { g.Remote = "https://git.example.test" }), hostRule},
		"bad port":          {with(ssh, func(g *GitInput) { g.Remote = "ssh://git@git.example.test:org/repo.git" }), hostRule},
		"https userinfo":    {with(https, func(g *GitInput) { g.Remote = "https://user:token@git.example.test/org/repo.git" }), "https git remote must not carry user information"},
		"https user only":   {with(https, func(g *GitInput) { g.Remote = "https://git@git.example.test/org/repo.git" }), "https git remote must not carry user information"},
		"ssh password":      {with(ssh, func(g *GitInput) { g.Remote = "ssh://git:hunter2@git.example.test/org/repo.git" }), "ssh git remote must not carry a password"},
		"option ref":        {with(https, func(g *GitInput) { g.Ref = "--upload-pack=touch" }), refRule},
		"spaced ref":        {with(https, func(g *GitInput) { g.Ref = "main branch" }), refRule},
		"dotted ref":        {with(https, func(g *GitInput) { g.Ref = "main..other" }), refRule},
		"double slash ref":  {with(https, func(g *GitInput) { g.Ref = "release//1" }), refRule},
		"slash end ref":     {with(https, func(g *GitInput) { g.Ref = "release/" }), refRule},
		"dot end ref":       {with(https, func(g *GitInput) { g.Ref = "main." }), refRule},
		"lock end ref":      {with(https, func(g *GitInput) { g.Ref = "main.lock" }), refRule},
		"long ref":          {with(https, func(g *GitInput) { g.Ref = strings.Repeat("r", MaxGitRef+1) }), refRule},
		"no ref":            {with(https, func(g *GitInput) { g.Ref = "" }), refRule},
		"traversal":         {with(https, func(g *GitInput) { g.Path = "../outside" }), pathRule},
		"inner traversal":   {with(https, func(g *GitInput) { g.Path = "a/../../b" }), pathRule},
		"absolute path":     {with(https, func(g *GitInput) { g.Path = "/etc" }), pathRule},
		"option path":       {with(https, func(g *GitInput) { g.Path = "-c" }), pathRule},
		"long path":         {with(https, func(g *GitInput) { g.Path = strings.Repeat("a/", 256) + "a" }), pathRule},
		"no default path":   {with(https, func(g *GitInput) { g.Remote = "https://git.example.test/" }), pathRule},
		"undeclared":        {with(https, func(g *GitInput) { g.Credential = "other" }), gitCredentialRule},
		"an env credential": {with(https, func(g *GitInput) { g.Credential = "env-token" }), gitCredentialRule},
		"ssh without key":   {with(ssh, func(g *GitInput) { g.HostKey = "" }), "ssh git remote requires host_key"},
		"https with key":    {with(https, func(g *GitInput) { g.HostKey = hostKey }), "host_key applies only to an ssh git remote"},
		"one-field key":     {with(ssh, func(g *GitInput) { g.HostKey = "ssh-ed25519\n" }), keyRule},
		"blank key":         {with(ssh, func(g *GitInput) { g.HostKey = "\n\n" }), keyRule},
		"control key":       {with(ssh, func(g *GitInput) { g.HostKey = "git.example.test ssh-ed25519 AAAA\x1b[2J\n" }), keyRule},
		"long key":          {with(ssh, func(g *GitInput) { g.HostKey = strings.Repeat(hostKey, MaxGitHostKey/len(hostKey)+1) }), keyRule},
		"keyscan lines":     {with(ssh, func(g *GitInput) { g.HostKey = "# comment\n" + hostKey + "\n" + hostKey }), ""},
	} {
		t.Run(name, func(t *testing.T) {
			submission := validSubmission()
			submission.Wherry.Credentials = []TaskCredential{{Name: "forge-token", File: "token"}, {Name: "env-token", Env: "FORGE_TOKEN"}}
			submission.Wherry.Git = &test.git
			// The record round-trips through a signature, so a reader applies the same rules.
			encoded, signErr := SignRecord(submission, wireKey)
			if signErr == nil {
				signErr = ParseRecord(encoded, wireKeys, &Submission{})
			}
			if test.want == "" && signErr != nil || test.want != "" && (signErr == nil || signErr.Error() != test.want) {
				t.Fatalf("error %v, want %q", signErr, test.want)
			}
		})
	}
}
func TestTermsTextIsDeterministic(t *testing.T) {
	if got, want := TermsText(map[string]string{"region": "eu", "lab/arch": "arm64"}), "lab/arch=arm64,region=eu"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestLabelKeyGrammar(t *testing.T) {
	for _, key := range []string{
		"a", "region", "Region", "nomad.node_pool", "lab/gpu", "lab.example.com/gpu", "team-1/a.b_c",
		strings.Repeat("a", 63), strings.Repeat("a", 253) + "/x",
		"notwherry.io/x", "wherry.io.example/x", "wherry.iox/x",
	} {
		if !validKey(key) {
			t.Errorf("valid key %q rejected", key)
		}
	}
	for _, key := range []string{
		"", "/x", "a/", "a/b/c", "-a", "a-", ".a", "a b", "a:b", "é",
		strings.Repeat("a", 64), "lab/" + strings.Repeat("a", 64), strings.Repeat("a", 254) + "/x",
		"Lab/x", "lab_x/y", "-lab/x", "lab-/x", "lab..x/y", ".lab/x",
		"wherry.io/x", "sub.wherry.io/x", "a.b.wherry.io/x",
	} {
		if validKey(key) {
			t.Errorf("invalid key %q accepted", key)
		}
	}
}

func TestValidateLabels(t *testing.T) {
	for name, test := range map[string]struct {
		labels map[string]string
		ok     bool
	}{
		"nil":            {nil, true},
		"empty":          {map[string]string{}, true},
		"valid":          {map[string]string{"region": "eu", "lab/gpu": "a100", "zone": "Eu-West_1.b"}, true},
		"empty value":    {map[string]string{"region": ""}, false},
		"long value":     {map[string]string{"region": strings.Repeat("a", 64)}, false},
		"bad value":      {map[string]string{"region": "eu west"}, false},
		"dash value":     {map[string]string{"region": "-eu"}, false},
		"empty key":      {map[string]string{"": "eu"}, false},
		"bad key":        {map[string]string{"re gion": "eu"}, false},
		"reserved key":   {map[string]string{"wherry.io/pool": "linux"}, false},
		"one bad of two": {map[string]string{"region": "eu", "zone": ""}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateLabels(test.labels); (err == nil) != test.ok {
				t.Fatalf("error %v, want ok=%v", err, test.ok)
			}
		})
	}
}

func TestValidateRequirements(t *testing.T) {
	for name, test := range map[string]struct {
		requirements []Requirement
		ok           bool
	}{
		"none":                    {nil, true},
		"in":                      {[]Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu", "us"}}}, true},
		"not in":                  {[]Requirement{{Key: "region", Operator: OpNotIn, Values: []string{"eu"}}}, true},
		"exists":                  {[]Requirement{{Key: "lab/gpu", Operator: OpExists}}, true},
		"does not exist":          {[]Requirement{{Key: "lab/gpu", Operator: OpDoesNotExist}}, true},
		"distinct keys":           {[]Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu"}}, {Key: "lab/gpu", Operator: OpExists}}, true},
		"in without values":       {[]Requirement{{Key: "region", Operator: OpIn}}, false},
		"in empty values":         {[]Requirement{{Key: "region", Operator: OpIn, Values: []string{}}}, false},
		"not in without values":   {[]Requirement{{Key: "region", Operator: OpNotIn}}, false},
		"in duplicate values":     {[]Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu", "eu"}}}, false},
		"in invalid value":        {[]Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu west"}}}, false},
		"not in empty value":      {[]Requirement{{Key: "region", Operator: OpNotIn, Values: []string{""}}}, false},
		"exists with values":      {[]Requirement{{Key: "lab/gpu", Operator: OpExists, Values: []string{"a100"}}}, false},
		"does not exist values":   {[]Requirement{{Key: "lab/gpu", Operator: OpDoesNotExist, Values: []string{"a100"}}}, false},
		"unknown operator":        {[]Requirement{{Key: "region", Operator: "Gt", Values: []string{"1"}}}, false},
		"lowercase operator":      {[]Requirement{{Key: "region", Operator: "in", Values: []string{"eu"}}}, false},
		"empty operator":          {[]Requirement{{Key: "region"}}, false},
		"duplicate key":           {[]Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu"}}, {Key: "region", Operator: OpExists}}, false},
		"invalid key":             {[]Requirement{{Key: "re gion", Operator: OpExists}}, false},
		"reserved key":            {[]Requirement{{Key: "wherry.io/pool", Operator: OpExists}}, false},
		"invalid after valid one": {[]Requirement{{Key: "region", Operator: OpExists}, {Key: "zone", Operator: OpIn}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateRequirements(test.requirements); (err == nil) != test.ok {
				t.Fatalf("error %v, want ok=%v", err, test.ok)
			}
		})
	}
}

func TestSatisfies(t *testing.T) {
	labels := map[string]string{"region": "eu", "lab/gpu": "a100"}
	for name, test := range map[string]struct {
		selector []Requirement
		want     bool
	}{
		"empty selector":         {nil, true},
		"in match":               {[]Requirement{{Key: "region", Operator: OpIn, Values: []string{"us", "eu"}}}, true},
		"in mismatch":            {[]Requirement{{Key: "region", Operator: OpIn, Values: []string{"us"}}}, false},
		"in absent":              {[]Requirement{{Key: "zone", Operator: OpIn, Values: []string{"eu"}}}, false},
		"not in match":           {[]Requirement{{Key: "region", Operator: OpNotIn, Values: []string{"eu"}}}, false},
		"not in mismatch":        {[]Requirement{{Key: "region", Operator: OpNotIn, Values: []string{"us"}}}, true},
		"not in absent":          {[]Requirement{{Key: "zone", Operator: OpNotIn, Values: []string{"eu"}}}, true},
		"exists present":         {[]Requirement{{Key: "lab/gpu", Operator: OpExists}}, true},
		"exists absent":          {[]Requirement{{Key: "lab/tpu", Operator: OpExists}}, false},
		"does not exist present": {[]Requirement{{Key: "lab/gpu", Operator: OpDoesNotExist}}, false},
		"does not exist absent":  {[]Requirement{{Key: "lab/tpu", Operator: OpDoesNotExist}}, true},
		"unknown operator":       {[]Requirement{{Key: "region", Operator: "Gt", Values: []string{"eu"}}}, false},
		"all hold":               {[]Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu"}}, {Key: "lab/gpu", Operator: OpExists}}, true},
		"one fails":              {[]Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu"}}, {Key: "lab/gpu", Operator: OpDoesNotExist}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := Satisfies(test.selector, labels); got != test.want {
				t.Fatalf("Satisfies = %v, want %v", got, test.want)
			}
		})
	}
	if !Satisfies([]Requirement{{Key: "region", Operator: OpDoesNotExist}}, nil) || Satisfies([]Requirement{{Key: "region", Operator: OpExists}}, nil) {
		t.Fatal("nil labels must hold no keys")
	}
}

func TestValidateOffers(t *testing.T) {
	offer := Offer{Profile: "nomad", Agent: "claude"}
	for name, test := range map[string]struct {
		offers []Offer
		ok     bool
	}{
		"one":               {[]Offer{offer}, true},
		"distinct profiles": {[]Offer{offer, {Profile: "kubernetes", Agent: "claude"}}, true},
		"none":              {nil, true},
		"empty":             {[]Offer{}, true},
		"duplicate profile": {[]Offer{offer, {Profile: "nomad", Agent: "codex"}}, false},
		"negative limit":    {[]Offer{{Profile: "nomad", Agent: "claude", InlineLimit: -1}}, false},
		"missing agent":     {[]Offer{{Profile: "nomad"}}, false},
		"uppercase agent":   {[]Offer{{Profile: "nomad", Agent: "Claude"}}, false},
		"missing profile":   {[]Offer{{Agent: "claude"}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateOffers(test.offers); (err == nil) != test.ok {
				t.Fatalf("error %v, want ok=%v", err, test.ok)
			}
		})
	}
}

// An offer is a profile and its agent; labels belong to the configuration record, so a signed offer carrying them
// does not decode. The payload is the producer's advertisement with labels added to its offer.
func TestGitInputPlacement(t *testing.T) {
	clash := validSubmission()
	clash.Wherry.Git = &GitInput{Remote: "ssh://git@git.example.test/org/repo.git", Ref: "main", HostKey: hostKey}
	clash.Message.Parts = append(clash.Message.Parts, Part{Raw: StdBytes("x"), Filename: KnownHostsFile})
	if err := clash.validate(); err == nil || err.Error() != "an input named known_hosts conflicts with the git host key" {
		t.Fatalf("input named known_hosts beside a host key: %v", err)
	}
	clash.Wherry.Git = &GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"}
	if err := clash.validate(); err != nil {
		t.Fatalf("input named known_hosts without a host key: %v", err)
	}
}

func TestGitVCSHost(t *testing.T) {
	for remote, want := range map[string]string{
		"https://Git.Example.Test/org/repo.git":         "git.example.test",
		"https://git.example.test:443/org/repo.git":     "git.example.test",
		"https://git.example.test:8443/org/repo.git":    "git.example.test:8443",
		"ssh://git@git.example.test/org/repo.git":       "git.example.test",
		"ssh://git@git.example.test:22/org/repo.git":    "git.example.test",
		"ssh://git@git.example.test:443/org/repo.git":   "git.example.test:443",
		"ssh://git@git.example.test:2222/org/repo.git":  "git.example.test:2222",
		"https://git.example.test.evil.test/org/x.git":  "git.example.test.evil.test",
		"https://git.example.test@evil.test/org/x.git":  "evil.test",
		"ssh://git.example.test@evil.test:22/org/x.git": "evil.test",
	} {
		if got := (GitInput{Remote: remote}).VCSHost(); got != want {
			t.Errorf("VCS host of %s = %q, want %q", remote, got, want)
		}
	}
	for remote, want := range map[string]string{
		"https://Git.Example.Test/org/repo.git":        "https://git.example.test",
		"https://git.example.test:8443/org/repo.git":   "https://git.example.test:8443",
		"ssh://git@git.example.test:2222/org/repo.git": "ssh://git.example.test:2222",
	} {
		if got := (GitInput{Remote: remote}).Origin(); got != want {
			t.Errorf("origin of %s = %q, want %q", remote, got, want)
		}
	}
	for host, ok := range map[string]bool{
		"github.com": true, "git.example.test:2222": true, "a": true, "a:65535": true,
		"": false, "GitHub.com": false, "a:0": false, "a:65536": false, "a:01": false, "a:": false, "-a": false, "a-": false, "a:b": false, "a/b": false,
	} {
		if ValidVCSHost(host) != ok {
			t.Errorf("ValidVCSHost(%q) = %v, want %v", host, !ok, ok)
		}
	}
}

func TestS3InputsAndOutputsValidation(t *testing.T) {
	sha := strings.Repeat("a", 64)
	accepted := validSubmission()
	accepted.Wherry.S3Inputs = []S3Input{{Name: "dataset.bin", Size: MaxS3Object, SHA256: sha}, {Name: "empty", SHA256: sha}}
	accepted.Wherry.Outputs = []ExpectedOutput{{Name: "report.pdf", Storage: StorageS3}, {Name: "notes.md"}}
	if err := accepted.validate(); err != nil {
		t.Fatalf("valid S3 inputs and outputs: %v", err)
	}
	for name, edit := range map[string]func(*Submission){
		"invalid name":   func(s *Submission) { s.Wherry.S3Inputs[0].Name = "../x" },
		"duplicate name": func(s *Submission) { s.Wherry.S3Inputs[1].Name = "dataset.bin" },
		"inline name": func(s *Submission) {
			s.Message.Parts = append(s.Message.Parts, Part{Raw: StdBytes("x"), Filename: "dataset.bin"})
		},
		"over 5 GiB":       func(s *Submission) { s.Wherry.S3Inputs[0].Size = MaxS3Object + 1 },
		"negative size":    func(s *Submission) { s.Wherry.S3Inputs[0].Size = -1 },
		"uppercase hash":   func(s *Submission) { s.Wherry.S3Inputs[0].SHA256 = strings.Repeat("A", 64) },
		"other storage":    func(s *Submission) { s.Wherry.Outputs[0].Storage = "gcs" },
		"over 256 entries": func(s *Submission) { s.Wherry.S3Inputs = slices.Repeat(s.Wherry.S3Inputs[:1], MaxParts) },
	} {
		submission := validSubmission()
		submission.Wherry.S3Inputs = slices.Clone(accepted.Wherry.S3Inputs)
		submission.Wherry.Outputs = slices.Clone(accepted.Wherry.Outputs)
		edit(submission)
		if err := submission.validate(); err == nil {
			t.Errorf("%s: validated", name)
		}
	}
	clash := validSubmission()
	clash.Wherry.Git = &GitInput{Remote: "ssh://git@git.example.test/org/repo.git", Ref: "main", HostKey: hostKey}
	clash.Wherry.S3Inputs = []S3Input{{Name: KnownHostsFile, SHA256: sha}}
	if err := clash.validate(); err == nil || err.Error() != "an input named known_hosts conflicts with the git host key" {
		t.Fatalf("S3 input named known_hosts beside a host key: %v", err)
	}
}

func TestSealRequestNamesS3Objects(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	request := func(inputs, outputs []string) *SealRequest {
		return &SealRequest{ReceiverBinding: receiverBinding(), Key: jose.JSONWebKey{Key: &key.PublicKey}, KeyID: signed.SealKeyID(&key.PublicKey),
			Credentials: []string{}, S3Inputs: inputs, S3Outputs: outputs, ExpiryMS: 1}
	}
	if err := request([]string{"data.bin"}, []string{"out/report.pdf"}).validate(); err != nil {
		t.Fatalf("a request for S3 URLs alone: %v", err)
	}
	for name, invalid := range map[string]*SealRequest{
		"nothing":           request(nil, nil),
		"input path":        request([]string{"a/b"}, nil),
		"duplicate input":   request([]string{"a", "a"}, nil),
		"output dot dot":    request(nil, []string{"../x"}),
		"duplicate output":  request(nil, []string{"x", "x"}),
		"input leading dot": request([]string{".hidden"}, nil),
	} {
		if err := invalid.validate(); err == nil {
			t.Errorf("%s: validated", name)
		}
	}
}

func TestS3Keys(t *testing.T) {
	if got := S3InputKey("tenant-a", "run-a", "data.bin"); got != "wherry/tenant-a/run-a/inputs/data.bin" {
		t.Errorf("input key %s", got)
	}
	if got := S3OutputKey("tenant-a", "run-a", "out/report.pdf"); got != "wherry/tenant-a/run-a/outputs/out/report.pdf" {
		t.Errorf("output key %s", got)
	}
	if prefix := S3Prefix("tenant-a", "run-a"); !strings.HasSuffix(prefix, "/") || strings.HasPrefix(S3Prefix("tenant-a", "run-ab"), prefix) {
		t.Errorf("prefix %s holds another run's prefix", prefix)
	}
}

func TestCutUTF8KeepsWholeRunes(t *testing.T) {
	for _, c := range []struct {
		s    string
		n    int
		want string
	}{
		{"abc", 5, "abc"},
		{"abc", 3, "abc"},
		{"abc", 2, "ab"},
		{"a€b", 3, "a"},
		{"a€b", 4, "a€"},
		{"€", 0, ""},
	} {
		if got := CutUTF8(c.s, c.n); got != c.want {
			t.Errorf("CutUTF8(%q, %d) = %q, want %q", c.s, c.n, got, c.want)
		}
	}
}

func TestValidHash(t *testing.T) {
	for value, want := range map[string]bool{
		strings.Repeat("a1", 32): true,
		strings.Repeat("A1", 32): false,
		strings.Repeat("a", 63):  false,
		strings.Repeat("g", 64):  false,
		strings.Repeat("a1", 33): false,
		"":                       false,
	} {
		if got := ValidHash(value); got != want {
			t.Errorf("ValidHash(%q) = %v, want %v", value, got, want)
		}
	}
}

func TestTaskHashIsHexSHA256(t *testing.T) {
	if got := TaskHash([]byte("abc")); got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("TaskHash = %s", got)
	}
}

func TestRandomID(t *testing.T) {
	first, second := RandomID("run-"), RandomID("run-")
	random, prefixed := strings.CutPrefix(first, "run-")
	if decoded, err := hex.DecodeString(random); first == second || !prefixed || err != nil || len(decoded) != 16 {
		t.Fatalf("ids %q and %q; want distinct, prefixed, 128 bits in hex", first, second)
	}
}

// The receiver hands a plugin the inputs without checking names again, so decoding is where they are unique.
func TestInputNamesAreUniqueAtDecode(t *testing.T) {
	repeated := validSubmission()
	repeated.Message.Parts = append(repeated.Message.Parts, Part{Raw: StdBytes("a"), Filename: "data.csv"}, Part{Raw: StdBytes("b"), Filename: "data.csv"})
	if err := repeated.validate(); err == nil || err.Error() != "input file names must be unique" {
		t.Fatalf("two inputs named data.csv: %v", err)
	}
	shadowed := validSubmission()
	shadowed.Message.Parts = append(shadowed.Message.Parts, Part{Raw: StdBytes("a"), Filename: "dataset.bin"})
	shadowed.Wherry.S3Inputs = []S3Input{{Name: "dataset.bin", Size: 1, SHA256: strings.Repeat("0", 64)}}
	if err := shadowed.validate(); err == nil || !strings.Contains(err.Error(), "distinct from the run's input files") {
		t.Fatalf("an input and an S3 input named dataset.bin: %v", err)
	}
}
