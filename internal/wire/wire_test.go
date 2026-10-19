package wire

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"reflect"
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
			Tenant: "tenant-1", Host: "host-1", Pool: "linux", Run: "run-1", Turn: 1,
			Placement:  Placement{Agent: "claude", Selector: []Requirement{{Key: "region", Operator: OpIn, Values: []string{"eu"}}}},
			DeadlineMS: 2_000, QueueDeadlineMS: 1_000,
		},
	}
}

func receiverBinding() ReceiverBinding {
	return ReceiverBinding{Receiver: "receiver-1", Tenant: "tenant-1", Pool: "linux", Run: "run-1", Turn: 1, Attempt: 1}
}

func validStatus() *RunStatus {
	return &RunStatus{
		V: 1,
		StatusUpdate: TaskStatusUpdateEvent{TaskID: "run-1", ContextID: "run-1", Status: TaskStatus{
			State: TaskStateWorking, Timestamp: "2026-10-17T10:00:00.000Z",
			Message: &Message{MessageID: "status-1", Role: RoleAgent, Parts: []Part{{Text: "working"}}, Extensions: []string{ExtensionURI}},
		}},
		Wherry: StatusBinding{ReceiverBinding: receiverBinding(), EventID: "event-1", Kind: "heartbeat", TaskHash: wireHash},
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
	return &Echo{V: 1, Receiver: "receiver-1", Tenant: "tenant-1", Pool: "linux", Run: "run-1", Turn: 1, Placement: submission.Wherry.Placement, TaskHash: wireHash, MessageID: "message-1"}
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
		"unknown member":   replaceOnce(t, encoded, `"run":"run-1"`, `"run":"run-1","unexpected":true`),
		"trailing value":   append(bytes.Clone(encoded), " {}"...),
		"float integer":    replaceOnce(t, encoded, `"turn":1`, `"turn":1.5`),
	} {
		t.Run(name, func(t *testing.T) {
			if err := parseJSON(t, "tenant-1.host-1", payload, &Submission{}); err == nil {
				t.Fatal("non-strict payload accepted")
			}
		})
	}
}

func TestTurnAndAttemptRequired(t *testing.T) {
	for name, test := range map[string]struct {
		value   Record
		members []string
	}{
		"submission":    {validSubmission(), []string{`"turn":1`}},
		"cancel":        {&Cancel{V: 1, Tenant: "tenant-1", Host: "host-1", Pool: "linux", Run: "run-1", Turn: 1}, []string{`"turn":1`}},
		"echo":          {validEcho(), []string{`"turn":1`}},
		"status":        {validStatus(), []string{`"turn":1`, `"attempt":1`}},
		"chunk":         {validChunk(), []string{`"turn":1`, `"attempt":1`}},
		"result":        {validResult(), []string{`"turn":1`, `"attempt":1`}},
		"start failure": {&StartFailure{V: 1, Wherry: receiverBinding(), Adapter: "nomad", Agent: "claude", Class: "launch"}, []string{`"turn":1`, `"attempt":1`}},
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

func TestAttemptFilterOrdersByTurn(t *testing.T) {
	var filter AttemptFilter
	if !filter.Admit("r", 1, 2) {
		t.Fatal("first attempt rejected")
	}
	if filter.Admit("r", 1, 1) {
		t.Fatal("lower attempt admitted")
	}
	if !filter.Admit("r", 1, 2) {
		t.Fatal("equal attempt rejected")
	}
	if !filter.Admit("r", 1, 3) {
		t.Fatal("higher attempt rejected")
	}
	if !filter.Admit("r", 2, 1) {
		t.Fatal("higher turn with restarted attempt rejected")
	}
	if filter.Admit("r", 1, 99) {
		t.Fatal("lower turn with higher attempt admitted")
	}
	if !filter.Admit("other", 1, 1) {
		t.Fatal("other run rejected")
	}
}

func TestTerminalStates(t *testing.T) {
	terminal := map[TaskState]bool{TaskStateCompleted: true, TaskStateFailed: true, TaskStateCanceled: true, TaskStateRejected: true}
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
		"empty raw":    {`"parts":[{"raw":""}]`, true},
		"url":          {`"parts":[{"url":"https://example.test/a"}]`, true},
		"data":         {`"parts":[{"data":{"k":[1,2]}}]`, true},
		"null data":    {`"parts":[{"data":null}]`, true},
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
			submission, err := parseSubmissionParts(t, `"parts":[{"raw":"`+input+`"}]`)
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
	submission.Message.Parts = []Part{{Data: data, Metadata: metadata}}
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
		"part data not object": {`"parts":[{"data":[1]}]`, true},
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

func TestGitPartValidation(t *testing.T) {
	const prompt = `{"text":"hello"},`
	part := func(url, metadata string) string {
		return `{"url":"` + url + `","mediaType":"application/vnd.wherry.git","metadata":` + metadata + `}`
	}
	valid := part("https://git.example.test/org/repo.git", `{"ref":"main"}`)
	for name, test := range map[string]struct {
		parts string
		want  string
	}{
		"https":            {valid, ""},
		"ssh":              {part("ssh://git.example.test/org/repo.git", `{"ref":"v1.2.0","path":"src/repo"}`), ""},
		"commit":           {part("https://git.example.test/org/repo", `{"ref":"0123456789abcdef0123456789abcdef01234567"}`), ""},
		"http":             {part("http://git.example.test/org/repo.git", `{"ref":"main"}`), "git url must be an https or ssh url with a host"},
		"file":             {part("file:///srv/repo.git", `{"ref":"main"}`), "git url must be an https or ssh url with a host"},
		"scp form":         {part("git@git.example.test:org/repo.git", `{"ref":"main"}`), "git url must be an https or ssh url with a host"},
		"no host":          {part("https:///org/repo.git", `{"ref":"main"}`), "git url must be an https or ssh url with a host"},
		"https userinfo":   {part("https://user:token@git.example.test/org/repo.git", `{"ref":"main"}`), "https git url must not carry user information"},
		"https user only":  {part("https://git@git.example.test/org/repo.git", `{"ref":"main"}`), "https git url must not carry user information"},
		"ssh git user":     {part("ssh://git@git.example.test/org/repo.git", `{"ref":"main"}`), ""},
		"ssh other user":   {part("ssh://deploy@git.example.test/org/repo.git", `{"ref":"main"}`), "ssh git url user must be git"},
		"ssh empty user":   {part("ssh://@git.example.test/org/repo.git", `{"ref":"main"}`), "ssh git url user must be git"},
		"ssh password":     {part("ssh://git:hunter2@git.example.test/org/repo.git", `{"ref":"main"}`), "ssh git url must not carry a password"},
		"ssh empty pass":   {part("ssh://git:@git.example.test/org/repo.git", `{"ref":"main"}`), "ssh git url must not carry a password"},
		"query":            {part("https://git.example.test/org/repo.git?token=x", `{"ref":"main"}`), "git url must not carry a query or fragment"},
		"fragment":         {part("https://git.example.test/org/repo.git#x", `{"ref":"main"}`), "git url must not carry a query or fragment"},
		"no metadata":      {`{"url":"https://git.example.test/org/repo.git","mediaType":"application/vnd.wherry.git"}`, "git part metadata requires a ref"},
		"no ref":           {part("https://git.example.test/org/repo.git", `{"path":"repo"}`), "git part metadata requires a ref"},
		"unknown member":   {part("https://git.example.test/org/repo.git", `{"ref":"main","depth":5}`), "git part metadata requires a ref"},
		"option ref":       {part("https://git.example.test/org/repo.git", `{"ref":"--upload-pack=touch"}`), "git ref must be a branch, tag or commit name"},
		"spaced ref":       {part("https://git.example.test/org/repo.git", `{"ref":"main branch"}`), "git ref must be a branch, tag or commit name"},
		"dotted ref":       {part("https://git.example.test/org/repo.git", `{"ref":"main..other"}`), "git ref must be a branch, tag or commit name"},
		"double slash ref": {part("https://git.example.test/org/repo.git", `{"ref":"release//1"}`), "git ref must be a branch, tag or commit name"},
		"slash end ref":    {part("https://git.example.test/org/repo.git", `{"ref":"release/"}`), "git ref must be a branch, tag or commit name"},
		"dot end ref":      {part("https://git.example.test/org/repo.git", `{"ref":"main."}`), "git ref must be a branch, tag or commit name"},
		"lock end ref":     {part("https://git.example.test/org/repo.git", `{"ref":"main.lock"}`), "git ref must be a branch, tag or commit name"},
		"traversal":        {part("https://git.example.test/org/repo.git", `{"ref":"main","path":"../outside"}`), "git path must be a relative path without . or .. segments"},
		"inner traversal":  {part("https://git.example.test/org/repo.git", `{"ref":"main","path":"a/../../b"}`), "git path must be a relative path without . or .. segments"},
		"absolute path":    {part("https://git.example.test/org/repo.git", `{"ref":"main","path":"/etc"}`), "git path must be a relative path without . or .. segments"},
		"no default path":  {part("https://git.example.test/", `{"ref":"main"}`), "git path must be a relative path without . or .. segments"},
		"text with git":    {`{"text":"x","mediaType":"application/vnd.wherry.git"}`, "git part must be a url part"},
		"two git parts":    {valid + "," + valid, "a submission carries at most one git part"},
		"other url parts":  {valid + `,{"url":"https://example.test/a"}`, ""},
		"plain url scheme": {`{"url":"file:///srv/repo.git"}`, ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseSubmissionParts(t, `"parts":[`+prompt+test.parts+`]`)
			if test.want == "" && err != nil || test.want != "" && (err == nil || err.Error() != test.want) {
				t.Fatalf("parse error %v, want %q", err, test.want)
			}
		})
	}
	git, err := ParseGitPart(Part{URL: "https://git.example.test/org/repo.git", MediaType: GitMediaType, Metadata: jsontext.Value(`{"ref":"main"}`)})
	if err != nil || git != (GitContext{URL: "https://git.example.test/org/repo.git", Ref: "main", Path: "repo"}) {
		t.Fatalf("parsed %#v, %v; want the repository name as the default path", git, err)
	}
	submission := validSubmission()
	submission.Message.Parts = append(submission.Message.Parts, Part{URL: "https://token@git.example.test/org/repo.git", MediaType: GitMediaType, Metadata: jsontext.Value(`{"ref":"main"}`)})
	if err := submission.validate(); err == nil || strings.Contains(err.Error(), "token") {
		t.Fatalf("validate error %v, want a fixed rejection", err)
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
	offer := Offer{Adapter: "nomad", Agent: "claude", Labels: map[string]string{"runtime": "podman"}}
	for name, test := range map[string]struct {
		offers []Offer
		ok     bool
	}{
		"one":               {[]Offer{offer}, true},
		"no labels":         {[]Offer{{Adapter: "nomad", Agent: "claude"}}, true},
		"distinct adapters": {[]Offer{offer, {Adapter: "kubernetes", Agent: "claude"}}, true},
		"same agent twice":  {[]Offer{offer, {Adapter: "kubernetes", Agent: offer.Agent, Labels: offer.Labels}}, true},
		"none":              {nil, false},
		"duplicate adapter": {[]Offer{offer, {Adapter: "nomad", Agent: "codex"}}, false},
		"missing agent":     {[]Offer{{Adapter: "nomad", Labels: offer.Labels}}, false},
		"uppercase agent":   {[]Offer{{Adapter: "nomad", Agent: "Claude"}}, false},
		"missing adapter":   {[]Offer{{Agent: "claude"}}, false},
		"invalid label":     {[]Offer{{Adapter: "nomad", Agent: "claude", Labels: map[string]string{"runtime": ""}}}, false},
		"reserved label":    {[]Offer{{Adapter: "nomad", Agent: "claude", Labels: map[string]string{"wherry.io/runtime": "podman"}}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateOffers(test.offers); (err == nil) != test.ok {
				t.Fatalf("error %v, want ok=%v", err, test.ok)
			}
		})
	}
}
