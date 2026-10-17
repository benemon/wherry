package wire

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

var protoFieldPattern = regexp.MustCompile(`(?m)^\s*(?:repeated\s+)?([\w.<>]+)\s+([a-z][a-z0-9_]*)\s*=\s*\d+`)

func vendoredProto(t *testing.T) string {
	t.Helper()
	proto, err := os.ReadFile("testdata/a2a/a2a.proto")
	if err != nil {
		t.Fatal(err)
	}
	return string(proto)
}

// protoFields maps each field of a proto message, by JSON name, to its proto type.
func protoFields(t *testing.T, proto, message string) map[string]string {
	t.Helper()
	fields := map[string]string{}
	for _, match := range protoFieldPattern.FindAllStringSubmatch(protoBlock(t, proto, "message "+message), -1) {
		fields[lowerCamel(match[2])] = match[1]
	}
	return fields
}

func TestModeledA2AFieldsExistInVendoredProto(t *testing.T) {
	proto := vendoredProto(t)
	for protoName, modeled := range map[string]any{
		"Message":                 Message{},
		"Part":                    Part{},
		"Artifact":                Artifact{},
		"Task":                    Task{},
		"TaskStatus":              TaskStatus{},
		"TaskStatusUpdateEvent":   TaskStatusUpdateEvent{},
		"TaskArtifactUpdateEvent": TaskArtifactUpdateEvent{},
		"CancelTaskRequest":       CancelTaskRequest{},
	} {
		fields := protoFields(t, proto, protoName)
		modeledFields := map[string]bool{}
		modeledType := reflect.TypeOf(modeled)
		for i := 0; i < modeledType.NumField(); i++ {
			jsonName := strings.Split(modeledType.Field(i).Tag.Get("json"), ",")[0]
			modeledFields[jsonName] = true
			if _, ok := fields[jsonName]; !ok {
				t.Errorf("%s.%s is not in the vendored proto", protoName, jsonName)
			}
		}
		for field := range fields {
			if !modeledFields[field] {
				t.Errorf("%s.%s is in the vendored proto but not modelled", protoName, field)
			}
		}
	}
}

func TestUsedTaskStatesExistInVendoredProto(t *testing.T) {
	block := protoBlock(t, vendoredProto(t), "enum TaskState")
	for state := range taskStates {
		if !regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(string(state)) + `\s*=`).MatchString(block) {
			t.Errorf("%s is not in the vendored TaskState enum", state)
		}
	}
}

func TestRecordsAreA2AObjects(t *testing.T) {
	proto := vendoredProto(t)
	payloads := validPayloads(t)
	runID := payloads[TypeSubmission].(*SubmissionRecord).Wherry.RunID
	rootRunID := payloads[TypeSubmission].(*SubmissionRecord).Wherry.Lineage.RootRunID
	for recordType, test := range map[RecordType]struct {
		field, message, taskID, contextID string
	}{
		TypeSubmission:     {"message", "Message", "taskId", "contextId"},
		TypeStatus:         {"statusUpdate", "TaskStatusUpdateEvent", "taskId", "contextId"},
		TypeArtifactUpdate: {"artifactUpdate", "TaskArtifactUpdateEvent", "taskId", "contextId"},
		TypeTask:           {"task", "Task", "id", "contextId"},
	} {
		t.Run(string(recordType), func(t *testing.T) {
			encoded, err := MarshalPayload(recordType, payloads[recordType])
			if err != nil {
				t.Fatal(err)
			}
			var record map[string]jsontext.Value
			if err := json.Unmarshal(encoded, &record); err != nil {
				t.Fatal(err)
			}
			if len(record) != 2 || record["wherry"] == nil || record[test.field] == nil {
				t.Fatalf("record members %v, want %s and wherry", reflect.ValueOf(record).MapKeys(), test.field)
			}
			var object map[string]any
			if err := json.Unmarshal(record[test.field], &object); err != nil {
				t.Fatal(err)
			}
			checkProtoValue(t, proto, test.message, test.field, object)
			if got := object[test.taskID]; got != runID {
				t.Errorf("%s.%s = %v, want run_id %q", test.field, test.taskID, got, runID)
			}
			if got := object[test.contextID]; got != rootRunID {
				t.Errorf("%s.%s = %v, want root run id %q", test.field, test.contextID, got, rootRunID)
			}
		})
	}
}

func TestCancelRecordIsA2ACancelTaskRequest(t *testing.T) {
	proto := vendoredProto(t)
	cancel := &CancelRecord{CancelRequest: CancelTaskRequest{ID: "run-1", Metadata: jsontext.Value(`{"reason":"superseded"}`)}}
	encoded, err := MarshalPayload(TypeCancel, cancel)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]jsontext.Value
	if err := json.Unmarshal(encoded, &record); err != nil {
		t.Fatal(err)
	}
	if len(record) != 1 || record["cancelRequest"] == nil {
		t.Fatalf("record members %v, want cancelRequest alone", reflect.ValueOf(record).MapKeys())
	}
	var object map[string]any
	if err := json.Unmarshal(record["cancelRequest"], &object); err != nil {
		t.Fatal(err)
	}
	checkProtoValue(t, proto, "CancelTaskRequest", "cancelRequest", object)
	if object["id"] != "run-1" || object["tenant"] != nil {
		t.Errorf("cancelRequest %v, want id run-1 and no tenant", object)
	}
	for name, mutate := range map[string]func(*CancelRecord){
		"no id":     func(value *CancelRecord) { value.CancelRequest.ID = "" },
		"tenant":    func(value *CancelRecord) { value.CancelRequest.Tenant = "acme" },
		"metadata":  func(value *CancelRecord) { value.CancelRequest.Metadata = jsontext.Value(`["a"]`) },
		"uppercase": func(value *CancelRecord) { value.CancelRequest.ID = "Run-1" },
	} {
		invalid := *cancel
		mutate(&invalid)
		if _, err := MarshalPayload(TypeCancel, &invalid); err == nil {
			t.Errorf("%s: invalid cancel marshalled", name)
		}
	}
}

func TestSubmissionIDsMustMatchExtension(t *testing.T) {
	for name, mutate := range map[string]func(*SubmissionRecord){
		"taskId":    func(value *SubmissionRecord) { value.Message.TaskID = "run-2" },
		"no taskId": func(value *SubmissionRecord) { value.Message.TaskID = "" },
		"contextId": func(value *SubmissionRecord) { value.Message.ContextID = "run-2" },
	} {
		t.Run(name, func(t *testing.T) {
			submission := validPayloads(t)[TypeSubmission].(*SubmissionRecord)
			mutate(submission)
			if _, err := MarshalPayload(TypeSubmission, submission); err == nil {
				t.Fatal("submission whose message ids disagree with its extension marshalled")
			}
		})
	}
}

// checkProtoValue checks every member of a decoded JSON object against the vendored proto message,
// descending into message-typed fields; Struct and Value fields are opaque.
func checkProtoValue(t *testing.T, proto, message, path string, value any) {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Errorf("%s is %T, want a JSON object for %s", path, value, message)
		return
	}
	fields := protoFields(t, proto, message)
	for name, member := range object {
		protoType, ok := fields[name]
		if !ok {
			t.Errorf("%s.%s is not a field of %s", path, name, message)
			continue
		}
		elements := []any{member}
		if list, ok := member.([]any); ok && repeatedField(t, proto, message, name) {
			elements = list
		}
		for _, element := range elements {
			switch {
			case protoType == "google.protobuf.Timestamp":
				if text, ok := element.(string); !ok || !rfc3339(text) {
					t.Errorf("%s.%s = %v, want an RFC 3339 string", path, name, element)
				}
			case protoType == "TaskState" || protoType == "Role":
				text, _ := element.(string)
				if !regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(text) + `\s*=`).MatchString(protoBlock(t, proto, "enum "+protoType)) {
					t.Errorf("%s.%s = %v is not in enum %s", path, name, element, protoType)
				}
			case strings.Contains(proto, "\nmessage "+protoType+" {"):
				checkProtoValue(t, proto, protoType, path+"."+name, element)
			}
		}
	}
}

func repeatedField(t *testing.T, proto, message, field string) bool {
	t.Helper()
	for _, match := range regexp.MustCompile(`(?m)^\s*repeated\s+[\w.<>]+\s+([a-z][a-z0-9_]*)\s*=`).FindAllStringSubmatch(protoBlock(t, proto, "message "+message), -1) {
		if lowerCamel(match[1]) == field {
			return true
		}
	}
	return false
}

func rfc3339(text string) bool {
	_, err := time.Parse(time.RFC3339, text)
	return err == nil
}

func protoBlock(t *testing.T, proto, declaration string) string {
	t.Helper()
	start := strings.Index(proto, declaration+" {")
	if start < 0 {
		t.Fatalf("%s not found", declaration)
	}
	end := strings.Index(proto[start:], "\n}")
	if end < 0 {
		t.Fatalf("end of %s not found", declaration)
	}
	return proto[start : start+end]
}

func lowerCamel(snake string) string {
	parts := strings.Split(snake, "_")
	for i := 1; i < len(parts); i++ {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}
