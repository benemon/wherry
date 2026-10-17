package wire

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestModeledA2AFieldsExistInVendoredProto(t *testing.T) {
	proto, err := os.ReadFile("testdata/a2a/a2a.proto")
	if err != nil {
		t.Fatal(err)
	}
	for protoName, modeled := range map[string]any{
		"Message":  Message{},
		"Part":     Part{},
		"Artifact": Artifact{},
	} {
		block := protoBlock(t, string(proto), "message "+protoName)
		fields := map[string]bool{}
		fieldPattern := regexp.MustCompile(`(?m)^\s*(?:repeated\s+)?[\w.<>]+\s+([a-z][a-z0-9_]*)\s*=\s*\d+`)
		for _, match := range fieldPattern.FindAllStringSubmatch(block, -1) {
			fields[lowerCamel(match[1])] = true
		}
		modeledType := reflect.TypeOf(modeled)
		for i := 0; i < modeledType.NumField(); i++ {
			jsonName := strings.Split(modeledType.Field(i).Tag.Get("json"), ",")[0]
			if !fields[jsonName] {
				t.Errorf("%s.%s is not in the vendored proto", protoName, jsonName)
			}
		}
	}
}

func TestUsedTaskStatesExistInVendoredProto(t *testing.T) {
	proto, err := os.ReadFile("testdata/a2a/a2a.proto")
	if err != nil {
		t.Fatal(err)
	}
	block := protoBlock(t, string(proto), "enum TaskState")
	for state := range taskStates {
		if !regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(string(state)) + `\s*=`).MatchString(block) {
			t.Errorf("%s is not in the vendored TaskState enum", state)
		}
	}
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
