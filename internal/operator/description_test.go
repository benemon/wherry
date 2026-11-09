package operator

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"gopkg.in/yaml.v3"
)

func descriptionTerms(description string) Terms {
	terms := modelTerms()
	terms.Agents[0].Credentials[0].Description = description
	return terms
}
func TestCredentialDescriptionValidation(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	for _, description := range []string{"", strings.Repeat("界", 80), strings.Repeat("界", 81), "x\x1b", "x\n", "x\r", "x\t", "x\x00", "x\x7f", "x\u0085", "x\u2028", "x\u2029", "x\xff"} {
		t.Run(strings.ReplaceAll(description, "界", "u"), func(t *testing.T) {
			valid := description == "" || description == strings.Repeat("界", 80)
			terms := descriptionTerms(description)
			check := func(err error) {
				t.Helper()
				if (err == nil) != valid {
					t.Fatalf("validation = %v, valid %v", err, valid)
				}
				if err != nil && (!strings.Contains(err.Error(), "agent "+terms.Agents[0].Name) || !strings.Contains(err.Error(), "credential "+terms.Agents[0].Credentials[0].Name) || strings.Contains(err.Error(), description)) {
					t.Fatalf("unsafe or unqualified error: %v", err)
				}
			}
			check(service.PoolApply(context.Background(), "pool-a", terms, true))
			encoded, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms})
			check(err)
			if valid {
				record, err := DecodeRecord(encoded)
				check(err)
				if record.Terms.Agents[0].Credentials[0].Description != description {
					t.Fatal("description lost")
				}
				if description == "" && bytes.Contains(encoded, []byte("description")) {
					t.Fatal("empty description encoded")
				}
			} else if description != "x\xff" {
				raw, err := json.Marshal(Record{Version: RecordVersion, AtMS: 1, Kind: "pool", Pool: "pool-a", Terms: &terms})
				if err != nil {
					t.Fatal(err)
				}
				_, err = DecodeRecord(raw)
				check(err)
			} else {
				validTerms := descriptionTerms("placeholder")
				raw, err := json.Marshal(Record{Version: RecordVersion, AtMS: 1, Kind: "pool", Pool: "pool-a", Terms: &validTerms})
				if err != nil {
					t.Fatal(err)
				}
				raw = bytes.Replace(raw, []byte("placeholder"), []byte(description), 1)
				_, err = DecodeRecord(raw)
				if err == nil || strings.Contains(err.Error(), description) {
					t.Fatal("invalid UTF-8 not safely rejected", err)
				}
			}
		})
	}
}
func TestCredentialDescriptionRoundTrips(t *testing.T) {
	terms := descriptionTerms("Account token")
	for _, format := range []string{"json", "yaml"} {
		var encoded []byte
		var err error
		var decoded Terms
		if format == "json" {
			encoded, err = json.Marshal(terms)
			if err == nil {
				err = json.Unmarshal(encoded, &decoded)
			}
		} else {
			original := terms.Agents[0].Credentials[0]
			var credential AgentCredential
			encoded, err = yaml.Marshal(original)
			if err == nil {
				err = yaml.Unmarshal(encoded, &credential)
			}
			if err != nil || !reflect.DeepEqual(original, credential) {
				t.Fatalf("yaml: %v", err)
			}
			continue
		}
		if err != nil || !reflect.DeepEqual(terms, decoded) {
			t.Fatalf("%s: %v", format, err)
		}
	}
	if !reflect.DeepEqual(terms.Agents[0].Targets(), modelTerms().Agents[0].Targets()) {
		t.Fatal("delivery changed")
	}
	encoded, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms})
	if err != nil {
		t.Fatal(err)
	}
	var legacy struct {
		Terms struct {
			Agents []struct {
				Credentials []struct {
					wire.TaskCredential
					Required *bool `json:"required"`
				} `json:"credentials"`
			} `json:"agents"`
		} `json:"terms"`
	}
	if err := json.Unmarshal(encoded, &legacy, signed.Tolerant); err != nil || len(legacy.Terms.Agents[0].Credentials) == 0 {
		t.Fatal(err)
	}
}
func TestCredentialDescriptionOnlyApply(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	for i, description := range []string{"", "Account token", "Account token"} {
		terms := descriptionTerms(description)
		changes := kafkaChanges(t, service)
		before, err := service.LoadState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
			t.Fatal(err)
		}
		if got := kafkaChanges(t, service); (i == 2 && got != changes) || (i < 2 && got[0] != changes[0]+1) {
			t.Fatalf("apply %d: changes %v -> %v", i, changes, got)
		}
		after, err := service.LoadState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if after.Pools["pool-a"].Agents[0].Credentials[0].Description != description {
			t.Fatal("description update lost")
		}
		if description == "Account token" && before.Pools["pool-a"].Agents[0].Credentials[0].Description == description && !sameTerms(after.Pools["pool-a"], before.Pools["pool-a"]) {
			t.Fatal("repeat changed terms")
		}
	}
}

func TestCredentialDescriptionErrorDoesNotEchoInvalidName(t *testing.T) {
	terms := descriptionTerms("unsafe\ntext")
	terms.Agents[0].Credentials[0].Name = terms.Agents[0].Credentials[0].Description
	if err := validateTerms(withDefaults(terms)); err == nil || strings.Contains(err.Error(), "unsafe\ntext") {
		t.Fatal(err)
	}
}

func TestValidateDescription(t *testing.T) {
	for _, s := range []string{"", strings.Repeat("界", 80), "hello\u202e"} {
		if err := ValidateDescription(s); err != nil {
			t.Errorf("valid description: %v", err)
		}
	}
	for _, s := range []string{strings.Repeat("界", 81), "\a", "\u2028", "\u2029", string([]byte{255})} {
		if err := ValidateDescription(s); err == nil || err.Error() != "invalid description" {
			t.Errorf("invalid description accepted: %q, %v", s, err)
		}
	}
}

func TestDuplicateAgentRefused(t *testing.T) {
	terms := modelTerms()
	terms.Agents = append(terms.Agents, terms.Agents[0])
	if err := validateTerms(terms); err == nil || err.Error() != "agent agent-a is listed twice" {
		t.Fatalf("duplicate agent: %v", err)
	}
}
