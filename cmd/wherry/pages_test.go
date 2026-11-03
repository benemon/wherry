package main

import (
	"os"
	"regexp"
	"testing"

	"gopkg.in/yaml.v3"
)

// The docs site embeds the wherry-plugins examples at the commit WHERRY_PLUGINS_REF pins, not at a branch.
func TestPagesChecksOutPinnedPlugins(t *testing.T) {
	encoded, err := os.ReadFile("../../.github/workflows/pages.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Env  map[string]string
		Jobs map[string]struct {
			Steps []struct {
				With map[string]string
			}
		}
	}
	if err := yaml.Unmarshal(encoded, &workflow); err != nil {
		t.Fatal(err)
	}
	if ref := workflow.Env["WHERRY_PLUGINS_REF"]; !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(ref) {
		t.Fatalf("WHERRY_PLUGINS_REF = %q, want a full commit SHA", ref)
	}
	checkouts := 0
	for _, job := range workflow.Jobs {
		for _, step := range job.Steps {
			if step.With["repository"] == "benemon/wherry-plugins" {
				checkouts++
				if ref := step.With["ref"]; ref != "${{ env.WHERRY_PLUGINS_REF }}" {
					t.Errorf("wherry-plugins checked out at %q, want ${{ env.WHERRY_PLUGINS_REF }}", ref)
				}
			}
		}
	}
	if checkouts == 0 {
		t.Fatal("pages.yml checks out no wherry-plugins")
	}
}
