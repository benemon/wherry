package adapter

import "testing"

func TestAgentEnv(t *testing.T) {
	env := AgentEnv(map[string]string{"KEEP": "1", "GIT_SSH_COMMAND": "evil", "GIT_TERMINAL_PROMPT": "1"})
	if env["KEEP"] != "1" {
		t.Fatalf("configured KEEP dropped: %v", env)
	}
	if _, ok := env["GIT_SSH_COMMAND"]; ok {
		t.Fatalf("AgentUnset name kept: %v", env)
	}
	if env["GIT_TERMINAL_PROMPT"] != "0" {
		t.Fatalf("GIT_TERMINAL_PROMPT = %q, want AgentEnvironment's 0", env["GIT_TERMINAL_PROMPT"])
	}
	if got := AgentEnv(nil); len(got) != len(AgentEnvironment) {
		t.Fatalf("AgentEnv(nil) has %d names, want %d", len(got), len(AgentEnvironment))
	}
}

func TestReservedEnv(t *testing.T) {
	for _, name := range []string{"HOME", "WHERRY_INPUT_DIR", "WHERRY_OUTPUT_DIR", "WHERRY_CREDENTIALS_DIR", "WHERRY_MODEL", "GIT_CONFIG_GLOBAL"} {
		if !ReservedEnv(name) {
			t.Errorf("ReservedEnv(%s) = false", name)
		}
	}
	if ReservedEnv("MODEL_API_KEY") {
		t.Error("ReservedEnv(MODEL_API_KEY) = true")
	}
}
