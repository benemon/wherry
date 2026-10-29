package client

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"slices"

	"github.com/benemon/wherry/internal/wire"
)

// Collected is what Collect wrote for a run.
type Collected struct {
	// Outputs are the run's verified outputs, inline then S3, each with the Path it was written to.
	Outputs []Output
	// Rejected are the outputs this host could not verify.
	Rejected []wire.UnreturnedOutput
}

// CollectError is the step of Collect that failed: "write outputs" or "S3 outputs".
type CollectError struct {
	Step string
	Err  error
}

func (e *CollectError) Error() string { return e.Step + ": " + e.Err.Error() }

func (e *CollectError) Unwrap() error { return e.Err }

// Collect writes a run's verified outputs, S3 ones through DownloadOutputs, beneath dir, by default
// <state_dir>/runs/<run>/outputs. A run whose queue deadline passed unclaimed has nothing collected. Its error is a
// *CollectError.
func (h *Host) Collect(ctx context.Context, runID string, result AwaitResult, dir string) (Collected, error) {
	var collected Collected
	if result.Unclaimed != "" {
		return collected, nil
	}
	if len(result.Outputs) > 0 {
		paths, err := h.WriteOutputs(runID, dir, result.Outputs)
		if err != nil {
			return Collected{}, &CollectError{Step: "write outputs", Err: err}
		}
		for i, output := range result.Outputs {
			output.Path = paths[i]
			collected.Outputs = append(collected.Outputs, output)
		}
	}
	downloaded, rejected, err := h.DownloadOutputs(ctx, runID, result, dir)
	if err != nil {
		return Collected{}, &CollectError{Step: "S3 outputs", Err: err}
	}
	collected.Outputs = append(collected.Outputs, downloaded...)
	collected.Rejected = slices.Concat(result.Rejected, rejected)
	return collected, nil
}

// ViewLines drops what a reader of a Claude stream-json transcript gets nothing from: each thinking block's signature,
// and system thinking_tokens lines. The feed keeps both.
func ViewLines(lines []TranscriptLine) []TranscriptLine {
	kept := make([]TranscriptLine, 0, len(lines))
	for _, line := range lines {
		data := line.Part.Raw
		if data == nil {
			data = []byte(line.Part.Text)
		}
		var event struct {
			Type    string `json:"type"`
			Subtype string `json:"subtype"`
		}
		if line.Part.URL == "" && json.Unmarshal(data, &event) == nil {
			if event.Type == "system" && event.Subtype == "thinking_tokens" {
				continue
			}
			if unsigned, ok := withoutSignatures(data); ok {
				line.Part.Raw, line.Part.Text = unsigned, ""
			}
		}
		kept = append(kept, line)
	}
	return kept
}

// withoutSignatures returns an assistant event with the signature of each thinking block in its message's content
// removed, and false when it has none.
func withoutSignatures(data []byte) ([]byte, bool) {
	var event, message map[string]jsontext.Value
	var content []map[string]jsontext.Value
	var kind string
	if json.Unmarshal(data, &event) != nil || json.Unmarshal(event["type"], &kind) != nil || kind != "assistant" || json.Unmarshal(event["message"], &message) != nil || json.Unmarshal(message["content"], &content) != nil {
		return nil, false
	}
	removed := false
	for _, block := range content {
		if _, signed := block["signature"]; signed && json.Unmarshal(block["type"], &kind) == nil && kind == "thinking" {
			delete(block, "signature")
			removed = true
		}
	}
	if !removed {
		return nil, false
	}
	message["content"], _ = json.Marshal(content, json.Deterministic(true)) // values decoded from valid JSON always encode
	event["message"], _ = json.Marshal(message, json.Deterministic(true))
	unsigned, _ := json.Marshal(event, json.Deterministic(true))
	return unsigned, true
}
