package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"slices"

	"github.com/benemon/wherry/internal/wire"
)

// Collected is what Collect wrote for a run.
type Collected struct {
	// Outputs are the run's verified outputs, inline then S3, each with the Path it was written to.
	Outputs []Output
	// Transcript is the run's transcript as written to transcript.jsonl, with its Path.
	Transcript Output
	// Lines are the transcript's lines as ViewLines keeps them.
	Lines []TranscriptLine
	// Rejected are the outputs this host could not verify.
	Rejected []wire.UnreturnedOutput
}

// Collect writes a run's verified outputs beneath dir, by default <state_dir>/runs/<run>/outputs, downloads and
// verifies its S3 outputs there, deleting the run's prefix only once the run is terminal and every one verified, and
// writes its transcript there as transcript.jsonl. A run whose queue deadline passed unclaimed has nothing collected.
func (h *Host) Collect(ctx context.Context, runID string, result AwaitResult, dir string) (Collected, error) {
	var collected Collected
	if result.Unclaimed != "" {
		return collected, nil
	}
	if len(result.Outputs) > 0 {
		paths, err := h.WriteOutputs(runID, dir, result.Outputs)
		if err != nil {
			return Collected{}, fmt.Errorf("write outputs: %w", err)
		}
		for i, output := range result.Outputs {
			output.Path = paths[i]
			collected.Outputs = append(collected.Outputs, output)
		}
	}
	downloaded, rejected, err := h.DownloadOutputs(ctx, runID, result, dir)
	if err != nil {
		return Collected{}, fmt.Errorf("S3 outputs: %w", err)
	}
	collected.Outputs = append(collected.Outputs, downloaded...)
	collected.Rejected = slices.Concat(result.Rejected, rejected)
	if collected.Transcript, collected.Lines, err = h.writeTranscript(ctx, runID, dir); err != nil {
		return Collected{}, fmt.Errorf("write transcript: %w", err)
	}
	return collected, nil
}

// writeTranscript writes runID's transcript, as transcript prints it, to dir or else the run's outputs directory, and
// returns it as an output with the path it was written to, and its lines.
func (h *Host) writeTranscript(ctx context.Context, runID, dir string) (Output, []TranscriptLine, error) {
	result, err := h.Transcript(ctx, runID)
	if err != nil {
		return Output{}, nil, err
	}
	lines := ViewLines(result.Lines)
	var data []byte
	for _, line := range lines {
		data = AppendLine(data, line)
	}
	sum := sha256.Sum256(data)
	transcript := Output{Name: wire.TranscriptFile, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data)), Data: data}
	paths, err := h.WriteOutputs(runID, dir, []Output{transcript})
	if err != nil {
		return Output{}, nil, err
	}
	transcript.Path = paths[0]
	return transcript, lines, nil
}

// AppendLine appends line to data as transcript.jsonl holds it, with its newline; a line the receiver did not store
// appends nothing.
func AppendLine(data []byte, line TranscriptLine) []byte {
	switch part := line.Part; {
	case part.URL != "":
		return data
	case part.Raw != nil:
		return append(append(data, part.Raw...), '\n')
	default:
		return append(append(data, part.Text...), '\n')
	}
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
