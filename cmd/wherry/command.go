package main

import (
	"context"
	"crypto/ed25519"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
)

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: wherry <init|key|receivers|submit|continue|cancel|await|status|events|receiver|admin|mcp>")
	}
	if args[0] == "receiver" {
		return runReceiver(args[1:], stdout, stderr)
	}
	if args[0] == "admin" {
		return runAdmin(args[1:], stdout, stderr)
	}
	configPath, err := busclient.DefaultConfigPath()
	if err != nil {
		return err
	}
	config, err := busclient.LoadConfig(configPath)
	if err != nil {
		return err
	}
	if args[0] == "init" {
		if len(args) != 1 {
			return errors.New("usage: wherry init")
		}
		keyID, err := busclient.Init(context.Background(), config)
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, keyID)
		return nil
	}
	if args[0] == "key" {
		if len(args) != 1 {
			return errors.New("usage: wherry key")
		}
		key, err := trust.LoadSigningKeyFile(config.HostKey)
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, signed.KeyID(key.Public().(ed25519.PublicKey)))
		return nil
	}
	if args[0] == "mcp" {
		return runMCP(context.Background(), config, args[1:])
	}
	command, ok := map[string]func(context.Context, *busclient.Host, []string, io.Writer, io.Writer) error{
		"receivers": runReceivers, "submit": runSubmit, "continue": runContinue, "resume": runContinue,
		"cancel": runCancel, "await": runAwait, "status": runStatus, "events": runEvents,
	}[args[0]]
	if !ok {
		return fmt.Errorf("unknown command %q", args[0])
	}
	client, err := busclient.Connect(config)
	if err != nil {
		return err
	}
	defer client.Close()
	return command(context.Background(), client, args[1:], stdout, stderr)
}

func runReceivers(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("receivers", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "emit JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: wherry receivers [--json]")
	}
	receivers, err := client.Receivers(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	if *jsonOutput {
		encoded, err := json.Marshal(receivers, json.Deterministic(true))
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, string(encoded))
	} else {
		for _, receiver := range receivers {
			fmt.Fprintf(stdout, "receiver=%s pool=%s capacity=%d in_flight=%d heartbeat_age=%s status=%s\n",
				receiver.ReceiverID, receiver.Pool, receiver.Capacity, receiver.InFlight, receiver.HeartbeatAge, receiver.Status)
			for _, adapter := range receiver.Adapters {
				fmt.Fprintf(stdout, "  adapter=%s agent=%s ready=%t labels=%s\n", adapter.Name, adapter.Agent, adapter.Ready, wire.TermsText(adapter.Labels))
			}
			for _, seal := range receiver.Seals {
				fmt.Fprintf(stdout, "  seal=%s pool=%s expires=%s\n", seal.Name, seal.Pool, time.UnixMilli(seal.ExpMS).UTC().Format(time.RFC3339))
			}
		}
	}
	for _, receiver := range receivers {
		for _, seal := range receiver.Seals {
			if left := time.UnixMilli(seal.ExpMS).Sub(now); left <= 0 {
				fmt.Fprintf(stderr, "seal %s on receiver %s has expired\n", seal.Name, receiver.ReceiverID)
			} else if left < 7*24*time.Hour {
				fmt.Fprintf(stderr, "seal %s on receiver %s expires in %dd\n", seal.Name, receiver.ReceiverID, int(math.Ceil(left.Hours()/24)))
			}
		}
	}
	return nil
}

func runContinue(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	const continueUsage = "usage: wherry continue RUN_ID [--prompt TEXT] [--deadline DURATION] [--queue-deadline DURATION]"
	if len(args) == 0 {
		return errors.New(continueUsage)
	}
	runID := args[0]
	flags := flag.NewFlagSet("continue", flag.ContinueOnError)
	flags.SetOutput(stderr)
	prompt := flags.String("prompt", "continue", "continuation prompt")
	deadline := flags.Duration("deadline", 30*time.Minute, "task deadline")
	queueDeadline := flags.Duration("queue-deadline", 10*time.Minute, "queue deadline")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New(continueUsage)
	}
	if *deadline <= 0 || *queueDeadline <= 0 {
		return errors.New("deadlines must be positive")
	}
	result, err := client.Continue(ctx, runID, busclient.ContinueInput{
		Prompt: *prompt, Deadline: *deadline, QueueDeadline: *queueDeadline,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "run_id=%s turn=%d\n", result.RunID, result.Turn)
	warnSkipped(stderr, result.Skipped)
	return nil
}

func runCancel(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: wherry cancel RUN_ID")
	}
	result, err := client.Cancel(ctx, args[0])
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "run_id=%s\n", result.RunID)
	warnSkipped(stderr, result.Skipped)
	return nil
}

func runSubmit(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("submit", flag.ContinueOnError)
	flags.SetOutput(stderr)
	agent := flags.String("agent", "", "agent to run (default: the pool's default agent)")
	selectorText := flags.String("selector", "", "labels the adapter must carry: key=v|v, key!=v|v, key, !key, comma-separated")
	prompt := flags.String("prompt", "", "prompt text")
	promptFile := flags.String("prompt-file", "", "prompt file")
	gitURL := flags.String("git", "", "git repository URL to check out for the agent")
	gitRef := flags.String("git-ref", "", "branch, tag or commit to check out")
	gitPath := flags.String("git-path", "", "checkout directory (default: the repository name)")
	deadline := flags.Duration("deadline", 30*time.Minute, "task deadline")
	queueDeadline := flags.Duration("queue-deadline", 10*time.Minute, "queue deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: wherry submit [--agent NAME] [--selector REQUIREMENTS] <--prompt TEXT|--prompt-file PATH> [--git URL --git-ref REF [--git-path PATH]] [--deadline DURATION] [--queue-deadline DURATION]")
	}
	var parts []wire.Part
	if wasSet(flags, "git") || wasSet(flags, "git-ref") || wasSet(flags, "git-path") {
		if *gitURL == "" || *gitRef == "" {
			return errors.New("submit requires --git and --git-ref together")
		}
		metadata, _ := json.Marshal(wire.GitMetadata{Ref: *gitRef, Path: *gitPath}) // strings always encode
		parts = append(parts, wire.Part{URL: *gitURL, MediaType: wire.GitMediaType, Metadata: metadata})
	}
	promptSet := wasSet(flags, "prompt")
	promptFileSet := wasSet(flags, "prompt-file")
	if promptSet == promptFileSet {
		return errors.New("submit requires exactly one of --prompt or --prompt-file")
	}
	if *deadline <= 0 || *queueDeadline <= 0 {
		return errors.New("deadlines must be positive")
	}
	if promptFileSet {
		contents, err := os.ReadFile(*promptFile)
		if err != nil {
			return err
		}
		*prompt = string(contents)
	}
	selector, err := parseRequirements(*selectorText)
	if err != nil {
		return fmt.Errorf("selector: %w", err)
	}
	result, err := client.Submit(ctx, busclient.SubmitInput{
		Agent: *agent, Selector: selector, Prompt: *prompt, Parts: parts,
		Deadline: *deadline, QueueDeadline: *queueDeadline,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "run_id=%s pool=%s\n", result.RunID, result.Pool)
	return nil
}

func runAwait(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	const awaitUsage = "usage: wherry await RUN_ID [--timeout 30m]"
	if len(args) == 0 {
		return errors.New(awaitUsage)
	}
	runID := args[0]
	flags := flag.NewFlagSet("await", flag.ContinueOnError)
	flags.SetOutput(stderr)
	timeout := flags.Duration("timeout", 30*time.Minute, "wait timeout")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New(awaitUsage)
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	result, err := client.Await(ctx, runID, *timeout)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, result.Status.State)
	if result.Task != nil {
		for _, artifact := range result.Task.Artifacts {
			for _, part := range artifact.Parts {
				fmt.Fprintln(stdout, partText(part))
			}
		}
	} else if result.Status.Message != nil {
		for _, part := range result.Status.Message.Parts {
			fmt.Fprintln(stdout, partText(part))
		}
	}
	if result.Status.State == wire.TaskStateAuthRequired {
		fmt.Fprintf(stderr, "continue with: wherry continue %s\n", runID)
	}
	warnSkipped(stderr, result.Skipped)
	if result.Status.State != wire.TaskStateCompleted {
		return fmt.Errorf("run ended in %s", result.Status.State)
	}
	return nil
}

func runStatus(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	switch len(args) {
	case 0:
		result, err := client.Runs(ctx)
		if err != nil {
			return err
		}
		for _, run := range result.Runs {
			fmt.Fprintf(stdout, "%s pool=%s turn=%d %s\n", run.RunID, run.Pool, run.Turn, run.Status.State)
		}
		warnSkipped(stderr, result.Skipped)
		return nil
	case 1:
		result, err := client.Status(ctx, args[0])
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, result.Status.State)
		fmt.Fprintf(stdout, "turn=%d\n", result.Turn)
		if result.Status.Message != nil {
			for _, part := range result.Status.Message.Parts {
				fmt.Fprintln(stdout, partText(part))
			}
		}
		if result.Status.State == wire.TaskStateAuthRequired {
			fmt.Fprintf(stderr, "continue with: wherry continue %s\n", args[0])
		}
		warnSkipped(stderr, result.Skipped)
		return nil
	}
	return errors.New("usage: wherry status [RUN_ID]")
}

func partText(part wire.Part) string {
	switch {
	case part.Raw != nil:
		return fmt.Sprintf("[raw %s %d bytes]", part.MediaType, len(part.Raw))
	case part.URL != "":
		return fmt.Sprintf("[url %s]", part.URL)
	case part.Data != nil:
		return string(part.Data)
	}
	return part.Text
}

func runEvents(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: wherry events RUN_ID")
	}
	result, err := client.Events(ctx, args[0])
	if err != nil {
		return err
	}
	for _, record := range result.Statuses {
		status := record.StatusUpdate.Status
		fmt.Fprintf(stdout, "turn=%d attempt=%d kind=%s state=%s receiver=%s time=%s",
			record.Wherry.Turn, record.Wherry.Attempt, record.Wherry.Kind, status.State, record.Wherry.Receiver, status.Timestamp)
		if status.Message != nil {
			texts := make([]string, 0, len(status.Message.Parts))
			for _, part := range status.Message.Parts {
				texts = append(texts, partText(part))
			}
			fmt.Fprintf(stdout, " message=%q", strings.Join(texts, " "))
		}
		fmt.Fprintln(stdout)
	}
	positions := slices.Collect(maps.Keys(result.Chunks))
	sort.Slice(positions, func(i, j int) bool {
		if positions[i].Turn != positions[j].Turn {
			return positions[i].Turn < positions[j].Turn
		}
		return positions[i].Attempt < positions[j].Attempt
	})
	for _, position := range positions {
		fmt.Fprintf(stdout, "turn=%d attempt=%d transcript_chunks=%d\n", position.Turn, position.Attempt, result.Chunks[position])
	}
	warnSkipped(stderr, result.Skipped)
	return nil
}

// parseRequirements reads key=v|v (In), key!=v|v (NotIn), key (Exists) and !key (DoesNotExist), comma-separated.
func parseRequirements(value string) ([]wire.Requirement, error) {
	requirements := []wire.Requirement{}
	if value == "" {
		return requirements, nil
	}
	for _, term := range strings.Split(value, ",") {
		requirement := wire.Requirement{Key: term, Operator: wire.OpExists}
		if key, values, ok := strings.Cut(term, "!="); ok {
			requirement = wire.Requirement{Key: key, Operator: wire.OpNotIn, Values: strings.Split(values, "|")}
		} else if key, values, ok := strings.Cut(term, "="); ok {
			requirement = wire.Requirement{Key: key, Operator: wire.OpIn, Values: strings.Split(values, "|")}
		} else if key, ok := strings.CutPrefix(term, "!"); ok {
			requirement = wire.Requirement{Key: key, Operator: wire.OpDoesNotExist}
		}
		requirements = append(requirements, requirement)
	}
	if err := wire.ValidateRequirements(requirements); err != nil {
		return nil, err
	}
	return requirements, nil
}

func wasSet(flags *flag.FlagSet, name string) bool {
	found := false
	flags.Visit(func(flag *flag.Flag) {
		if flag.Name == name {
			found = true
		}
	})
	return found
}

func warnSkipped(stderr io.Writer, count int) {
	if count > 0 {
		fmt.Fprintf(stderr, "skipped %d unverifiable records\n", count)
	}
}
