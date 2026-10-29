package main

import (
	"context"
	"crypto/ed25519"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
)

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: wherry <init|key|key rotate|receivers|submit|cancel|wait|status|events|logs|cleanup|receiver run|admin|mcp|dev|version>")
	}
	if args[0] == "receiver" {
		return runReceiver(args[1:], stdout, stderr)
	}
	if args[0] == "admin" {
		return runAdmin(args[1:], stdout, stderr)
	}
	if args[0] == "dev" {
		return runDev(args[1:], stdout, stderr)
	}
	if args[0] == "version" {
		if len(args) != 1 {
			return errors.New("usage: wherry version")
		}
		version, revision, modified := "(devel)", "", false
		if info, ok := debug.ReadBuildInfo(); ok {
			version = info.Main.Version
			for _, setting := range info.Settings {
				switch setting.Key {
				case "vcs.revision":
					revision = setting.Value
				case "vcs.modified":
					modified = setting.Value == "true"
				}
			}
		}
		fmt.Fprintf(stdout, "version=%s\nrevision=%s\n", version, revision)
		if modified {
			fmt.Fprintln(stdout, "modified=true")
		}
		return nil
	}
	if args[0] == "init" {
		flags := flag.NewFlagSet("init", flag.ContinueOnError)
		flags.SetOutput(stderr)
		configFile := flags.String("config-file", "", "host configuration file")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("usage: wherry init [--config-file FILE]")
		}
		config, err := loadConfig(*configFile)
		if err != nil {
			return err
		}
		keyID, err := busclient.Init(context.Background(), config)
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, keyID)
		return nil
	}
	if args[0] == "key" {
		rotate := len(args) > 1 && args[1] == "rotate"
		if rotate {
			args = args[1:]
		}
		flags := flag.NewFlagSet("key", flag.ContinueOnError)
		flags.SetOutput(stderr)
		configFile := flags.String("config-file", "", "host configuration file")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("usage: wherry key [rotate] [--config-file FILE]")
		}
		config, err := loadConfig(*configFile)
		if err != nil {
			return err
		}
		if rotate {
			old, next, err := busclient.RotateKey(context.Background(), config)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "old_key_id=%s new_key_id=%s\n", old, next)
			return nil
		}
		// Connecting settles a rotation that left the next key behind.
		if _, err := os.Stat(config.HostKey + ".next"); err == nil {
			client, err := busclient.Connect(config, log.New(stderr, "", 0))
			if err != nil {
				return err
			}
			client.Close()
		}
		key, err := trust.LoadSigningKeyFile(config.HostKey)
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, signed.KeyID(key.Public().(ed25519.PublicKey)))
		return nil
	}
	if args[0] == "mcp" {
		return runMCP(context.Background(), args[1:])
	}
	command, ok := map[string]func(context.Context, *busclient.Host, []string, io.Writer, io.Writer) error{
		"receivers": runReceivers, "submit": runSubmit,
		"cancel": runCancel, "wait": runWait, "status": runStatus, "events": runEvents, "logs": runLogs, "cleanup": runCleanup,
	}[args[0]]
	if !ok {
		return fmt.Errorf("unknown command %q", args[0])
	}
	return command(context.Background(), nil, args[1:], stdout, stderr)
}

// parseAnywhere parses flags given before, between or after the positional arguments, which it returns in order.
func parseAnywhere(flags *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := flags.Parse(args); err != nil {
			return nil, err
		}
		if flags.NArg() == 0 {
			return positional, nil
		}
		positional, args = append(positional, flags.Arg(0)), flags.Args()[1:]
	}
}

func loadConfig(path string) (busclient.Config, error) {
	if path == "" {
		var err error
		if path, err = busclient.DefaultConfigPath(); err != nil {
			return busclient.Config{}, err
		}
	}
	return busclient.LoadConfig(path)
}

func commandClient(client *busclient.Host, configFile string, stderr io.Writer) (*busclient.Host, func(), error) {
	if client != nil {
		return client, func() {}, nil
	}
	config, err := loadConfig(configFile)
	if err != nil {
		return nil, nil, err
	}
	client, err = busclient.Connect(config, log.New(stderr, "", 0))
	if err != nil {
		return nil, nil, err
	}
	return client, client.Close, nil
}

func runReceivers(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("receivers", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configFile := flags.String("config-file", "", "host configuration file")
	jsonOutput := flags.Bool("json", false, "emit JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: wherry receivers [--json]")
	}
	client, closeClient, err := commandClient(client, *configFile, stderr)
	if err != nil {
		return err
	}
	defer closeClient()
	receivers, err := client.Receivers(ctx)
	if err != nil {
		return err
	}
	if *jsonOutput {
		encoded, err := json.Marshal(receivers, json.Deterministic(true))
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, string(encoded))
	} else {
		for _, receiver := range receivers {
			age := "-"
			if !receiver.LastAdvertised.IsZero() {
				age = max(time.Since(receiver.LastAdvertised), 0).Round(time.Second).String()
			}
			fmt.Fprintf(stdout, "receiver=%s pool=%s capacity=%d in_flight=%d heartbeat_age=%s status=%s\n",
				receiver.ReceiverID, receiver.Pool, receiver.Capacity, receiver.InFlight, age, receiver.Status)
			for _, profile := range receiver.Profiles {
				fmt.Fprintf(stdout, "  profile=%s plugin=%s agent=%s ready=%t labels=%s", profile.Name, profile.Plugin, profile.Agent, profile.Ready, wire.TermsText(profile.Labels))
				if profile.InlineLimit > 0 {
					fmt.Fprintf(stdout, " inline_limit=%d", profile.InlineLimit)
				}
				if len(profile.Models) > 0 {
					fmt.Fprintf(stdout, " models=%s default_model=%s", strings.Join(profile.Models, ","), profile.DefaultModel)
				}
				if len(profile.Credentials) > 0 {
					names := make([]string, 0, len(profile.Credentials))
					for _, credential := range profile.Credentials {
						var notes []string
						if !credential.Configured {
							notes = append(notes, "missing")
						}
						if credential.Optional {
							notes = append(notes, "optional")
						}
						if len(notes) > 0 {
							names = append(names, credential.Name+"("+strings.Join(notes, ",")+")")
						} else {
							names = append(names, credential.Name)
						}
					}
					fmt.Fprintf(stdout, " credentials=%s", strings.Join(names, ","))
				}
				fmt.Fprintln(stdout)
			}
		}
	}
	return nil
}

func runCancel(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("cancel", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configFile := flags.String("config-file", "", "host configuration file")
	positional, err := parseAnywhere(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return errors.New("usage: wherry cancel RUN_ID")
	}
	client, closeClient, err := commandClient(client, *configFile, stderr)
	if err != nil {
		return err
	}
	defer closeClient()
	result, err := client.Cancel(ctx, positional[0])
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
	configFile := flags.String("config-file", "", "host configuration file")
	agent := flags.String("agent", "", "agent to run (default: the pool's default agent)")
	model := flags.String("model", "", "model the agent runs, one of its models as receivers lists them (default: the agent's default model)")
	selectorText := flags.String("selector", "", "labels the profile must carry: key=v|v, key!=v|v, key, !key, comma-separated")
	prompt := flags.String("prompt", "", "prompt text")
	promptFile := flags.String("prompt-file", "", "prompt file, or - for standard input")
	gitRemote := flags.String("git", "", "REMOTE@REF: git repository the agent clones, an https or ssh URL or user@host:path, at a branch, tag or commit")
	gitPath := flags.String("git-path", "", "checkout directory, relative (default: the repository name)")
	gitChanges := flags.String("git-changes", "", "patch or bundle: ask for the agent's commits back as changes.patch or changes.bundle")
	var inputs, s3Inputs []string
	flags.Func("input", "FILE[:s3]: a file to deliver to the agent's input directory, through the pool's bucket with :s3 (repeatable)", func(value string) error {
		path, suffix, found := strings.Cut(value, ":")
		switch {
		case !found:
			inputs = append(inputs, path)
		case suffix == "s3":
			s3Inputs = append(s3Inputs, path)
		default:
			return fmt.Errorf("want FILE[:s3], not %q", value)
		}
		return nil
	})
	var credentials []wire.TaskCredential
	flags.Func("credential", "NAME:file=FILE or NAME:env=VAR: deliver configured credential NAME as file FILE in the agent's credentials directory or in its environment variable VAR (repeatable)", func(value string) error {
		name, target, _ := strings.Cut(value, ":")
		kind, at, _ := strings.Cut(target, "=")
		switch {
		case kind == "file" && at != "":
			credentials = append(credentials, wire.TaskCredential{Name: name, File: at})
		case kind == "env" && at != "":
			credentials = append(credentials, wire.TaskCredential{Name: name, Env: at})
		default:
			return errors.New("want NAME:file=FILE or NAME:env=VAR")
		}
		return nil
	})
	var outputs []wire.ExpectedOutput
	flags.Func("output", "NAME[:required][:s3]: a file the agent is asked to leave in its output directory, returned through the pool's bucket with :s3 (repeatable)", outputFlag(&outputs))
	deadline := flags.Duration("deadline", busclient.DefaultDeadline, "task deadline")
	queueDeadline := flags.Duration("queue-deadline", busclient.DefaultQueueDeadline, "queue deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: wherry submit [--agent NAME] [--model NAME] [--selector REQUIREMENTS] <--prompt TEXT|--prompt-file PATH|-> [--git REMOTE@REF [--git-path PATH] [--git-changes patch|bundle]] [--input FILE[:s3]]... [--credential NAME:file=FILE|NAME:env=VAR]... [--output NAME[:required][:s3]]... [--deadline DURATION] [--queue-deadline DURATION]")
	}
	parts, err := inputParts(inputs)
	if err != nil {
		return err
	}
	var git *wire.GitInput
	if *gitRemote != "" {
		at := strings.LastIndex(*gitRemote, "@")
		if at < 0 {
			return errors.New("--git takes REMOTE@REF")
		}
		git = &wire.GitInput{Remote: (*gitRemote)[:at], Ref: (*gitRemote)[at+1:], Path: *gitPath}
	} else if wasSet(flags, "git-path") || wasSet(flags, "git-changes") {
		return errors.New("--git-path and --git-changes need --git")
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
		var contents []byte
		var err error
		if *promptFile == "-" {
			contents, err = io.ReadAll(stdin)
		} else {
			contents, err = os.ReadFile(*promptFile)
		}
		if err != nil {
			return err
		}
		if len(contents) == 0 {
			return errors.New("--prompt-file is empty")
		}
		*prompt = string(contents)
	}
	selector, err := parseRequirements(*selectorText)
	if err != nil {
		return fmt.Errorf("selector: %w", err)
	}
	var stored []busclient.S3File
	for _, path := range s3Inputs {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		stored = append(stored, busclient.S3File{Name: filepath.Base(path), Body: file})
	}
	client, closeClient, err := commandClient(client, *configFile, stderr)
	if err != nil {
		return err
	}
	defer closeClient()
	result, err := client.Submit(ctx, busclient.SubmitInput{
		Agent: *agent, Model: *model, Selector: selector, Prompt: *prompt, Parts: parts, Credentials: credentials, Outputs: outputs, Git: git, GitChanges: *gitChanges,
		S3Inputs: stored, Deadline: *deadline, QueueDeadline: *queueDeadline,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "run_id=%s pool=%s\n", result.RunID, result.Pool)
	return nil
}

// inputParts reads each input file into a raw part named by its base name.
func inputParts(paths []string) ([]wire.Part, error) {
	var parts []wire.Part
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		parts = append(parts, wire.Part{Raw: append(wire.StdBytes{}, data...), Filename: filepath.Base(path)})
	}
	return parts, nil
}

// outputFlag parses NAME[:required][:s3], its suffixes in either order, into outputs.
func outputFlag(outputs *[]wire.ExpectedOutput) func(string) error {
	return func(value string) error {
		fields := strings.Split(value, ":")
		output := wire.ExpectedOutput{Name: fields[0]}
		for _, suffix := range fields[1:] {
			switch {
			case suffix == "required" && !output.Required:
				output.Required = true
			case suffix == "s3" && output.Storage == "":
				output.Storage = wire.StorageS3
			default:
				return fmt.Errorf("want NAME[:required][:s3], not %q", value)
			}
		}
		*outputs = append(*outputs, output)
		return nil
	}
}

// stdin is a variable so tests can give submit a prompt on standard input.
var stdin io.Reader = os.Stdin

// defaultWaitTimeout covers a run at the default queue deadline and run deadline.
const defaultWaitTimeout = busclient.DefaultQueueDeadline + busclient.DefaultDeadline

// streamRetry separates a failed read of a run's transcript that logs follows from the next, and bounds each wait for
// more of it.
const streamRetry = progressInterval

// exitError is an error wherry exits with code for.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }

func (e *exitError) Unwrap() error { return e.err }

// stateCodes are wait's exit codes for a run that ends in a state other than COMPLETED, and timeoutCode for its
// timeout passing with the run still going.
var stateCodes = map[wire.TaskState]int{
	wire.TaskStateFailed: 2, wire.TaskStateCanceled: 3, wire.TaskStateRejected: 4,
}

const timeoutCode = 6

func runWait(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	const waitUsage = "usage: wherry wait RUN_ID [--timeout 2h10m] [--outputs-dir DIR]"
	flags := flag.NewFlagSet("wait", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, waitUsage)
		fmt.Fprintln(stderr, "Stopping wait can leave a credential request unanswered, ending the run FAILED after the seal wait.")
		flags.PrintDefaults()
	}
	configFile := flags.String("config-file", "", "host configuration file")
	timeout := flags.Duration("timeout", defaultWaitTimeout, "wait timeout")
	outputsDir := flags.String("outputs-dir", "", "directory for the run's outputs (default: <state_dir>/runs/RUN_ID/outputs)")
	positional, err := parseAnywhere(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return errors.New(waitUsage)
	}
	runID := positional[0]
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	client, closeClient, err := commandClient(client, *configFile, stderr)
	if err != nil {
		return err
	}
	defer closeClient()
	result, err := client.Await(ctx, runID, *timeout)
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return &exitError{timeoutCode, err}
	}
	if err != nil {
		return err
	}
	collected, collectErr := client.Collect(ctx, runID, result, *outputsDir)
	if result.Unclaimed != "" {
		return errors.New(result.Unclaimed)
	}
	if result.Model != "" {
		fmt.Fprintf(stderr, "model=%s\n", result.Model)
	}
	if result.Status.Message != nil {
		for _, part := range result.Status.Message.Parts {
			if replyPreview(result, part) {
				fmt.Fprintln(stdout, string(result.Reply.Data))
				continue
			}
			fmt.Fprintln(stdout, partText(part))
		}
	}
	switch {
	case result.Reply == nil:
	case result.Reply.Rejected != "":
		fmt.Fprintf(stderr, "reply rejected by this host: %s; printed its preview\n", result.Reply.Rejected)
	case result.Reply.Cut:
		fmt.Fprintln(stderr, "reply cut at the pool's reply limit")
	}
	if collectErr != nil {
		return collectErr
	}
	for _, output := range collected.Outputs {
		fmt.Fprintf(stdout, "output=%s path=%s sha256=%s size=%d\n", output.Name, output.Path, output.SHA256, output.Size)
		switch output.Name {
		case wire.GitPatch:
			fmt.Fprintf(stderr, "apply with: git am %s\n", output.Path)
		case wire.GitBundle:
			fmt.Fprintf(stderr, "fetch with: git fetch %s HEAD\n", output.Path)
		}
	}
	for _, output := range append(result.NotReturned, collected.Rejected...) {
		fmt.Fprintf(stderr, "output %s not returned: %s\n", output.Name, output.Reason)
	}
	warnRedacted(stderr, result.Redacted)
	warnSkipped(stderr, result.Skipped)
	if state := result.Status.State; state != wire.TaskStateCompleted {
		code, ok := stateCodes[state]
		if !ok {
			code = 1
		}
		return &exitError{code, fmt.Errorf("run ended in %s", state)}
	}
	if len(collected.Rejected) > 0 {
		return fmt.Errorf("%d outputs failed verification", len(collected.Rejected))
	}
	return nil
}

func runCleanup(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("cleanup", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configFile := flags.String("config-file", "", "host configuration file")
	positional, err := parseAnywhere(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return errors.New("usage: wherry cleanup RUN_ID")
	}
	client, closeClient, err := commandClient(client, *configFile, stderr)
	if err != nil {
		return err
	}
	defer closeClient()
	runID := positional[0]
	if err := client.Cleanup(ctx, runID); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "run_id=%s\n", runID)
	return nil
}

func runStatus(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configFile := flags.String("config-file", "", "host configuration file")
	positional, err := parseAnywhere(flags, args)
	if err != nil {
		return err
	}
	if len(positional) > 1 {
		return errors.New("usage: wherry status [RUN_ID]")
	}
	client, closeClient, err := commandClient(client, *configFile, stderr)
	if err != nil {
		return err
	}
	defer closeClient()
	switch len(positional) {
	case 0:
		result, err := client.Runs(ctx)
		if err != nil {
			return err
		}
		for _, run := range result.Runs {
			model := ""
			if run.Model != "" {
				model = " model=" + run.Model
			}
			fmt.Fprintf(stdout, "%s pool=%s%s %s\n", run.RunID, run.Pool, model, run.Status.State)
		}
		warnSkipped(stderr, result.Skipped)
		return nil
	case 1:
		result, err := client.Status(ctx, positional[0])
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, result.Status.State)
		if result.Model != "" {
			fmt.Fprintf(stdout, "model=%s\n", result.Model)
		}
		if result.Status.Message != nil {
			for _, part := range result.Status.Message.Parts {
				fmt.Fprintln(stdout, partText(part))
			}
		}
		if result.Unclaimed != "" {
			fmt.Fprintln(stdout, result.Unclaimed)
		}
		warnRedacted(stderr, result.Redacted)
		warnSkipped(stderr, result.Skipped)
		return nil
	}
	return errors.New("usage: wherry status [RUN_ID]")
}

// replyPreview reports whether part is the preview of result's verified reply, which prints whole in its place.
func replyPreview(result busclient.AwaitResult, part wire.Part) bool {
	if result.Reply == nil || result.Reply.Rejected != "" || result.Task == nil || len(result.Task.Artifacts) == 0 || len(result.Task.Artifacts[0].Parts) == 0 {
		return false
	}
	return part.Data == nil && partText(part) == partText(result.Task.Artifacts[0].Parts[0])
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
	flags := flag.NewFlagSet("events", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configFile := flags.String("config-file", "", "host configuration file")
	positional, err := parseAnywhere(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return errors.New("usage: wherry events RUN_ID")
	}
	client, closeClient, err := commandClient(client, *configFile, stderr)
	if err != nil {
		return err
	}
	defer closeClient()
	result, err := client.Events(ctx, positional[0])
	if err != nil {
		return err
	}
	for _, record := range result.Statuses {
		status := record.StatusUpdate.Status
		fmt.Fprintf(stdout, "attempt=%d kind=%s state=%s receiver=%s time=%s",
			record.Wherry.Attempt, record.Wherry.Kind, status.State, record.Wherry.Receiver, status.Timestamp)
		if status.Message != nil {
			texts := make([]string, 0, len(status.Message.Parts))
			for _, part := range status.Message.Parts {
				texts = append(texts, partText(part))
			}
			fmt.Fprintf(stdout, " message=%q", strings.Join(texts, " "))
		}
		fmt.Fprintln(stdout)
	}
	attempts := slices.Collect(maps.Keys(result.Chunks))
	slices.Sort(attempts)
	for _, attempt := range attempts {
		fmt.Fprintf(stdout, "attempt=%d transcript_chunks=%d\n", attempt, result.Chunks[attempt])
	}
	warnRedacted(stderr, result.Redacted)
	warnSkipped(stderr, result.Skipped)
	return nil
}

func runLogs(ctx context.Context, client *busclient.Host, args []string, stdout, stderr io.Writer) error {
	const logsUsage = "usage: wherry logs RUN_ID [-f|--follow]"
	flags := flag.NewFlagSet("logs", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configFile := flags.String("config-file", "", "host configuration file")
	var follow bool
	flags.BoolVar(&follow, "f", false, "print lines as the agent prints them until the run ends")
	flags.BoolVar(&follow, "follow", false, "print lines as the agent prints them until the run ends")
	positional, err := parseAnywhere(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return errors.New(logsUsage)
	}
	runID := positional[0]
	client, closeClient, err := commandClient(client, *configFile, stderr)
	if err != nil {
		return err
	}
	defer closeClient()
	// printed is the seq past which lines are yet to print; lines print in seq order and once each.
	printed := int64(-1)
	printLines := func(lines []busclient.TranscriptLine) {
		for _, line := range busclient.ViewLines(lines) {
			if line.Seq <= printed {
				continue
			}
			printed = line.Seq
			switch part := line.Part; {
			case part.URL != "":
				var pointer wire.PointerMetadata
				json.Unmarshal(part.Metadata, &pointer) // validation decoded it
				fmt.Fprintf(stderr, "line at offset %d not stored: sha256=%s size=%d\n", line.Seq, pointer.SHA256, pointer.Size)
			case part.Raw != nil:
				fmt.Fprintf(stdout, "%s\n", part.Raw)
			default:
				fmt.Fprintln(stdout, part.Text)
			}
		}
	}
	if follow {
		stream := client.StreamTranscript(runID)
		warned := false
		for {
			transcript, err := stream.Next(ctx)
			if errors.Is(err, busclient.ErrRunNotFound) || ctx.Err() != nil {
				return err
			}
			if err != nil {
				// A failed read is retried; the transcript read once the run ends holds every line.
				select {
				case <-ctx.Done():
				case <-time.After(streamRetry):
				}
				continue
			}
			printLines(transcript.Lines)
			if stream.Ended() {
				break
			}
			if !warned && stream.Unanswered() {
				fmt.Fprintf(stderr, "run is waiting for credentials; run wherry wait %s\n", runID)
				warned = true
			}
			waitCtx, stopWait := context.WithTimeout(ctx, streamRetry)
			stream.Wait(waitCtx)
			stopWait()
		}
	}
	result, err := client.Transcript(ctx, runID)
	if err != nil {
		return err
	}
	printLines(result.Lines)
	warnRedacted(stderr, result.Redacted)
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

func warnRedacted(stderr io.Writer, counts map[string]int) {
	for _, name := range slices.Sorted(maps.Keys(counts)) {
		fmt.Fprintf(stderr, "%s redacted %d times\n", name, counts[name])
	}
}

func warnSkipped(stderr io.Writer, count int) {
	if count > 0 {
		fmt.Fprintf(stderr, "skipped %d unverifiable records\n", count)
	}
}
