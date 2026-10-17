package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json/jsontext"
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
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
)

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: wherry <init|key|enrol|secret|receivers|submit|continue|cancel|await|events|receiver>")
	}
	if args[0] == "receiver" {
		return runReceiver(args[1:], stdout, stderr)
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
		fingerprint, err := busclient.InitUserKey(config.UserKey)
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, fingerprint)
		return nil
	}
	if args[0] == "key" {
		if len(args) != 1 {
			return errors.New("usage: wherry key")
		}
		key, err := trust.LoadSigningKeyFile(config.UserKey)
		if err != nil {
			return err
		}
		public := key.Public().(ed25519.PublicKey)
		fmt.Fprintf(stdout, "public_key=%s fingerprint=%s\n", base64.RawURLEncoding.EncodeToString(public), wire.Fingerprint(public))
		return nil
	}
	client, err := busclient.Connect(config)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx := context.Background()

	switch args[0] {
	case "enrol":
		return runEnrol(ctx, client, args[1:], stdout, stderr)
	case "secret":
		return runSecret(ctx, client, args[1:], stdout, stderr)
	case "receivers":
		return runReceivers(ctx, client, args[1:], stdout, stderr)
	case "submit":
		return runSubmit(ctx, client, args[1:], stdout, stderr)
	case "continue", "resume":
		return runContinue(ctx, client, args[1:], stdout, stderr)
	case "cancel":
		return runCancel(ctx, client, args[1:], stdout, stderr)
	case "await":
		return runAwait(ctx, client, args[1:], stdout, stderr)
	case "events":
		return runEvents(ctx, client, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runReceivers(ctx context.Context, client *busclient.Client, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("receivers", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "emit JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: wherry receivers [--json]")
	}
	receivers, skipped, err := client.Receivers(ctx)
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
			if receiver.Revoked {
				fmt.Fprintf(stdout, "receiver=%s pool=%s generation=%d floor_generation=%d status=%s\n",
					receiver.ReceiverID, receiver.Pool, receiver.Generation, receiver.Floor, receiver.Status)
				continue
			}
			fmt.Fprintf(stdout, "receiver=%s pool=%s generation=%d labels=%s selector=%s capacity=%d in_flight=%d heartbeat_age=%s status=%s\n",
				receiver.ReceiverID, receiver.Pool, receiver.Generation,
				wire.TermsText(receiver.Labels), formatRequirements(receiver.Selector),
				receiver.Capacity, receiver.InFlight, receiver.HeartbeatAge, receiver.Status)
			for _, grant := range receiver.Adapters {
				fmt.Fprintf(stdout, "  adapter=%s platform=%s defaults=%s\n", grant.Name, grant.Platform, wire.TermsText(grant.Defaults))
				for _, offer := range receiver.Offers {
					if offer.Adapter == grant.Name {
						fmt.Fprintf(stdout, "    offer %s\n", wire.TermsText(offer.Labels))
					}
				}
			}
			for _, seal := range receiver.Secrets {
				fmt.Fprintf(stdout, "  secret=%s version=%d expires=%s\n", seal.Name, seal.Version, time.UnixMilli(seal.ExpMS).UTC().Format(time.RFC3339))
			}
		}
	}
	for _, receiver := range receivers {
		for _, seal := range receiver.Secrets {
			if left := time.UnixMilli(seal.ExpMS).Sub(now); left <= 0 {
				fmt.Fprintf(stderr, "secret %s on receiver %s has expired\n", seal.Name, receiver.ReceiverID)
			} else if left < 7*24*time.Hour {
				fmt.Fprintf(stderr, "secret %s on receiver %s expires in %dd\n", seal.Name, receiver.ReceiverID, int(math.Ceil(left.Hours()/24)))
			}
		}
	}
	warnSkipped(stderr, skipped)
	return nil
}

func runEnrol(ctx context.Context, client *busclient.Client, args []string, stdout, stderr io.Writer) error {
	if len(args) == 1 && args[0] == "list" {
		enrolments, skipped, err := client.Enrolments(ctx)
		if err != nil {
			return err
		}
		for _, enrolment := range enrolments {
			if enrolment.Approved {
				renewal := ""
				if enrolment.RenewalRequested {
					renewal = " renewal=requested"
				}
				fmt.Fprintf(stdout, "receiver=%s request=%s labels=%s approved=true generation=%d%s\n",
					enrolment.ReceiverID, enrolment.RequestHash, wire.TermsText(enrolment.Labels), enrolment.Generation, renewal)
			} else {
				fmt.Fprintf(stdout, "receiver=%s request=%s labels=%s approved=false\n",
					enrolment.ReceiverID, enrolment.RequestHash, wire.TermsText(enrolment.Labels))
			}
			for _, adapter := range enrolment.Adapters {
				fmt.Fprintf(stdout, "  adapter=%s platform=%s\n", adapter.Name, adapter.Platform)
				for _, offer := range adapter.Offers {
					fmt.Fprintf(stdout, "    offer %s\n", wire.TermsText(offer.Labels))
				}
			}
		}
		warnSkipped(stderr, skipped)
		return nil
	}
	if len(args) == 2 && args[0] == "revoke" {
		result, err := client.Revoke(ctx, args[1])
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "receiver=%s floor_generation=%d\n", result.ReceiverID, result.Floor)
		fmt.Fprintln(stderr, "credentials sealed to this receiver are not rotated; rotate them at their source")
		warnSkipped(stderr, result.Skipped)
		return nil
	}
	if len(args) == 2 && args[0] == "renew" {
		result, err := client.Renew(ctx, args[1])
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "receiver=%s generation=%d renewed=true\n", result.ReceiverID, result.Generation)
		warnSkipped(stderr, result.Skipped)
		return nil
	}
	const enrolUsage = "usage: wherry enrol <list|revoke RECEIVER_ID|renew RECEIVER_ID|approve REQUEST_HASH [--pool POOL] --labels k=v,.. --grants FILE --selector k=v|v,.. [--expires DURATION]>"
	if len(args) < 2 || args[0] != "approve" {
		return errors.New(enrolUsage)
	}
	requestHash := args[1]
	flags := flag.NewFlagSet("enrol approve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	pool := flags.String("pool", "", "pool name (default: the receiver ID)")
	labelsText := flags.String("labels", "", "approved receiver labels")
	grantsPath := flags.String("grants", "", "JSON file holding the adapter grants")
	selectorText := flags.String("selector", "", "approved selector")
	renewal := flags.String("renewal", "auto", "renewal policy")
	expires := flags.Duration("expires", 30*24*time.Hour, "approval lifetime")
	if err := flags.Parse(args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New(enrolUsage)
	}
	if !flagSet(flags, "labels", "grants", "selector") {
		return errors.New("enrol approve requires --labels, --grants and --selector")
	}
	if *renewal != "auto" && *renewal != "manual" {
		return errors.New("renewal must be auto or manual")
	}
	if *expires <= 0 {
		return errors.New("expires must be positive")
	}
	labels, err := parseTerms(*labelsText)
	if err != nil {
		return fmt.Errorf("labels: %w", err)
	}
	encodedGrants, err := os.ReadFile(*grantsPath)
	if err != nil {
		return fmt.Errorf("grants: %w", err)
	}
	var grants []wire.AdapterGrant
	if err := json.Unmarshal(encodedGrants, &grants, json.RejectUnknownMembers(true), jsontext.AllowDuplicateNames(false)); err != nil {
		return fmt.Errorf("grants: %w", err)
	}
	selector, err := parseRequirements(*selectorText)
	if err != nil {
		return fmt.Errorf("selector: %w", err)
	}
	result, err := client.Approve(ctx, requestHash, *pool, labels, grants, selector, *renewal, *expires)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "receiver=%s generation=%d\n", result.ReceiverID, result.Generation)
	warnSkipped(stderr, result.Skipped)
	return nil
}

func runSecret(ctx context.Context, client *busclient.Client, args []string, stdout, stderr io.Writer) error {
	const secretUsage = "usage: wherry secret put NAME --to RECEIVER_ID --from-file PATH [--expires DURATION]"
	if len(args) < 3 || args[0] != "put" {
		return errors.New(secretUsage)
	}
	name := args[1]
	flags := flag.NewFlagSet("secret put", flag.ContinueOnError)
	flags.SetOutput(stderr)
	receiverID := flags.String("to", "", "receiver ID")
	path := flags.String("from-file", "", "secret file")
	expires := flags.Duration("expires", 30*24*time.Hour, "seal lifetime")
	if err := flags.Parse(args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New(secretUsage)
	}
	if !flagSet(flags, "to", "from-file") {
		return errors.New("secret put requires --to and --from-file")
	}
	if *expires <= 0 {
		return errors.New("expires must be positive")
	}
	result, err := client.PutSecret(ctx, name, *receiverID, *path, *expires)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "name=%s receiver=%s version=%d\n", result.Name, result.ReceiverID, result.Version)
	for _, waiting := range result.Waiting {
		fmt.Fprintf(stdout, "waiting run_id=%s turn=%d\n", waiting.RunID, waiting.Turn)
	}
	warnSkipped(stderr, result.Skipped)
	return nil
}

func runContinue(ctx context.Context, client *busclient.Client, args []string, stdout, stderr io.Writer) error {
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

func runCancel(ctx context.Context, client *busclient.Client, args []string, stdout, stderr io.Writer) error {
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

func runSubmit(ctx context.Context, client *busclient.Client, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("submit", flag.ContinueOnError)
	flags.SetOutput(stderr)
	selectorText := flags.String("selector", "", "task selector")
	labelsText := flags.String("labels", "", "task labels")
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
		return errors.New("usage: wherry submit --selector k=v|v,.. --labels k=v,.. <--prompt TEXT|--prompt-file PATH> [--git URL --git-ref REF [--git-path PATH]] [--deadline DURATION] [--queue-deadline DURATION]")
	}
	var parts []wire.Part
	if wasSet(flags, "git") || wasSet(flags, "git-ref") || wasSet(flags, "git-path") {
		if *gitURL == "" || *gitRef == "" {
			return errors.New("submit requires --git and --git-ref together")
		}
		metadata, _ := json.Marshal(wire.GitMetadata{Ref: *gitRef, Path: *gitPath}) // strings always encode
		parts = append(parts, wire.Part{URL: *gitURL, MediaType: wire.GitMediaType, Metadata: metadata})
	}
	if !flagSet(flags, "selector", "labels") {
		return errors.New("submit requires --selector and --labels")
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
	labels, err := parseTerms(*labelsText)
	if err != nil {
		return fmt.Errorf("labels: %w", err)
	}
	result, err := client.Submit(ctx, busclient.SubmitInput{
		Selector: selector, Labels: labels, Prompt: *prompt, Parts: parts,
		Deadline: *deadline, QueueDeadline: *queueDeadline,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "run_id=%s pool=%s\n", result.RunID, result.Pool)
	warnSkipped(stderr, result.Skipped)
	return nil
}

func runAwait(ctx context.Context, client *busclient.Client, args []string, stdout, stderr io.Writer) error {
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
	if !result.RevokedAt.IsZero() {
		fmt.Fprintln(stderr, strings.TrimSpace(revokedSigner(result.RevokedAt)))
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

// revokedSigner marks a record signed by a receiver revoked at revokedAt, or returns "" when revokedAt is zero.
func revokedSigner(revokedAt time.Time) string {
	if revokedAt.IsZero() {
		return ""
	}
	return " signer=revoked-before=" + revokedAt.UTC().Format(time.RFC3339Nano)
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

func runEvents(ctx context.Context, client *busclient.Client, args []string, stdout, stderr io.Writer) error {
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
			record.Wherry.Turn, record.Wherry.Attempt, record.Wherry.Kind, status.State, record.Wherry.ReceiverID, status.Timestamp)
		if status.Message != nil {
			texts := make([]string, 0, len(status.Message.Parts))
			for _, part := range status.Message.Parts {
				texts = append(texts, partText(part))
			}
			fmt.Fprintf(stdout, " message=%q", strings.Join(texts, " "))
		}
		fmt.Fprintln(stdout, revokedSigner(record.RevokedAt))
	}
	if result.Cancelled {
		fmt.Fprintf(stdout, "kind=cancel state=%s signer=user\n", wire.TaskStateCanceled)
	}
	positions := slices.Collect(maps.Keys(result.Chunks))
	sort.Slice(positions, func(i, j int) bool {
		if positions[i].Turn != positions[j].Turn {
			return positions[i].Turn < positions[j].Turn
		}
		return positions[i].Attempt < positions[j].Attempt
	})
	for _, position := range positions {
		fmt.Fprintf(stdout, "turn=%d attempt=%d transcript_chunks=%d%s\n", position.Turn, position.Attempt, result.Chunks[position], revokedSigner(result.ChunksRevokedAt[position]))
	}
	warnSkipped(stderr, result.Skipped)
	return nil
}

func parseTerms(value string) (map[string]string, error) {
	terms := make(map[string]string)
	if value == "" {
		return terms, nil
	}
	for _, term := range strings.Split(value, ",") {
		key, item, ok := strings.Cut(term, "=")
		if !ok || key == "" || item == "" {
			return nil, fmt.Errorf("invalid term %q", term)
		}
		if _, exists := terms[key]; exists {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		terms[key] = item
	}
	return terms, nil
}

func parseRequirements(value string) ([]wire.Requirement, error) {
	requirements := []wire.Requirement{}
	if value == "" {
		return requirements, nil
	}
	seen := make(map[string]bool)
	for _, term := range strings.Split(value, ",") {
		key, item, ok := strings.Cut(term, "=")
		if !ok || key == "" || item == "" {
			return nil, fmt.Errorf("invalid term %q", term)
		}
		if seen[key] {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		seen[key] = true
		values := strings.Split(item, "|")
		for _, value := range values {
			if value == "" {
				return nil, fmt.Errorf("invalid term %q", term)
			}
		}
		requirements = append(requirements, wire.Requirement{Key: key, Operator: wire.OpIn, Values: values})
	}
	return requirements, nil
}

func formatRequirements(requirements []wire.Requirement) string {
	formatted := make([]string, 0, len(requirements))
	for _, requirement := range requirements {
		formatted = append(formatted, requirement.Key+"="+strings.Join(requirement.Values, "|"))
	}
	sort.Strings(formatted)
	return strings.Join(formatted, ",")
}

func flagSet(flags *flag.FlagSet, names ...string) bool {
	for _, name := range names {
		if !wasSet(flags, name) {
			return false
		}
	}
	return true
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
