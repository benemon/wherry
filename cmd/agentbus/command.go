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
	"os"
	"sort"
	"strings"
	"time"

	busclient "github.com/benemon/agentbus/internal/client"
	"github.com/benemon/agentbus/internal/trust"
	"github.com/benemon/agentbus/internal/wire"
)

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: agentbus <init|key|enrol|secret|receivers|submit|await|events|receiver>")
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
			return errors.New("usage: agentbus init")
		}
		fingerprint, err := busclient.InitUserKey(config.UserKey)
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, fingerprint)
		return nil
	}
	if args[0] == "key" {
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
		return errors.New("usage: agentbus receivers [--json]")
	}
	receivers, skipped, err := client.Receivers(ctx)
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
				fmt.Fprintf(stdout, "receiver=%s request=%s labels=%s approved=true generation=%d\n",
					enrolment.ReceiverID, enrolment.RequestHash, wire.TermsText(enrolment.Labels), enrolment.Generation)
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
	if len(args) < 2 || args[0] != "approve" {
		return errors.New("usage: agentbus enrol <list|approve REQUEST_HASH [--pool POOL] --labels k=v,.. --grants FILE --selector k=v|v,..>")
	}
	requestHash := args[1]
	flags := flag.NewFlagSet("enrol approve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	pool := flags.String("pool", "", "pool name (default: the receiver ID)")
	labelsText := flags.String("labels", "", "approved receiver labels")
	grantsPath := flags.String("grants", "", "JSON file holding the adapter grants")
	selectorText := flags.String("selector", "", "approved selector")
	renewal := flags.String("renewal", "auto", "renewal policy")
	days := flags.Int("days", 30, "approval lifetime in days")
	if err := flags.Parse(args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || !flagSet(flags, "labels", "grants", "selector") {
		return errors.New("approve requires --labels, --grants and --selector")
	}
	if *renewal != "auto" && *renewal != "manual" {
		return errors.New("renewal must be auto or manual")
	}
	if *days <= 0 {
		return errors.New("days must be positive")
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
	result, err := client.Approve(ctx, requestHash, *pool, labels, grants, selector, *renewal, *days)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "receiver=%s generation=%d\n", result.ReceiverID, result.Generation)
	warnSkipped(stderr, result.Skipped)
	return nil
}

func runSecret(ctx context.Context, client *busclient.Client, args []string, stdout, stderr io.Writer) error {
	if len(args) < 3 || args[0] != "put" {
		return errors.New("usage: agentbus secret put NAME --to RECEIVER_ID --from-file PATH")
	}
	name := args[1]
	flags := flag.NewFlagSet("secret put", flag.ContinueOnError)
	flags.SetOutput(stderr)
	receiverID := flags.String("to", "", "receiver ID")
	path := flags.String("from-file", "", "secret file")
	if err := flags.Parse(args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || !flagSet(flags, "to", "from-file") {
		return errors.New("secret put requires --to and --from-file")
	}
	result, err := client.PutSecret(ctx, name, *receiverID, *path)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "name=%s receiver=%s version=%d\n", result.Name, result.ReceiverID, result.Version)
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
	deadline := flags.Duration("deadline", 30*time.Minute, "task deadline")
	queueDeadline := flags.Duration("queue-deadline", 10*time.Minute, "queue deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || !flagSet(flags, "selector", "labels") {
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
		Selector: selector, Labels: labels, Prompt: *prompt,
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
	if len(args) == 0 {
		return errors.New("usage: agentbus await RUN_ID [--timeout 30m]")
	}
	runID := args[0]
	flags := flag.NewFlagSet("await", flag.ContinueOnError)
	flags.SetOutput(stderr)
	timeout := flags.Duration("timeout", 30*time.Minute, "wait timeout")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *timeout <= 0 {
		return errors.New("await accepts one run ID and a positive timeout")
	}
	result, err := client.Await(ctx, runID, *timeout)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, result.Reply.State)
	for _, artifact := range result.Reply.Artifacts {
		for _, part := range artifact.Parts {
			fmt.Fprintln(stdout, part.Text)
		}
	}
	warnSkipped(stderr, result.Skipped)
	if result.Reply.State != wire.TaskStateCompleted {
		return fmt.Errorf("run ended in %s", result.Reply.State)
	}
	return nil
}

func runEvents(ctx context.Context, client *busclient.Client, args []string, stdout, stderr io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: agentbus events RUN_ID")
	}
	result, err := client.Events(ctx, args[0])
	if err != nil {
		return err
	}
	for _, event := range result.Events {
		fmt.Fprintf(stdout, "attempt=%d kind=%s state=%s receiver=%s time=%s\n",
			event.Attempt, event.Kind, event.State, event.ReceiverID,
			time.UnixMilli(event.AtMS).UTC().Format(time.RFC3339Nano))
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
