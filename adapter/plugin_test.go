package adapter

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/adapter/internal/adapterv1"
	"github.com/hashicorp/go-hclog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	runCredential      = "sk-ant-oat01-run-credential-never-logged"
	platformCredential = "nomad-platform-token-never-logged"
)

var testPlugin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "wherry-adapter-test")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	testPlugin = filepath.Join(dir, "wherry-plugin-fake")
	if output, err := exec.Command("go", "build", "-o", testPlugin, "./internal/testplugin").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build test plugin: %v\n%s", err, output)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type recorded struct {
	PID      int      `json:"pid"`
	Env      string   `json:"env"`
	Call     string   `json:"call"`
	Settings Settings `json:"settings"`
	Run      *Run     `json:"run"`
}

func fixed(path string) func() (string, error) {
	return func() (string, error) { return path, nil }
}

// launch starts the test plugin with a fresh record file and returns both.
func launch(t *testing.T) (*Plugin, string, *syncBuffer) {
	t.Helper()
	record := filepath.Join(t.TempDir(), "record.jsonl")
	t.Setenv("WHERRY_TESTPLUGIN_RECORD", record)
	logs := &syncBuffer{}
	p, err := Launch(fixed(testPlugin), logs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p, record, logs
}

func records(t *testing.T, path string) []recorded {
	t.Helper()
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var all []recorded
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry recorded
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatal(err)
		}
		all = append(all, entry)
	}
	return all
}

func configured(t *testing.T, p *Plugin, name, config string) Adapter {
	t.Helper()
	a, err := p.Configure(Settings{Name: name, Pool: "pool-a", Agent: "claude", Config: []byte(config)})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func pid(p *Plugin) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client.ReattachConfig().Pid
}

func kill(t *testing.T, p *Plugin) int {
	t.Helper()
	killed := pid(p)
	if err := syscall.Kill(killed, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	return killed
}

func shortRestart(t *testing.T) {
	previous := restartBackoff
	restartBackoff = 300 * time.Millisecond
	t.Cleanup(func() { restartBackoff = previous })
}

// eventually polls check until it returns nil or ten seconds pass.
func eventually(t *testing.T, what string, check func() error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := check()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %v", what, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestCallsReachTheAdapterTheyName(t *testing.T) {
	p, record, _ := launch(t)
	a, b := configured(t, p, "a", "{}"), configured(t, p, "b", "{}")
	for name, adapter := range map[string]Adapter{"a": a, "b": b} {
		outcome, handle, err := adapter.Dispatch(context.Background(), Run{ID: "run-" + name, Adapter: name, Agent: "claude"})
		if err != nil || outcome != OutcomeCreated || handle.Name != name+"/run-"+name {
			t.Fatalf("adapter %s dispatch: %v %+v %v; want its own instance's handle", name, outcome, handle, err)
		}
	}
	for _, entry := range records(t, record) {
		if entry.Call == "dispatch" && entry.Run.ID != "run-"+entry.Settings.Name {
			t.Fatalf("instance %s dispatched %s", entry.Settings.Name, entry.Run.ID)
		}
	}
}

func TestConfigureCarriesInlineLimit(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "a", `{"inline_limit":49152}`)
	if got := a.InlineLimit(); got != 49152 {
		t.Fatalf("inline limit = %d, want 49152", got)
	}
}

func TestDispatchCarriesTheRun(t *testing.T) {
	t.Setenv("WHERRY_TESTPLUGIN_ENV", "from-the-receiver")
	p, record, _ := launch(t)
	a := configured(t, p, "a", "{}")
	run := Run{ID: "run-a", Adapter: "a", Agent: "claude", Model: "sonnet", Attempt: 3, Prompt: []byte("hello"), Credential: []byte(runCredential),
		Inputs:      []File{{Name: "data.csv", Data: []byte("a,b\n")}, {Name: "empty", Data: []byte{}}},
		Credentials: []Secret{{Name: "deploy-key", File: "id_ed25519", Value: []byte(runCredential)}},
		Fetches:     []Fetch{{Name: "dataset.bin", URL: "https://store.example.test/bucket/key?X-Amz-Signature=0", Size: 5 << 20, SHA256: strings.Repeat("a", 64)}}}
	if _, _, err := a.Dispatch(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.Dispatch(context.Background(), Run{ID: "run-b", Adapter: "a", Agent: "claude"}); err != nil {
		t.Fatal(err)
	}
	var dispatched []*Run
	for _, entry := range records(t, record) {
		if entry.Call == "dispatch" {
			dispatched = append(dispatched, entry.Run)
			if entry.Env != "from-the-receiver" {
				t.Fatalf("plugin environment %q; want the receiver's", entry.Env)
			}
		}
	}
	got := dispatched[0]
	if len(dispatched) != 2 || got.ID != run.ID || got.Model != "sonnet" || got.Attempt != 3 || string(got.Prompt) != "hello" || string(got.Credential) != runCredential ||
		!reflect.DeepEqual(got.Inputs, []File{{Name: "data.csv", Data: []byte("a,b\n")}, {Name: "empty"}}) || !reflect.DeepEqual(got.Credentials, run.Credentials) ||
		!reflect.DeepEqual(got.Fetches, run.Fetches) {
		t.Fatalf("plugin got %+v; want %+v", got, run)
	}
	if dispatched[1].Credential != nil || dispatched[1].Model != "" {
		t.Fatalf("run without credential or model arrived as %+v", dispatched[1])
	}
}

func TestFollowResumesFromOffset(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "a", "{}")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lines := make(chan Line, 4)
	ended := make(chan error, 1)
	go func() {
		_, err := a.Follow(ctx, Handle{Data: []byte("h")}, 41, lines)
		ended <- err
	}()
	line := <-lines
	if line.Offset != 41 || !strings.Contains(string(line.Data), `"result"`) {
		t.Fatalf("first line %+v from offset 41; want the result line", line)
	}
	cancel()
	if err := <-ended; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled follow returned %v", err)
	}
}

func TestTerminalReasonCrossesTheLink(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "a", `{"never_started":"failed to interpolate ${BAD:-x}"}`)
	terminal, err := a.Follow(context.Background(), Handle{Data: []byte("h")}, 0, make(chan Line, 4))
	if want := (Terminal{Class: "validation", Node: "fake-node", Reason: "failed to interpolate ${BAD:-x}"}); err != nil || terminal != want {
		t.Fatalf("follow = %+v, %v; want %+v", terminal, err, want)
	}
}

func TestPanicStaysInItsCall(t *testing.T) {
	p, _, logs := launch(t)
	a, b := configured(t, p, "a", `{"panic":"Sweep"}`), configured(t, p, "b", "{}")
	before := pid(p)
	if err := a.Sweep(context.Background()); err == nil || errors.Is(err, ErrLinkLost) || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("panicking sweep returned %v; want that call's error", err)
	}
	if err := b.Sweep(context.Background()); err != nil {
		t.Fatalf("other adapter after a panic: %v", err)
	}
	if ready, err := a.Ready(context.Background()); !ready || err != nil {
		t.Fatalf("panicked adapter's next call: %v %v", ready, err)
	}
	streaming := configured(t, p, "c", `{"panic":"Follow"}`)
	_, err := streaming.Follow(context.Background(), Handle{}, 0, make(chan Line, 4))
	if err == nil || errors.Is(err, ErrLinkLost) || !strings.Contains(err.Error(), "adapter c panicked") {
		t.Fatalf("panicking follow returned %v; want that call's error, naming its adapter", err)
	}
	if err := b.Sweep(context.Background()); err != nil {
		t.Fatalf("other adapter after a follow panic: %v", err)
	}
	// The plugin's log reaches the receiver asynchronously, and go-plugin re-logs its fields in map order and
	// interleaves concurrent entries, so the log is awaited for the adapter alone.
	eventually(t, "follow panic logged with its adapter", func() error {
		if !strings.Contains(logs.String(), "adapter=c") {
			return fmt.Errorf("logs:\n%s", logs.String())
		}
		return nil
	})
	if after := pid(p); after != before {
		t.Fatalf("plugin process changed from %d to %d", before, after)
	}
}

func TestConfigureFailureStaysWithItsAdapter(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "a", "{}")
	if _, err := p.Configure(Settings{Name: "bad", Config: []byte(`{"fail":true}`)}); err == nil || !strings.Contains(err.Error(), "configured to fail") {
		t.Fatalf("failing configure returned %v", err)
	}
	if ready, err := a.Ready(context.Background()); !ready || err != nil {
		t.Fatalf("adapter after another's configure failure: %v %v", ready, err)
	}
}

func TestRestartConfiguresEveryAdapter(t *testing.T) {
	shortRestart(t)
	p, record, _ := launch(t)
	a, b := configured(t, p, "a", "{}"), configured(t, p, "b", "{}")
	killed := kill(t, p)
	eventually(t, "link lost while the plugin is down", func() error {
		if _, err := a.Ready(context.Background()); !errors.Is(err, ErrLinkLost) {
			return fmt.Errorf("ready returned %v", err)
		}
		return nil
	})
	eventually(t, "both adapters back after the restart", func() error {
		for _, adapter := range []Adapter{a, b} {
			if _, _, err := adapter.Dispatch(context.Background(), Run{ID: "after"}); err != nil {
				return err
			}
		}
		return nil
	})
	configured := map[string]bool{}
	for _, entry := range records(t, record) {
		if entry.Call == "configure" && entry.PID != killed {
			configured[entry.Settings.Name] = true
		}
	}
	if !configured["a"] || !configured["b"] {
		t.Fatalf("restarted process configured %v; want a and b", configured)
	}
	if restarts := p.Restarts(); restarts != 1 {
		t.Fatalf("restarts = %d; want 1", restarts)
	}
}

func TestOneProcessPerPlugin(t *testing.T) {
	p, record, _ := launch(t)
	configured(t, p, "a", "{}")
	configured(t, p, "b", "{}")
	pids := map[int]bool{}
	for _, entry := range records(t, record) {
		pids[entry.PID] = true
	}
	if len(pids) != 1 {
		t.Fatalf("adapters configured in processes %v; want one", pids)
	}
}

func TestFollowLinkLossIsDistinct(t *testing.T) {
	shortRestart(t)
	p, _, _ := launch(t)
	a := configured(t, p, "a", "{}")
	lines := make(chan Line, 4)
	ended := make(chan error, 1)
	go func() {
		_, err := a.Follow(context.Background(), Handle{}, 0, lines)
		ended <- err
	}()
	<-lines
	kill(t, p)
	if err := <-ended; !errors.Is(err, ErrLinkLost) {
		t.Fatalf("follow over a killed plugin returned %v; want ErrLinkLost", err)
	}
}

// dial connects to the plugin's socket as a party other than the receiver.
func dial(t *testing.T, p *Plugin, creds credentials.TransportCredentials) adapterv1.AdapterClient {
	t.Helper()
	p.mu.Lock()
	addr := p.client.ReattachConfig().Addr
	p.mu.Unlock()
	conn, err := grpc.NewClient("passthrough:///plugin", grpc.WithTransportCredentials(creds),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, addr.Network(), addr.String())
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return adapterv1.NewAdapterClient(conn)
}

func TestPlaintextConnectionFails(t *testing.T) {
	p, _, _ := launch(t)
	configured(t, p, "a", "{}")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := dial(t, p, insecure.NewCredentials()).Ready(ctx, &adapterv1.ReadyRequest{Adapter: "a"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("plaintext call returned %v; want a refused connection", err)
	}
}

func TestTLSWithoutClientCertificateFails(t *testing.T) {
	p, _, _ := launch(t)
	configured(t, p, "a", "{}")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The server's certificate is accepted, so only the missing client certificate can fail the call.
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12})
	if _, err := dial(t, p, creds).Ready(ctx, &adapterv1.ReadyRequest{Adapter: "a"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("call without a client certificate returned %v; want a refused connection", err)
	}
}

func TestRefusesPluginWithoutMutualTLS(t *testing.T) {
	record := filepath.Join(t.TempDir(), "record.jsonl")
	t.Setenv("WHERRY_TESTPLUGIN_RECORD", record)
	t.Setenv("WHERRY_TESTPLUGIN_PLAINTEXT", "1")
	p, err := Launch(fixed(testPlugin), &syncBuffer{})
	if err == nil {
		_, configureErr := p.Configure(Settings{Name: "a", Platform: platformCredential})
		p.Close()
		t.Fatalf("launched a plugin that offered no mutual TLS; configure returned %v", configureErr)
	}
	if got := records(t, record); len(got) != 0 {
		t.Fatalf("plugin without mutual TLS received %+v", got)
	}
}

func TestSecretsStayOutOfLogs(t *testing.T) {
	shortRestart(t)
	previous := logLevel
	logLevel = hclog.Trace
	t.Cleanup(func() { logLevel = previous })
	p, record, logs := launch(t)
	a, err := p.Configure(Settings{Name: "a", Pool: "pool-a", Agent: "claude", Credential: "model", Config: []byte("{}"), Platform: platformCredential})
	if err != nil {
		t.Fatal(err)
	}
	const (
		taskCredential = "ghp-task-credential-never-logged"
		fetchURL       = "https://s3.example/bucket/wherry/tenant-a/run-a/inputs/data.bin?X-Amz-Signature=fetch-signature-never-logged"
	)
	run := Run{ID: "run-a", Prompt: []byte("hello"), Credential: []byte(runCredential),
		Credentials: []Secret{{Name: "deploy-key", File: "id_ed25519", Value: []byte(taskCredential)}},
		Fetches:     []Fetch{{Name: "data.bin", URL: fetchURL, Size: 4, SHA256: strings.Repeat("0", 64)}}}
	if _, _, err := a.Dispatch(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	killed := kill(t, p)
	eventually(t, "adapter back after the restart", func() error {
		_, err := a.Ready(context.Background())
		return err
	})
	reconfigured := false
	for _, entry := range records(t, record) {
		reconfigured = reconfigured || entry.Call == "configure" && entry.PID != killed && entry.Settings.Platform == platformCredential
	}
	output := logs.String()
	if !reconfigured || !strings.Contains(output, "plugin restarted") || !strings.Contains(output, "starting plugin") {
		t.Fatalf("restart did not carry the platform credential, or the logs miss the link's own lines:\n%s", output)
	}
	for _, secret := range []string{runCredential, platformCredential, taskCredential, fetchURL, "fetch-signature-never-logged"} {
		if strings.Contains(output, secret) {
			t.Fatalf("logs carry a credential:\n%s", output)
		}
	}
}

func TestConfigureReportsDirectories(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "plain", "{}")
	if got := a.Directories(); got != (Directories{Input: "/fake/inputs", Output: "/fake/outputs", Credentials: "/fake/credentials"}) {
		t.Fatalf("directories %+v; want the plugin's", got)
	}
}

func TestMetricsCrossTheLink(t *testing.T) {
	p, _, _ := launch(t)
	samples, err := configured(t, p, "a", "{}").Metrics(context.Background())
	want := []Sample{
		{Family: "wherry_adapter_cleanup_pending", Labels: map[string]string{"adapter": "a"}, Kind: KindGauge, Value: 2},
		{Family: "wherry_adapter_placement_seconds", Kind: KindHistogram, Histogram: Histogram{Buckets: []uint64{0, 1, 2}, Sum: 3.5, Count: 2}},
	}
	if err != nil || len(samples) != len(want) {
		t.Fatalf("metrics = %+v, %v; want %+v", samples, err, want)
	}
	for i := range want {
		if samples[i].Family != want[i].Family || samples[i].Kind != want[i].Kind || samples[i].Value != want[i].Value ||
			!reflect.DeepEqual(samples[i].Histogram, want[i].Histogram) || len(samples[i].Labels) != len(want[i].Labels) || samples[i].Labels["adapter"] != want[i].Labels["adapter"] {
			t.Fatalf("sample %d = %+v; want %+v", i, samples[i], want[i])
		}
	}
}

func TestLargeDispatch(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "a", "{}")
	if _, _, err := a.Dispatch(context.Background(), Run{ID: "run-large", Prompt: bytes.Repeat([]byte("p"), 5<<20)}); err != nil {
		t.Fatalf("5 MiB prompt: %v", err)
	}
	if _, _, err := a.Dispatch(context.Background(), Run{ID: "run-huge", Prompt: bytes.Repeat([]byte("p"), maxMessage+1)}); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("prompt over the limit returned %v; want ErrMessageTooLarge", err)
	}
	if ready, err := a.Ready(context.Background()); !ready || err != nil {
		t.Fatalf("adapter after an oversized message: %v %v", ready, err)
	}
}

func TestCloseStopsRestarts(t *testing.T) {
	shortRestart(t)
	p, record, _ := launch(t)
	configured(t, p, "a", "{}")
	closed := pid(p)
	p.Close()
	time.Sleep(4 * restartBackoff)
	for _, entry := range records(t, record) {
		if entry.PID != closed {
			t.Fatalf("plugin started again as %d after Close", entry.PID)
		}
	}
}

func TestInputTooLargeCrossesTheLink(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "a", `{"too_large":"data.csv"}`)
	outcome, _, err := a.Dispatch(context.Background(), Run{ID: "run-a", Inputs: []File{{Name: "data.csv", Data: []byte("x")}}})
	var tooLarge *InputTooLargeError
	if outcome != OutcomeNothingCreated || !errors.As(err, &tooLarge) || tooLarge.Name != "data.csv" {
		t.Fatalf("dispatch = %v, %v; want nothing created and input data.csv too large", outcome, err)
	}
}

func collected(t *testing.T, a Adapter, limits Limits) ([]Output, error) {
	t.Helper()
	outputs := make(chan Output, 8)
	err := a.Collect(context.Background(), Handle{Data: []byte("h")}, limits, outputs)
	close(outputs)
	var all []Output
	for output := range outputs {
		all = append(all, output)
	}
	return all, err
}

func TestCollectJoinsEachOutputsPieces(t *testing.T) {
	p, _, _ := launch(t)
	outputs, err := collected(t, configured(t, p, "a", `{"outputs":true}`), Limits{Files: 256, FileBytes: 8 << 20, TotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(outputs) != 3 || outputs[0].Name != "big.bin" || !bytes.Equal(outputs[0].Data, bytes.Repeat([]byte("b"), 2<<20+1)) || outputs[0].Size != 2<<20+1 {
		t.Fatalf("collected %d outputs; want big.bin joined from its pieces first", len(outputs))
	}
	if empty := outputs[1]; empty.Name != "empty" || empty.Data == nil || len(empty.Data) != 0 {
		t.Fatalf("empty output %+v; want read with no data", empty)
	}
	if link := outputs[2]; link.Name != "link" || link.Data != nil || link.Mode != fs.ModeSymlink || link.Size != 4 {
		t.Fatalf("link output %+v; want unread with its type", link)
	}
}

// Streamed outputs pass over the link as Bodies, whatever FileBytes says, while the plugin's Collect waits on each.
func TestCollectStreamsTheOutputsTheLimitsName(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "a", `{"outputs":true}`)
	outputs := make(chan Output)
	ended := make(chan error, 1)
	go func() {
		defer close(outputs)
		ended <- a.Collect(context.Background(), Handle{Data: []byte("h")}, Limits{Files: 256, FileBytes: 1, TotalBytes: 1, Stream: []string{"big.bin", "empty", "link"}, StreamBytes: 4 << 20}, outputs)
	}()
	read := map[string][]byte{}
	var link Output
	for output := range outputs {
		if output.Body == nil {
			link = output
			continue
		}
		data, err := io.ReadAll(output.Body)
		output.Body.Close()
		if err != nil {
			t.Fatalf("read %s: %v", output.Name, err)
		}
		read[output.Name] = data
	}
	if err := <-ended; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read["big.bin"], bytes.Repeat([]byte("b"), 2<<20+1)) || read["empty"] == nil || len(read["empty"]) != 0 || len(read) != 2 {
		t.Fatalf("streamed %d outputs; want big.bin whole and empty", len(read))
	}
	if link.Name != "link" || link.Data != nil || link.Mode != fs.ModeSymlink {
		t.Fatalf("link output %+v; want unread", link)
	}
}

func TestCollectRefusesAStreamOverTheStreamLimit(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "a", `{"outputs":true}`)
	outputs := make(chan Output)
	ended := make(chan error, 1)
	go func() {
		defer close(outputs)
		ended <- a.Collect(context.Background(), Handle{Data: []byte("h")}, Limits{Files: 256, FileBytes: 8 << 20, TotalBytes: 8 << 20, Stream: []string{"big.bin"}, StreamBytes: 2 << 20}, outputs)
	}()
	var readErr error
	for output := range outputs {
		if output.Body != nil {
			_, readErr = io.ReadAll(output.Body)
			output.Body.Close()
		}
	}
	if err := <-ended; err == nil || readErr == nil {
		t.Fatalf("collect = %v and read %v; want a stream over its limit refused on both sides", err, readErr)
	}
}

func TestCollectRefusesAnOutputOverTheFileLimit(t *testing.T) {
	p, _, _ := launch(t)
	if _, err := collected(t, configured(t, p, "a", `{"outputs":true}`), Limits{Files: 256, FileBytes: 2 << 20, TotalBytes: 8 << 20}); err == nil {
		t.Fatal("collect passed an output over the file limit")
	}
}

// The fake's Collect fails with a plain error, as a plugin built before Error does: the core gets its text alone.
func TestCollectFailureCrossesTheLink(t *testing.T) {
	p, _, _ := launch(t)
	outputs, err := collected(t, configured(t, p, "a", "{}"), Limits{Files: 256, FileBytes: 8 << 20, TotalBytes: 8 << 20})
	var failure *Error
	if err == nil || len(outputs) != 0 || errors.As(err, &failure) || err.Error() != "no outputs configured" {
		t.Fatalf("collect = %d outputs, %v; want the adapter's error text alone", len(outputs), err)
	}
}

func TestErrorCrossesTheLinkWithItsOpAndClass(t *testing.T) {
	p, _, _ := launch(t)
	err := configured(t, p, "a", `{"sweep_error":"status 403: permission denied"}`).Sweep(context.Background())
	var failure *Error
	if !errors.As(err, &failure) || failure.Op != "list jobs" || failure.Class != "denied" || failure.Err.Error() != "status 403: permission denied" {
		t.Fatalf("sweep = %#v; want an Error with the adapter's op, class and text", err)
	}
}

func TestErrorWithoutCauseIsItsOp(t *testing.T) {
	if got := (&Error{Op: "list jobs", Class: "denied"}).Error(); got != "list jobs" {
		t.Fatalf("Error() = %q, want the op alone", got)
	}
}

func TestServeHandsTheAdapterItsLogger(t *testing.T) {
	p, _, logs := launch(t)
	configured(t, p, "logged", "{}")
	eventually(t, "the plugin's own log line relayed", func() error {
		if output := logs.String(); !strings.Contains(output, "configuring adapter") || !strings.Contains(output, "adapter=logged") {
			return fmt.Errorf("logs:\n%s", output)
		}
		return nil
	})
}

// A plugin built against protocol version 6 still carries turn fields and is incompatible.
func TestVersionSixPluginIsRefused(t *testing.T) {
	script := filepath.Join(t.TempDir(), "wherry-plugin-old")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho '1|6|tcp|127.0.0.1:1|grpc'\nsleep 5\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	p, err := Launch(fixed(script), &syncBuffer{})
	if err == nil {
		p.Close()
		t.Fatal("launched a version 6 plugin")
	}
	if !strings.Contains(err.Error(), "incompatible API version with plugin. Plugin version: 6, Client versions: [7]") {
		t.Fatalf("launch error %v; want the version named", err)
	}
}
