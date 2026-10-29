package pluginsdk

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
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

	"github.com/benemon/wherry/pluginsdk/internal/pluginv1"
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
	dir, err := os.MkdirTemp("", "wherry-pluginsdk-test")
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

// sha256Of is the SHA-256 of the file at path.
func sha256Of(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	return sum[:]
}

// launch starts the test plugin with a fresh record file and returns both.
func launch(t *testing.T) (*Process, string, *syncBuffer) {
	t.Helper()
	record := filepath.Join(t.TempDir(), "record.jsonl")
	t.Setenv("WHERRY_TESTPLUGIN_RECORD", record)
	logs := &syncBuffer{}
	p, err := Launch("fake", fixed(testPlugin), sha256Of(t, testPlugin), logs)
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

func configured(t *testing.T, p *Process, name, config string) Plugin {
	t.Helper()
	a, err := p.Configure(Settings{Name: name, Pool: "pool-a", Agent: "claude", Config: []byte(config)})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func pid(p *Process) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client.ReattachConfig().Pid
}

func kill(t *testing.T, p *Process) int {
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

func TestCallsReachTheProfileTheyName(t *testing.T) {
	p, record, _ := launch(t)
	a, b := configured(t, p, "a", "{}"), configured(t, p, "b", "{}")
	for name, profile := range map[string]Plugin{"a": a, "b": b} {
		outcome, handle, err := profile.Dispatch(context.Background(), Run{ID: "run-" + name, Profile: name, Agent: "claude"})
		if err != nil || outcome != OutcomeCreated || handle.Name != name+"/run-"+name {
			t.Fatalf("profile %s dispatch: %v %+v %v; want its own instance's handle", name, outcome, handle, err)
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

func TestConfigureReportsVersion(t *testing.T) {
	p, _, _ := launch(t)
	if version, _ := p.Version(); version != "" {
		t.Fatalf("version before Configure = %q, want none", version)
	}
	configured(t, p, "a", "{}")
	if version, protocol := p.Version(); version != "v0.0.0-test" || protocol != 10 {
		t.Fatalf("version = %q, protocol %d; want v0.0.0-test, 10", version, protocol)
	}
}

func TestDispatchCarriesTheRun(t *testing.T) {
	t.Setenv("WHERRY_TESTPLUGIN_ENV", "from-the-receiver")
	p, record, _ := launch(t)
	a := configured(t, p, "a", "{}")
	run := Run{ID: "run-a", Profile: "a", Agent: "claude", Model: "sonnet", Attempt: 3, Prompt: []byte("hello"),
		Inputs:      []File{{Name: "data.csv", Data: []byte("a,b\n")}, {Name: "empty", Data: []byte{}}},
		Credentials: []Secret{{Name: "model", Env: "MODEL_API_KEY", Value: []byte(runCredential)}, {Name: "deploy-key", File: "id_ed25519", Value: []byte(runCredential)}},
		Fetches:     []Fetch{{Name: "dataset.bin", URL: "https://store.example.test/bucket/key?X-Amz-Signature=0", Size: 5 << 20, SHA256: strings.Repeat("a", 64)}}}
	if _, _, err := a.Dispatch(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.Dispatch(context.Background(), Run{ID: "run-b", Profile: "a", Agent: "claude"}); err != nil {
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
	if len(dispatched) != 2 || got.ID != run.ID || got.Model != "sonnet" || got.Attempt != 3 || string(got.Prompt) != "hello" ||
		!reflect.DeepEqual(got.Inputs, []File{{Name: "data.csv", Data: []byte("a,b\n")}, {Name: "empty"}}) || !reflect.DeepEqual(got.Credentials, run.Credentials) ||
		!reflect.DeepEqual(got.Fetches, run.Fetches) {
		t.Fatalf("plugin got %+v; want %+v", got, run)
	}
	if dispatched[1].Credentials != nil || dispatched[1].Model != "" {
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
		t.Fatalf("other profile after a panic: %v", err)
	}
	if ready, err := a.Ready(context.Background()); !ready || err != nil {
		t.Fatalf("panicked profile's next call: %v %v", ready, err)
	}
	streaming := configured(t, p, "c", `{"panic":"Follow"}`)
	_, err := streaming.Follow(context.Background(), Handle{}, 0, make(chan Line, 4))
	if err == nil || errors.Is(err, ErrLinkLost) || !strings.Contains(err.Error(), "profile c panicked") {
		t.Fatalf("panicking follow returned %v; want that call's error, naming its profile", err)
	}
	if err := b.Sweep(context.Background()); err != nil {
		t.Fatalf("other profile after a follow panic: %v", err)
	}
	// The plugin's log reaches the receiver asynchronously, and go-plugin re-logs its fields in map order and
	// interleaves concurrent entries, so the log is awaited for the profile alone.
	eventually(t, "follow panic logged with its profile", func() error {
		if !strings.Contains(logs.String(), "profile=c") {
			return fmt.Errorf("logs:\n%s", logs.String())
		}
		return nil
	})
	if after := pid(p); after != before {
		t.Fatalf("plugin process changed from %d to %d", before, after)
	}
}

func TestConfigureFailureStaysWithItsProfile(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "a", "{}")
	if _, err := p.Configure(Settings{Name: "bad", Config: []byte(`{"fail":true}`)}); err == nil || !strings.Contains(err.Error(), "configured to fail") {
		t.Fatalf("failing configure returned %v", err)
	}
	if ready, err := a.Ready(context.Background()); !ready || err != nil {
		t.Fatalf("profile after another's configure failure: %v %v", ready, err)
	}
}

func TestRestartConfiguresEveryProfile(t *testing.T) {
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
	eventually(t, "both profiles back after the restart", func() error {
		for _, profile := range []Plugin{a, b} {
			if _, _, err := profile.Dispatch(context.Background(), Run{ID: "after"}); err != nil {
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

// A plugin that stops answering without exiting is killed and started again; the call it hung ends.
func TestHungPluginIsRestarted(t *testing.T) {
	shortRestart(t)
	previous := pingInterval
	pingInterval = 200 * time.Millisecond
	t.Cleanup(func() { pingInterval = previous })
	p, _, _ := launch(t)
	a := configured(t, p, "a", `{"hang":true}`)
	hung := pid(p)
	// Close waits on a stopped process; one the supervisor failed to kill is resumed so the test ends.
	t.Cleanup(func() { _ = syscall.Kill(hung, syscall.SIGCONT) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := a.Ready(ctx); !errors.Is(err, ErrLinkLost) {
		t.Fatalf("ready on a hung plugin returned %v; want the link lost before its context ended", err)
	}
	eventually(t, "the profile back after the restart", func() error {
		_, _, err := a.Dispatch(context.Background(), Run{ID: "after"})
		return err
	})
	if after := pid(p); after == hung {
		t.Fatalf("plugin process is still %d", hung)
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
		t.Fatalf("profiles configured in processes %v; want one", pids)
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
func dial(t *testing.T, p *Process, creds credentials.TransportCredentials) pluginv1.PluginClient {
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
	return pluginv1.NewPluginClient(conn)
}

func TestPlaintextConnectionFails(t *testing.T) {
	p, _, _ := launch(t)
	configured(t, p, "a", "{}")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := dial(t, p, insecure.NewCredentials()).Ready(ctx, &pluginv1.ReadyRequest{Profile: "a"}); status.Code(err) != codes.Unavailable {
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
	if _, err := dial(t, p, creds).Ready(ctx, &pluginv1.ReadyRequest{Profile: "a"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("call without a client certificate returned %v; want a refused connection", err)
	}
}

func TestRefusesPluginWithoutMutualTLS(t *testing.T) {
	record := filepath.Join(t.TempDir(), "record.jsonl")
	t.Setenv("WHERRY_TESTPLUGIN_RECORD", record)
	t.Setenv("WHERRY_TESTPLUGIN_PLAINTEXT", "1")
	p, err := Launch("fake", fixed(testPlugin), sha256Of(t, testPlugin), &syncBuffer{})
	if err == nil {
		_, configureErr := p.Configure(Settings{Name: "a", Credential: platformCredential})
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
	a, err := p.Configure(Settings{Name: "a", Pool: "pool-a", Agent: "claude", Config: []byte("{}"), Credential: platformCredential})
	if err != nil {
		t.Fatal(err)
	}
	const (
		taskCredential = "ghp-task-credential-never-logged"
		fetchURL       = "https://s3.example/bucket/wherry/tenant-a/run-a/inputs/data.bin?X-Amz-Signature=fetch-signature-never-logged"
	)
	run := Run{ID: "run-a", Prompt: []byte("hello"),
		Credentials: []Secret{{Name: "model", Env: "MODEL_API_KEY", Value: []byte(runCredential)}, {Name: "deploy-key", File: "id_ed25519", Value: []byte(taskCredential)}},
		Fetches:     []Fetch{{Name: "data.bin", URL: fetchURL, Size: 4, SHA256: strings.Repeat("0", 64)}}}
	if _, _, err := a.Dispatch(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	killed := kill(t, p)
	eventually(t, "profile back after the restart", func() error {
		_, err := a.Ready(context.Background())
		return err
	})
	reconfigured := false
	for _, entry := range records(t, record) {
		reconfigured = reconfigured || entry.Call == "configure" && entry.PID != killed && entry.Settings.Credential == platformCredential
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
		{Family: "wherry_plugin_cleanup_pending", Labels: map[string]string{"profile": "a"}, Kind: KindGauge, Value: 2},
		{Family: "wherry_plugin_placement_seconds", Kind: KindHistogram, Histogram: Histogram{Buckets: []uint64{0, 1, 2}, Sum: 3.5, Count: 2}},
	}
	if err != nil || len(samples) != len(want) {
		t.Fatalf("metrics = %+v, %v; want %+v", samples, err, want)
	}
	for i := range want {
		if samples[i].Family != want[i].Family || samples[i].Kind != want[i].Kind || samples[i].Value != want[i].Value ||
			!reflect.DeepEqual(samples[i].Histogram, want[i].Histogram) || len(samples[i].Labels) != len(want[i].Labels) || samples[i].Labels["profile"] != want[i].Labels["profile"] {
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
		t.Fatalf("profile after an oversized message: %v %v", ready, err)
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

// go-plugin checks the binary against the pin before each start, the first and every restart, and the refusal names
// the type and both hashes.
func TestChecksumIsCheckedAtStartAndRestart(t *testing.T) {
	shortRestart(t)
	t.Setenv("WHERRY_TESTPLUGIN_RECORD", filepath.Join(t.TempDir(), "record.jsonl"))
	binary := filepath.Join(t.TempDir(), "wherry-plugin-fake")
	content, err := os.ReadFile(testPlugin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, content, 0o700); err != nil {
		t.Fatal(err)
	}
	pin := sha256Of(t, binary)
	other := sha256.Sum256([]byte("other"))
	if p, err := Launch("fake", fixed(binary), other[:], &syncBuffer{}); err == nil {
		p.Close()
		t.Fatal("launched a binary whose SHA-256 is not the pin")
	} else if want := fmt.Sprintf("plugin type fake: %s has SHA-256 %x and the pin is %x", binary, pin, other); err.Error() != want {
		t.Fatalf("launch error %v; want %q", err, want)
	}
	logs := &syncBuffer{}
	p, err := Launch("fake", fixed(binary), pin, logs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	configured(t, p, "a", "{}")
	// A running binary cannot be written in place, so the changed one replaces it.
	replacement := binary + ".new"
	if err := os.WriteFile(replacement, append(content, 0), 0o700); err != nil {
		t.Fatal(err)
	}
	changed := sha256Of(t, replacement)
	if err := os.Rename(replacement, binary); err != nil {
		t.Fatal(err)
	}
	kill(t, p)
	want := fmt.Sprintf("plugin type fake: %s has SHA-256 %x and the pin is %x", binary, changed, pin)
	eventually(t, "the restart refused", func() error {
		if !strings.Contains(logs.String(), want) {
			return fmt.Errorf("logs:\n%s", logs.String())
		}
		return nil
	})
	if restarts := p.Restarts(); restarts != 0 {
		t.Fatalf("restarts = %d; want the changed binary refused", restarts)
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

func TestFetchFailureCrossesTheLink(t *testing.T) {
	p, _, _ := launch(t)
	a := configured(t, p, "a", `{"fetch_fails":"data.csv"}`)
	outcome, _, err := a.Dispatch(context.Background(), Run{ID: "run-a", Fetches: []Fetch{{Name: "data.csv", URL: "https://store.example/data.csv"}}})
	var failure *Error
	if outcome != OutcomeNothingCreated || !errors.As(err, &failure) || failure.Class != ClassFetch || failure.Op != "fetch data.csv" {
		t.Fatalf("dispatch = %v, %v; want nothing created and a fetch failure", outcome, err)
	}
}

func collected(t *testing.T, a Plugin, limits Limits) ([]Output, error) {
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
		t.Fatalf("collect = %d outputs, %v; want the plugin's error text alone", len(outputs), err)
	}
}

func TestErrorCrossesTheLinkWithItsOpAndClass(t *testing.T) {
	p, _, _ := launch(t)
	err := configured(t, p, "a", `{"sweep_error":"status 403: permission denied"}`).Sweep(context.Background())
	var failure *Error
	if !errors.As(err, &failure) || failure.Op != "list jobs" || failure.Class != "denied" || failure.Err.Error() != "status 403: permission denied" {
		t.Fatalf("sweep = %#v; want an Error with the plugin's op, class and text", err)
	}
}

func TestErrorWithoutCauseIsItsOp(t *testing.T) {
	if got := (&Error{Op: "list jobs", Class: "denied"}).Error(); got != "list jobs" {
		t.Fatalf("Error() = %q, want the op alone", got)
	}
}

func TestServeHandsTheProfileItsLogger(t *testing.T) {
	p, _, logs := launch(t)
	configured(t, p, "logged", "{}")
	eventually(t, "the plugin's own log line relayed", func() error {
		if output := logs.String(); !strings.Contains(output, "configuring profile") || !strings.Contains(output, "profile=logged") {
			return fmt.Errorf("logs:\n%s", output)
		}
		return nil
	})
}

// A plugin serving only protocol version 9, the one before this, or only 11, the one after, is refused.
func TestOtherProtocolVersionsAreRefused(t *testing.T) {
	for _, version := range []int{9, 11} {
		script := filepath.Join(t.TempDir(), "wherry-plugin-other")
		if err := os.WriteFile(script, fmt.Appendf(nil, "#!/bin/sh\necho '1|%d|tcp|127.0.0.1:1|grpc'\nsleep 5\n", version), 0o700); err != nil {
			t.Fatal(err)
		}
		p, err := Launch("other", fixed(script), sha256Of(t, script), &syncBuffer{})
		if err == nil {
			p.Close()
			t.Fatalf("launched a version %d plugin", version)
		}
		if want := fmt.Sprintf("incompatible API version with plugin. Plugin version: %d, Client versions: [10]", version); !strings.Contains(err.Error(), want) {
			t.Fatalf("launch error %v; want %q", err, want)
		}
	}
}

// A plugin and a receiver built before VersionedPlugins, naming protocol version 10 through
// HandshakeConfig.ProtocolVersion, run with this SDK's.
func TestProtocolTenBeforeVersionedPlugins(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "wherry-plugin-protocol10")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = filepath.Join("testdata", "protocol10")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the protocol 10 fixture: %v\n%s", err, output)
	}
	t.Run("its plugin under this Launch", func(t *testing.T) {
		p, err := Launch("protocol10", fixed(binary), sha256Of(t, binary), &syncBuffer{})
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		configured(t, p, "a", "{}")
		if version, protocol := p.Version(); version != "v0.0.0-protocol10" || protocol != 10 {
			t.Fatalf("version = %q, protocol %d; want v0.0.0-protocol10, 10", version, protocol)
		}
	})
	t.Run("this Serve under its Launch", func(t *testing.T) {
		t.Setenv("WHERRY_TESTPLUGIN_RECORD", filepath.Join(t.TempDir(), "record.jsonl"))
		if output, err := exec.Command(binary, testPlugin).Output(); err != nil || string(output) != "v0.0.0-test 10\n" {
			t.Fatalf("launch = %q, %v; want version v0.0.0-test and protocol 10", output, err)
		}
	})
}

// A plugin serves protocol version 10 only under the WHERRY_PLUGIN cookie, and refuses to serve without it.
func TestServeHandshakeUsesThePluginCookie(t *testing.T) {
	if handshake.MagicCookieKey != "WHERRY_PLUGIN" {
		t.Fatalf("handshake %+v; want cookie key WHERRY_PLUGIN", handshake)
	}
	serve := func(env ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, testPlugin)
		cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "TMPDIR=" + t.TempDir()}, env...)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
		line, err := bufio.NewReader(stdout).ReadString('\n')
		return line, err
	}
	line, err := serve("WHERRY_PLUGIN=" + handshake.MagicCookieValue)
	if err != nil || !strings.HasPrefix(line, "1|10|") {
		t.Fatalf("handshake line %q, %v; want core protocol 1 and plugin protocol 10", line, err)
	}
	if line, err := serve(); err == nil || line != "" {
		t.Fatalf("served %q without the cookie", line)
	}
}
