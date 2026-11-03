package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/hashicorp/nomad/api"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

const (
	devNomadAddr  = "http://127.0.0.1:4646"
	devBroker     = "127.0.0.1:9093"
	devStorage    = "http://127.0.0.1:18080"
	devBucket     = "wherry-dev"
	devPool       = "dev"
	devTenant     = "dev"
	devHost       = "dev"
	devReceiver   = "dev-1"
	devSuperUser  = "wherry-admin"
	devSmokeInput = "smoke-input.txt"
)

// devPorts are the fixed ports of an instance, all on 127.0.0.1; storage adds 18080.
var devPorts = []int{4646, 4647, 4648, 9093}

// nomadDenylist is Nomad 2.1.1's default env.denylist (command/agent/host DefaultEnvDenyList), which setting the option replaces.
var nomadDenylist = []string{
	"CONSUL_TOKEN", "CONSUL_HTTP_TOKEN", "CONSUL_HTTP_TOKEN_FILE", "NOMAD_TOKEN", "VAULT_TOKEN",
	"CONSUL_LICENSE", "NOMAD_LICENSE", "VAULT_LICENSE", "CONSUL_LICENSE_PATH", "NOMAD_LICENSE_PATH", "VAULT_LICENSE_PATH",
	"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_METADATA_URL",
	"GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_OAUTH_ACCESS_TOKEN",
}

// devCredential is a credential an agent's terms take, at its environment target, and the variables up copies it
// from, the first one set.
type devCredential struct {
	name, target string
	from         []string
}

var devCredentials = map[string][]devCredential{
	"claude": {{"claude-oauth-token", "CLAUDE_CODE_OAUTH_TOKEN", []string{"CLAUDE_CODE_OAUTH_TOKEN"}}, {"anthropic-api-key", "ANTHROPIC_API_KEY", []string{"ANTHROPIC_API_KEY"}}},
	"codex":  {{"openai-api-key", "CODEX_API_KEY", []string{"CODEX_API_KEY", "OPENAI_API_KEY"}}},
	"gemini": {{"gemini-api-key", "GEMINI_API_KEY", []string{"GEMINI_API_KEY"}}},
	"mock":   {{"mock-token", "MOCK_TOKEN", []string{"WHERRY_DEV_MOCK_TOKEN"}}},
}

// devCommands are the harness commands of the raw_exec profiles; container profiles run them from /local.
var devCommands = map[string]string{
	"claude": `exec claude -p --output-format stream-json --verbose --allowedTools "Read,Write,Edit,Glob,Grep,Bash" --add-dir "$WHERRY_INPUT_DIR" --add-dir "$WHERRY_OUTPUT_DIR" < prompt.txt`,
	"codex":  `exec codex exec --json --ephemeral --skip-git-repo-check --sandbox workspace-write - < prompt.txt`,
	"gemini": `exec gemini --yolo --output-format stream-json -p "" < prompt.txt`,
}

// devEnv is what up renders an instance's environment from.
type devEnv struct {
	instance devInstance
	manifest devManifest
	// driver is the Nomad driver of the runtime's containers: podman on Linux with podman, else docker.
	driver string
	// socket is the runtime's API socket as a unix:// URI.
	socket string
	// uid and gid are the invoking user's, which the Kafka task runs as under docker.
	uid, gid int
	// selinux labels the Kafka bind mounts under podman.
	selinux  bool
	denylist []string
	images   devAgentImages
}

func (e devEnv) storage() bool { return e.manifest.Storage != nil }

func (e devEnv) ports() []int {
	if e.storage() {
		return append(slices.Clone(devPorts), 18080)
	}
	return devPorts
}

func devUp(ctx context.Context, instance devInstance, runtimeFlag string, images devAgentImages, stdout, stderr io.Writer) (err error) {
	current := ""
	step := func(name string) {
		current = name
		fmt.Fprintln(stderr, name)
	}
	// A preflight refusal is the whole error.
	defer func() {
		if err != nil && current != "" && current != "preflight" {
			err = fmt.Errorf("step %s: %w", current, err)
		}
	}()

	step("preflight")
	if err := refuseAnyUp(instance); err != nil {
		return err
	}
	manifest, err := readManifest(instance)
	if err != nil {
		return err
	}
	if manifest == nil {
		return fmt.Errorf("nothing acquired for instance %s; run wherry dev init first", instance.name)
	}
	if err := verifyCache(instance, *manifest); err != nil {
		return err
	}
	if !devSources.runtimeOK(manifest.Runtime) {
		return fmt.Errorf("%s does not answer version", manifest.Runtime)
	}
	if runtimeFlag != "" && runtimeFlag != manifest.Runtime {
		return fmt.Errorf("instance %s was acquired for %s; run wherry dev init --runtime %s", instance.name, manifest.Runtime, runtimeFlag)
	}
	env := devEnv{instance: instance, manifest: *manifest, driver: "docker", uid: os.Getuid(), gid: os.Getgid(), images: images}
	if err := sameSnapshot(*manifest); err != nil {
		return err
	}
	for _, port := range env.ports() {
		if err := portFree(port); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(instance.env()); err != nil {
		return err
	}
	for _, dir := range []string{"nomad/data", "kafka/cfg", "kafka/data", "tls", "receiver/state", "host/credentials"} {
		if err := os.MkdirAll(instance.env(dir), 0o700); err != nil {
			return err
		}
	}

	step("harnesses")
	agents := devAgents(*manifest, images)

	step("credentials")
	sources := map[string]credential.Source{}
	copied := []string{}
	for _, agent := range agents {
		for _, devCredential := range devCredentials[agent] {
			for _, variable := range devCredential.from {
				value := os.Getenv(variable)
				if value == "" {
					continue
				}
				path := instance.env("host", "credentials", devCredential.name)
				if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
					return err
				}
				sources[devCredential.name] = credential.Source{File: path}
				copied = append(copied, variable)
				break
			}
		}
	}
	env.denylist = denylist(manifest.EnvDenylist, copied)

	step("tls")
	ca, err := devTLS(instance)
	if err != nil {
		return err
	}

	step("super-user")
	secret := make([]byte, 24)
	rand.Read(secret)
	superPassword := hex.EncodeToString(secret)
	if err := os.WriteFile(instance.env("kafka", "cfg", "super-user-password"), []byte(superPassword), 0o600); err != nil {
		return err
	}

	step("image")
	pulled := []devImage{manifest.Images["kafka"]}
	if manifest.Storage != nil {
		pulled = append(pulled, *manifest.Storage)
	}
	for _, image := range pulled {
		id, err := runtimeOutput(ctx, manifest.Runtime, "image", "inspect", "--format", "{{.Id}}", image.Tag)
		if err != nil || "sha256:"+strings.TrimPrefix(strings.TrimSpace(id), "sha256:") != image.ID {
			return fmt.Errorf("image %s is no longer in the runtime; run wherry dev init", image.Tag)
		}
	}

	step("nomad")
	nomad, err := devNomad()
	if err != nil {
		return err
	}
	stopWait := waiting(stderr, "nomad")
	nomadPID, podmanPID, err := startNomad(ctx, nomad, &env)
	stopWait()
	if err != nil {
		return err
	}

	step("kafka")
	stopWait = waiting(stderr, "kafka")
	kafkaAlloc, err := startKafka(ctx, nomad, env, ca, superPassword)
	stopWait()
	if err != nil {
		return err
	}

	var storageKeys *[2]string
	s3Alloc := ""
	if env.storage() {
		step("storage")
		stopWait = waiting(stderr, "storage")
		s3Alloc, err = startStorage(ctx, nomad, env)
		stopWait()
		if err != nil {
			return err
		}
		step("s3-user")
		if storageKeys, err = storageUser(ctx, nomad, s3Alloc); err != nil {
			return err
		}
		for i, name := range []string{"s3-access-key", "s3-secret-key"} {
			sources[name] = credential.Source{File: instance.env("host", "credentials", name)}
			if err := os.WriteFile(sources[name].File, []byte(storageKeys[i]), 0o600); err != nil {
				return err
			}
		}
		step("bucket")
		if err := createBucket(ctx, *storageKeys); err != nil {
			return err
		}
	}

	step("operator")
	service, err := devOperator(ctx, instance, devBroker)
	if err != nil {
		return err
	}
	defer service.Operator.Close()

	step("bootstrap")
	if err := devBootstrap(ctx, env, agents, *service); err != nil {
		return err
	}

	step("host")
	hostConfig, configPath, err := placeHost(ctx, &env, sources, stderr)
	if err != nil {
		return err
	}

	step("receiver")
	stopWait = waiting(stderr, "receiver")
	err = startReceiver(ctx, nomad, env, hostConfig)
	stopWait()
	if err != nil {
		return err
	}

	step("smoke")
	stopWait = waiting(stderr, "smoke")
	err = smoke(ctx, env, hostConfig)
	stopWait()
	if err != nil {
		return err
	}

	step("ready")
	state := devUpState{NomadPID: nomadPID, PodmanPID: podmanPID, Runtime: manifest.Runtime, KafkaAlloc: kafkaAlloc, S3Alloc: s3Alloc, Ports: env.ports(), Started: time.Now().UTC().Truncate(time.Second)}
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(instance.env("up.json"), encoded, 0o600); err != nil {
		return err
	}
	current = ""
	printReady(stdout, env, configPath)
	return nil
}

// verifyCache checks every file the manifest records against its SHA-256.
func verifyCache(instance devInstance, manifest devManifest) error {
	files := map[string]string{manifest.Snapshot.File: manifest.Snapshot.SHA256}
	for _, artefact := range []*devArtefact{manifest.Nomad, manifest.PodmanDriver} {
		if artefact != nil {
			files[artefact.File] = artefact.SHA256
		}
	}
	if manifest.Plugin != nil {
		files[manifest.Plugin.File] = manifest.Plugin.SHA256
	}
	for file, want := range files {
		if sum, err := fileSHA256(instance.cache(file)); err != nil || sum != want {
			return fmt.Errorf("%s does not match the manifest; run wherry dev init", instance.cache(file))
		}
	}
	return nil
}

// sameSnapshot refuses a running binary other than the snapshot, which the receiver job runs.
func sameSnapshot(manifest devManifest) error {
	self, err := os.Executable()
	if err == nil {
		self, err = filepath.EvalSymlinks(self)
	}
	if err != nil {
		return err
	}
	if sum, err := fileSHA256(self); err != nil || sum != manifest.Snapshot.SHA256 {
		return errors.New("the running wherry differs from the snapshot; run wherry dev init")
	}
	return nil
}

func portFree(port int) error {
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("port %d is in use", port)
	}
	return listener.Close()
}

// devAgents are the manifest's harnesses, the --agent-image names and mock: claude, codex, gemini, mock, then the
// image agents outside that list, in flag order.
func devAgents(manifest devManifest, images devAgentImages) []string {
	var agents []string
	for _, name := range []string{"claude", "codex", "gemini", "mock"} {
		if name == "mock" || slices.Contains(manifest.Agents, name) || slices.ContainsFunc(images, func(image devAgentImage) bool { return image.name == name }) {
			agents = append(agents, name)
		}
	}
	for _, image := range images {
		if !slices.Contains(agents, image.name) {
			agents = append(agents, image.name)
		}
	}
	return agents
}

// denylist is Nomad's default env.denylist, the credential-shaped names init saw and the variables up copied into
// credential files, without repeats.
func denylist(fromInit, copied []string) []string {
	names := slices.Concat(nomadDenylist, fromInit, copied)
	slices.Sort(names)
	return slices.Compact(names)
}

// waiting rewrites one terminal line with name and the seconds waited, until the returned func is called.
func waiting(stderr io.Writer, name string) func() {
	terminal, ok := stderr.(*os.File)
	if !ok || !isTerminal(terminal) {
		return func() {}
	}
	done, finished := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		start := time.Now()
		for {
			select {
			case <-done:
				fmt.Fprintln(stderr)
				return
			case <-time.After(time.Second):
				fmt.Fprintf(stderr, "\r%s %s %ds", name, `-\|/`[int(time.Since(start).Seconds())%4:][:1], int(time.Since(start).Seconds()))
			}
		}
	}()
	return func() { close(done); <-finished }
}

// devTLS writes the instance CA and the broker's certificate, returning the CA certificate's PEM.
func devTLS(instance devInstance) ([]byte, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber: randomSerial(), Subject: pkix.Name{CommonName: "wherry-dev-ca"}, NotBefore: now.Add(-time.Minute), NotAfter: now.AddDate(0, 0, 365),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	caCert, _ := x509.ParseCertificate(caDER)
	brokerKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	brokerTemplate := &x509.Certificate{
		SerialNumber: randomSerial(), Subject: pkix.Name{CommonName: "wherry-dev-broker"}, NotBefore: now.Add(-time.Minute), NotAfter: now.AddDate(0, 0, 365),
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, DNSNames: []string{"localhost"},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	brokerDER, err := x509.CreateCertificate(rand.Reader, brokerTemplate, caCert, &brokerKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	// The Kafka broker reads its TLS keys only as PEM, so these stay PEM where wherry's own key files are JWKs.
	keyPEM := func(key *ecdsa.PrivateKey) []byte {
		der, _ := x509.MarshalPKCS8PrivateKey(key)
		return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	brokerPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: brokerDER})
	files := map[string][]byte{
		"tls/ca.key": keyPEM(caKey), "tls/ca.pem": caPEM, "tls/broker.key": keyPEM(brokerKey), "tls/broker.pem": brokerPEM,
		"kafka/cfg/server.pem": append(slices.Clone(brokerPEM), keyPEM(brokerKey)...), "kafka/cfg/ca.pem": caPEM,
	}
	for path, data := range files {
		if err := os.WriteFile(instance.env(path), data, 0o600); err != nil {
			return nil, err
		}
	}
	return caPEM, nil
}

func randomSerial() *big.Int {
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	return serial
}

// renderAgentHCL is the Nomad dev agent's configuration.
func renderAgentHCL(env devEnv) string {
	var b strings.Builder
	fmt.Fprintf(&b, "bind_addr  = \"127.0.0.1\"\ndata_dir   = %q\nlog_level  = \"INFO\"\n", env.instance.env("nomad", "data"))
	if env.driver == "podman" {
		fmt.Fprintf(&b, "plugin_dir = %q\n", env.instance.cache("nomad-plugins"))
	}
	fmt.Fprintf(&b, "\nclient {\n  options {\n    \"env.denylist\" = %q\n  }\n}\n", strings.Join(env.denylist, ","))
	b.WriteString("\nplugin \"raw_exec\" {\n  config {\n    enabled = true\n  }\n}\n")
	if env.driver == "podman" {
		fmt.Fprintf(&b, "\nplugin \"nomad-driver-podman\" {\n  config {\n    socket_path = %q\n  }\n}\n", env.socket)
	} else {
		fmt.Fprintf(&b, "\nplugin \"docker\" {\n  config {\n    endpoint = %q\n    volumes {\n      enabled = true\n    }\n    gc {\n      image = false\n    }\n    extra_labels = [\"job_name\"]\n  }\n}\n", env.socket)
	}
	return b.String()
}

// renderServerProperties is the broker's server.properties, clusterID recorded as a comment.
func renderServerProperties(clusterID string) string {
	return "# cluster.id=" + clusterID + `
node.id=1
process.roles=broker,controller
controller.quorum.bootstrap.servers=127.0.0.1:9094
listeners=INTERNAL://127.0.0.1:9092,CONTROLLER://127.0.0.1:9094,SCRAM://0.0.0.0:9093
advertised.listeners=INTERNAL://127.0.0.1:9092,SCRAM://127.0.0.1:9093
listener.security.protocol.map=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,SCRAM:SASL_SSL
controller.listener.names=CONTROLLER
inter.broker.listener.name=INTERNAL
sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.scram.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
ssl.keystore.type=PEM
ssl.keystore.location=/cfg/server.pem
ssl.truststore.type=PEM
ssl.truststore.location=/cfg/ca.pem
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:wherry-admin;User:ANONYMOUS
auto.create.topics.enable=false
connections.max.reauth.ms=300000
transaction.max.timeout.ms=60000
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
default.replication.factor=1
min.insync.replicas=1
log.dirs=/data/kraft
`
}

// kafkaScript reads the super user's password as $(cat /cfg/...) from the read-only bind mount, so it never enters the
// job spec.
func kafkaScript(clusterID string) string {
	return "/opt/kafka/bin/kafka-storage.sh format --standalone -t " + clusterID + " -c /cfg/server.properties --ignore-formatted" +
		` --add-scram "SCRAM-SHA-512=[name=wherry-admin,password=$(cat /cfg/super-user-password)]"` +
		" && exec /opt/kafka/bin/kafka-server-start.sh /cfg/server.properties"
}

func jobID(instance devInstance, kind string) string { return "wherry-" + instance.name + "-" + kind }

func serviceJob(id string, group *api.TaskGroup) *api.Job {
	return &api.Job{ID: new(id), Name: new(id), Type: new(api.JobTypeService), Datacenters: []string{"*"}, TaskGroups: []*api.TaskGroup{group}}
}

// kafkaJob runs the broker from the manifest's local tag on the published port 9093.
func kafkaJob(env devEnv, clusterID string) *api.Job {
	mounts := []string{env.instance.env("kafka", "cfg") + ":/cfg:ro", env.instance.env("kafka", "data") + ":/data"}
	config := map[string]any{
		"image": env.manifest.Images["kafka"].Tag, "force_pull": false, "ports": []string{"scram"},
		"entrypoint": []string{"/bin/sh", "-c"}, "args": []string{kafkaScript(clusterID)},
	}
	task := &api.Task{
		Name: "kafka", Driver: env.driver, Env: map[string]string{"LOG_DIR": "/data/logs"},
		Resources: &api.Resources{CPU: new(1000), MemoryMB: new(1536)},
	}
	// The broker reads the user's 0600 files in /cfg, so it runs as the user; keep-id maps that uid unchanged.
	task.User = fmt.Sprintf("%d:%d", env.uid, env.gid)
	if env.driver == "podman" {
		config["userns"] = "keep-id"
		if env.selinux {
			mounts = []string{mounts[0] + ",Z", mounts[1] + ":Z"}
		}
	}
	config["volumes"] = mounts
	task.Config = config
	return serviceJob(jobID(env.instance, "kafka"), &api.TaskGroup{
		Name: new("kafka"), Count: new(1),
		Networks:      []*api.NetworkResource{{ReservedPorts: []api.Port{{Label: "scram", Value: 9093, To: 9093}}}},
		RestartPolicy: &api.RestartPolicy{Attempts: new(0), Mode: new(api.RestartPolicyModeFail)},
		Tasks:         []*api.Task{task},
	})
}

// storageJob runs ceph-aio's RGW from the manifest's local tag on the published port 18080.
func storageJob(env devEnv) *api.Job {
	return serviceJob(jobID(env.instance, "s3"), &api.TaskGroup{
		Name: new("s3"), Count: new(1),
		Networks:      []*api.NetworkResource{{ReservedPorts: []api.Port{{Label: "s3", Value: 18080, To: 8000}}}},
		RestartPolicy: &api.RestartPolicy{Attempts: new(0), Mode: new(api.RestartPolicyModeFail)},
		Tasks: []*api.Task{{
			Name: "s3", Driver: env.driver, Env: map[string]string{"ENABLE_CEPHFS": "false"},
			Config:    map[string]any{"image": env.manifest.Storage.Tag, "force_pull": false, "ports": []string{"s3"}},
			Resources: &api.Resources{CPU: new(1000), MemoryMB: new(2048)},
		}},
	})
}

// receiverJob runs the snapshot's receiver under raw_exec; it exits on a configuration change and restarts.
func receiverJob(env devEnv) *api.Job {
	return serviceJob(jobID(env.instance, "receiver"), &api.TaskGroup{
		Name: new("receiver"), Count: new(1),
		RestartPolicy: &api.RestartPolicy{Attempts: new(10), Interval: new(5 * time.Minute), Delay: new(2 * time.Second), Mode: new(api.RestartPolicyModeDelay)},
		Tasks: []*api.Task{{
			Name: "receiver", Driver: "raw_exec",
			Config: map[string]any{"command": env.instance.cache("wherry"), "args": []string{"receiver", "run", "--config-file", env.instance.env("receiver", "config.yaml")}},
		}},
	})
}

// startNomad starts the dev agent and waits for a ready node with raw_exec and the runtime's driver healthy.
func startNomad(ctx context.Context, nomad *api.Client, env *devEnv) (int, int, error) {
	podmanPID := 0
	runtimeName := env.manifest.Runtime
	switch {
	case runtimeName == "docker":
		host, err := runtimeOutput(ctx, "docker", "context", "inspect", "--format", `{{(index .Endpoints "docker").Host}}`)
		if err != nil {
			return 0, 0, err
		}
		env.socket = strings.TrimSpace(host)
	case runtime.GOOS == "darwin":
		path, err := runtimeOutput(ctx, "podman", "machine", "inspect", "--format", "{{.ConnectionInfo.PodmanSocket.Path}}")
		if err != nil {
			return 0, 0, err
		}
		env.socket = "unix://" + strings.TrimSpace(path)
	default:
		env.driver = "podman"
		_, err := os.Stat("/sys/fs/selinux/enforce")
		env.selinux = err == nil
		info, err := runtimeOutput(ctx, "podman", "info", "--format", "{{.Host.RemoteSocket.Exists}} {{.Host.RemoteSocket.Path}}")
		if err != nil {
			return 0, 0, err
		}
		exists, path, _ := strings.Cut(strings.TrimSpace(info), " ")
		env.socket = "unix://" + strings.TrimPrefix(path, "unix://")
		if exists != "true" {
			socket := env.instance.env("podman.sock")
			if podmanPID, err = detach(env.instance.env("podman.log"), env.instance.env("podman.pid"), "podman", "system", "service", "--time=0", "unix://"+socket); err != nil {
				return 0, 0, err
			}
			env.socket = "unix://" + socket
			if err := poll(ctx, 30*time.Second, func() error { _, err := os.Stat(socket); return err }); err != nil {
				return 0, podmanPID, fmt.Errorf("podman system service made no socket: %w", err)
			}
		}
	}
	config := env.instance.env("nomad", "agent.hcl")
	if err := os.WriteFile(config, []byte(renderAgentHCL(*env)), 0o600); err != nil {
		return 0, podmanPID, err
	}
	pid, err := detach(env.instance.env("nomad", "nomad.log"), env.instance.env("nomad", "pid"), env.instance.cache("nomad"), "agent", "-dev", "-config", config)
	if err != nil {
		return 0, podmanPID, err
	}
	q := (&api.QueryOptions{}).WithContext(ctx)
	err = poll(ctx, 2*time.Minute, func() error {
		if !alive(pid) {
			return errStop{fmt.Errorf("nomad exited; see %s", env.instance.env("nomad", "nomad.log"))}
		}
		if _, err := nomad.Agent().Self(); err != nil {
			return err
		}
		nodes, _, err := nomad.Nodes().List(q)
		if err != nil {
			return err
		}
		for _, stub := range nodes {
			if stub.Status != api.NodeStatusReady {
				continue
			}
			node, _, err := nomad.Nodes().Info(stub.ID, q)
			if err != nil {
				continue
			}
			rawExec, driver := node.Drivers["raw_exec"], node.Drivers[env.driver]
			if rawExec != nil && rawExec.Healthy && driver != nil && driver.Detected && driver.Healthy {
				return nil
			}
		}
		return fmt.Errorf("no ready node with raw_exec and %s healthy", env.driver)
	})
	return pid, podmanPID, err
}

// detach starts argv in its own session, its output appended to logPath, and writes its pid to pidPath.
func detach(logPath, pidPath string, argv ...string) (int, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return 0, err
	}
	defer logFile.Close()
	command := exec.Command(argv[0], argv[1:]...)
	command.Stdout, command.Stderr = logFile, logFile
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		return 0, err
	}
	pid := command.Process.Pid
	command.Process.Release()
	return pid, os.WriteFile(pidPath, []byte(strconv.Itoa(pid)), 0o600)
}

// errStop ends a poll at once.
type errStop struct{ error }

// poll calls check every second until it succeeds, returns errStop, or bound passes, returning its last error.
func poll(ctx context.Context, bound time.Duration, check func() error) error {
	deadline := time.Now().Add(bound)
	for {
		err := check()
		var stop errStop
		switch {
		case err == nil:
			return nil
		case errors.As(err, &stop):
			return stop.error
		case time.Now().After(deadline):
			return fmt.Errorf("not ready after %s: %w", bound, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// startKafka waits for an authenticated metadata request, since rootless podman's port forwarder accepts connections
// while the container is down.
func startKafka(ctx context.Context, nomad *api.Client, env devEnv, ca []byte, superPassword string) (string, error) {
	raw := make([]byte, 16)
	clusterID := "-"
	for strings.HasPrefix(clusterID, "-") {
		rand.Read(raw)
		clusterID = base64.RawURLEncoding.EncodeToString(raw)
	}
	if err := os.WriteFile(env.instance.env("kafka", "cfg", "server.properties"), []byte(renderServerProperties(clusterID)), 0o600); err != nil {
		return "", err
	}
	alloc, err := runJob(ctx, nomad, kafkaJob(env, clusterID))
	if err != nil {
		return "", err
	}
	opts, err := operator.KafkaOpts(devBroker, ca, devSuperUser, superPassword)
	if err != nil {
		return alloc, err
	}
	return alloc, poll(ctx, 3*time.Minute, func() error {
		if err := allocRunning(ctx, nomad, alloc); err != nil {
			return errStop{err}
		}
		client, err := kgo.NewClient(opts...)
		if err != nil {
			return err
		}
		defer client.Close()
		attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err = kadm.NewClient(client).BrokerMetadata(attempt)
		return err
	})
}

// runJob registers job and waits for its allocation to run, returning the allocation's ID.
func runJob(ctx context.Context, nomad *api.Client, job *api.Job) (string, error) {
	if _, _, err := nomad.Jobs().Register(job, (&api.WriteOptions{}).WithContext(ctx)); err != nil {
		return "", err
	}
	alloc := ""
	err := poll(ctx, 5*time.Minute, func() error {
		allocs, _, err := nomad.Jobs().Allocations(*job.ID, false, (&api.QueryOptions{}).WithContext(ctx))
		if err != nil {
			return err
		}
		if len(allocs) == 0 {
			return errors.New("no allocation yet")
		}
		latest := slices.MaxFunc(allocs, func(a, b *api.AllocationListStub) int { return cmp.Compare(a.CreateIndex, b.CreateIndex) })
		alloc = latest.ID
		return allocRunning(ctx, nomad, alloc)
	})
	return alloc, err
}

// allocRunning is nil for a running allocation, errStop for one that ended, naming its last task event.
func allocRunning(ctx context.Context, nomad *api.Client, id string) error {
	alloc, _, err := nomad.Allocations().Info(id, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return err
	}
	switch alloc.ClientStatus {
	case api.AllocClientStatusRunning:
		return nil
	case api.AllocClientStatusPending:
		return errors.New("allocation pending")
	}
	last := ""
	for task, state := range alloc.TaskStates {
		if len(state.Events) > 0 {
			event := state.Events[len(state.Events)-1]
			last += fmt.Sprintf("; task %s: %s %s", task, event.Type, event.DisplayMessage)
		}
	}
	return errStop{fmt.Errorf("allocation %s is %s%s", id, alloc.ClientStatus, last)}
}

// startStorage registers ceph-aio and waits for the image's health check; Nomad reports the task running long
// before RGW answers.
func startStorage(ctx context.Context, nomad *api.Client, env devEnv) (string, error) {
	alloc, err := runJob(ctx, nomad, storageJob(env))
	if err != nil {
		return "", err
	}
	return alloc, poll(ctx, 5*time.Minute, func() error {
		id, err := runtimeOutput(ctx, env.manifest.Runtime, "ps", "-q", "--filter", "label=com.hashicorp.nomad.alloc_id="+alloc)
		if err != nil || strings.TrimSpace(id) == "" {
			return fmt.Errorf("no container for allocation %s", alloc)
		}
		health, err := runtimeOutput(ctx, env.manifest.Runtime, "inspect", "--format", "{{.State.Health.Status}}", strings.Fields(id)[0])
		if err != nil {
			return err
		}
		if status := strings.TrimSpace(health); status != "healthy" {
			return fmt.Errorf("ceph-aio is %s", status)
		}
		return nil
	})
}

// storageUser creates the RGW user inside the storage task, returning the access and secret keys radosgw-admin
// generates and prints; no key travels in the exec request.
func storageUser(ctx context.Context, nomad *api.Client, alloc string) (*[2]string, error) {
	var stdout, stderr bytes.Buffer
	code, err := devExec(ctx, nomad, alloc, "s3", []string{"radosgw-admin", "user", "create", "--uid=wherry-dev", "--display-name=wherry dev"}, &stdout, &stderr)
	if err == nil && code != 0 {
		err = fmt.Errorf("exit status %d", code)
	}
	if err != nil {
		return nil, fmt.Errorf("radosgw-admin user create: %w: %s", err, bytes.TrimSpace(stderr.Bytes()))
	}
	var user struct {
		Keys []struct {
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		} `json:"keys"`
	}
	// stdout holds the keys, so the error carries none of it.
	if json.Unmarshal(stdout.Bytes(), &user) != nil || len(user.Keys) == 0 || user.Keys[0].AccessKey == "" || user.Keys[0].SecretKey == "" {
		return nil, errors.New("radosgw-admin user create printed no user with keys")
	}
	return &[2]string{user.Keys[0].AccessKey, user.Keys[0].SecretKey}, nil
}

// devExec runs command in task of allocation alloc; a variable so tests can fake the exec.
var devExec = func(ctx context.Context, nomad *api.Client, alloc, task string, command []string, stdout, stderr io.Writer) (int, error) {
	info, _, err := nomad.Allocations().Info(alloc, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return 0, err
	}
	return nomad.Allocations().Exec(ctx, info, task, false, command, strings.NewReader(""), stdout, stderr, nil, nil)
}

func createBucket(ctx context.Context, keys [2]string) error {
	store, err := minio.New(strings.TrimPrefix(devStorage, "http://"), &minio.Options{Creds: credentials.NewStaticV4(keys[0], keys[1], ""), BucketLookup: minio.BucketLookupPath})
	if err != nil {
		return err
	}
	return poll(ctx, time.Minute, func() error { return store.MakeBucket(ctx, devBucket, minio.MakeBucketOptions{}) })
}

// devTerms are pool dev's terms: each agent with its credentials, every one optional.
func devTerms(agents []string, defaultAgent string, storage bool) operator.Terms {
	optional := false
	terms := operator.Terms{DefaultAgent: defaultAgent, DeliveryLimit: 2, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 10}
	for _, agent := range agents {
		agentTerms := operator.AgentTerms{Name: agent, Credentials: []operator.AgentCredential{}}
		for _, credential := range devCredentials[agent] {
			agentTerms.Credentials = append(agentTerms.Credentials, operator.AgentCredential{
				TaskCredential: wire.TaskCredential{Name: credential.name, Env: credential.target}, Required: &optional,
			})
		}
		terms.Agents = append(terms.Agents, agentTerms)
	}
	if storage {
		terms.Storage = &operator.Storage{Endpoint: devStorage, Bucket: devBucket}
	}
	return terms
}

// devProfiles are receiver dev-1's profiles, one per agent: the harnesses and the mock under raw_exec on this
// machine, an --agent-image agent in its container.
func devProfiles(env devEnv, agents []string) []operator.ProfileSettings {
	images := env.images
	var profiles []operator.ProfileSettings
	for _, agent := range agents {
		runtimeConfig := map[string]any{"driver": "raw_exec"}
		labels := map[string]string{"isolation": "host"}
		if index := slices.IndexFunc(images, func(image devAgentImage) bool { return image.name == agent }); index >= 0 {
			config := map[string]any{"image": images[index].image, "force_pull": false}
			if command, ok := devCommands[agent]; ok {
				config["command"], config["args"] = "/bin/sh", []string{"-c", `mkdir -p "$WHERRY_OUTPUT_DIR" && cd /local && ` + command}
			}
			runtimeConfig = map[string]any{"driver": env.driver, "config": config}
			labels = map[string]string{"isolation": "container"}
		} else if agent == "mock" {
			runtimeConfig["config"] = map[string]any{"command": "/bin/sh", "args": []string{"-c", `cd "$NOMAD_TASK_DIR" && exec '` + env.instance.cache("wherry") + `' dev mock`}}
		} else {
			runtimeConfig["config"] = map[string]any{"command": "/bin/sh", "args": []string{"-c", `mkdir -p "$WHERRY_OUTPUT_DIR" && cd "$NOMAD_TASK_DIR" && ` + devCommands[agent]}}
		}
		profiles = append(profiles, operator.ProfileSettings{
			Name: agent, Plugin: "nomad", Agent: agent, Labels: labels,
			Config: map[string]any{"address": devNomadAddr, "namespace": "default", "runtime": runtimeConfig},
		})
	}
	return profiles
}

// devOperator provisions as operator init does, on the instance's operator working directory env/operator, and
// connects as wherry.operator through it.
func devOperator(ctx context.Context, instance devInstance, broker string) (*operator.Service, error) {
	dir := instance.env("operator")
	options := map[string]string{"broker": broker, "ca-file": instance.env("tls", "ca.pem"), "super-user": devSuperUser, "super-user-password-file": instance.env("kafka", "cfg", "super-user-password")}
	if err := operatorInit(ctx, dir, options, io.Discard); err != nil {
		return nil, err
	}
	return operatorService(dir)
}

// devBootstrap makes the operator's calls for the instance, its bundles in the operator working directory, and gives
// the receiver its config.yaml.
func devBootstrap(ctx context.Context, env devEnv, agents []string, service operator.Service) error {
	instance, dir := env.instance, env.instance.env("operator")
	if err := service.PoolApply(ctx, devPool, devTerms(agents, env.manifest.DefaultAgent, env.storage()), false); err != nil {
		return fmt.Errorf("pool apply: %w", err)
	}
	if err := service.TenantAdd(ctx, devTenant, devPool); err != nil {
		return fmt.Errorf("pool entitle: %w", err)
	}
	hostBundle, err := bundlePath(dir, "hosts", true, devTenant, devHost)
	if err != nil {
		return err
	}
	if err := service.HostAdd(ctx, devTenant, devHost, hostBundle, false); err != nil {
		return fmt.Errorf("host add: %w", err)
	}
	receiverBundle, err := bundlePath(dir, "receivers", true, devReceiver)
	if err != nil {
		return err
	}
	receiver := operator.ReceiverConfig{Receiver: devReceiver, Pool: devPool, Capacity: 2, Profiles: devProfiles(env, agents),
		PluginSHA256: map[string]string{"nomad": env.manifest.Plugin.SHA256}}
	if err := service.ReceiverAdd(ctx, receiver, service.Broker, receiverBundle); err != nil {
		return fmt.Errorf("receiver apply: %w", err)
	}
	local := map[string]string{"bundle": receiverBundle, "ca_file": instance.env("tls", "ca.pem"), "state_dir": instance.env("receiver", "state"), "plugin_dir": instance.cache("plugins")}
	return writeYAML(instance.env("receiver", "config.yaml"), local)
}

// devHostStep runs after up's check of the standard host paths, with "", and after each file it creates there, with its
// path; a variable so tests can race and interrupt up.
var devHostStep = func(path string) {}

// placeHost writes the host's bundle, config.yaml and key, and registers the host. The default instance, without
// WHERRY_CONFIG set, creates them at the standard host paths when nothing is there, and falls back to env/host when
// anything is or appears there; it never replaces a file. Each other instance writes them in env/host.
func placeHost(ctx context.Context, env *devEnv, sources map[string]credential.Source, stderr io.Writer) (busclient.Config, string, error) {
	instance := env.instance
	if instance.name == "default" {
		if err := removeHostFiles(instance, stderr); err != nil {
			return busclient.Config{}, "", err
		}
		env.manifest.HostFiles = nil
		if os.Getenv("WHERRY_CONFIG") == "" {
			dir, err := busclient.ConfigDir()
			if err != nil {
				return busclient.Config{}, "", err
			}
			taken := slices.ContainsFunc([]string{filepath.Join(dir, "config.yaml"), filepath.Join(dir, "bundle.yaml"), filepath.Join(dir, "host.key")}, func(path string) bool {
				_, err := os.Lstat(path)
				return !errors.Is(err, os.ErrNotExist)
			})
			devHostStep("")
			if !taken {
				host, config, err := writeHost(env, sources, dir, true)
				if err == nil {
					return registerHost(ctx, host, config)
				}
				if !errors.Is(err, fs.ErrExist) {
					return busclient.Config{}, "", err
				}
				if err := removeHostFiles(instance, stderr); err != nil {
					return busclient.Config{}, "", err
				}
				env.manifest.HostFiles = nil
			}
		}
	}
	host, config, err := writeHost(env, sources, instance.env("host"), false)
	if err != nil {
		return busclient.Config{}, "", err
	}
	return registerHost(ctx, host, config)
}

func registerHost(ctx context.Context, host busclient.Config, config string) (busclient.Config, string, error) {
	if _, _, err := busclient.Init(ctx, host); err != nil {
		return busclient.Config{}, "", fmt.Errorf("init: %w", err)
	}
	return host, config, nil
}

// writeHost creates the host's config.yaml, bundle and key in dir, each failing if anything is there; the host's state
// stays in the instance, so down never removes state at the standard paths. When record is set, the manifest records
// each path before its create, and its device, inode, size and modification time after, so that down removes only what
// up created.
func writeHost(env *devEnv, sources map[string]credential.Source, dir string, record bool) (busclient.Config, string, error) {
	instance := env.instance
	stateDir := instance.env("host", "state")
	for _, path := range []string{dir, stateDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return busclient.Config{}, "", err
		}
	}
	dir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return busclient.Config{}, "", err
	}
	source, err := bundlePath(instance.env("operator"), "hosts", false, devTenant, devHost)
	if err != nil {
		return busclient.Config{}, "", err
	}
	bundle, err := os.ReadFile(source)
	if err != nil {
		return busclient.Config{}, "", err
	}
	host := busclient.Config{
		Bundle: filepath.Join(dir, "bundle.yaml"), CAFile: instance.env("tls", "ca.pem"), HostKey: filepath.Join(dir, "host.key"), StateDir: stateDir, Credentials: sources,
	}
	if env.storage() {
		host.Storage = map[string]busclient.StorageConfig{devPool: {AccessKey: "s3-access-key", SecretKey: "s3-secret-key"}}
	}
	config, err := yaml.Marshal(hostConfigFile(host))
	if err != nil {
		return busclient.Config{}, "", err
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return busclient.Config{}, "", err
	}
	jwk, err := (&jose.JSONWebKey{Key: key}).MarshalJSON()
	if err != nil {
		return busclient.Config{}, "", err
	}
	save := func() error { return nil }
	if record {
		env.manifest.HostFiles = &devHostFiles{Files: map[string]devHostFile{}}
		save = func() error { return writeManifest(instance, env.manifest) }
	}
	recorded := env.manifest.HostFiles
	configPath := filepath.Join(dir, "config.yaml")
	for _, file := range []struct {
		path string
		data []byte
	}{{configPath, config}, {host.Bundle, bundle}, {host.HostKey, jwk}} {
		if record {
			recorded.Files[file.path] = devHostFile{}
			if err := save(); err != nil {
				return busclient.Config{}, "", err
			}
		}
		id, err := createFile(file.path, file.data)
		if err != nil {
			if record && errors.Is(err, fs.ErrExist) {
				delete(recorded.Files, file.path)
				err = errors.Join(err, save())
			}
			return busclient.Config{}, "", err
		}
		if record {
			devHostStep(file.path)
			recorded.Files[file.path] = id
			if err := save(); err != nil {
				return busclient.Config{}, "", err
			}
		}
	}
	return host, configPath, nil
}

// createFile creates path with data, mode 0600, failing if anything is there, a dangling symlink included, and returns
// the device and inode of the file it created.
func createFile(path string, data []byte) (devHostFile, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return devHostFile{}, err
	}
	defer file.Close()
	// The umask may narrow the mode OpenFile asks for.
	if err := file.Chmod(0o600); err != nil {
		return devHostFile{}, err
	}
	if _, err := file.Write(data); err != nil {
		return devHostFile{}, err
	}
	if err := file.Sync(); err != nil {
		return devHostFile{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return devHostFile{}, err
	}
	return fileID(info), file.Close()
}

// fileID is the device and inode info names, with its size and modification time.
func fileID(info os.FileInfo) devHostFile {
	stat := info.Sys().(*syscall.Stat_t)
	return devHostFile{Device: uint64(stat.Dev), Inode: stat.Ino, Size: info.Size(), ModTime: info.ModTime().UnixNano()}
}

// removeHostFiles removes what the instance's manifest records up creating at the standard host paths, then clears the
// record. It removes a file only while it is the one up created; what it leaves it names. A manifest it cannot read may
// record files, so it refuses.
func removeHostFiles(instance devInstance, stderr io.Writer) error {
	manifest, err := readManifest(instance)
	if err != nil {
		config, _ := busclient.ConfigDir()
		return fmt.Errorf("%w; it may record host files wherry dev up created: check %s, %s and %s by hand, then remove %s",
			err, filepath.Join(config, "config.yaml"), filepath.Join(config, "bundle.yaml"), filepath.Join(config, "host.key"), instance.cache("manifest.json"))
	}
	if manifest == nil || manifest.HostFiles == nil {
		return nil
	}
	recorded := manifest.HostFiles
	var failures []error
	for _, path := range slices.Sorted(maps.Keys(recorded.Files)) {
		if err := notCreated(path, recorded.Files[path]); err != nil {
			fmt.Fprintf(stderr, "left %s: %v\n", path, err)
			continue
		}
		failures = append(failures, os.Remove(path))
	}
	if err := errors.Join(failures...); err != nil {
		return err
	}
	manifest.HostFiles = nil
	return writeManifest(instance, *manifest)
}

// notCreated says why path is not the file up created as entry records, nil when it is. A crash before the device and
// inode are recorded leaves the file: its bytes alone do not show who created it.
func notCreated(path string, entry devHostFile) error {
	if real, err := filepath.EvalSymlinks(filepath.Dir(path)); err != nil || real != filepath.Dir(path) {
		return errors.New("its directory is no longer the one wherry dev up created it in")
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("it is gone")
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("it is not what wherry dev up created")
	}
	if entry.Inode == 0 {
		return errors.New("wherry dev up did not record creating it")
	}
	id := fileID(info)
	if id.Device != entry.Device || id.Inode != entry.Inode {
		return errors.New("it was replaced after wherry dev up created it")
	}
	if id != entry {
		return errors.New("it changed after wherry dev up created it")
	}
	return nil
}

// hostConfigFile is config as its YAML file holds it, with only the set fields.
func hostConfigFile(config busclient.Config) map[string]any {
	file := map[string]any{"bundle": config.Bundle, "ca_file": config.CAFile, "host_key": config.HostKey, "state_dir": config.StateDir}
	if len(config.Credentials) > 0 {
		sources := map[string]map[string]string{}
		for name, source := range config.Credentials {
			sources[name] = map[string]string{"file": source.File}
		}
		file["credentials"] = sources
	}
	if len(config.Storage) > 0 {
		storage := map[string]map[string]string{}
		for pool, keys := range config.Storage {
			storage[pool] = map[string]string{"access_key": keys.AccessKey, "secret_key": keys.SecretKey}
		}
		file["storage"] = storage
	}
	return file
}

func writeYAML(path string, value any) error {
	encoded, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0o600)
}

// startReceiver registers the receiver job and waits until dev-1 is live with every profile ready.
func startReceiver(ctx context.Context, nomad *api.Client, env devEnv, hostConfig busclient.Config) error {
	if _, err := runJob(ctx, nomad, receiverJob(env)); err != nil {
		return err
	}
	host, err := busclient.Connect(hostConfig, log.New(io.Discard, "", 0))
	if err != nil {
		return err
	}
	defer host.Close()
	return poll(ctx, 3*time.Minute, func() error {
		receivers, err := host.Receivers(ctx)
		if err != nil {
			return err
		}
		for _, receiver := range receivers {
			if receiver.ReceiverID != devReceiver {
				continue
			}
			if receiver.Status != "live" {
				return fmt.Errorf("receiver %s is %s", devReceiver, receiver.Status)
			}
			for _, profile := range receiver.Profiles {
				if !profile.Ready {
					return fmt.Errorf("profile %s is not ready", profile.Name)
				}
			}
			return nil
		}
		return fmt.Errorf("receiver %s is not listed", devReceiver)
	})
}

// smoke runs the mock once, through the bucket with storage, and checks that echo.txt holds the prompt.
func smoke(ctx context.Context, env devEnv, hostConfig busclient.Config) error {
	host, err := busclient.Connect(hostConfig, log.New(io.Discard, "", 0))
	if err != nil {
		return err
	}
	defer host.Close()
	const prompt = "mock: smoke"
	input := busclient.SubmitInput{
		Agent: "mock", Prompt: prompt, Outputs: []wire.ExpectedOutput{{Name: "echo.txt", Required: true}},
		StartBy: time.Now().Add(busclient.DefaultStartBy), EndBy: time.Now().Add(busclient.DefaultEndBy),
	}
	if env.storage() {
		input.S3Inputs = []busclient.S3File{{Name: devSmokeInput, Body: strings.NewReader("smoke input\n")}}
		input.Outputs[0].Storage = wire.StorageS3
	}
	// The receiver publishes the pool's first custodian snapshot shortly after it starts.
	var submitted busclient.SubmitResult
	for deadline := time.Now().Add(time.Minute); ; {
		submitted, err = host.Submit(ctx, input)
		if !errors.Is(err, busclient.ErrNoSnapshot) && !errors.Is(err, busclient.ErrCustodianUnavailable) || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if err != nil {
		return err
	}
	result, err := host.Await(ctx, submitted.RunID, 3*time.Minute)
	if err != nil {
		return fmt.Errorf("run %s: %w", submitted.RunID, err)
	}
	if result.Status.State != wire.TaskStateCompleted {
		return fmt.Errorf("run %s ended %s", submitted.RunID, result.Status.State)
	}
	dir := env.instance.env("host", "smoke")
	if _, err := host.Collect(ctx, submitted.RunID, result, dir); err != nil {
		return fmt.Errorf("run %s: %w", submitted.RunID, err)
	}
	echoed, err := os.ReadFile(filepath.Join(dir, "echo.txt"))
	if err != nil || string(echoed) != prompt {
		return fmt.Errorf("run %s returned echo.txt %q, %v; want the prompt", submitted.RunID, echoed, err)
	}
	if env.storage() {
		message := ""
		if result.Status.Message != nil {
			for _, part := range result.Status.Message.Parts {
				message += part.Text
			}
		}
		if !strings.Contains(message, "input "+devSmokeInput+" ") {
			return fmt.Errorf("run %s: the mock saw no S3 input %s", submitted.RunID, devSmokeInput)
		}
	}
	return nil
}

// printReady names config in the commands it prints unless up wrote it at the standard path, where every host command
// reads it by default.
func printReady(stdout io.Writer, env devEnv, config string) {
	m := env.manifest
	fmt.Fprintf(stdout, "ready: instance %s (nomad %s, kafka %s, plugin %s, wherry %s)\n",
		env.instance.name, m.Nomad.Version, strings.TrimPrefix(m.Images["kafka"].Digest, "sha256:")[:12], m.Plugin.Tag, m.Snapshot.Version)
	fmt.Fprintf(stdout, "config: %s\n", config)
	variable := ""
	if m.HostFiles == nil {
		variable = "--env WHERRY_CONFIG=" + shellQuote(config) + " "
		fmt.Fprintf(stdout, "export WHERRY_CONFIG=%s\n", shellQuote(config))
	}
	fmt.Fprintln(stdout, `submit: wherry submit --prompt "..."`)
	for _, harness := range m.Agents {
		switch harness {
		case "claude", "codex":
			fmt.Fprintf(stdout, "%-7s %s mcp add wherry-%s %s-- wherry host mcp\n", harness+":", harness, env.instance.name, variable)
		case "gemini":
			fmt.Fprintf(stdout, "%-7s gemini mcp add %swherry-%s wherry host mcp\n", "gemini:", variable, env.instance.name)
		}
	}
}

// devNomad is a client of the dev agent's HTTP API, which has ACLs disabled.
func devNomad() (*api.Client, error) {
	return api.NewClient(&api.Config{Address: devNomadAddr})
}

// devDown tears the environment down; the pid file finds the Nomad agent of an up that failed before up.json.
func devDown(ctx context.Context, instance devInstance, stdout, stderr io.Writer) error {
	manifest, _ := readManifest(instance)
	state := upState(instance)
	if state == nil {
		state = leftover(instance)
	}
	if state == nil {
		fmt.Fprintf(stdout, "instance %s is not up\n", instance.name)
		return errors.Join(removeHostFiles(instance, stderr), os.RemoveAll(instance.env()))
	}
	if state.Runtime == "" && manifest != nil {
		state.Runtime = manifest.Runtime
	}
	var failures []error
	fail := func(step string, err error) {
		if err != nil {
			failures = append(failures, fmt.Errorf("step %s: %w", step, err))
		}
	}
	fmt.Fprintln(stderr, "jobs")
	var allocs []string
	nomad, err := devNomad()
	if err == nil {
		allocs, err = stopJobs(ctx, nomad, instance)
	}
	fail("jobs", err)
	for _, alloc := range []string{state.KafkaAlloc, state.S3Alloc} {
		if alloc != "" && !slices.Contains(allocs, alloc) {
			allocs = append(allocs, alloc)
		}
	}
	fmt.Fprintln(stderr, "containers")
	if state.Runtime != "" {
		forced, err := waitContainers(ctx, state.Runtime, allocs, time.Minute, time.Second)
		if len(forced) > 0 {
			fmt.Fprintf(stderr, "containers: removed %s by force after %s\n", strings.Join(forced, ", "), time.Minute)
		}
		fail("containers", err)
	}
	fmt.Fprintln(stderr, "nomad")
	for _, pid := range []int{state.NomadPID, state.PodmanPID} {
		fail("nomad", stopProcess(pid, 30*time.Second))
	}
	fmt.Fprintln(stderr, "ports")
	for _, port := range append(slices.Clone(devPorts), 18080) {
		fail("ports", portFree(port))
	}
	fmt.Fprintln(stderr, "data")
	fail("data", os.RemoveAll(instance.env()))
	fail("host", removeHostFiles(instance, stderr))
	fmt.Fprintf(stdout, "down: instance %s\n", instance.name)
	if manifest != nil {
		for _, harness := range manifest.Agents {
			fmt.Fprintf(stdout, "%s mcp remove wherry-%s\n", harness, instance.name)
		}
	}
	return errors.Join(failures...)
}

// leftover is the state of an environment whose up failed after starting Nomad, nil when its Nomad is not running.
func leftover(instance devInstance) *devUpState {
	pid := func(path string) int {
		data, _ := os.ReadFile(path)
		pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
		return pid
	}
	state := &devUpState{NomadPID: pid(instance.env("nomad", "pid")), PodmanPID: pid(instance.env("podman.pid"))}
	if !alive(state.NomadPID) {
		if alive(state.PodmanPID) {
			return &devUpState{PodmanPID: state.PodmanPID}
		}
		return nil
	}
	return state
}

// stopJobs purges the receiver, every agent job, storage and Kafka, in that order, returning every allocation it saw.
func stopJobs(ctx context.Context, nomad *api.Client, instance devInstance) ([]string, error) {
	q, w := (&api.QueryOptions{}).WithContext(ctx), (&api.WriteOptions{}).WithContext(ctx)
	jobs, _, err := nomad.Jobs().ListOptions(&api.JobListOptions{Fields: &api.JobListFields{Meta: true}}, q)
	if err != nil {
		return nil, err
	}
	order := []string{jobID(instance, "receiver")}
	for _, job := range jobs {
		if _, ok := job.Meta["run_id"]; ok {
			order = append(order, job.ID)
		}
	}
	order = append(order, jobID(instance, "s3"), jobID(instance, "kafka"))
	var seen []string
	var failures []error
	for _, id := range order {
		if !slices.ContainsFunc(jobs, func(job *api.JobListStub) bool { return job.ID == id }) {
			continue
		}
		allocs, _, err := nomad.Jobs().Allocations(id, false, q)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		// A purge drops the allocations from the server at once, before the client has stopped their tasks, so the
		// purge waits for them to end.
		if _, _, err := nomad.Jobs().Deregister(id, false, w); err != nil {
			failures = append(failures, err)
			continue
		}
		for _, alloc := range allocs {
			seen = append(seen, alloc.ID)
			failures = append(failures, poll(ctx, time.Minute, func() error {
				current, _, err := nomad.Allocations().Info(alloc.ID, q)
				switch {
				case notFound(err):
					return nil
				case err != nil:
					return err
				case current.ClientTerminalStatus():
					return nil
				}
				return fmt.Errorf("job %s allocation %s is %s", id, alloc.ID, current.ClientStatus)
			}))
		}
		_, _, err = nomad.Jobs().Deregister(id, true, w)
		failures = append(failures, err)
	}
	return seen, errors.Join(failures...)
}

// notFound reports whether err is a 404 from the Nomad API.
func notFound(err error) bool {
	var response api.UnexpectedResponseError
	return errors.As(err, &response) && response.StatusCode() == http.StatusNotFound
}

// devContainers lists the runtime's containers of an allocation and removes containers; a variable so tests can fake
// the runtime.
var devContainers = struct {
	list   func(ctx context.Context, runtime, alloc string) ([]string, error)
	remove func(ctx context.Context, runtime string, ids []string) error
}{
	list: func(ctx context.Context, runtime, alloc string) ([]string, error) {
		// A label filter matches the whole value.
		output, err := runtimeOutput(ctx, runtime, "ps", "-a", "-q", "--filter", "label=com.hashicorp.nomad.alloc_id="+alloc)
		return strings.Fields(output), err
	},
	remove: func(ctx context.Context, runtime string, ids []string) error {
		_, err := runtimeOutput(ctx, runtime, append([]string{"rm", "-f"}, ids...)...)
		return err
	},
}

// waitContainers waits until the runtime lists no container of allocs; past bound it removes those left by ID and
// returns them.
func waitContainers(ctx context.Context, runtime string, allocs []string, bound, interval time.Duration) ([]string, error) {
	deadline := time.Now().Add(bound)
	for {
		var left []string
		for _, alloc := range allocs {
			ids, err := devContainers.list(ctx, runtime, alloc)
			if err != nil {
				return nil, err
			}
			left = append(left, ids...)
		}
		if len(left) == 0 {
			return nil, nil
		}
		if time.Now().After(deadline) {
			return left, devContainers.remove(ctx, runtime, left)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

// stopProcess sends SIGTERM to pid and, when it outlives bound, SIGKILL.
func stopProcess(pid int, bound time.Duration) error {
	if !alive(pid) {
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return err
	}
	deadline := time.Now().Add(bound)
	for alive(pid) {
		if time.Now().After(deadline) {
			syscall.Kill(pid, syscall.SIGKILL)
			time.Sleep(time.Second)
			if alive(pid) {
				return fmt.Errorf("process %d survived SIGKILL", pid)
			}
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}
