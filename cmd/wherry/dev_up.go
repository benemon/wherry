package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ecdsa"
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
	"log"
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

	"github.com/benemon/wherry/internal/admin"
	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/wire"
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
	devAdminUser  = "wherry-admin"
	devSmokeInput = "smoke-input.txt"
)

// devPorts are the fixed ports of an instance, all on 127.0.0.1; storage adds 18080.
var devPorts = []int{4646, 4647, 4648, 9093}

// nomadDenylist is Nomad 2.0.7's default env.denylist, which setting the option replaces.
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
	for _, dir := range []string{"nomad/data", "kafka/cfg", "kafka/data", "tls", "admin", "receiver/state", "host/state", "host/credentials"} {
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

	step("passwords")
	passwords := map[string]string{}
	for _, user := range []string{"admin", "operator"} {
		secret := make([]byte, 24)
		rand.Read(secret)
		passwords[user] = hex.EncodeToString(secret)
		for _, dir := range []string{"kafka/cfg", "admin"} {
			if err := os.WriteFile(instance.env(dir, user+"-password"), []byte(passwords[user]), 0o600); err != nil {
				return err
			}
		}
	}
	if err := os.WriteFile(instance.env("admin", "admin.env"), []byte(devAdminEnv(instance)), 0o600); err != nil {
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
	kafkaAlloc, err := startKafka(ctx, nomad, env, ca, passwords["admin"])
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

	step("operator-acl")
	root, operator, err := devAdminClients(ca, passwords)
	if err != nil {
		return err
	}
	defer root.Close()
	defer operator.Close()
	results, err := kadm.NewClient(root).CreateACLs(ctx, operatorACL())
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Err != nil {
			return result.Err
		}
	}

	step("bootstrap")
	hostConfig, err := devBootstrap(ctx, env, agents, admin.Service{Admin: root, Operator: operator, Broker: devBroker, CA: string(ca)}, sources)
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
	printReady(stdout, env)
	return nil
}

// operatorACL is the operator's ACL on wherry.admin, which the cluster's preparer grants before wherry admin runs.
func operatorACL() *kadm.ACLBuilder {
	return kadm.NewACLs().Allow("User:"+admin.OperatorPrincipal).AllowHosts("*").Topics(admin.AdminTopic).
		Operations(kadm.OpRead, kadm.OpWrite, kadm.OpDescribe).ResourcePatternType(kadm.ACLPatternLiteral)
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
		"kafka/cfg/server.pem": append(slices.Clone(brokerPEM), keyPEM(brokerKey)...), "kafka/cfg/ca.pem": caPEM, "admin/ca.pem": caPEM,
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

// devAdminEnv exports the trust broker settings wherry admin reads.
func devAdminEnv(instance devInstance) string {
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'" }
	return "export WHERRY_TRUST_BROKER=" + devBroker + "\n" +
		"export WHERRY_TRUST_CA=" + quote(instance.env("admin", "ca.pem")) + "\n" +
		"export WHERRY_TRUST_ADMIN_USER=" + devAdminUser + "\n" +
		"export WHERRY_TRUST_ADMIN_PASSWORD_FILE=" + quote(instance.env("admin", "admin-password")) + "\n" +
		"export WHERRY_TRUST_OPERATOR_PASSWORD_FILE=" + quote(instance.env("admin", "operator-password")) + "\n"
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

// kafkaScript reads the passwords as $(cat /cfg/...) from the read-only bind mount, so they never enter the job spec.
func kafkaScript(clusterID string) string {
	return "/opt/kafka/bin/kafka-storage.sh format --standalone -t " + clusterID + " -c /cfg/server.properties --ignore-formatted" +
		` --add-scram "SCRAM-SHA-512=[name=wherry-admin,password=$(cat /cfg/admin-password)]"` +
		` --add-scram "SCRAM-SHA-512=[name=wherry.operator,password=$(cat /cfg/operator-password)]"` +
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
func startKafka(ctx context.Context, nomad *api.Client, env devEnv, ca []byte, adminPassword string) (string, error) {
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
	opts, err := admin.KafkaOpts(devBroker, ca, devAdminUser, adminPassword)
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

// devAdminClients connect to the broker as wherry-admin and as the operator.
func devAdminClients(ca []byte, passwords map[string]string) (*kgo.Client, *kgo.Client, error) {
	var clients []*kgo.Client
	for _, user := range [][2]string{{devAdminUser, passwords["admin"]}, {admin.OperatorPrincipal, passwords["operator"]}} {
		opts, err := admin.KafkaOpts(devBroker, ca, user[0], user[1])
		if err != nil {
			return nil, nil, err
		}
		client, err := kgo.NewClient(append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
		if err != nil {
			return nil, nil, err
		}
		clients = append(clients, client)
	}
	return clients[0], clients[1], nil
}

// devTerms are pool dev's terms: each agent with its credentials, every one optional.
func devTerms(agents []string, defaultAgent string, storage bool) admin.Terms {
	optional := false
	terms := admin.Terms{DefaultAgent: defaultAgent, DeliveryLimit: 2, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 10}
	for _, agent := range agents {
		agentTerms := admin.AgentTerms{Name: agent, Credentials: []admin.AgentCredential{}}
		for _, credential := range devCredentials[agent] {
			agentTerms.Credentials = append(agentTerms.Credentials, admin.AgentCredential{
				TaskCredential: wire.TaskCredential{Name: credential.name, Env: credential.target}, Required: &optional,
			})
		}
		terms.Agents = append(terms.Agents, agentTerms)
	}
	if storage {
		terms.Storage = &admin.Storage{Endpoint: devStorage, Bucket: devBucket}
	}
	return terms
}

// devProfiles are receiver dev-1's profiles, one per agent: the harnesses and the mock under raw_exec on this
// machine, an --agent-image agent in its container.
func devProfiles(env devEnv, agents []string) []admin.ProfileSettings {
	images := env.images
	var profiles []admin.ProfileSettings
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
		profiles = append(profiles, admin.ProfileSettings{
			Name: agent, Plugin: "nomad", Agent: agent, Labels: labels,
			Config: map[string]any{"address": devNomadAddr, "namespace": "default", "runtime": runtimeConfig},
		})
	}
	return profiles
}

// devBootstrap makes the operator's admin calls for the instance and registers its host.
func devBootstrap(ctx context.Context, env devEnv, agents []string, service admin.Service, sources map[string]credential.Source) (busclient.Config, error) {
	instance := env.instance
	if err := service.PoolCreate(ctx, devPool, devTerms(agents, env.manifest.DefaultAgent, env.storage())); err != nil {
		return busclient.Config{}, fmt.Errorf("pool create: %w", err)
	}
	if err := service.TenantAdd(ctx, devTenant, devPool); err != nil {
		return busclient.Config{}, fmt.Errorf("pool entitle: %w", err)
	}
	if err := service.HostAdd(ctx, devTenant, devHost, instance.env("host", "dev.bundle")); err != nil {
		return busclient.Config{}, fmt.Errorf("host add: %w", err)
	}
	receiver := admin.ReceiverConfig{Receiver: devReceiver, Pool: devPool, Capacity: 2, Profiles: devProfiles(env, agents),
		PluginSHA256: map[string]string{"nomad": env.manifest.Plugin.SHA256}}
	if err := service.ReceiverAdd(ctx, receiver, instance.env("receiver", "dev-1.bundle")); err != nil {
		return busclient.Config{}, fmt.Errorf("receiver add: %w", err)
	}
	host := busclient.Config{
		Bundle: instance.env("host", "dev.bundle"), HostKey: instance.env("host", "host.key"), StateDir: instance.env("host", "state"), Credentials: sources,
	}
	if env.storage() {
		host.Storage = &busclient.StorageConfig{AccessKey: "s3-access-key", SecretKey: "s3-secret-key"}
	}
	if err := writeYAML(instance.env("host", "config.yaml"), hostConfigFile(host)); err != nil {
		return busclient.Config{}, err
	}
	local := map[string]string{"bundle": instance.env("receiver", "dev-1.bundle"), "state_dir": instance.env("receiver", "state"), "plugin_dir": instance.cache("plugins")}
	if err := writeYAML(instance.env("receiver", "config.yaml"), local); err != nil {
		return busclient.Config{}, err
	}
	if _, err := busclient.Init(ctx, host); err != nil {
		return busclient.Config{}, fmt.Errorf("init: %w", err)
	}
	return host, nil
}

// hostConfigFile is config as its YAML file holds it, with only the set fields.
func hostConfigFile(config busclient.Config) map[string]any {
	file := map[string]any{"bundle": config.Bundle, "host_key": config.HostKey, "state_dir": config.StateDir}
	if len(config.Credentials) > 0 {
		sources := map[string]map[string]string{}
		for name, source := range config.Credentials {
			sources[name] = map[string]string{"file": source.File}
		}
		file["credentials"] = sources
	}
	if config.Storage != nil {
		file["storage"] = map[string]string{"access_key": config.Storage.AccessKey, "secret_key": config.Storage.SecretKey}
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
	submitted, err := host.Submit(ctx, input)
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

func printReady(stdout io.Writer, env devEnv) {
	m := env.manifest
	config := env.instance.env("host", "config.yaml")
	binary := env.instance.cache("wherry")
	fmt.Fprintf(stdout, "ready: instance %s (nomad %s, kafka %s, plugin %s, wherry %s)\n",
		env.instance.name, m.Nomad.Version, strings.TrimPrefix(m.Images["kafka"].Digest, "sha256:")[:12], m.Plugin.Tag, m.Snapshot.Version)
	fmt.Fprintf(stdout, "config: %s\n", config)
	fmt.Fprintf(stdout, "submit: wherry submit --config-file %s --prompt \"...\"\n", config)
	for _, harness := range m.Agents {
		switch harness {
		case "claude", "codex":
			fmt.Fprintf(stdout, "%-7s %s mcp add wherry-%s -- %s mcp --config-file %s\n", harness+":", harness, env.instance.name, binary, config)
		case "gemini":
			fmt.Fprintf(stdout, "%-7s gemini mcp add wherry-%s %s mcp --config-file %s\n", "gemini:", env.instance.name, binary, config)
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
		return os.RemoveAll(instance.env())
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
