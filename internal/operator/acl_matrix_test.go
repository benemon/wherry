package operator

import (
	"bytes"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ACLs and OperatorACLs grant exactly this matrix over two pools, pool-b suspended, two tenants, two live hosts, one
// ended host, two receivers of pool-a and one of pool-b: a principal, a resource type, a resource (a prefix with *) and
// its operations.
func TestACLMatrix(t *testing.T) {
	terms := modelTerms()
	records := []Record{{Version: 1, Kind: "tenant", Tenant: "tenant-a", AtMS: 1}, {Version: 1, Kind: "tenant", Tenant: "tenant-b", AtMS: 1},
		{Version: 1, Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}, {Version: 1, Kind: "pool", Pool: "pool-b", Terms: &terms, AtMS: 1},
		{Version: 1, Kind: "host", Tenant: "tenant-a", Host: "host-a", AtMS: 1}, {Version: 1, Kind: "host", Tenant: "tenant-b", Host: "host-b", AtMS: 1},
		{Version: 1, Kind: "host", Tenant: "tenant-a", Host: "host-c", AtMS: 1},
		{Version: 1, Kind: "entitlement", Tenant: "tenant-a", Pool: "pool-a", AtMS: 1}, {Version: 1, Kind: "entitlement", Tenant: "tenant-a", Pool: "pool-b", AtMS: 1},
		{Version: 1, Kind: "entitlement", Tenant: "tenant-b", Pool: "pool-a", AtMS: 1},
	}
	for _, config := range []ReceiverConfig{modelConfig("receiver-a", "pool-a"), modelConfig("receiver-b", "pool-a"), modelConfig("receiver-c", "pool-b")} {
		copy := config
		records = append(records, Record{Version: 1, Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &copy, AtMS: 1})
	}
	records = append(records, Record{Version: 1, Kind: "end", Tenant: "tenant-a", Host: "host-c", AtMS: 1}, Record{Version: 1, Kind: "suspend", Pool: "pool-b", AtMS: 1})
	state, err := Fold(records)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, acl := range append(ACLs(state), OperatorACLs...) {
		resource := acl.Resource
		if acl.Prefix {
			resource += "*"
		}
		got = append(got, strings.Join([]string{acl.Principal, acl.ResourceType, resource, strings.Join(acl.Operations, ",")}, " "))
	}
	want := []string{
		"wherry.operator cluster kafka-cluster ALTER,ALTER_CONFIGS",
		"wherry.operator group p-* ALTER_CONFIGS",
		"wherry.operator topic wherry.* CREATE,ALTER_CONFIGS,READ,WRITE",
		"wherry.rcv.receiver-a group p-pool-a READ,DESCRIBE,DESCRIBE_CONFIGS",
		"wherry.rcv.receiver-a group p-pool-a.custodian READ,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.host.tenant-a.host-a READ,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.host.tenant-a.host-c READ,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.host.tenant-b.host-b READ,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.operator READ,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.pool.pool-a.custodian READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.pool.pool-a.events READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.pool.pool-a.receivers READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.pool.pool-a.tasks.* READ,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.receiver.receiver-a READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.receiver.receiver-a.progress READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.receiver.receiver-b READ,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.receiver.receiver-b.progress READ,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.tenant.tenant-a WRITE,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.tenant.tenant-a.transcript WRITE,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.tenant.tenant-b WRITE,DESCRIBE",
		"wherry.rcv.receiver-a topic wherry.tenant.tenant-b.transcript WRITE,DESCRIBE",
		"wherry.rcv.receiver-a transactional p.pool-a.* WRITE,DESCRIBE",
		"wherry.rcv.receiver-b group p-pool-a READ,DESCRIBE,DESCRIBE_CONFIGS",
		"wherry.rcv.receiver-b group p-pool-a.custodian READ,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.host.tenant-a.host-a READ,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.host.tenant-a.host-c READ,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.host.tenant-b.host-b READ,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.operator READ,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.pool.pool-a.custodian READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.pool.pool-a.events READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.pool.pool-a.receivers READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.pool.pool-a.tasks.* READ,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.receiver.receiver-a READ,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.receiver.receiver-a.progress READ,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.receiver.receiver-b READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.receiver.receiver-b.progress READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.tenant.tenant-a WRITE,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.tenant.tenant-a.transcript WRITE,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.tenant.tenant-b WRITE,DESCRIBE",
		"wherry.rcv.receiver-b topic wherry.tenant.tenant-b.transcript WRITE,DESCRIBE",
		"wherry.rcv.receiver-b transactional p.pool-a.* WRITE,DESCRIBE",
		"wherry.rcv.receiver-c group p-pool-b READ,DESCRIBE,DESCRIBE_CONFIGS",
		"wherry.rcv.receiver-c group p-pool-b.custodian READ,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.host.tenant-a.host-a READ,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.host.tenant-a.host-c READ,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.operator READ,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.pool.pool-b.custodian READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.pool.pool-b.events READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.pool.pool-b.receivers READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.pool.pool-b.tasks.* READ,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.receiver.receiver-c READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.receiver.receiver-c.progress READ,WRITE,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.tenant.tenant-a WRITE,DESCRIBE",
		"wherry.rcv.receiver-c topic wherry.tenant.tenant-a.transcript WRITE,DESCRIBE",
		"wherry.rcv.receiver-c transactional p.pool-b.* WRITE,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.host.tenant-a.host-a READ,WRITE,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.operator READ,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.pool.pool-a.custodian READ,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.pool.pool-a.events WRITE,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.pool.pool-a.receivers READ,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.pool.pool-a.tasks.* WRITE,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.pool.pool-b.custodian READ,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.pool.pool-b.events WRITE,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.pool.pool-b.receivers READ,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.pool.pool-b.tasks.* DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.receiver.receiver-a READ,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.receiver.receiver-b READ,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.receiver.receiver-c READ,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.tenant.tenant-a READ,DESCRIBE",
		"wherry.tnt.tenant-a.host-a topic wherry.tenant.tenant-a.transcript READ,DESCRIBE",
		"wherry.tnt.tenant-b.host-b topic wherry.host.tenant-b.host-b READ,WRITE,DESCRIBE",
		"wherry.tnt.tenant-b.host-b topic wherry.operator READ,DESCRIBE",
		"wherry.tnt.tenant-b.host-b topic wherry.pool.pool-a.custodian READ,DESCRIBE",
		"wherry.tnt.tenant-b.host-b topic wherry.pool.pool-a.events WRITE,DESCRIBE",
		"wherry.tnt.tenant-b.host-b topic wherry.pool.pool-a.receivers READ,DESCRIBE",
		"wherry.tnt.tenant-b.host-b topic wherry.pool.pool-a.tasks.* WRITE,DESCRIBE",
		"wherry.tnt.tenant-b.host-b topic wherry.receiver.receiver-a READ,DESCRIBE",
		"wherry.tnt.tenant-b.host-b topic wherry.receiver.receiver-b READ,DESCRIBE",
		"wherry.tnt.tenant-b.host-b topic wherry.tenant.tenant-b READ,DESCRIBE",
		"wherry.tnt.tenant-b.host-b topic wherry.tenant.tenant-b.transcript READ,DESCRIBE",
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		for _, line := range got {
			if !slices.Contains(want, line) {
				t.Errorf("granted, not in the matrix: %s", line)
			}
		}
		for _, line := range want {
			if !slices.Contains(got, line) {
				t.Errorf("in the matrix, not granted: %s", line)
			}
		}
		t.Fatalf("%d grants, want %d", len(got), len(want))
	}
}

// operations-operator.md's STRIMZI_IGNORED_USERS_PATTERN matches every principal wherry creates and no other user.
func TestStrimziIgnoredUsersPatternCoversThePrincipals(t *testing.T) {
	page, err := os.ReadFile("../../docs/operations-operator.md")
	if err != nil {
		t.Fatal(err)
	}
	_, after, _ := bytes.Cut(page, []byte("STRIMZI_IGNORED_USERS_PATTERN"))
	block := regexp.MustCompile("(?m)^```[a-z]*\n(.+)\n```$").FindSubmatch(after)
	if block == nil {
		t.Fatal("docs/operations-operator.md has no code block after STRIMZI_IGNORED_USERS_PATTERN")
	}
	pattern, err := regexp.Compile(string(block[1]))
	if err != nil {
		t.Fatalf("STRIMZI_IGNORED_USERS_PATTERN %s: %v", block[1], err)
	}
	for _, principal := range []string{OperatorPrincipal, TenantPrincipal("tenant-a", "host-a"), TenantPrincipal("t", "h"), ReceiverPrincipal("receiver-a"), ReceiverPrincipal("r")} {
		if !pattern.MatchString(principal) {
			t.Errorf("STRIMZI_IGNORED_USERS_PATTERN %s does not match %s", pattern, principal)
		}
	}
	for _, user := range []string{"admin", "kafka-super-user", "wherry", "wherry.operator2", "my-wherry.rcv.r", "wherry.tnt.", "wherry.rcv."} {
		if pattern.MatchString(user) {
			t.Errorf("STRIMZI_IGNORED_USERS_PATTERN %s matches %s", pattern, user)
		}
	}
}

// Clients built from KafkaOpts send the broker no client metrics (KIP-714).
func TestKafkaOptsDisableClientMetrics(t *testing.T) {
	opts, err := KafkaOpts("127.0.0.1:1", nil, "user", "password")
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if disabled, _ := client.OptValue(kgo.DisableClientMetrics).(bool); !disabled {
		t.Fatal("KafkaOpts leaves client metrics enabled")
	}
}
