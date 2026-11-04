package operator

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"

	"github.com/benemon/wherry/internal/wire"
)

const OperatorTopic = "wherry.operator"
const OperatorPrincipal = "wherry.operator"

func TenantPrincipal(tenant, host string) string { return "wherry.tnt." + tenant + "." + host }
func ReceiverPrincipal(receiver string) string   { return "wherry.rcv." + receiver }
func ReceiverTopic(receiver string) string       { return "wherry.receiver." + receiver }
func ReceiverProgressTopic(receiver string) string {
	return "wherry.receiver." + receiver + ".progress"
}
func PoolTasksTopic(pool string) string          { return "wherry.pool." + pool + ".tasks" }
func PoolTasksPrefix(pool string) string         { return "wherry.pool." + pool + ".tasks." }
func PoolEventsTopic(pool string) string         { return "wherry.pool." + pool + ".events" }
func CustodianTopic(pool string) string          { return "wherry.pool." + pool + ".custodian" }
func PoolReceiversTopic(pool string) string      { return "wherry.pool." + pool + ".receivers" }
func CustodianGroup(pool string) string          { return "p-" + pool + ".custodian" }
func TenantTopic(tenant string) string           { return "wherry.tenant." + tenant }
func TenantTranscriptTopic(tenant string) string { return "wherry.tenant." + tenant + ".transcript" }
func HostTopic(tenant, host string) string       { return "wherry.host." + tenant + "." + host }
func PoolGroup(pool string) string               { return "p-" + pool }
func TransactionalPrefix(pool string) string     { return "p." + pool + "." }

func validateNames(values ...string) error {
	for _, value := range values {
		if !wire.ValidName(value) {
			return errors.New("invalid name")
		}
	}
	return nil
}

// Signature is the canonical encoding of a profile's signature under terms: the agent, then key=value for each key of
// terms' routable labels, sorted, that labels carries, each element followed by a line feed. Neither = nor a line feed
// is in a label key or value.
func Signature(terms Terms, agent string, labels map[string]string) string {
	var encoding strings.Builder
	encoding.WriteString(agent + "\n")
	for _, key := range slices.Sorted(slices.Values(terms.Routable)) {
		if value, ok := labels[key]; ok {
			encoding.WriteString(key + "=" + value + "\n")
		}
	}
	return encoding.String()
}

// SignatureTopic is the task topic of a profile's signature in pool: the agent in clear and the SHA-256 of the
// signature's encoding in lowercase hex.
func SignatureTopic(pool string, terms Terms, agent string, labels map[string]string) string {
	sum := sha256.Sum256([]byte(Signature(terms, agent, labels)))
	return PoolTasksPrefix(pool) + agent + "." + hex.EncodeToString(sum[:])
}
