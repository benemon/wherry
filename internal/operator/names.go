package operator

import (
	"errors"

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
