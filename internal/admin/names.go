package admin

import (
	"errors"
	"regexp"
)

const AdminTopic = "wherry.admin"
const ReceiversTopic = "wherry.receivers"

var namePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?$`)

func ValidName(name string) bool { return namePattern.MatchString(name) }

func TenantPrincipal(tenant, host string) string { return "wherry.tnt." + tenant + "." + host }
func ReceiverPrincipal(receiver string) string   { return "wherry.rcv." + receiver }
func ReceiverTopic(receiver string) string       { return "wherry.receiver." + receiver }
func PoolTasksTopic(pool string) string          { return "wherry.pool." + pool + ".tasks" }
func PoolEventsTopic(pool string) string         { return "wherry.pool." + pool + ".events" }
func TenantTopic(tenant string) string           { return "wherry.tenant." + tenant }
func HostTopic(tenant, host string) string       { return "wherry.host." + tenant + "." + host }
func PoolGroup(pool string) string               { return "p-" + pool }
func TransactionalPrefix(pool string) string     { return "p." + pool + "." }

func validateNames(values ...string) error {
	for _, value := range values {
		if !ValidName(value) {
			return errors.New("invalid name")
		}
	}
	return nil
}
