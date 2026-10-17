package wire

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

type topicDeclaration struct {
	name    string
	configs map[string]string
}

var topicDeclarations = []topicDeclaration{
	{name: "tasks.queue", configs: map[string]string{"cleanup.policy": "delete", "retention.ms": "-1"}},
	{name: "tasks.events", configs: map[string]string{"cleanup.policy": "delete", "retention.ms": "-1"}},
	{name: "tasks.control", configs: map[string]string{"cleanup.policy": "compact"}},
	{name: "transcripts", configs: map[string]string{"cleanup.policy": "delete", "retention.ms": "2592000000"}},
	{name: "receivers.capabilities", configs: map[string]string{"cleanup.policy": "compact"}},
	{name: "receivers.state", configs: map[string]string{"cleanup.policy": "delete", "retention.ms": "-1"}},
	{name: "receivers.secrets", configs: map[string]string{
		"cleanup.policy":        "compact",
		"segment.ms":            "3600000",
		"delete.retention.ms":   "3600000",
		"min.compaction.lag.ms": "0",
	}},
}

// ApplyTopics creates and configures the fixed Agentbus topics.
func ApplyTopics(ctx context.Context, client *kadm.Client, replicationFactor int16) error {
	for _, declaration := range topicDeclarations {
		configs := make(map[string]*string, len(declaration.configs))
		changes := make([]kadm.AlterConfig, 0, len(declaration.configs))
		for name, value := range declaration.configs {
			value := value
			configs[name] = &value
			changes = append(changes, kadm.AlterConfig{Op: kadm.SetConfig, Name: name, Value: &value})
		}

		created, err := client.CreateTopics(ctx, 1, replicationFactor, configs, declaration.name)
		if err != nil {
			return fmt.Errorf("create topic %s: %w", declaration.name, err)
		}
		response, ok := created[declaration.name]
		if !ok {
			return fmt.Errorf("create topic %s: missing response", declaration.name)
		}
		if response.Err != nil && !errors.Is(response.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", declaration.name, response.Err)
		}

		altered, err := client.AlterTopicConfigs(ctx, changes, declaration.name)
		if err != nil {
			return fmt.Errorf("configure topic %s: %w", declaration.name, err)
		}
		alterResponse, err := altered.On(declaration.name, nil)
		if err != nil {
			return fmt.Errorf("configure topic %s: %w", declaration.name, err)
		}
		if alterResponse.Err != nil {
			return fmt.Errorf("configure topic %s: %w", declaration.name, alterResponse.Err)
		}
	}
	return nil
}
