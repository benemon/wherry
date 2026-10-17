package wire

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestApplyTopicsIsIdempotentAndPreservesConfigs(t *testing.T) {
	cluster := kfake.MustCluster(kfake.NumBrokers(1))
	t.Cleanup(cluster.Close)
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := ApplyTopics(ctx, admin, 1); err != nil {
		t.Fatal(err)
	}
	before := readDeclaredConfigs(t, ctx, admin)
	if err := ApplyTopics(ctx, admin, 1); err != nil {
		t.Fatal(err)
	}
	after := readDeclaredConfigs(t, ctx, admin)
	for topic, expected := range before {
		for name, value := range expected {
			if after[topic][name] != value {
				t.Errorf("%s %s changed from %q to %q", topic, name, value, after[topic][name])
			}
		}
	}
}

func readDeclaredConfigs(t *testing.T, ctx context.Context, admin *kadm.Client) map[string]map[string]string {
	t.Helper()
	names := make([]string, 0, len(topicDeclarations))
	for _, declaration := range topicDeclarations {
		names = append(names, declaration.name)
	}
	resources, err := admin.DescribeTopicConfigs(ctx, names...)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]map[string]string, len(topicDeclarations))
	for _, declaration := range topicDeclarations {
		resource, err := resources.On(declaration.name, nil)
		if err != nil {
			t.Fatal(err)
		}
		if resource.Err != nil {
			t.Fatal(resource.Err)
		}
		actual := make(map[string]string, len(declaration.configs))
		for _, config := range resource.Configs {
			if _, wanted := declaration.configs[config.Key]; wanted && config.Value != nil {
				actual[config.Key] = *config.Value
			}
		}
		for name, value := range declaration.configs {
			if actual[name] != value {
				t.Errorf("%s %s: got %q, want %q", declaration.name, name, actual[name], value)
			}
		}
		result[declaration.name] = actual
	}
	return result
}
