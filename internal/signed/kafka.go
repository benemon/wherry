package signed

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func PublishStatus(ctx context.Context, client *kgo.Client, pool, tenant, eventID string, encoded []byte, readBack func(context.Context, string) (bool, bool, error)) error {
	for attempt := 0; attempt < 2; attempt++ {
		if err := client.BeginTransaction(); err != nil {
			return err
		}
		records := []*kgo.Record{{Topic: "wherry.pool." + pool + ".events", Key: []byte(eventID), Value: encoded}, {Topic: "wherry.tenant." + tenant, Key: []byte(eventID), Value: encoded}}
		if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
			_ = client.EndTransaction(ctx, kgo.TryAbort)
			return fmt.Errorf("produce status transaction: %w", err)
		}
		if err := client.EndTransaction(ctx, kgo.TryCommit); err == nil {
			return nil
		}
		if readBack == nil {
			return errors.New("status transaction outcome is ambiguous")
		}
		poolSeen, tenantSeen, err := readBack(ctx, eventID)
		if err != nil {
			return fmt.Errorf("status transaction read-back: %w", err)
		}
		if poolSeen && tenantSeen {
			return nil
		}
		if poolSeen != tenantSeen {
			return errors.New("status transaction is only partly visible")
		}
	}
	return errors.New("status transaction could not be confirmed")
}
