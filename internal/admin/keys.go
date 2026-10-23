package admin

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/twmb/franz-go/pkg/kgo"
)

// RotateKey replaces key, the owner's key in the file at path, with a new key and returns the key RecoverKey then
// leaves at path. The new key is written to path.next before the rotation is published to the owner's key topic.
func RotateKey(ctx context.Context, client *kgo.Client, owner, topic, path string, key ed25519.PrivateKey) (ed25519.PrivateKey, error) {
	next, err := trust.GenerateSigningKeyFile(path + ".next")
	if err != nil {
		return nil, err
	}
	record, err := signed.RotateKey(owner, key, next)
	if err != nil {
		return nil, err
	}
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(owner), Value: record}).FirstErr(); err != nil {
		return nil, fmt.Errorf("publish key rotation: %w", err)
	}
	return RecoverKey(ctx, client, owner, topic, path)
}

// RecoverKey returns the owner's key in the file at path once a rotation that left path.next is settled by the owner's
// key topic: path.next replaces the file when the topic's current key is path.next's, and is removed when it is the
// file's.
func RecoverKey(ctx context.Context, client *kgo.Client, owner, topic, path string) (ed25519.PrivateKey, error) {
	key, err := trust.LoadSigningKeyFile(path)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(path + ".next"); errors.Is(err, os.ErrNotExist) {
		return key, nil
	} else if err != nil {
		return nil, err
	}
	keys, err := ReadKeys(ctx, client, map[string]string{owner: topic})
	if err != nil {
		return nil, err
	}
	current := keys[owner].Current
	if current.Equal(key.Public()) {
		if err := os.Remove(path + ".next"); err != nil {
			return nil, err
		}
		return key, nil
	}
	next, err := trust.LoadSigningKeyFile(path + ".next")
	if err != nil {
		return nil, err
	}
	if !current.Equal(next.Public()) {
		return nil, errors.New("key topic's current key is neither the key file's nor the next key file's")
	}
	if err := trust.ReplaceKeyFile(path+".next", path); err != nil {
		return nil, err
	}
	return next, nil
}
