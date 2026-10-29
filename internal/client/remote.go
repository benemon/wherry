package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
)

// Upload is a presigned PUT a remote client sends a file to before submit names it by ID.
type Upload struct {
	ID      string
	URL     string
	Expires time.Time
}

// uploadRecord is what PresignUpload was told of the file.
type uploadRecord struct {
	Pool   string `json:"pool"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func (h *Host) uploadPath(id string) (string, bool) {
	if !filepath.IsLocal(id) || filepath.Base(id) != id {
		return "", false
	}
	return filepath.Join(h.stateDir, "uploads", id+".json"), true
}

// PresignUpload returns a PUT URL, valid for lifetime, for a file of size bytes and SHA-256 sum to be delivered as the
// input name, in the bucket of the first of the tenant's pools with storage.
func (h *Host) PresignUpload(ctx context.Context, name string, size int64, sum string, lifetime time.Duration) (Upload, error) {
	if decoded, err := hex.DecodeString(sum); err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != sum {
		return Upload{}, fmt.Errorf("%w: sha256 must be 64 lowercase hex digits", ErrInvalidInput)
	}
	if !wire.ValidFileName(name) || size < 0 || size > wire.MaxS3Object {
		return Upload{}, fmt.Errorf("%w: an upload is a portable file name and at most 5 GiB", ErrInvalidInput)
	}
	v, err := h.load(ctx)
	if err != nil {
		return Upload{}, err
	}
	pools := slices.Sorted(maps.Keys(v.state.Entitlements[h.tenant]))
	if len(pools) == 0 {
		return Upload{}, ErrNoMatchingReceiver
	}
	pool := pools[max(slices.IndexFunc(pools, func(pool string) bool { return v.state.Pools[pool].Storage != nil }), 0)]
	storage := v.state.Pools[pool].Storage
	store, err := h.bucket(ctx, pool, storage)
	if err != nil {
		return Upload{}, err
	}
	id := wire.RandomID("upload-")
	put, err := store.PresignedPutObject(ctx, storage.Bucket, wire.S3InputKey(h.tenant, id, name), min(lifetime, maxPresign))
	if err != nil {
		return Upload{}, err
	}
	path, _ := h.uploadPath(id)
	if err := writeState(path, uploadRecord{Pool: pool, Name: name, Size: size, SHA256: sum}); err != nil {
		return Upload{}, err
	}
	return Upload{ID: id, URL: put.String(), Expires: time.Now().Add(min(lifetime, maxPresign))}, nil
}

// Uploaded returns the file the upload id put, once its size and SHA-256 are those PresignUpload was given. The bytes
// are copied to an unlinked local file as they are hashed, so the bytes submit sends are the ones verified, whatever
// the PUT URL writes later.
func (h *Host) Uploaded(ctx context.Context, id string) (S3File, error) {
	path, ok := h.uploadPath(id)
	var record uploadRecord
	if ok {
		if err := readState(path, &record); errors.Is(err, os.ErrNotExist) {
			ok = false
		} else if err != nil {
			return S3File{}, err
		}
	}
	if !ok {
		return S3File{}, fmt.Errorf("%w: upload %s is not one this host issued", ErrInvalidInput, id)
	}
	v, err := h.load(ctx)
	if err != nil {
		return S3File{}, err
	}
	storage := v.state.Pools[record.Pool].Storage
	store, err := h.bucket(ctx, record.Pool, storage)
	if err != nil {
		return S3File{}, err
	}
	object, err := store.GetObject(ctx, storage.Bucket, wire.S3InputKey(h.tenant, id, record.Name), minio.GetObjectOptions{})
	if err != nil {
		return S3File{}, err
	}
	defer object.Close()
	file, err := os.CreateTemp(h.stateDir, "upload-*")
	if err != nil {
		return S3File{}, err
	}
	os.Remove(file.Name())
	sum := sha256.New()
	size, err := io.Copy(io.MultiWriter(file, sum), io.LimitReader(object, record.Size+1))
	if err == nil {
		_, err = file.Seek(0, io.SeekStart)
	}
	switch {
	case minio.ToErrorResponse(err).Code == "NoSuchKey":
		err = fmt.Errorf("%w: nothing was put to upload %s", ErrInvalidInput, id)
	case err == nil && (size != record.Size || hex.EncodeToString(sum.Sum(nil)) != record.SHA256):
		err = fmt.Errorf("%w: upload %s does not match its size and sha256", ErrInvalidInput, id)
	}
	if err != nil {
		file.Close()
		return S3File{}, err
	}
	return S3File{Name: record.Name, Body: file}, nil
}

// DeleteUpload deletes an upload's object and this host's record of it.
func (h *Host) DeleteUpload(ctx context.Context, id string) error {
	path, ok := h.uploadPath(id)
	var record uploadRecord
	if !ok || readState(path, &record) != nil {
		return nil
	}
	v, err := h.load(ctx)
	if err != nil {
		return err
	}
	storage := v.state.Pools[record.Pool].Storage
	store, err := h.bucket(ctx, record.Pool, storage)
	if err != nil {
		return err
	}
	if err := h.removePrefix(ctx, store, storage.Bucket, id); err != nil {
		return err
	}
	return os.Remove(path)
}
