package client

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3File is a local file a run's first turn delivers through its pool's bucket, as the input Name.
type S3File struct {
	Name string
	File *os.File
}

// getLifetime is how long a presigned GET for an S3 input lives; a PUT for an S3 output lives until its turn's
// deadline.
const getLifetime = 15 * time.Minute

// maxPresign is the longest lifetime S3 SigV4 allows a presigned URL.
const maxPresign = 7 * 24 * time.Hour

// storageTransport carries the client's S3 requests, nil for Go's default. A variable so tests can trust their TLS
// servers.
var storageTransport http.RoundTripper

// bucket opens a pool's bucket, path-style, with this host's storage credential, which never leaves the host: the
// receiver and the agent see only URLs it presigns.
func (h *Host) bucket(pool string, storage *admin.Storage) (*minio.Client, error) {
	if h.storage == nil {
		return nil, fmt.Errorf("%w: this host's configuration has no storage credential for S3 inputs and outputs", ErrInvalidInput)
	}
	if storage == nil {
		return nil, fmt.Errorf("%w: pool %s has no storage for S3 inputs and outputs", ErrInvalidInput, pool)
	}
	access, err := readKey(h.storage.AccessKeyFile)
	if err != nil {
		return nil, err
	}
	secret, err := readKey(h.storage.SecretKeyFile)
	if err != nil {
		return nil, err
	}
	return minio.New(strings.TrimPrefix(storage.Endpoint, "https://"), &minio.Options{
		Creds: credentials.NewStaticV4(access, secret, ""), Secure: true, BucketLookup: minio.BucketLookupPath, Transport: storageTransport,
	})
}

// readKey reads a storage key file that only its owner can read or write.
func readKey(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("storage key file: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("storage key file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("storage key file %s is not a regular file", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("storage key file %s is open to its group or others; chmod 600 it", path)
	}
	key, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("storage key file: %w", err)
	}
	return strings.TrimSpace(string(key)), nil
}

// uploadInputs puts each file under the run's prefix with one PUT and returns the S3 inputs with the size and SHA-256 of
// the bytes it sent.
func uploadInputs(ctx context.Context, store *minio.Client, bucket, tenant, run string, files []S3File) ([]wire.S3Input, error) {
	var inputs []wire.S3Input
	for _, file := range files {
		info, err := file.File.Stat()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() > wire.MaxS3Object {
			return nil, fmt.Errorf("%w: S3 input %s must be a regular file of at most 5 GiB", ErrInvalidInput, file.Name)
		}
		sum := sha256.New()
		if _, err := store.PutObject(ctx, bucket, wire.S3InputKey(tenant, run, file.Name), io.TeeReader(file.File, sum), info.Size(),
			minio.PutObjectOptions{DisableMultipart: true, ContentType: "application/octet-stream"}); err != nil {
			return nil, fmt.Errorf("upload S3 input %s: %w", file.Name, err)
		}
		inputs = append(inputs, wire.S3Input{Name: file.Name, Size: info.Size(), SHA256: hex.EncodeToString(sum.Sum(nil))})
	}
	return inputs, nil
}

// presign returns the seal values of a run's S3 inputs and outputs: a GET per input for getLifetime and a PUT per output
// of turn until deadline.
func presign(ctx context.Context, store *minio.Client, bucket, tenant, run string, turn int64, inputs, outputs []string, deadline time.Time) (map[string][]byte, error) {
	values := map[string][]byte{}
	for _, input := range inputs {
		get, err := store.PresignedGetObject(ctx, bucket, wire.S3InputKey(tenant, run, input), getLifetime, nil)
		if err != nil {
			return nil, fmt.Errorf("presign S3 input %s: %w", input, err)
		}
		values[signed.S3InputValue(input)] = []byte(get.String())
	}
	if len(outputs) == 0 {
		return values, nil
	}
	lifetime := min(time.Until(deadline).Truncate(time.Second)+time.Second, maxPresign)
	if lifetime < 2*time.Second {
		return nil, errors.New("the turn's deadline has passed")
	}
	for _, output := range outputs {
		put, err := store.PresignedPutObject(ctx, bucket, wire.S3OutputKey(tenant, run, turn, output), lifetime)
		if err != nil {
			return nil, fmt.Errorf("presign S3 output %s: %w", output, err)
		}
		values[signed.S3OutputValue(output)] = []byte(put.String())
	}
	return values, nil
}

// checkSealSize refuses a run whose seal would pass its limit: it seals the run's task credentials and URLs for its S3
// inputs and outputs, presigned as the answer will, to a throwaway key. The agent's own credential is not counted.
func (h *Host) checkSealSize(ctx context.Context, store *minio.Client, bucket, tenant, run string, credentials []wire.TaskCredential, inputs, outputs []string, deadline time.Time) error {
	values, err := presign(ctx, store, bucket, tenant, run, 1, inputs, outputs, deadline)
	if err != nil {
		return err
	}
	var names []string
	for _, credential := range credentials {
		if values[credential.Name], err = os.ReadFile(h.credentials[credential.Name]); err != nil {
			return fmt.Errorf("read credential %s: %w", credential.Name, errors.Unwrap(err))
		}
		names = append(names, credential.Name)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	// Fields the answer fills from the receiver's request are at their bounds.
	placeholder := strings.Repeat("x", 32)
	_, err = signed.Seal(signed.SealBinding{
		Tenant: tenant, Pool: placeholder, Receiver: placeholder, KeyID: signed.SealKeyID(&key.PublicKey), Run: run, Turn: 1, Attempt: 1 << 31,
		Credentials: []string{}, TaskCredentials: names, S3Inputs: inputs, S3Outputs: outputs, ExpiryMS: deadline.UnixMilli(),
	}, &key.PublicKey, signed.SealValues{Values: values, RedactionKey: make([]byte, 32)})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	return nil
}

// DownloadOutputs downloads a turn's S3 outputs beneath dir and verifies each against its result's size and SHA-256;
// once the run is terminal and every output verified, it deletes the run's prefix.
func (h *Host) DownloadOutputs(ctx context.Context, runID string, result AwaitResult, dir string) ([]Output, []wire.UnreturnedOutput, error) {
	local, err := h.localRun(runID)
	if err != nil || local == nil || len(local.S3Inputs)+len(local.S3Outputs) == 0 {
		return nil, nil, err
	}
	if dir == "" {
		dir = filepath.Join(h.stateDir, "runs", runID, "outputs", strconv.FormatInt(result.Turn, 10))
	}
	var verified []Output
	var rejected []wire.UnreturnedOutput
	if len(result.Stored) > 0 {
		v, err := h.load(ctx)
		if err != nil {
			return nil, nil, err
		}
		storage := v.state.Pools[local.Pool].Storage
		store, err := h.bucket(local.Pool, storage)
		if err != nil {
			return nil, nil, err
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, nil, err
		}
		root, err := os.OpenRoot(dir)
		if err != nil {
			return nil, nil, err
		}
		defer root.Close()
		for _, listed := range result.Stored {
			reason := ""
			switch {
			case !slices.Contains(local.S3Outputs, listed.Name):
				reason = "not an S3 output the run declares"
			case listed.Key != wire.S3OutputKey(h.tenant, runID, result.Turn, listed.Name):
				reason = "listed at a key outside the turn's prefix"
			default:
				reason = download(ctx, store, storage.Bucket, root, listed)
			}
			if reason != "" {
				rejected = append(rejected, wire.UnreturnedOutput{Name: listed.Name, Size: max(listed.Size, 0), Reason: "rejected by this host: " + reason})
				continue
			}
			verified = append(verified, Output{Name: listed.Name, SHA256: listed.SHA256, Size: listed.Size, Path: filepath.Join(dir, filepath.FromSlash(listed.Name))})
		}
	}
	if wire.Terminal(result.Status.State) && len(rejected) == 0 {
		if err := h.Cleanup(ctx, runID); err != nil {
			return verified, rejected, fmt.Errorf("delete the run's prefix: %w", err)
		}
	}
	return verified, rejected, nil
}

// download writes listed's object into root under its name, and returns why it does not verify, or "". A file already
// there that verifies is kept.
func download(ctx context.Context, store *minio.Client, bucket string, root *os.Root, listed wire.ReturnedOutput) string {
	if existing, err := root.Open(listed.Name); err == nil {
		matched := verifies(existing, listed)
		existing.Close()
		if matched {
			return ""
		}
	}
	if parent := path.Dir(listed.Name); parent != "." {
		if err := root.MkdirAll(parent, 0o700); err != nil {
			return "not written"
		}
	}
	object, err := store.GetObject(ctx, bucket, listed.Key, minio.GetObjectOptions{})
	if err != nil {
		return "not downloaded"
	}
	defer object.Close()
	file, err := root.OpenFile(listed.Name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "not written"
	}
	sum := sha256.New()
	size, err := io.Copy(io.MultiWriter(file, sum), io.LimitReader(object, listed.Size+1))
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil && size == listed.Size && hex.EncodeToString(sum.Sum(nil)) == listed.SHA256 {
		return ""
	}
	root.Remove(listed.Name)
	if err != nil {
		return "not downloaded"
	}
	return "size or hash mismatch"
}

// verifies reports whether file holds exactly listed's bytes.
func verifies(file io.Reader, listed wire.ReturnedOutput) bool {
	sum := sha256.New()
	size, err := io.Copy(sum, io.LimitReader(file, listed.Size+1))
	return err == nil && size == listed.Size && hex.EncodeToString(sum.Sum(nil)) == listed.SHA256
}

// Cleanup deletes every object under a run's prefix in its pool's bucket, and nothing outside it.
func (h *Host) Cleanup(ctx context.Context, runID string) error {
	if _, ok := h.runPath(runID); !ok {
		return fmt.Errorf("cleanup %s: %w", runID, ErrRunNotFound)
	}
	r, err := h.history(ctx, runID)
	if err != nil {
		return err
	}
	if r.pool == "" {
		return fmt.Errorf("cleanup %s: %w", runID, ErrRunNotFound)
	}
	storage := r.v.state.Pools[r.pool].Storage
	store, err := h.bucket(r.pool, storage)
	if err != nil {
		return err
	}
	return h.removePrefix(ctx, store, storage.Bucket, runID)
}

// removePrefix deletes every object a listing of the run's prefix returns that lies under the prefix.
func (h *Host) removePrefix(ctx context.Context, store *minio.Client, bucket, runID string) error {
	prefix := wire.S3Prefix(h.tenant, runID)
	objects := make(chan minio.ObjectInfo)
	var listErr error
	go func() {
		defer close(objects)
		for object := range store.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			switch {
			case object.Err != nil:
				listErr = object.Err
			case strings.HasPrefix(object.Key, prefix):
				objects <- object
			}
		}
	}()
	var removeErr error
	for removed := range store.RemoveObjects(ctx, bucket, objects, minio.RemoveObjectsOptions{}) {
		if removed.Err != nil && removeErr == nil {
			removeErr = fmt.Errorf("delete %s: %w", removed.ObjectName, removed.Err)
		}
	}
	return errors.Join(listErr, removeErr)
}
