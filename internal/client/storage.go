package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/redact"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel/trace"
)

// S3File is bytes a run delivers through its pool's bucket, as the input Name.
type S3File struct {
	Name string
	Body io.ReadSeeker
}

// getLifetime is how long a presigned GET for an S3 input lives; a run's POST policy for its S3 outputs lives until its
// task_ended_by plus the upload grace.
const getLifetime = 15 * time.Minute

// storageTransport carries the client's S3 requests, nil for Go's default. A variable so tests can trust their TLS
// servers.
var storageTransport http.RoundTripper

// bundleAccessKey and bundleSecretKey name the bundle's storage keys in the redaction markers that replace them.
const bundleAccessKey, bundleSecretKey = "bundle-access-key", "bundle-secret-key"

// store is a pool's bucket opened with this host's storage keys: the boundary every storage error and log line crosses.
// A store's answer can name the access key that signed the request, as InvalidAccessKeyId does, so every error a store
// returns and every line it logs has both key values replaced by their markers, whatever their length. A returned
// error unwraps to the store's own, so callers classify it as before.
type store struct {
	client *minio.Client
	forms  []redact.Form
	logger *log.Logger
}

// redactedError is a storage error whose text has the store's keys replaced by their markers; it unwraps to the error
// the store answered with.
type redactedError struct {
	err  error
	text string
}

func (e *redactedError) Error() string { return e.text }
func (e *redactedError) Unwrap() error { return e.err }

// wrap returns err with every form of the store's keys in its text replaced by its marker, or nil for nil.
func (s *store) wrap(err error) error {
	if err == nil {
		return nil
	}
	return &redactedError{err: err, text: s.redact(err.Error())}
}

// redact is text with every form of the store's keys replaced by its marker.
func (s *store) redact(text string) string {
	var out strings.Builder
	redact.To(&out, []byte(text), s.forms)
	return out.String()
}

// logf writes one line to the host's log with the store's keys replaced by their markers.
func (s *store) logf(format string, args ...any) {
	s.logger.Print(s.redact(fmt.Sprintf(format, args...)))
}

// credentialed reports whether this host holds storage keys for pool: its configuration maps the pool to credentials,
// or its bundle carries the tenant's pair.
func (h *Host) credentialed(pool string) bool {
	_, mapped := h.storage[pool]
	return mapped || h.bundleStorage != (operator.StorageKeys{})
}

// bucket opens a pool's bucket, path-style, with the keys the configuration maps the pool to, resolved through resolver,
// else the bundle's pair; the keys never leave the host, and either pair is held for redaction before any storage call,
// whatever its length.
func (h *Host) bucket(ctx context.Context, pool string, storage *operator.Storage, resolver *resolver) (*store, error) {
	keyNames, mapped := h.storage[pool]
	if !h.credentialed(pool) {
		return nil, fmt.Errorf("%w: this host's configuration has no storage credential for pool %s's S3 inputs and outputs, and its bundle carries no storage keys", ErrInvalidInput, pool)
	}
	if storage == nil {
		return nil, fmt.Errorf("%w: pool %s has no storage for S3 inputs and outputs", ErrInvalidInput, pool)
	}
	var keys [2]string
	names := [2]string{bundleAccessKey, bundleSecretKey}
	if mapped {
		names = [2]string{keyNames.AccessKey, keyNames.SecretKey}
		for i, name := range names {
			key, err := resolver.resolve(name, h.credentials[name])
			if err != nil {
				return nil, fmt.Errorf("read storage credential %s: %w", name, err)
			}
			keys[i] = strings.TrimSpace(string(key))
		}
	} else {
		keys = [2]string{h.bundleStorage.AccessKey, h.bundleStorage.SecretKey}
	}
	values := map[string][]byte{}
	for i, name := range names {
		holdStorageKey(name, []byte(keys[i]))
		values[name] = []byte(keys[i])
	}
	// minio drops the default port Host adds, as presigned URLs are signed without it.
	client, err := minio.New(storage.Host(), &minio.Options{
		Creds: credentials.NewStaticV4(keys[0], keys[1], ""), Secure: storage.Secure(), BucketLookup: minio.BucketLookupPath, Transport: storageTransport,
	})
	if err != nil {
		return nil, err
	}
	return &store{client: client, forms: redact.AddExact(nil, nil, values), logger: h.logger}, nil
}

// put writes size bytes of body at key in bucket with one PUT.
func (s *store) put(ctx context.Context, bucket, key string, body io.Reader, size int64) error {
	_, err := s.client.PutObject(ctx, bucket, key, body, size, minio.PutObjectOptions{DisableMultipart: true, ContentType: "application/octet-stream"})
	return s.wrap(err)
}

// presignGet is a GET URL for key in bucket that lives for lifetime.
func (s *store) presignGet(ctx context.Context, bucket, key string, lifetime time.Duration) (string, error) {
	get, err := s.client.PresignedGetObject(ctx, bucket, key, lifetime, nil)
	if err != nil {
		return "", s.wrap(err)
	}
	return get.String(), nil
}

// postPolicy is a POST policy for bucket that lets any object whose key starts with prefix be created, of at most
// maxSize bytes, until expires: the bucket's URL and the form fields, key among them set to prefix.
func (s *store) postPolicy(ctx context.Context, bucket, prefix string, maxSize int64, expires time.Time) (*signed.S3Policy, error) {
	policy := minio.NewPostPolicy()
	for _, err := range []error{policy.SetBucket(bucket), policy.SetKeyStartsWith(prefix), policy.SetContentLengthRange(0, maxSize), policy.SetExpires(expires.UTC())} {
		if err != nil {
			return nil, err
		}
	}
	target, fields, err := s.client.PresignedPostPolicy(ctx, policy)
	if err != nil {
		return nil, s.wrap(err)
	}
	return &signed.S3Policy{URL: target.String(), Fields: fields}, nil
}

// get opens the object at key in bucket for reading through a reader that wraps its errors as the store's other calls do
// and passes io.EOF through by identity; the store fetches at the first Read, so a refusal naming the signing key
// arrives there.
func (s *store) get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	object, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, s.wrap(err)
	}
	return &redactedReader{object: object, store: s}, nil
}

// redactedReader is an object the store opened, whose Read and Close errors have the store's keys replaced by their
// markers, as the errors of its other calls do.
type redactedReader struct {
	object io.ReadCloser
	store  *store
}

func (r *redactedReader) Read(p []byte) (int, error) {
	n, err := r.object.Read(p)
	if err == io.EOF {
		return n, err
	}
	return n, r.store.wrap(err)
}

func (r *redactedReader) Close() error { return r.store.wrap(r.object.Close()) }

// uploadInputs puts each file under the run's prefix with one PUT and returns the S3 inputs with the size and SHA-256 of
// the bytes it sent. Its span carries the count, the bytes and the pool terms' endpoint and bucket, never a URL or
// the store's answer.
func uploadInputs(ctx context.Context, store *store, storage *operator.Storage, tenant, run string, files []S3File) ([]wire.S3Input, error) {
	_, span := telemetry.ChildOf(ctx, "upload inputs", trace.SpanKindClient, telemetry.AttrCount.Int(len(files)),
		telemetry.AttrStorageEndpoint.String(storage.Endpoint), telemetry.AttrStorageBucket.String(storage.Bucket))
	defer span.End()
	inputs, err := putInputs(ctx, store, storage.Bucket, tenant, run, files)
	if err != nil {
		telemetry.Fail(span, telemetry.ClassFailed)
		return nil, err
	}
	var total int64
	for _, input := range inputs {
		total += input.Size
	}
	span.SetAttributes(telemetry.AttrSize.Int64(total))
	return inputs, nil
}

func putInputs(ctx context.Context, store *store, bucket, tenant, run string, files []S3File) ([]wire.S3Input, error) {
	var inputs []wire.S3Input
	for _, file := range files {
		if local, ok := file.Body.(*os.File); ok {
			info, err := local.Stat()
			if err != nil {
				return nil, err
			}
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("%w: S3 input %s must be a regular file of at most 5 GiB", ErrInvalidInput, file.Name)
			}
		}
		size, err := file.Body.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
		if _, err := file.Body.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		if size > wire.MaxS3Object {
			return nil, fmt.Errorf("%w: S3 input %s must be a regular file of at most 5 GiB", ErrInvalidInput, file.Name)
		}
		sum := sha256.New()
		if err := store.put(ctx, bucket, wire.S3InputKey(tenant, run, file.Name), io.TeeReader(file.Body, sum), size); err != nil {
			return nil, fmt.Errorf("upload S3 input %s: %w", file.Name, err)
		}
		inputs = append(inputs, wire.S3Input{Name: file.Name, Size: size, SHA256: hex.EncodeToString(sum.Sum(nil))})
	}
	return inputs, nil
}

// presign returns the GET URLs of a run's S3 inputs, under inputRun's prefix, which expire at getDeadline, and the
// run's POST policy for every object under its output prefix, which expires at deadline plus the upload grace, so a
// run stopped at deadline still uploads. A deadline that has passed is refused all the same.
func presign(ctx context.Context, store *store, bucket, tenant, inputRun, run string, inputs []string, getDeadline, deadline time.Time) (map[string]string, *signed.S3Policy, error) {
	urls := map[string]string{}
	if len(inputs) > 0 {
		lifetime := min(time.Until(getDeadline).Truncate(time.Second)+time.Second, operator.MaxPresign)
		if lifetime < 2*time.Second {
			return nil, nil, errors.New("task_started_by has passed")
		}
		for _, input := range inputs {
			get, err := store.presignGet(ctx, bucket, wire.S3InputKey(tenant, inputRun, input), lifetime)
			if err != nil {
				return nil, nil, fmt.Errorf("presign S3 input %s: %w", input, err)
			}
			urls[input] = get
		}
	}
	if time.Until(deadline) < time.Second {
		return nil, nil, errors.New("task_ended_by has passed")
	}
	policy, err := store.postPolicy(ctx, bucket, wire.S3OutputKey(tenant, run, ""), wire.MaxS3Object, deadline.Add(operator.UploadGrace))
	if err != nil {
		return nil, nil, fmt.Errorf("presign the S3 outputs' policy: %w", err)
	}
	return urls, policy, nil
}

// DownloadOutputs downloads a run's S3 outputs beneath dir and verifies each against its result's size and SHA-256;
// once the run is terminal and every output verified, it deletes the run's prefix, which holds whatever the policy let
// a receiver create there, listed or not.
func (h *Host) DownloadOutputs(ctx context.Context, runID string, result AwaitResult, dir string) ([]Output, []wire.UnreturnedOutput, error) {
	local, err := h.localRun(runID)
	if err != nil || local == nil || len(local.S3Inputs) == 0 && !local.Policy {
		return nil, nil, err
	}
	if dir == "" {
		dir = h.outputDir(runID)
	}
	var verified []Output
	var rejected []wire.UnreturnedOutput
	if len(result.Stored) > 0 {
		v, err := h.load(ctx)
		if err != nil {
			return nil, nil, err
		}
		storage := v.state.Pools[local.Pool].Storage
		store, err := h.bucket(ctx, local.Pool, storage, newResolver(ctx, ""))
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
			case !local.Policy:
				reason = "the run was issued no upload policy"
			case listed.Key != wire.S3OutputKey(h.tenant, runID, listed.Name):
				reason = "listed at a key outside the run's prefix"
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
func download(ctx context.Context, store *store, bucket string, root *os.Root, listed wire.ReturnedOutput) string {
	if existing, err := root.Open(listed.Name); err == nil {
		sum := sha256.New()
		size, err := io.Copy(sum, io.LimitReader(existing, listed.Size+1))
		existing.Close()
		if err == nil && size == listed.Size && hex.EncodeToString(sum.Sum(nil)) == listed.SHA256 {
			return ""
		}
	}
	if parent := path.Dir(listed.Name); parent != "." {
		if err := root.MkdirAll(parent, 0o700); err != nil {
			return "not written"
		}
	}
	object, err := store.get(ctx, bucket, listed.Key)
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

// Cleanup deletes every object under a run's prefix in its pool's bucket, and nothing outside it. It refuses a run whose
// run has no outcome, since its receiver may still read inputs or write outputs there. A resubmitted run's prefix holds
// the inputs of its replacements, so it is kept until the last replacement has an outcome, whose cleanup deletes it too.
// A run that ended key-lost and may still be resubmitted has not ended.
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
	if r.outcome == nil {
		return fmt.Errorf("%w: run %s has not ended", ErrInvalidInput, runID)
	}
	chain, err := h.chain(runID)
	if err != nil {
		return err
	}
	root := runID
	if r.local != nil && r.local.Task != nil {
		root = r.local.Task.Wherry.InputRun()
	}
	last := r
	if id := chain[len(chain)-1]; id != runID {
		if last, err = h.history(ctx, id); err != nil {
			return err
		}
	}
	if lost := last.resubmittable(time.Now()); last.outcome == nil || lost {
		switch {
		case lost && (last == r || root == runID):
			return fmt.Errorf("%w: run %s lost its pool key and is resubmittable until %s", ErrInvalidInput, last.run, wire.RFC3339(last.local.TaskStartedBy))
		case root == runID:
			return fmt.Errorf("%w: run %s's inputs serve its replacement %s, which has not ended", ErrInvalidInput, runID, last.run)
		}
		root = runID
	}
	storage := r.v.state.Pools[r.pool].Storage
	store, err := h.bucket(ctx, r.pool, storage, newResolver(ctx, ""))
	if err != nil {
		return err
	}
	if root != runID {
		if err := store.removePrefix(ctx, storage.Bucket, wire.S3Prefix(h.tenant, root)); err != nil {
			return err
		}
	}
	return store.removePrefix(ctx, storage.Bucket, wire.S3Prefix(h.tenant, runID))
}

// removePrefix deletes every object a listing of prefix returns that lies under it.
func (s *store) removePrefix(ctx context.Context, bucket, prefix string) error {
	var listErr error
	objects := func(yield func(minio.ObjectInfo) bool) {
		for object := range s.client.ListObjectsIter(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			switch {
			case object.Err != nil:
				listErr = object.Err
			case strings.HasPrefix(object.Key, prefix) && !yield(object):
				return
			}
		}
	}
	results, err := s.client.RemoveObjectsWithIter(ctx, bucket, objects, minio.RemoveObjectsOptions{})
	if err != nil {
		return s.wrap(err)
	}
	var removeErr error
	for removed := range results {
		if removed.Err != nil && removeErr == nil {
			removeErr = fmt.Errorf("delete %s: %w", removed.ObjectName, removed.Err)
		}
	}
	return s.wrap(errors.Join(listErr, removeErr))
}
