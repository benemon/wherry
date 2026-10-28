package client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// listing matches a kind of ListOffsets request by its isolation level and its partitions' timestamp.
type listing func(isolation int8, timestamp int64) bool

var (
	committedListing listing = func(isolation int8, timestamp int64) bool { return isolation == 1 && timestamp == -1 }
	endListing       listing = func(isolation int8, timestamp int64) bool { return isolation == 0 && timestamp == -1 }
	startListing     listing = func(_ int8, timestamp int64) bool { return timestamp == -2 }
	timeListing      listing = func(_ int8, timestamp int64) bool { return timestamp >= 0 }
)

// failListings has the cluster answer the first failures ListOffsets requests of kind for topic alone, or every one if
// failures is negative, with code for each partition, and returns the number of those requests it has seen.
func (f *fixture) failListings(t *testing.T, topic string, kind listing, failures int32, code int16) *atomic.Int32 {
	t.Helper()
	seen := &atomic.Int32{}
	f.cluster.ControlKey(int16(kmsg.ListOffsets), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		listed := request.(*kmsg.ListOffsetsRequest)
		if len(listed.Topics) != 1 || listed.Topics[0].Topic != topic || len(listed.Topics[0].Partitions) == 0 ||
			!kind(listed.IsolationLevel, listed.Topics[0].Partitions[0].Timestamp) {
			return nil, nil, false
		}
		if n := seen.Add(1); failures >= 0 && n > failures {
			return nil, nil, false
		}
		response := listed.ResponseKind().(*kmsg.ListOffsetsResponse)
		failed := kmsg.NewListOffsetsResponseTopic()
		failed.Topic = topic
		for _, partition := range listed.Topics[0].Partitions {
			out := kmsg.NewListOffsetsResponseTopicPartition()
			out.Partition = partition.Partition
			out.ErrorCode = code
			failed.Partitions = append(failed.Partitions, out)
		}
		response.Topics = append(response.Topics, failed)
		return response, nil, true
	})
	return seen
}

// Each listing of the host's lists again while its partition fails retriably, fails at once on any other partition
// error, and fails with its context's error when the retriable error outlasts it. OFFSET_NOT_AVAILABLE is a retriable
// error kgo never retries itself.
func TestHostListingsListAgainOnlyOnARetriablePartitionError(t *testing.T) {
	feed, transcript := admin.TenantTopic("tenant-a"), admin.TenantTranscriptTopic("tenant-a")
	for _, site := range []struct {
		name  string
		topic string
		kind  listing
		// setup prepares the fixture and returns the call that lists.
		setup func(t *testing.T, f *fixture) func(context.Context) error
	}{
		{"publish", feed, committedListing, func(t *testing.T, f *fixture) func(context.Context) error {
			return func(ctx context.Context) error {
				submission := &wire.Submission{Wherry: wire.SubmissionBinding{Run: "run-listed", Pool: "pool-a", DeadlineMS: time.Now().Add(time.Minute).UnixMilli()}}
				return f.host.publish(ctx, submission, []byte("task"), time.Now())
			}
		}},
		{"follow end", feed, committedListing, func(t *testing.T, f *fixture) func(context.Context) error {
			return func(ctx context.Context) error { _, err := f.host.follow(ctx); return err }
		}},
		{"follow start", feed, startListing, func(t *testing.T, f *fixture) func(context.Context) error {
			return func(ctx context.Context) error { _, err := f.host.follow(ctx); return err }
		}},
		{"readFeed", feed, committedListing, func(t *testing.T, f *fixture) func(context.Context) error {
			follower := following(t, f, f.host)
			return func(ctx context.Context) error {
				_, _, err := f.host.readFeed(ctx, follower, follower.start)
				return err
			}
		}},
		{"transcript end", transcript, endListing, func(t *testing.T, f *fixture) func(context.Context) error {
			runID := f.endedRun(t, 3).submission.Wherry.Run
			return func(ctx context.Context) error { _, err := f.host.Transcript(ctx, runID); return err }
		}},
		{"transcript start", transcript, timeListing, func(t *testing.T, f *fixture) func(context.Context) error {
			runID := f.endedRun(t, 3).submission.Wherry.Run
			return func(ctx context.Context) error { _, err := f.host.Transcript(ctx, runID); return err }
		}},
	} {
		for _, test := range []struct {
			name     string
			failures int32
			code     int16
			timeout  time.Duration
			want     error
			listings func(int32) bool
		}{
			{"retriable then listed", 3, kerr.OffsetNotAvailable.Code, 10 * time.Second, nil, func(n int32) bool { return n == 4 }},
			{"non-retriable", -1, kerr.UnknownServerError.Code, 5 * time.Second, kerr.UnknownServerError, func(n int32) bool { return n == 1 }},
			{"retriable past the deadline", -1, kerr.OffsetNotAvailable.Code, time.Second, context.DeadlineExceeded, func(n int32) bool { return n > 1 }},
		} {
			t.Run(site.name+"/"+test.name, func(t *testing.T) {
				f := newFixture(t)
				list := site.setup(t, f)
				seen := f.failListings(t, site.topic, site.kind, test.failures, test.code)
				ctx, cancel := context.WithTimeout(f.ctx, test.timeout)
				defer cancel()
				if err := list(ctx); !errors.Is(err, test.want) || !test.listings(seen.Load()) {
					t.Fatalf("%v after %d listings; want %v", err, seen.Load(), test.want)
				}
			})
		}
	}
}
