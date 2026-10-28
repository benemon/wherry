package admin

import (
	"cmp"
	"context"
	json "encoding/json/v2"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// Reader reads topics through one read_committed client, built from a plain client's options on its first read, so its
// reads share that client's connections and concurrent reads share its fetches. Reads of one topic run one at a time.
type Reader struct {
	base *kgo.Client
	// mu is held to build or close client.
	mu sync.Mutex
	// poll is held to poll client or change what it consumes, so every polled partition belongs to a read in reads.
	poll   chan struct{}
	client *kgo.Client
	closed bool
	reads  map[string]*read
}

// read is a read in progress of one topic up to the end of each of its partitions.
type read struct {
	partitions map[int32]kgo.Offset
	remaining  map[int32]int64
	records    []*kgo.Record
	err        error
	done       chan struct{}
	// end is the highest last stable offset listed for the topic.
	end int64
}

// NewReader returns a Reader that lists offsets and reads through a client built from client's options. client must be
// a plain client: a transactional ID in its options would fence its producer.
func NewReader(client *kgo.Client) *Reader {
	return &Reader{base: client, poll: make(chan struct{}, 1), reads: map[string]*read{}}
}

// Close closes the reader's client; reads in progress and later reads fail.
func (r *Reader) Close() {
	r.poll <- struct{}{}
	defer func() { <-r.poll }()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.client != nil {
		r.client.Close()
	}
}

// Admin returns an admin client over the reader's client.
func (r *Reader) Admin() (*kadm.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, kgo.ErrClientClosed
	}
	if r.client == nil {
		client, err := kgo.NewClient(append(slices.Clone(r.base.Opts()),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			// A trailing abort marker is the last offset below the bound; it must be seen to finish.
			kgo.KeepControlRecords(),
			kgo.DisableClientMetrics())...)
		if err != nil {
			return nil, err
		}
		r.client = client
	}
	return kadm.NewClient(r.client), nil
}

// ReadFrom reads topic from offset, or from the start of the topic's one partition where that is later, to its last
// stable offset.
func (r *Reader) ReadFrom(ctx context.Context, topic string, offset int64) ([]*kgo.Record, error) {
	records, _, err := r.ReadFromWithEnd(ctx, topic, offset)
	return records, err
}

// ReadFromWithEnd is ReadFrom that also returns the last stable offset it reads to.
func (r *Reader) ReadFromWithEnd(ctx context.Context, topic string, offset int64) ([]*kgo.Record, int64, error) {
	reads, err := r.readFrom(ctx, map[string]int64{topic: offset})
	if err != nil {
		return nil, 0, err
	}
	return reads[topic].records, reads[topic].end, reads[topic].err
}

// readFrom reads each topic as ReadFrom does from its offset, listing their offsets in one request of each kind and
// fetching them together. A topic's own failure is its read's error; the error returned fails every topic.
func (r *Reader) readFrom(ctx context.Context, offsets map[string]int64) (map[string]*read, error) {
	reads := map[string]*read{}
	if len(offsets) == 0 {
		// kadm lists every topic when it is given none.
		return reads, nil
	}
	admin, err := r.Admin()
	if err != nil {
		return nil, err
	}
	topics := slices.Collect(maps.Keys(offsets))
	var starts, ends kadm.ListedOffsets
	for {
		group, groupCtx := errgroup.WithContext(ctx)
		group.Go(func() (err error) {
			starts, err = admin.ListStartOffsets(groupCtx, topics...)
			return err
		})
		group.Go(func() (err error) {
			ends, err = admin.ListCommittedOffsets(groupCtx, topics...)
			return err
		})
		if err := group.Wait(); err != nil {
			return nil, err
		}
		// kgo retries a partition that is changing leader only while its listing evicts the topic's metadata, which the
		// concurrent listing may already have done.
		if !retriable(starts) && !retriable(ends) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(listBackoff):
		}
	}
	for topic, offset := range offsets {
		current := &read{remaining: map[int32]int64{}, partitions: map[int32]kgo.Offset{}, done: make(chan struct{})}
		reads[topic] = current
		for partition, end := range ends[topic] {
			start, ok := starts.Lookup(topic, partition)
			if current.err = cmp.Or(end.Err, start.Err); current.err != nil {
				break
			}
			current.end = max(current.end, end.Offset)
			if ok && max(start.Offset, offset) < end.Offset {
				current.remaining[partition] = end.Offset
				current.partitions[partition] = kgo.NewOffset().At(max(start.Offset, offset))
			}
		}
		if current.err != nil || len(current.remaining) == 0 {
			close(current.done)
		}
	}
	pending := func() *read {
		for _, current := range reads {
			select {
			case <-current.done:
			default:
				return current
			}
		}
		return nil
	}
	registered := false
	for {
		current := pending()
		if current == nil {
			return reads, nil
		}
		select {
		case <-current.done:
			continue
		case <-ctx.Done():
			if registered {
				// The client fetches a read's partitions until they are removed.
				for current := pending(); current != nil; current = pending() {
					select {
					case r.poll <- struct{}{}:
						for topic, current := range reads {
							if r.reads[topic] == current {
								r.finish(topic, current, ctx.Err())
							}
						}
						<-r.poll
					case <-current.done:
					}
				}
			}
			return nil, ctx.Err()
		case r.poll <- struct{}{}:
		}
		if !registered {
			var other *read
			for topic := range reads {
				if r.reads[topic] != nil {
					other = r.reads[topic]
				}
			}
			if other != nil {
				<-r.poll
				select {
				case <-other.done:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}
			if err := r.add(reads); err != nil {
				<-r.poll
				return nil, err
			}
			registered = true
		}
		select {
		case <-current.done:
		default:
			if ctx.Err() == nil {
				r.fetch(ctx)
			}
		}
		<-r.poll
	}
}

// listBackoff is the wait before listing offsets again after a retriable partition error.
const listBackoff = 100 * time.Millisecond

// retriable reports whether a listed partition failed with a retriable error; kadm's partition -1 stands for a topic it
// could not describe, which is not retried.
func retriable(listed kadm.ListedOffsets) bool {
	retry := false
	listed.Each(func(offset kadm.ListedOffset) {
		retry = retry || offset.Partition >= 0 && kerr.IsRetriable(offset.Err)
	})
	return retry
}

// ListOffsets lists topic's offsets with list, again after listBackoff while a listed partition carries a retriable
// error, and returns the last listing with its first partition error.
func ListOffsets(ctx context.Context, list func(context.Context, ...string) (kadm.ListedOffsets, error), topic string) (kadm.ListedOffsets, error) {
	for {
		listed, err := list(ctx, topic)
		if err != nil {
			return nil, err
		}
		// kgo retries a partition that is changing leader only while its listing evicts the topic's metadata, which a
		// concurrent listing of the topic through the same client may already have done.
		if !retriable(listed) {
			return listed, listed.Error()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(listBackoff):
		}
	}
}

// add starts consuming the partitions of the reads in progress among reads; the caller holds poll.
func (r *Reader) add(reads map[string]*read) error {
	if r.closed {
		return kgo.ErrClientClosed
	}
	partitions := map[string]map[int32]kgo.Offset{}
	for topic, current := range reads {
		select {
		case <-current.done:
		default:
			r.reads[topic] = current
			partitions[topic] = maps.Clone(current.partitions)
		}
	}
	r.client.AddConsumePartitions(partitions)
	return nil
}

// finish ends a read in reads with err and stops consuming its partitions; the caller holds poll.
func (r *Reader) finish(topic string, current *read, err error) {
	delete(r.reads, topic)
	r.client.RemoveConsumePartitions(map[string][]int32{topic: slices.Collect(maps.Keys(current.partitions))})
	if err != nil {
		current.records = nil
	}
	current.err = err
	close(current.done)
}

// fetch polls once for every read in progress; the caller holds poll.
func (r *Reader) fetch(ctx context.Context) {
	fetches := r.client.PollFetches(ctx)
	fetches.EachError(func(topic string, _ int32, err error) {
		// A topicless error is the poller's own context ending, which ends its read alone, or one that ends every read.
		if topic == "" && ctx.Err() != nil {
			return
		}
		for name, current := range r.reads {
			if topic == "" || name == topic {
				r.finish(name, current, err)
			}
		}
	})
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		current := r.reads[partition.Topic]
		if current == nil {
			return
		}
		end, wanted := current.remaining[partition.Partition]
		if !wanted {
			return
		}
		for _, record := range partition.Records {
			if record.Offset < end && !record.Attrs.IsControl() {
				current.records = append(current.records, record)
			}
			if record.Offset >= end-1 {
				delete(current.remaining, partition.Partition)
			}
		}
		if len(partition.Records) == 0 && partition.LastStableOffset >= end {
			delete(current.remaining, partition.Partition)
		}
		if len(current.remaining) == 0 {
			r.finish(partition.Topic, current, nil)
		}
	})
}

// Load folds wherry.admin as client's principal; it fails closed.
func Load(ctx context.Context, client *kgo.Client) (State, error) {
	reader := NewReader(client)
	defer reader.Close()
	return reader.Load(ctx)
}

// Load is the package's Load through r.
func (r *Reader) Load(ctx context.Context) (State, error) {
	raw, err := readRaw(ctx, r)
	if err != nil {
		return State{}, err
	}
	return load(raw)
}

func readRaw(ctx context.Context, reader *Reader) ([]rawRecord, error) {
	records, err := reader.ReadFrom(ctx, AdminTopic, 0)
	if err != nil {
		return nil, err
	}
	raw := make([]rawRecord, len(records))
	for i, record := range records {
		raw[i] = rawRecord{record.Offset, record.Value}
	}
	return raw, nil
}

// ReadKeys folds each owner's key topic. A record that breaks the
// registration rules is ignored and the fold continues from the prior state.
func ReadKeys(ctx context.Context, client *kgo.Client, topics map[string]string) (signed.Keys, error) {
	reader := NewReader(client)
	defer reader.Close()
	return reader.ReadKeys(ctx, topics)
}

// ReadKeys is the package's ReadKeys through r, which reads the topics together.
func (r *Reader) ReadKeys(ctx context.Context, topics map[string]string) (signed.Keys, error) {
	offsets := map[string]int64{}
	for _, topic := range topics {
		offsets[topic] = 0
	}
	reads, err := r.readFrom(ctx, offsets)
	if err != nil {
		return nil, err
	}
	keys := signed.Keys{}
	for owner, topic := range topics {
		if err := reads[topic].err; err != nil {
			return nil, err
		}
		state := signed.KeyState{}
		for _, record := range reads[topic].records {
			// ApplyKeyRecord returns the prior state with its error.
			state, _ = signed.ApplyKeyRecord(state, owner, record.Value, record.Timestamp)
		}
		keys[owner] = state
	}
	return keys, nil
}

// PoolKeyTopics names the key topics of a pool's receivers.
func PoolKeyTopics(state State, pool string) map[string]string {
	topics := map[string]string{}
	for receiver, config := range state.Receivers {
		if config.Pool == pool && !state.Ended[ReceiverPrincipal(receiver)] {
			topics[receiver] = ReceiverTopic(receiver)
		}
	}
	return topics
}

// HostKeyTopics names the key topics of the live hosts of tenants entitled
// to pool.
func HostKeyTopics(state State, pool string) map[string]string {
	topics := map[string]string{}
	for principal, host := range state.Hosts {
		if state.Entitlements[host.Tenant][pool] && !state.Ended[principal] {
			topics[signed.HostOwner(host.Tenant, host.Name)] = HostTopic(host.Tenant, host.Name)
		}
	}
	return topics
}

// ReadAdvertisements returns each receiver's latest authorised advertisement
// that is no older than liveness at now and no later than liveness ahead of it.
func (r *Reader) ReadAdvertisements(ctx context.Context, state State, keys signed.Keys, now time.Time, liveness time.Duration) (map[string]wire.Advertisement, error) {
	records, err := r.ReadFrom(ctx, ReceiversTopic, 0)
	if err != nil {
		return nil, err
	}
	// A record verifies only if its payload is the one read here unverified, so its receiver and time decide which
	// records could win before any is verified.
	type candidate struct {
		at    int64
		value []byte
	}
	candidates := map[string][]candidate{}
	for _, record := range slices.Backward(records) {
		jws, err := jose.ParseSignedCompact(string(record.Value), []jose.SignatureAlgorithm{jose.EdDSA})
		if err != nil {
			continue
		}
		var unverified struct {
			Receiver string `json:"receiver"`
			AtMS     int64  `json:"at_ms"`
		}
		if json.Unmarshal(jws.UnsafePayloadWithoutVerification(), &unverified) != nil {
			continue
		}
		if age := now.Sub(time.UnixMilli(unverified.AtMS)); age > liveness || age < -liveness {
			continue
		}
		candidates[unverified.Receiver] = append(candidates[unverified.Receiver], candidate{unverified.AtMS, record.Value})
	}
	latest := map[string]wire.Advertisement{}
	for receiver, records := range candidates {
		// Newest first; of equal times the later record, as they are read in reverse.
		slices.SortStableFunc(records, func(a, b candidate) int { return cmp.Compare(b.at, a.at) })
		for _, record := range records {
			if advertisement, err := AuthorizeAdvertisement(state, keys, record.value); err == nil {
				latest[receiver] = advertisement
				break
			}
		}
	}
	return latest, nil
}
