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
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// ReadTopic reads a topic from its start to its last stable offset with a
// dedicated read_committed reader built from client's options. client must be
// a plain client: a transactional ID in its options would fence its producer.
func ReadTopic(ctx context.Context, client *kgo.Client, topic string) ([]*kgo.Record, error) {
	return ReadTopicFrom(ctx, client, topic, 0)
}

// ReadTopicFrom is ReadTopic from offset, or from the start of the topic's one partition where that is later.
func ReadTopicFrom(ctx context.Context, client *kgo.Client, topic string, offset int64) ([]*kgo.Record, error) {
	reader := NewReader(client)
	defer reader.Close()
	return reader.ReadFrom(ctx, topic, offset)
}

// Reader reads topics as ReadTopicFrom does through one read_committed client, built from a plain client's options on
// its first read, so its reads share that client's connections and concurrent reads share its fetches. Reads of one
// topic run one at a time.
type Reader struct {
	base *kgo.Client
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
}

// NewReader returns a Reader that lists offsets through client and reads with a client built from its options.
func NewReader(client *kgo.Client) *Reader {
	return &Reader{base: client, poll: make(chan struct{}, 1), reads: map[string]*read{}}
}

// Close closes the reader's client; reads in progress and later reads fail.
func (r *Reader) Close() {
	r.poll <- struct{}{}
	defer func() { <-r.poll }()
	r.closed = true
	if r.client != nil {
		r.client.Close()
	}
}

// ReadFrom reads topic from offset, or from the start of the topic's one partition where that is later, to its last
// stable offset.
func (r *Reader) ReadFrom(ctx context.Context, topic string, offset int64) ([]*kgo.Record, error) {
	admin := kadm.NewClient(r.base)
	starts, err := admin.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	var ends kadm.ListedOffsets
	if err == nil {
		ends, err = admin.ListCommittedOffsets(ctx, topic)
	}
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return nil, err
	}
	current := &read{remaining: map[int32]int64{}, partitions: map[int32]kgo.Offset{}, done: make(chan struct{})}
	ends.Each(func(end kadm.ListedOffset) {
		if start, ok := starts.Lookup(topic, end.Partition); ok && max(start.Offset, offset) < end.Offset {
			current.remaining[end.Partition] = end.Offset
			current.partitions[end.Partition] = kgo.NewOffset().At(max(start.Offset, offset))
		}
	})
	if len(current.remaining) == 0 {
		return nil, nil
	}
	registered := false
	for {
		select {
		case <-current.done:
			return current.records, current.err
		case <-ctx.Done():
			if registered {
				// The client fetches a read's partitions until they are removed.
				select {
				case r.poll <- struct{}{}:
					if r.reads[topic] == current {
						r.finish(topic, current, ctx.Err())
					}
					<-r.poll
				case <-current.done:
				}
			}
			return nil, ctx.Err()
		case r.poll <- struct{}{}:
		}
		if !registered {
			if other := r.reads[topic]; other != nil {
				<-r.poll
				select {
				case <-other.done:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}
			if err := r.add(topic, current); err != nil {
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

// add starts consuming current's partitions; the caller holds poll.
func (r *Reader) add(topic string, current *read) error {
	if r.closed {
		return kgo.ErrClientClosed
	}
	if r.client == nil {
		client, err := kgo.NewClient(append(slices.Clone(r.base.Opts()),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			// A trailing abort marker is the last offset below the bound; it must be seen to finish.
			kgo.KeepControlRecords(),
			kgo.DisableClientMetrics())...)
		if err != nil {
			return err
		}
		r.client = client
	}
	r.reads[topic] = current
	r.client.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: maps.Clone(current.partitions)})
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

// ReadKeys is the package's ReadKeys through r, which reads the topics concurrently.
func (r *Reader) ReadKeys(ctx context.Context, topics map[string]string) (signed.Keys, error) {
	var mu sync.Mutex
	keys := signed.Keys{}
	group, ctx := errgroup.WithContext(ctx)
	for owner, topic := range topics {
		group.Go(func() error {
			records, err := r.ReadFrom(ctx, topic, 0)
			if err != nil {
				return err
			}
			state := signed.KeyState{}
			for _, record := range records {
				// ApplyKeyRecord returns the prior state with its error.
				state, _ = signed.ApplyKeyRecord(state, owner, record.Value, record.Timestamp)
			}
			mu.Lock()
			defer mu.Unlock()
			keys[owner] = state
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
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
