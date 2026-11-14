// Package fakebroker is an in-process stand-in for a Kafka topic and one consumer group (tests and the demo).
package fakebroker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/lag"
)

// maxWaitBucket caps the wait histogram (seconds).
const maxWaitBucket = 7 * 24 * 3600

// chunk is a run of messages produced at the same instant.
type chunk struct {
	at time.Time
	n  int64
}

// partition is one partition's log and the group's committed offset.
type partition struct {
	logEnd, committed int64
	queue             []chunk
}

// Broker models one topic, one consumer group and a pool of equal workers; it is safe for concurrent use.
type Broker struct {
	mu        sync.Mutex
	group     string
	topic     string
	parts     []*partition
	perWorker float64
	workers   int
	credit    []float64
	next      int // round-robin start for produce remainders
	waits     map[int64]int64
	maxWait   time.Duration
	processed int64
}

// New creates a broker with the given partitions and per-worker processing rate (messages/s).
func New(group, topic string, partitions int, perWorker float64) (*Broker, error) {
	if !lag.ValidGroup(group) {
		return nil, fmt.Errorf("invalid group %q", group)
	}
	if err := (lag.Offset{Group: group, Topic: topic}).Validate(); err != nil {
		return nil, err
	}
	if partitions < 1 || partitions > 10000 {
		return nil, fmt.Errorf("partitions %d outside [1, 10000]", partitions)
	}
	if !(perWorker > 0) {
		return nil, errors.New("per-worker rate must be positive")
	}
	b := &Broker{group: group, topic: topic, perWorker: perWorker, waits: map[int64]int64{}}
	for range partitions {
		b.parts = append(b.parts, &partition{})
	}
	return b, nil
}

// Produce appends n messages at time at, spread evenly over partitions.
func (b *Broker) Produce(at time.Time, n int64) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	np := int64(len(b.parts))
	each, rem := n/np, n%np
	for i := range np {
		p := b.parts[(int64(b.next)+i)%np]
		k := each
		if i < rem {
			k++
		}
		if k > 0 {
			p.logEnd += k
			p.queue = append(p.queue, chunk{at, k})
		}
	}
	b.next = int((int64(b.next) + rem) % np)
}

// SetWorkers changes the worker count (taking effect on the next Consume).
func (b *Broker) SetWorkers(n int) error {
	if n < 0 {
		return fmt.Errorf("negative worker count %d", n)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n != b.workers {
		b.workers = n
		b.credit = make([]float64, n)
	}
	return nil
}

// Workers reports the running worker count for the broker's group.
func (b *Broker) Workers(_ context.Context, group string) (int, error) {
	if group != b.group {
		return 0, fmt.Errorf("unknown group %s", group)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.workers, nil
}

// Consume lets every worker process up to perWorker*dt messages, oldest first, and returns how many were processed.
// Partition p belongs to worker p mod w, so workers beyond the partition count stay idle, as in Kafka.
func (b *Broker) Consume(now time.Time, dt time.Duration) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	active := min(b.workers, len(b.parts))
	var total int64
	for w := range active {
		b.credit[w] += b.perWorker * dt.Seconds()
		budget := int64(b.credit[w])
		done := b.serve(w, active, budget, now)
		total += done
		if done < budget {
			b.credit[w] = 0 // ran out of work: idle capacity is not banked
		} else {
			b.credit[w] -= float64(done)
		}
	}
	b.processed += total
	return total
}

// serve processes up to budget messages from worker w's partitions, oldest chunk first.
func (b *Broker) serve(w, active int, budget int64, now time.Time) int64 {
	var done int64
	for budget > 0 {
		var best *partition
		for i := w; i < len(b.parts); i += active {
			p := b.parts[i]
			if len(p.queue) > 0 && (best == nil || p.queue[0].at.Before(best.queue[0].at)) {
				best = p
			}
		}
		if best == nil {
			break
		}
		head := &best.queue[0]
		m := min(budget, head.n)
		head.n -= m
		best.committed += m
		b.recordWait(now.Sub(head.at), m)
		if head.n == 0 {
			best.queue = best.queue[1:]
		}
		budget -= m
		done += m
	}
	return done
}

// recordWait adds m messages that waited d to the histogram.
func (b *Broker) recordWait(d time.Duration, m int64) {
	if d > b.maxWait {
		b.maxWait = d
	}
	s := min(int64(d/time.Second), maxWaitBucket)
	b.waits[s] += m
}

// Offsets returns the group's committed and log-end offsets per partition.
func (b *Broker) Offsets(context.Context) ([]lag.Offset, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]lag.Offset, len(b.parts))
	for i, p := range b.parts {
		out[i] = lag.Offset{Group: b.group, Topic: b.topic, Partition: int32(i), Committed: p.committed, LogEnd: p.logEnd}
	}
	return out, nil
}

// Lag returns the uncommitted messages over all partitions.
func (b *Broker) Lag() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var total int64
	for _, p := range b.parts {
		total += p.logEnd - p.committed
	}
	return total
}

// WaitStats summarises how long processed messages waited between produce and processing.
type WaitStats struct {
	Processed int64
	Max       time.Duration
	P99       time.Duration // whole seconds, rounded down
	P50       time.Duration
}

// Waits returns the wait statistics so far.
func (b *Broker) Waits() WaitStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]int64, 0, len(b.waits))
	var total int64
	for k, v := range b.waits {
		keys = append(keys, k)
		total += v
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	st := WaitStats{Processed: b.processed, Max: b.maxWait}
	st.P50 = quantile(keys, b.waits, total, 0.50)
	st.P99 = quantile(keys, b.waits, total, 0.99)
	return st
}

// quantile returns the smallest bucket whose cumulative count reaches q of the total.
func quantile(keys []int64, counts map[int64]int64, total int64, q float64) time.Duration {
	if total == 0 {
		return 0
	}
	need := int64(q*float64(total) + 0.5)
	var seen int64
	for _, k := range keys {
		seen += counts[k]
		if seen >= need {
			return time.Duration(k) * time.Second
		}
	}
	return time.Duration(keys[len(keys)-1]) * time.Second
}
