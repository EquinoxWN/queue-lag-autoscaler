// Package rate turns successive offset snapshots into arrival and processing rates.
package rate

import (
	"fmt"
	"time"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/lag"
)

// Rates are messages per second for one consumer group.
// Only arrivals are smoothed: the processing rate depends on the worker count, which changes between intervals.
type Rates struct {
	Arrival float64 // growth of log-end offsets (producers), smoothed
	Process float64 // growth of committed offsets (consumers) over the last interval only
	Known   bool    // false until two snapshots were seen
}

// Estimator keeps the previous snapshot of one group and a smoothed rate.
type Estimator struct {
	alpha  float64
	prev   map[lag.Key]lag.Offset
	prevAt time.Time
	have   bool
	rates  Rates
}

// New returns an estimator; alpha in (0, 1] is the weight of the newest interval (1 = no smoothing).
func New(alpha float64) (*Estimator, error) {
	if !(alpha > 0 && alpha <= 1) {
		return nil, fmt.Errorf("smoothing alpha %v outside (0, 1]", alpha)
	}
	return &Estimator{alpha: alpha}, nil
}

// Observe records a snapshot taken at `at` and returns the updated rates.
// Elapsed time comes from time.Time.Sub, which uses the monotonic reading when both times carry one.
func (e *Estimator) Observe(at time.Time, g lag.Group) Rates {
	if !e.have {
		e.store(at, g)
		return e.rates
	}
	elapsed := at.Sub(e.prevAt).Seconds()
	if elapsed <= 0 {
		return e.rates // same or earlier instant: no new information
	}
	var produced, committed int64
	for k, now := range g.Partitions {
		before, ok := e.prev[k]
		if !ok {
			continue // new partition: no baseline yet
		}
		if d := now.LogEnd - before.LogEnd; d > 0 {
			produced += d
		}
		if before.Committed >= 0 && now.Committed >= before.Committed {
			committed += now.Committed - before.Committed
		}
		// a committed offset that moved backwards is a reset; it adds nothing
	}
	arrival, process := float64(produced)/elapsed, float64(committed)/elapsed
	if e.rates.Known {
		arrival = e.alpha*arrival + (1-e.alpha)*e.rates.Arrival
	}
	e.rates = Rates{Arrival: arrival, Process: process, Known: true}
	e.store(at, g)
	return e.rates
}

// store keeps a copy of the snapshot as the next baseline.
func (e *Estimator) store(at time.Time, g lag.Group) {
	e.prev = make(map[lag.Key]lag.Offset, len(g.Partitions))
	for k, o := range g.Partitions {
		e.prev[k] = o
	}
	e.prevAt = at
	e.have = true
}
