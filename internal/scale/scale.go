// Package scale decides how many workers a consumer group needs from its lag and rates.
package scale

import (
	"fmt"
	"math"
	"time"
)

// Limits on policy values, to catch typos such as a target of 0 or a million workers.
const (
	MinTarget  = time.Second
	MaxTarget  = 24 * time.Hour
	MaxWorkers = 10000
)

// Policy is the per-group scaling configuration.
type Policy struct {
	TargetDrain time.Duration // clear the current backlog within this time
	MinWorkers  int
	MaxWorkers  int
	WorkerRate  float64 // configured messages/s one worker handles; 0 = rely on measurements
	BusyLag     int64   // lag above which workers are treated as busy, so measured rate = capacity
	// CapToPartitions limits workers to the partition count (Kafka gives each partition to one consumer).
	CapToPartitions bool
}

// Validate checks the policy ranges.
func (p Policy) Validate() error {
	switch {
	case p.TargetDrain < MinTarget || p.TargetDrain > MaxTarget:
		return fmt.Errorf("target_drain %v outside [%v, %v]", p.TargetDrain, MinTarget, MaxTarget)
	case p.MinWorkers < 1:
		return fmt.Errorf("min_workers %d below 1 (scale to zero is not in M1)", p.MinWorkers)
	case p.MaxWorkers < p.MinWorkers || p.MaxWorkers > MaxWorkers:
		return fmt.Errorf("max_workers %d outside [min_workers=%d, %d]", p.MaxWorkers, p.MinWorkers, MaxWorkers)
	case p.WorkerRate < 0 || math.IsNaN(p.WorkerRate) || math.IsInf(p.WorkerRate, 0):
		return fmt.Errorf("worker_rate %v must be a finite number >= 0", p.WorkerRate)
	case p.BusyLag < 0:
		return fmt.Errorf("busy_lag %d below 0", p.BusyLag)
	}
	return nil
}

// Input is what the controller measured for one group.
type Input struct {
	Lag         int64
	Partitions  int
	Workers     int     // workers running now
	RateWorkers int     // workers that produced Process (the larger count across the interval); 0 = Workers
	Arrival     float64 // messages/s produced
	Process     float64 // messages/s committed
	RatesKnown  bool
	Remembered  float64 // last per-worker rate measured while busy; 0 = none
}

// Basis names where the per-worker capacity came from.
const (
	BasisMeasured   = "measured"    // measured while busy
	BasisRemembered = "remembered"  // last busy measurement
	BasisConfigured = "configured"  // policy worker_rate
	BasisIdle       = "idle-bound"  // measured while not busy: a lower bound on capacity
	BasisNone       = "none"        // nothing known yet
	LimitMin        = "min_workers" // desired raised to the minimum
	LimitMax        = "max_workers" // desired cut to the maximum
	LimitPartitions = "partitions"  // desired cut to the partition count
)

// Decision is the recommended worker count and how it was reached.
type Decision struct {
	Desired        int
	Required       float64       // messages/s needed: arrival + lag / target
	PerWorker      float64       // capacity used for one worker, messages/s
	Basis          string        // one of the Basis constants
	Limit          string        // empty, or one of the Limit constants
	ProjectedDrain time.Duration // time to clear the lag at Desired workers; -1 if it never clears
	Reason         string
}

// Decide applies Little's law to the backlog: to clear L messages within T while A keep arriving,
// the group must process A + L/T messages per second; workers = ceil(that / per-worker rate).
func Decide(p Policy, in Input) (Decision, error) {
	if err := p.Validate(); err != nil {
		return Decision{}, err
	}
	if err := in.validate(); err != nil {
		return Decision{}, err
	}
	arrival := 0.0
	if in.RatesKnown {
		arrival = in.Arrival
	}
	d := Decision{Required: arrival + float64(in.Lag)/p.TargetDrain.Seconds()}
	d.PerWorker, d.Basis = capacity(p, in)
	if d.Basis == BasisNone {
		d.Desired = in.Workers
		if in.Lag > 0 && in.Workers == 0 {
			d.Desired = 1 // start one worker so there is something to measure
		}
		d.Reason = "no throughput measurement yet: holding the current count"
	} else {
		d.Desired = int(math.Ceil(d.Required/d.PerWorker - 1e-9))
		d.Reason = fmt.Sprintf("need %.1f msg/s (arrival %.1f + lag %d / %v) at %.1f msg/s per worker (%s)",
			d.Required, arrival, in.Lag, p.TargetDrain, d.PerWorker, d.Basis)
		if d.Basis == BasisIdle && d.Desired > in.Workers && in.Workers > 0 {
			// workers keep up and the idle rate understates capacity: no evidence that more are needed
			d.Desired = in.Workers
			d.Reason += "; not busy, so holding the current count"
		}
	}
	upper, upperName := p.MaxWorkers, LimitMax
	if p.CapToPartitions && in.Partitions > 0 && in.Partitions < upper {
		upper, upperName = in.Partitions, LimitPartitions
	}
	if d.Desired < p.MinWorkers {
		d.Desired, d.Limit = p.MinWorkers, LimitMin
	}
	if d.Desired > upper {
		d.Desired, d.Limit = upper, upperName
	}
	d.ProjectedDrain = drainTime(in.Lag, arrival, float64(d.Desired)*d.PerWorker)
	return d, nil
}

// capacity picks the per-worker rate: measured while busy, then remembered, configured, idle bound.
func capacity(p Policy, in Input) (float64, string) {
	measured, n := 0.0, in.Workers
	if in.RateWorkers > 0 {
		n = in.RateWorkers
	}
	if in.RatesKnown && n > 0 && in.Process > 0 {
		measured = in.Process / float64(n)
	}
	switch {
	case measured > 0 && in.Lag > p.BusyLag:
		return measured, BasisMeasured
	case in.Remembered > 0:
		return in.Remembered, BasisRemembered
	case p.WorkerRate > 0:
		return p.WorkerRate, BasisConfigured
	case measured > 0:
		return measured, BasisIdle
	}
	return 0, BasisNone
}

// drainTime is lag / (capacity - arrival), or -1 when capacity does not exceed arrivals.
func drainTime(lag int64, arrival, capacity float64) time.Duration {
	if lag == 0 {
		return 0
	}
	net := capacity - arrival
	if net <= 0 {
		return -1
	}
	return time.Duration(float64(lag) / net * float64(time.Second))
}

// validate rejects negative or non-finite measurements.
func (in Input) validate() error {
	bad := func(f float64) bool { return f < 0 || math.IsNaN(f) || math.IsInf(f, 0) }
	switch {
	case in.Lag < 0:
		return fmt.Errorf("negative lag %d", in.Lag)
	case in.Workers < 0 || in.Workers > MaxWorkers || in.RateWorkers < 0 || in.RateWorkers > MaxWorkers:
		return fmt.Errorf("workers %d / rate workers %d outside [0, %d]", in.Workers, in.RateWorkers, MaxWorkers)
	case in.Partitions < 0:
		return fmt.Errorf("negative partition count %d", in.Partitions)
	case bad(in.Arrival) || bad(in.Process) || bad(in.Remembered):
		return fmt.Errorf("rates must be finite and >= 0 (arrival %v, process %v, remembered %v)", in.Arrival, in.Process, in.Remembered)
	}
	return nil
}
