// Package sim drives the controller against the in-process broker on a virtual clock.
package sim

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/controller"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/fakebroker"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/scale"
)

// Burst is a period of higher arrivals.
type Burst struct {
	Start    time.Duration
	Duration time.Duration
	Rate     float64 // messages/s during the burst
}

// Config describes one simulated run.
type Config struct {
	Partitions     int
	WorkerRate     float64 // true messages/s per worker in the simulated broker
	Baseline       float64 // messages/s outside bursts
	Bursts         []Burst
	Noise          float64 // arrivals vary uniformly by +-Noise each step
	Seed           uint64
	Duration       time.Duration
	Step           time.Duration
	Interval       time.Duration // controller tick
	Startup        time.Duration // delay before added workers start processing
	InitialWorkers int
	Policy         scale.Policy
	Scale          bool // false keeps InitialWorkers (the static reference run)
}

// Point is the state at one controller tick.
type Point struct {
	T       time.Duration
	Arrival float64
	Lag     int64
	Workers int
	Desired int
	Basis   string
}

// Report is the outcome of a run.
type Report struct {
	Produced, Processed int64
	FinalLag, MaxLag    int64
	MaxWait, P99, P50   time.Duration
	WorkerSeconds       float64
	PeakWorkers         int
	Decisions, Changes  int
	ShortDecisions      int // unlimited measured, remembered or configured decisions below the required rate (must be 0)
	Timeline            []Point
}

// Validate checks the run parameters.
func (c Config) Validate() error {
	switch {
	case c.Step <= 0 || c.Interval < c.Step || c.Interval%c.Step != 0:
		return fmt.Errorf("step %v and interval %v: need 0 < step <= interval, interval a multiple of step", c.Step, c.Interval)
	case c.Duration < c.Interval || c.Duration > 7*24*time.Hour:
		return fmt.Errorf("duration %v outside [interval, 7 days]", c.Duration)
	case c.Baseline < 0 || c.Noise < 0 || c.Noise >= 1:
		return errors.New("baseline must be >= 0 and noise in [0, 1)")
	case c.Startup < 0 || c.InitialWorkers < 0:
		return errors.New("startup and initial workers must be >= 0")
	}
	for _, b := range c.Bursts {
		if b.Start < 0 || b.Duration <= 0 || b.Rate < 0 {
			return fmt.Errorf("invalid burst %+v", b)
		}
	}
	return c.Policy.Validate()
}

// arrivalRate is the nominal rate at offset t.
func (c Config) arrivalRate(t time.Duration) float64 {
	for _, b := range c.Bursts {
		if t >= b.Start && t < b.Start+b.Duration {
			return b.Rate
		}
	}
	return c.Baseline
}

// pending is a scale-up waiting for its startup delay.
type pending struct {
	at time.Duration
	n  int
}

// Run simulates the whole period and returns the report.
func Run(cfg Config) (Report, error) {
	if err := cfg.Validate(); err != nil {
		return Report{}, err
	}
	const group = "sim-workers"
	b, err := fakebroker.New(group, "jobs", cfg.Partitions, cfg.WorkerRate)
	if err != nil {
		return Report{}, err
	}
	if err := b.SetWorkers(cfg.InitialWorkers); err != nil {
		return Report{}, err
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var now time.Duration
	ctl, err := controller.New(b, b, map[string]scale.Policy{group: cfg.Policy}, 0.5, func() time.Time { return start.Add(now) })
	if err != nil {
		return Report{}, err
	}
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	ctx := context.Background()
	var (
		rep    Report
		carry  float64
		queued []pending
	)
	workers := cfg.InitialWorkers
	ticksPerDecision := int(cfg.Interval / cfg.Step)
	for step := 0; now < cfg.Duration; step++ {
		if step%ticksPerDecision == 0 && cfg.Scale {
			recs, err := ctl.Tick(ctx)
			if err != nil {
				return Report{}, err
			}
			r := recs[0]
			rep.Decisions++
			// the idle bound understates capacity and holds on purpose (scale.Decide), so it is not counted
			if r.Limit == "" && r.Basis != scale.BasisNone && r.Basis != scale.BasisIdle && float64(r.Desired)*r.PerWorker < r.Required-1e-6 {
				rep.ShortDecisions++
			}
			rep.Timeline = append(rep.Timeline, Point{T: now, Arrival: r.ArrivalRate, Lag: r.Lag, Workers: workers, Desired: r.Desired, Basis: r.Basis})
			if queued, err = scaleTo(b, &workers, r.Desired, now, cfg.Startup, queued, &rep); err != nil {
				return Report{}, err
			}
		}
		if queued, err = startDue(b, &workers, now, queued); err != nil {
			return Report{}, err
		}
		rate := cfg.arrivalRate(now) * (1 + cfg.Noise*(2*rng.Float64()-1))
		carry += rate * cfg.Step.Seconds()
		n := int64(carry)
		carry -= float64(n)
		b.Produce(start.Add(now), n)
		rep.Produced += n
		now += cfg.Step
		b.Consume(start.Add(now), cfg.Step)
		rep.WorkerSeconds += float64(workers) * cfg.Step.Seconds()
		rep.MaxLag = max(rep.MaxLag, b.Lag())
		rep.PeakWorkers = max(rep.PeakWorkers, workers)
	}
	w := b.Waits()
	rep.Processed, rep.MaxWait, rep.P99, rep.P50, rep.FinalLag = w.Processed, w.Max, w.P99, w.P50, b.Lag()
	return rep, nil
}

// scaleTo applies a decision: scale-down at once, scale-up after the startup delay.
func scaleTo(b *fakebroker.Broker, workers *int, desired int, now, startup time.Duration, queued []pending, rep *Report) ([]pending, error) {
	target := *workers
	if len(queued) > 0 {
		target = queued[len(queued)-1].n
	}
	if desired == target {
		return queued, nil
	}
	rep.Changes++
	if desired == *workers {
		return nil, nil // cancels a pending scale-up
	}
	if desired < *workers || startup == 0 {
		*workers = desired
		return nil, b.SetWorkers(desired)
	}
	return append(queued[:0], pending{at: now + startup, n: desired}), nil
}

// startDue applies scale-ups whose startup delay has passed.
func startDue(b *fakebroker.Broker, workers *int, now time.Duration, queued []pending) ([]pending, error) {
	if len(queued) > 0 && queued[0].at <= now {
		*workers = queued[0].n
		return queued[1:], b.SetWorkers(*workers)
	}
	return queued, nil
}

// Demo is the default scenario used by `make bench` and the README numbers.
func Demo(scaleOn bool) Config {
	return Config{
		Partitions: 32, WorkerRate: 50, Baseline: 100, Noise: 0.2, Seed: 7,
		Bursts: []Burst{
			{Start: 5 * time.Minute, Duration: time.Minute, Rate: 1500},
			{Start: 20 * time.Minute, Duration: 3 * time.Minute, Rate: 600},
			{Start: 35 * time.Minute, Duration: 30 * time.Second, Rate: 3000},
		},
		Duration: 50 * time.Minute, Step: time.Second, Interval: 15 * time.Second, Startup: 10 * time.Second,
		InitialWorkers: 2, Scale: scaleOn,
		Policy: scale.Policy{TargetDrain: 2 * time.Minute, MinWorkers: 1, MaxWorkers: 32, BusyLag: 100, CapToPartitions: true},
	}
}
