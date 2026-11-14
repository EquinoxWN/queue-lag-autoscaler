// Package controller runs the read-measure-decide loop for each configured consumer group.
package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/lag"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/rate"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/scale"
)

// Source returns the current offsets of every group it can see.
type Source interface {
	Offsets(ctx context.Context) ([]lag.Offset, error)
}

// WorkerCounter reports how many workers a group runs now.
type WorkerCounter interface {
	Workers(ctx context.Context, group string) (int, error)
}

// Static is a WorkerCounter with fixed counts (M1 does not read Kubernetes).
type Static map[string]int

// Workers returns the configured count.
func (s Static) Workers(_ context.Context, group string) (int, error) {
	n, ok := s[group]
	if !ok {
		return 0, fmt.Errorf("no worker count for group %s", group)
	}
	return n, nil
}

// Recommendation is one decision for one group, as printed by the CLI.
type Recommendation struct {
	At                    time.Time `json:"at"`
	Group                 string    `json:"group"`
	Found                 bool      `json:"found"`
	Partitions            int       `json:"partitions"`
	Lag                   int64     `json:"lag"`
	ArrivalRate           float64   `json:"arrival_per_s"`
	ProcessRate           float64   `json:"process_per_s"`
	RatesKnown            bool      `json:"rates_known"`
	Workers               int       `json:"workers"`
	Desired               int       `json:"desired"`
	Required              float64   `json:"required_per_s"`
	PerWorker             float64   `json:"per_worker_per_s"`
	Basis                 string    `json:"basis"`
	Limit                 string    `json:"limit,omitempty"`
	ProjectedDrainSeconds float64   `json:"projected_drain_s"`
	Reason                string    `json:"reason"`
}

// Controller holds one rate estimator per group; Tick is safe to call from several goroutines.
type Controller struct {
	mu          sync.Mutex
	src         Source
	workers     WorkerCounter
	policies    map[string]scale.Policy
	est         map[string]*rate.Estimator
	remembered  map[string]float64
	prevWorkers map[string]int
	now         func() time.Time
}

// New validates the policies and builds a controller; now defaults to time.Now.
func New(src Source, workers WorkerCounter, policies map[string]scale.Policy, smoothing float64, now func() time.Time) (*Controller, error) {
	if src == nil || workers == nil {
		return nil, errors.New("controller needs a source and a worker counter")
	}
	if len(policies) == 0 {
		return nil, errors.New("controller needs at least one group policy")
	}
	if now == nil {
		now = time.Now
	}
	c := &Controller{src: src, workers: workers, policies: map[string]scale.Policy{}, est: map[string]*rate.Estimator{},
		remembered: map[string]float64{}, prevWorkers: map[string]int{}, now: now}
	for name, p := range policies {
		if !lag.ValidGroup(name) {
			return nil, fmt.Errorf("invalid group name %q", name)
		}
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("group %s: %w", name, err)
		}
		e, err := rate.New(smoothing)
		if err != nil {
			return nil, err
		}
		c.policies[name], c.est[name] = p, e
	}
	return c, nil
}

// Tick reads one snapshot and returns a recommendation per configured group, sorted by name.
func (c *Controller) Tick(ctx context.Context) ([]Recommendation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	at := c.now() // keeps the monotonic reading for rate intervals
	offs, err := c.src.Offsets(ctx)
	if err != nil {
		return nil, fmt.Errorf("read offsets: %w", err)
	}
	groups, err := lag.ByGroup(offs)
	if err != nil {
		return nil, fmt.Errorf("read offsets: %w", err)
	}
	names := make([]string, 0, len(c.policies))
	for n := range c.policies {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]Recommendation, 0, len(names))
	for _, name := range names {
		r, err := c.decide(ctx, at, name, groups)
		if err != nil {
			return nil, fmt.Errorf("group %s: %w", name, err)
		}
		out = append(out, r)
	}
	return out, nil
}

// decide measures one group and applies its policy.
func (c *Controller) decide(ctx context.Context, at time.Time, name string, groups map[string]lag.Group) (Recommendation, error) {
	workers, err := c.workers.Workers(ctx, name)
	if err != nil {
		return Recommendation{}, err
	}
	r := Recommendation{At: at.UTC(), Group: name, Workers: workers}
	g, ok := groups[name]
	if !ok {
		r.Desired, r.Basis, r.ProjectedDrainSeconds = workers, scale.BasisNone, -1
		r.Reason = "group not found in the source: holding the current count"
		return r, nil
	}
	rates := c.est[name].Observe(at, g)
	// the processing rate covers the interval since the last tick; dividing by the larger worker count
	// of its two ends never overstates what one worker handled
	rateWorkers := max(workers, c.prevWorkers[name])
	stable := c.prevWorkers[name] == workers
	c.prevWorkers[name] = workers
	in := scale.Input{
		Lag: g.Lag(), Partitions: len(g.Partitions), Workers: workers, RateWorkers: rateWorkers,
		Arrival: rates.Arrival, Process: rates.Process, RatesKnown: rates.Known, Remembered: c.remembered[name],
	}
	d, err := scale.Decide(c.policies[name], in)
	if err != nil {
		return Recommendation{}, err
	}
	if d.Basis == scale.BasisMeasured && stable {
		c.remembered[name] = d.PerWorker // only a clean interval: the worker count did not change
	}
	r.Found, r.Partitions, r.Lag = true, in.Partitions, in.Lag
	r.ArrivalRate, r.ProcessRate, r.RatesKnown = rates.Arrival, rates.Process, rates.Known
	r.Desired, r.Required, r.PerWorker, r.Basis, r.Limit, r.Reason = d.Desired, d.Required, d.PerWorker, d.Basis, d.Limit, d.Reason
	r.ProjectedDrainSeconds = -1
	if d.ProjectedDrain >= 0 {
		r.ProjectedDrainSeconds = d.ProjectedDrain.Seconds()
	}
	return r, nil
}

// Run ticks every interval until ctx ends; it stops after maxFailures consecutive failed ticks.
func (c *Controller) Run(ctx context.Context, interval time.Duration, maxFailures int, emit func([]Recommendation) error, onErr func(error)) error {
	if interval <= 0 {
		return fmt.Errorf("interval %v must be positive", interval)
	}
	if maxFailures < 1 {
		return fmt.Errorf("maxFailures %d must be at least 1", maxFailures)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	failures := 0
	for {
		recs, err := c.Tick(ctx)
		if err == nil {
			failures = 0
			if err := emit(recs); err != nil {
				return fmt.Errorf("emit: %w", err)
			}
		} else {
			if ctx.Err() != nil {
				return nil
			}
			failures++
			onErr(err)
			if failures >= maxFailures {
				return fmt.Errorf("%d consecutive failed ticks, last: %w", failures, err)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
