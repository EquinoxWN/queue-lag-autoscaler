package scale

import (
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

func policy() Policy {
	return Policy{TargetDrain: 2 * time.Minute, MinWorkers: 1, MaxWorkers: 50, CapToPartitions: true}
}

func TestLittlesLawWorkedExample(t *testing.T) {
	// 12,000 behind, 100/s arriving, target 2 min: need 100 + 12000/120 = 200/s.
	// 4 workers process 160/s while busy: 40/s each, so ceil(200/40) = 5 workers.
	d, err := Decide(policy(), Input{Lag: 12000, Partitions: 32, Workers: 4, Arrival: 100, Process: 160, RatesKnown: true})
	if err != nil {
		t.Fatal(err)
	}
	if d.Desired != 5 || d.Basis != BasisMeasured || d.Limit != "" || math.Abs(d.Required-200) > 1e-9 || d.PerWorker != 40 {
		t.Fatalf("got %+v", d)
	}
	// 5 workers = 200/s, net 100/s over arrivals: 12000/100 = 120 s
	if d.ProjectedDrain != 2*time.Minute {
		t.Fatalf("projected drain %v, want 2m", d.ProjectedDrain)
	}
	if !strings.Contains(d.Reason, "need 200.0 msg/s") {
		t.Fatalf("reason %q", d.Reason)
	}
}

func TestExactMultipleNeedsNoExtraWorker(t *testing.T) {
	// required exactly 3 workers' worth must give 3, not 4 (float rounding guard)
	d, _ := Decide(policy(), Input{Lag: 0, Workers: 1, Arrival: 0.3, Process: 0.1, RatesKnown: true, Remembered: 0.1})
	if d.Desired != 3 {
		t.Fatalf("desired %d, want 3 (0.3/0.1 in floating point is 2.9999999999999996)", d.Desired)
	}
}

func TestLimits(t *testing.T) {
	base := Input{Lag: 120000, Partitions: 8, Workers: 2, Arrival: 0, Process: 20, RatesKnown: true} // 10/s per worker
	d, _ := Decide(policy(), base)
	if d.Desired != 8 || d.Limit != LimitPartitions {
		t.Fatalf("partition cap: got %d %q, want 8 partitions", d.Desired, d.Limit)
	}
	p := policy()
	p.CapToPartitions = false
	p.MaxWorkers = 20
	if d, _ = Decide(p, base); d.Desired != 20 || d.Limit != LimitMax {
		t.Fatalf("max cap: got %d %q, want 20 max_workers", d.Desired, d.Limit)
	}
	// with 20 workers at 10/s the 120,000 backlog clears in 600 s
	if d.ProjectedDrain != 10*time.Minute {
		t.Fatalf("projected %v, want 10m", d.ProjectedDrain)
	}
	p.MinWorkers = 3
	if d, _ = Decide(p, Input{Lag: 0, Workers: 5, Arrival: 1, Process: 1, RatesKnown: true, Remembered: 50}); d.Desired != 3 || d.Limit != LimitMin {
		t.Fatalf("min: got %d %q, want 3 min_workers", d.Desired, d.Limit)
	}
	// partitions above max: max applies
	if d, _ = Decide(policy(), Input{Lag: 1e9, Partitions: 64, Workers: 1, Process: 1, RatesKnown: true}); d.Desired != 50 || d.Limit != LimitMax {
		t.Fatalf("got %d %q, want 50 max_workers", d.Desired, d.Limit)
	}
	// unknown partition count (0): no partition cap
	if d, _ = Decide(policy(), Input{Lag: 1e9, Workers: 1, Process: 1, RatesKnown: true}); d.Desired != 50 {
		t.Fatalf("got %d, want 50", d.Desired)
	}
}

func TestCapacityBasisOrder(t *testing.T) {
	p := policy()
	p.BusyLag = 100
	p.WorkerRate = 25
	cases := []struct {
		name  string
		in    Input
		basis string
		per   float64
	}{
		{"busy: measured wins", Input{Lag: 101, Workers: 2, Process: 60, RatesKnown: true, Remembered: 99}, BasisMeasured, 30},
		{"lag at busy_lag is not busy", Input{Lag: 100, Workers: 2, Process: 60, RatesKnown: true, Remembered: 99}, BasisRemembered, 99},
		{"idle: remembered", Input{Lag: 0, Workers: 2, Process: 60, RatesKnown: true, Remembered: 45}, BasisRemembered, 45},
		{"idle, none remembered: configured", Input{Lag: 0, Workers: 2, Process: 60, RatesKnown: true}, BasisConfigured, 25},
		{"busy but stalled: configured", Input{Lag: 5000, Workers: 2, Process: 0, RatesKnown: true}, BasisConfigured, 25},
		{"rates unknown: configured", Input{Lag: 5000, Workers: 2, Process: 60}, BasisConfigured, 25},
		{"rate workers divide the measurement", Input{Lag: 500, Workers: 2, RateWorkers: 6, Process: 60, RatesKnown: true}, BasisMeasured, 10},
	}
	for _, c := range cases {
		d, err := Decide(p, c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if d.Basis != c.basis || math.Abs(d.PerWorker-c.per) > 1e-9 {
			t.Errorf("%s: basis %s per %v, want %s %v", c.name, d.Basis, d.PerWorker, c.basis, c.per)
		}
	}
	p.WorkerRate = 0
	if d, _ := Decide(p, Input{Lag: 0, Workers: 2, Process: 60, RatesKnown: true}); d.Basis != BasisIdle || d.PerWorker != 30 {
		t.Fatalf("idle bound: got %s %v", d.Basis, d.PerWorker)
	}
	// idle bound never scales up: arrivals a little above processing would otherwise ratchet the count up
	d, _ := Decide(p, Input{Lag: 50, Workers: 4, Arrival: 66, Process: 60, RatesKnown: true})
	if d.Basis != BasisIdle || d.Desired != 4 || !strings.Contains(d.Reason, "not busy") {
		t.Fatalf("idle bound scale-up: got %+v", d)
	}
	// but it may scale down
	if d, _ = Decide(p, Input{Lag: 0, Workers: 4, Arrival: 10, Process: 60, RatesKnown: true}); d.Desired != 1 {
		t.Fatalf("idle bound scale-down: got %d, want 1", d.Desired)
	}
}

func TestNoMeasurementHoldsOrStartsOne(t *testing.T) {
	p := policy()
	p.MinWorkers = 1
	d, _ := Decide(p, Input{Lag: 500, Workers: 4})
	if d.Desired != 4 || d.Basis != BasisNone {
		t.Fatalf("hold: got %+v", d)
	}
	d, _ = Decide(p, Input{Lag: 500, Workers: 0})
	if d.Desired != 1 {
		t.Fatalf("start one: got %d", d.Desired)
	}
	p.MinWorkers = 2
	if d, _ = Decide(p, Input{Lag: 0, Workers: 0}); d.Desired != 2 || d.Limit != LimitMin {
		t.Fatalf("min applies with no measurement: got %+v", d)
	}
	if d.ProjectedDrain != 0 {
		t.Fatalf("no lag drains in 0, got %v", d.ProjectedDrain)
	}
}

func TestProjectedDrainNeverWhenCapacityBelowArrivals(t *testing.T) {
	p := policy()
	p.MaxWorkers = 2
	p.CapToPartitions = false
	d, _ := Decide(p, Input{Lag: 10, Workers: 2, Arrival: 100, Process: 20, RatesKnown: true})
	if d.Desired != 2 || d.ProjectedDrain != -1 {
		t.Fatalf("got desired %d drain %v, want 2 and -1", d.Desired, d.ProjectedDrain)
	}
	// capacity exactly equal to arrivals: the backlog never shrinks either
	d, _ = Decide(p, Input{Lag: 10, Workers: 2, Arrival: 100, Process: 100, RatesKnown: true})
	if d.Desired != 2 || d.ProjectedDrain != -1 {
		t.Fatalf("equal capacity: got desired %d drain %v, want 2 and -1", d.Desired, d.ProjectedDrain)
	}
}

func TestArrivalIgnoredWhenRatesUnknown(t *testing.T) {
	p := policy()
	p.WorkerRate = 10
	d, _ := Decide(p, Input{Lag: 1200, Workers: 1, Arrival: 1000})
	if d.Required != 10 || d.Desired != 1 {
		t.Fatalf("got required %v desired %d, want 10 and 1", d.Required, d.Desired)
	}
}

func TestRandomizedSufficientAndMinimal(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 20000 {
		p := Policy{
			TargetDrain: time.Duration(1+rng.IntN(3600)) * time.Second,
			MinWorkers:  1 + rng.IntN(3), MaxWorkers: 4 + rng.IntN(200),
			WorkerRate: float64(rng.IntN(3)) * rng.Float64() * 100, BusyLag: int64(rng.IntN(1000)),
			CapToPartitions: rng.IntN(2) == 0,
		}
		in := Input{
			Lag: rng.Int64N(5_000_000), Partitions: rng.IntN(300), Workers: rng.IntN(100),
			Arrival: rng.Float64() * 5000, Process: rng.Float64() * 5000, RatesKnown: rng.IntN(5) > 0,
			Remembered: float64(rng.IntN(2)) * rng.Float64() * 200,
		}
		d, err := Decide(p, in)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		upper := p.MaxWorkers
		if p.CapToPartitions && in.Partitions > 0 {
			upper = min(upper, in.Partitions)
		}
		if d.Desired > upper || (d.Desired < p.MinWorkers && upper >= p.MinWorkers) {
			t.Fatalf("case %d: desired %d outside [%d, %d]: %+v %+v", i, d.Desired, p.MinWorkers, upper, p, in)
		}
		if d.Basis == BasisNone || d.Limit != "" {
			continue
		}
		capacity := float64(d.Desired) * d.PerWorker
		if capacity < d.Required*(1-1e-9) {
			t.Fatalf("case %d: %d workers x %v = %v below required %v", i, d.Desired, d.PerWorker, capacity, d.Required)
		}
		if d.Desired > p.MinWorkers && float64(d.Desired-1)*d.PerWorker >= d.Required*(1+1e-9) {
			t.Fatalf("case %d: %d workers is more than needed for %v at %v each", i, d.Desired, d.Required, d.PerWorker)
		}
		if d.ProjectedDrain > p.TargetDrain+time.Millisecond {
			t.Fatalf("case %d: projected %v beyond target %v", i, d.ProjectedDrain, p.TargetDrain)
		}
	}
}

func TestPolicyValidation(t *testing.T) {
	bad := map[string]Policy{
		"target too short": {TargetDrain: 500 * time.Millisecond, MinWorkers: 1, MaxWorkers: 2},
		"target too long":  {TargetDrain: 25 * time.Hour, MinWorkers: 1, MaxWorkers: 2},
		"min zero":         {TargetDrain: time.Minute, MinWorkers: 0, MaxWorkers: 2},
		"max below min":    {TargetDrain: time.Minute, MinWorkers: 3, MaxWorkers: 2},
		"max too large":    {TargetDrain: time.Minute, MinWorkers: 1, MaxWorkers: MaxWorkers + 1},
		"negative rate":    {TargetDrain: time.Minute, MinWorkers: 1, MaxWorkers: 2, WorkerRate: -1},
		"NaN rate":         {TargetDrain: time.Minute, MinWorkers: 1, MaxWorkers: 2, WorkerRate: math.NaN()},
		"Inf rate":         {TargetDrain: time.Minute, MinWorkers: 1, MaxWorkers: 2, WorkerRate: math.Inf(1)},
		"negative busy":    {TargetDrain: time.Minute, MinWorkers: 1, MaxWorkers: 2, BusyLag: -1},
	}
	for name, p := range bad {
		if _, err := Decide(p, Input{}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := (Policy{TargetDrain: time.Second, MinWorkers: 1, MaxWorkers: 1}).Validate(); err != nil {
		t.Fatalf("edge policy rejected: %v", err)
	}
}

func TestInputValidation(t *testing.T) {
	bad := map[string]Input{
		"negative lag":        {Lag: -1},
		"negative workers":    {Workers: -1},
		"too many workers":    {Workers: MaxWorkers + 1},
		"bad rate workers":    {RateWorkers: -1},
		"negative partitions": {Partitions: -1},
		"negative arrival":    {Arrival: -1},
		"NaN process":         {Process: math.NaN()},
		"Inf remembered":      {Remembered: math.Inf(1)},
	}
	for name, in := range bad {
		if _, err := Decide(policy(), in); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
