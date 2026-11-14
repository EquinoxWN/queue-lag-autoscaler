package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/fakebroker"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/lag"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/scale"
)

// clock is a manual clock for tests.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func policies() map[string]scale.Policy {
	return map[string]scale.Policy{"g": {TargetDrain: time.Minute, MinWorkers: 1, MaxWorkers: 20, CapToPartitions: true}}
}

func TestTwoTicksMeasureAndDecide(t *testing.T) {
	b, _ := fakebroker.New("g", "jobs", 16, 10)
	_ = b.SetWorkers(2)
	clk := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("x", 3600))}
	c, err := New(b, b, policies(), 1, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	b.Produce(clk.t, 3000)
	recs, err := c.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := recs[0]
	if r.RatesKnown || r.Lag != 3000 || r.Desired != 2 || r.Basis != scale.BasisNone || r.Partitions != 16 {
		t.Fatalf("first tick %+v", r)
	}
	if r.At.Location() != time.UTC {
		t.Fatalf("time %v not UTC", r.At)
	}
	// 10 s: 600 more arrive (60/s), 2 workers process 10/s each = 200
	b.Produce(clk.t, 600)
	clk.t = clk.t.Add(10 * time.Second)
	b.Consume(clk.t, 10*time.Second)
	recs, err = c.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r = recs[0]
	// lag 3400; need 60 + 3400/60 = 116.7/s at 10/s per worker -> 12 workers
	if !r.RatesKnown || r.Lag != 3400 || r.ArrivalRate != 60 || r.ProcessRate != 20 || r.PerWorker != 10 || r.Desired != 12 || r.Basis != scale.BasisMeasured {
		t.Fatalf("second tick %+v", r)
	}
	if r.ProjectedDrainSeconds <= 0 || r.ProjectedDrainSeconds > 60 {
		t.Fatalf("projected %v s, want within the 60 s target", r.ProjectedDrainSeconds)
	}
}

func TestRememberedOnlyFromStableIntervals(t *testing.T) {
	b, _ := fakebroker.New("g", "jobs", 8, 10)
	_ = b.SetWorkers(2)
	clk := &clock{t: time.Unix(0, 0)}
	c, _ := New(b, b, policies(), 1, clk.now)
	ctx := context.Background()
	step := func(produce int64) Recommendation {
		b.Produce(clk.t, produce)
		clk.t = clk.t.Add(10 * time.Second)
		b.Consume(clk.t, 10*time.Second)
		recs, err := c.Tick(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return recs[0]
	}
	step(0)
	if r := step(1000); r.Basis != scale.BasisMeasured || r.PerWorker != 10 {
		t.Fatalf("busy tick %+v", r)
	}
	// worker count changes: the measurement is divided by the larger count and not remembered
	_ = b.SetWorkers(4)
	r := step(0) // 4 workers drain 400 of the 800 left
	if r.Basis != scale.BasisMeasured || r.PerWorker != 10 {
		t.Fatalf("after change %+v", r)
	}
	_ = b.SetWorkers(1)
	r = step(0) // 1 worker processed 100 in 10 s (10/s), divided by the 4 workers at the interval start
	if r.PerWorker != 2.5 {
		t.Fatalf("per worker %v, want 10/s divided by the 4 workers at the interval start", r.PerWorker)
	}
	// drain to zero: idle, remembered value is the last stable busy one (10), not 2.5
	_ = b.SetWorkers(20)
	step(0)
	r = step(0)
	if r.Lag != 0 || r.Basis != scale.BasisRemembered || r.PerWorker != 10 {
		t.Fatalf("idle tick %+v, want remembered 10", r)
	}
}

func TestMissingGroupHoldsCount(t *testing.T) {
	b, _ := fakebroker.New("other", "jobs", 2, 10)
	c, _ := New(b, Static{"g": 3}, policies(), 1, nil)
	recs, err := c.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r := recs[0]; r.Found || r.Desired != 3 || !strings.Contains(r.Reason, "not found") || r.ProjectedDrainSeconds != -1 {
		t.Fatalf("got %+v", r)
	}
}

type failing struct{ err error }

func (f failing) Offsets(context.Context) ([]lag.Offset, error) { return nil, f.err }

type badOffsets struct{}

func (badOffsets) Offsets(context.Context) ([]lag.Offset, error) {
	o := lag.Offset{Group: "g", Topic: "t", LogEnd: 1}
	return []lag.Offset{o, o}, nil
}

func TestTickErrors(t *testing.T) {
	ctx := context.Background()
	c, _ := New(failing{errors.New("boom")}, Static{"g": 1}, policies(), 1, nil)
	if _, err := c.Tick(ctx); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("source error: %v", err)
	}
	c, _ = New(badOffsets{}, Static{"g": 1}, policies(), 1, nil)
	if _, err := c.Tick(ctx); err == nil {
		t.Fatal("duplicate partitions accepted")
	}
	b, _ := fakebroker.New("g", "t", 1, 1)
	c, _ = New(b, Static{}, policies(), 1, nil)
	if _, err := c.Tick(ctx); err == nil {
		t.Fatal("missing worker count accepted")
	}
}

func TestNewValidation(t *testing.T) {
	b, _ := fakebroker.New("g", "t", 1, 1)
	if _, err := New(nil, b, policies(), 1, nil); err == nil {
		t.Error("nil source accepted")
	}
	if _, err := New(b, nil, policies(), 1, nil); err == nil {
		t.Error("nil worker counter accepted")
	}
	if _, err := New(b, b, nil, 1, nil); err == nil {
		t.Error("no policies accepted")
	}
	if _, err := New(b, b, map[string]scale.Policy{"a b": policies()["g"]}, 1, nil); err == nil {
		t.Error("bad group name accepted")
	}
	if _, err := New(b, b, map[string]scale.Policy{"g": {}}, 1, nil); err == nil {
		t.Error("invalid policy accepted")
	}
	if _, err := New(b, b, policies(), 0, nil); err == nil {
		t.Error("smoothing 0 accepted")
	}
}

// flaky fails the first n calls.
type flaky struct {
	mu sync.Mutex
	n  int
	b  *fakebroker.Broker
}

func (f *flaky) Offsets(ctx context.Context) ([]lag.Offset, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n > 0 {
		f.n--
		return nil, errors.New("temporary")
	}
	return f.b.Offsets(ctx)
}

func TestRunStopsAfterConsecutiveFailures(t *testing.T) {
	c, _ := New(failing{errors.New("down")}, Static{"g": 1}, policies(), 1, nil)
	var seen int
	err := c.Run(context.Background(), time.Millisecond, 3, func([]Recommendation) error { return nil }, func(error) { seen++ })
	if err == nil || !strings.Contains(err.Error(), "3 consecutive") || seen != 3 {
		t.Fatalf("err %v, errors seen %d", err, seen)
	}
}

func TestRunRecoversAndStopsOnCancel(t *testing.T) {
	b, _ := fakebroker.New("g", "t", 1, 1)
	src := &flaky{n: 2, b: b}
	c, _ := New(src, Static{"g": 1}, policies(), 1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	emits, errs := 0, 0
	err := c.Run(ctx, time.Millisecond, 3, func([]Recommendation) error {
		emits++
		if emits == 3 {
			cancel()
		}
		return nil
	}, func(error) { errs++ })
	if err != nil || emits != 3 || errs != 2 {
		t.Fatalf("err %v emits %d errs %d", err, emits, errs)
	}
	if err := c.Run(context.Background(), 0, 1, nil, nil); err == nil {
		t.Fatal("zero interval accepted")
	}
	if err := c.Run(context.Background(), time.Second, 0, nil, nil); err == nil {
		t.Fatal("maxFailures 0 accepted")
	}
	c2, _ := New(b, Static{"g": 1}, policies(), 1, nil)
	if err := c2.Run(context.Background(), time.Millisecond, 1, func([]Recommendation) error { return errors.New("pipe closed") }, func(error) {}); err == nil {
		t.Fatal("emit error ignored")
	}
}

func TestConcurrentTicks(t *testing.T) {
	b, _ := fakebroker.New("g", "t", 4, 10)
	_ = b.SetWorkers(2)
	c, _ := New(b, b, policies(), 0.5, nil)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 50 {
				b.Produce(time.Now(), 5)
				b.Consume(time.Now(), time.Millisecond*100)
				if _, err := c.Tick(context.Background()); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}
