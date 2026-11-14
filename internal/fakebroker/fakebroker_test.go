package fakebroker

import (
	"context"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestProduceSpreadsEvenly(t *testing.T) {
	b, _ := New("g", "t", 4, 1)
	b.Produce(t0, 10) // 3,3,2,2
	b.Produce(t0, 2)  // remainder continues round robin: partitions 2,3
	offs, _ := b.Offsets(context.Background())
	want := []int64{3, 3, 3, 3}
	for i, o := range offs {
		if o.LogEnd != want[i] || o.Committed != 0 {
			t.Fatalf("partition %d: %+v, want log end %d", i, o, want[i])
		}
	}
	b.Produce(t0, 0)
	b.Produce(t0, -5)
	if b.Lag() != 12 {
		t.Fatalf("lag %d, want 12 (zero and negative produce add nothing)", b.Lag())
	}
}

func TestConsumeRespectsCapacityAndPartitions(t *testing.T) {
	b, _ := New("g", "t", 2, 10)
	b.Produce(t0, 1000)
	_ = b.SetWorkers(5) // only 2 partitions: 3 workers idle
	if n := b.Consume(t0.Add(time.Second), time.Second); n != 20 {
		t.Fatalf("processed %d, want 2 active workers x 10 = 20", n)
	}
	_ = b.SetWorkers(1)
	if n := b.Consume(t0.Add(2*time.Second), 3*time.Second); n != 30 {
		t.Fatalf("processed %d, want 30", n)
	}
	_ = b.SetWorkers(0)
	if n := b.Consume(t0.Add(3*time.Second), time.Second); n != 0 {
		t.Fatalf("no workers processed %d", n)
	}
	if b.Lag() != 950 {
		t.Fatalf("lag %d, want 950", b.Lag())
	}
	if err := b.SetWorkers(-1); err == nil {
		t.Fatal("negative workers accepted")
	}
}

func TestFractionalCreditCarriesButIdleIsNotBanked(t *testing.T) {
	b, _ := New("g", "t", 1, 0.5)
	_ = b.SetWorkers(1)
	b.Produce(t0, 10)
	var total int64
	for i := 1; i <= 4; i++ {
		total += b.Consume(t0.Add(time.Duration(i)*time.Second), time.Second)
	}
	if total != 2 {
		t.Fatalf("0.5/s for 4 s processed %d, want 2", total)
	}
	b2, _ := New("g", "t", 1, 10)
	_ = b2.SetWorkers(1)
	b2.Consume(t0, 100*time.Second) // idle: no work, nothing banked
	b2.Produce(t0, 500)
	if n := b2.Consume(t0.Add(time.Second), time.Second); n != 10 {
		t.Fatalf("processed %d after idle period, want 10", n)
	}
}

func TestOldestFirstAndWaitStats(t *testing.T) {
	b, _ := New("g", "t", 2, 100)
	_ = b.SetWorkers(1) // one worker serves both partitions
	b.Produce(t0, 1)    // partition 0 at t0
	b.Produce(t0.Add(5*time.Second), 1)
	b.Produce(t0.Add(9*time.Second), 98)
	b.Consume(t0.Add(10*time.Second), time.Second)
	w := b.Waits()
	if w.Processed != 100 || w.Max != 10*time.Second {
		t.Fatalf("stats %+v, want 100 processed, max wait 10 s", w)
	}
	if w.P50 != time.Second || w.P99 != 5*time.Second {
		t.Fatalf("p50 %v p99 %v, want 1 s and 5 s", w.P50, w.P99)
	}
	empty, _ := New("g", "t", 1, 1)
	if s := empty.Waits(); s.P99 != 0 || s.Max != 0 {
		t.Fatalf("empty stats %+v", s)
	}
}

func TestWorkersAndValidation(t *testing.T) {
	b, _ := New("g", "t", 1, 1)
	_ = b.SetWorkers(3)
	if n, err := b.Workers(context.Background(), "g"); n != 3 || err != nil {
		t.Fatalf("%d %v", n, err)
	}
	if _, err := b.Workers(context.Background(), "other"); err == nil {
		t.Fatal("unknown group accepted")
	}
	for name, f := range map[string]func() error{
		"bad group":       func() error { _, err := New("a b", "t", 1, 1); return err },
		"bad topic":       func() error { _, err := New("g", "", 1, 1); return err },
		"zero partitions": func() error { _, err := New("g", "t", 0, 1); return err },
		"zero rate":       func() error { _, err := New("g", "t", 1, 0); return err },
	} {
		if f() == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
