package rate

import (
	"math"
	"testing"
	"time"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/lag"
)

// group builds a one-group snapshot from (committed, logEnd) pairs per partition.
func group(pairs ...[2]int64) lag.Group {
	g := lag.Group{Name: "g", Partitions: map[lag.Key]lag.Offset{}}
	for i, p := range pairs {
		k := lag.Key{Topic: "t", Partition: int32(i)}
		g.Partitions[k] = lag.Offset{Group: "g", Topic: "t", Partition: int32(i), Committed: p[0], LogEnd: p[1]}
	}
	return g
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestFirstObservationHasNoRates(t *testing.T) {
	e, _ := New(1)
	r := e.Observe(time.Unix(0, 0), group([2]int64{0, 10}))
	if r.Known || r.Arrival != 0 || r.Process != 0 {
		t.Fatalf("first snapshot gave %+v", r)
	}
}

func TestRatesFromTwoSnapshots(t *testing.T) {
	e, _ := New(1)
	t0 := time.Unix(1000, 0)
	e.Observe(t0, group([2]int64{100, 200}, [2]int64{50, 80}))
	r := e.Observe(t0.Add(10*time.Second), group([2]int64{160, 260}, [2]int64{90, 100}))
	// produced (60 + 20) / 10 s = 8/s; committed (60 + 40) / 10 s = 10/s
	if !r.Known || !near(r.Arrival, 8) || !near(r.Process, 10) {
		t.Fatalf("got %+v, want arrival 8 and process 10", r)
	}
}

func TestResetsNewPartitionsAndFirstCommitsAddNothing(t *testing.T) {
	e, _ := New(1)
	t0 := time.Unix(0, 0)
	e.Observe(t0, group([2]int64{500, 600}, [2]int64{lag.NoCommit, 30}))
	// partition 0 reset to 100 (committed moved back), partition 1 first commit, partition 2 is new
	r := e.Observe(t0.Add(5*time.Second), group([2]int64{100, 650}, [2]int64{30, 30}, [2]int64{7, 9}))
	if !near(r.Process, 0) || !near(r.Arrival, 10) {
		t.Fatalf("got %+v, want process 0 and arrival 50/5 = 10", r)
	}
}

func TestNonPositiveElapsedKeepsPreviousRates(t *testing.T) {
	e, _ := New(1)
	t0 := time.Unix(0, 0)
	e.Observe(t0, group([2]int64{0, 0}))
	first := e.Observe(t0.Add(2*time.Second), group([2]int64{4, 8}))
	for _, at := range []time.Time{t0.Add(2 * time.Second), t0} {
		if r := e.Observe(at, group([2]int64{1000, 1000})); r != first {
			t.Fatalf("elapsed <= 0 changed rates to %+v", r)
		}
	}
	// the baseline was not replaced: the next real interval measures from t0+2s
	r := e.Observe(t0.Add(4*time.Second), group([2]int64{10, 18}))
	if !near(r.Process, 3) || !near(r.Arrival, 5) {
		t.Fatalf("got %+v, want process 3 and arrival 5", r)
	}
}

func TestSmoothingAppliesToArrivalsOnly(t *testing.T) {
	e, _ := New(0.25)
	t0 := time.Unix(0, 0)
	e.Observe(t0, group([2]int64{0, 0}))
	e.Observe(t0.Add(time.Second), group([2]int64{100, 100}))        // arrival 100, process 100
	r := e.Observe(t0.Add(2*time.Second), group([2]int64{100, 300})) // instant arrival 200, process 0
	if !near(r.Arrival, 0.25*200+0.75*100) {
		t.Fatalf("arrival %v, want 125", r.Arrival)
	}
	if !near(r.Process, 0) {
		t.Fatalf("process %v must be the last interval only, want 0", r.Process)
	}
}

func TestNewRejectsBadAlpha(t *testing.T) {
	for _, a := range []float64{0, -0.1, 1.01, math.NaN()} {
		if _, err := New(a); err == nil {
			t.Errorf("alpha %v accepted", a)
		}
	}
	if _, err := New(1); err != nil {
		t.Fatal(err)
	}
}
