package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/controller"
)

// decode reads JSON lines into recommendations keyed by group.
func decode(t *testing.T, out string) map[string]controller.Recommendation {
	t.Helper()
	recs := map[string]controller.Recommendation{}
	dec := json.NewDecoder(strings.NewReader(out))
	for dec.More() {
		var r controller.Recommendation
		if err := dec.Decode(&r); err != nil {
			t.Fatal(err)
		}
		recs[r.Group] = r
	}
	return recs
}

func TestPlanFromShippedSnapshots(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"plan", "-config", "../../examples/config.json",
		"-before", "../../examples/describe-before.txt", "-after", "../../examples/describe-after.txt", "-elapsed", "60s"}, &out, &out)
	if err != nil {
		t.Fatal(err)
	}
	recs := decode(t, out.String())
	o := recs["orders-workers"]
	// lag 10,600; arrival 1,200/60 s = 20/s; 2 workers committed 2,400/60 s = 40/s, 20/s each;
	// need 20 + 10600/300 = 55.3/s -> 3 workers; drain 10600 / (60 - 20) = 265 s
	if o.Lag != 10600 || o.ArrivalRate != 20 || o.ProcessRate != 40 || o.PerWorker != 20 || o.Desired != 3 ||
		o.Basis != "measured" || math.Abs(o.ProjectedDrainSeconds-265) > 1e-6 {
		t.Fatalf("orders %+v", o)
	}
	e := recs["emails"]
	// lag 30, arrival 0.5/s, no progress measured -> configured 10/s: need 0.5 + 30/60 = 1/s -> 1 worker
	if e.Lag != 30 || e.ArrivalRate != 0.5 || e.Basis != "configured" || e.Desired != 1 || e.Required != 1 {
		t.Fatalf("emails %+v", e)
	}
}

func TestRunOnceFromSnapshotAndURL(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"run", "-config", "../../examples/config.json",
		"-snapshot", "../../examples/describe-before.txt", "-format", "describe", "-once"}, &out, &out)
	if err != nil {
		t.Fatal(err)
	}
	if r := decode(t, out.String())["orders-workers"]; r.Lag != 11800 || r.RatesKnown || r.Desired != 2 {
		t.Fatalf("snapshot run %+v", r)
	}
	body, err := os.ReadFile("../../internal/lag/testdata/kafka_exporter.prom")
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	defer s.Close()
	out.Reset()
	err = run(context.Background(), []string{"run", "-config", "../../examples/config.json", "-url", s.URL + "/metrics", "-once"}, &out, &out)
	if err != nil {
		t.Fatal(err)
	}
	recs := decode(t, out.String())
	if recs["orders-workers"].Lag != 5900 || recs["emails"].Lag != 25 {
		t.Fatalf("url run %+v", recs)
	}
}

func TestBadArguments(t *testing.T) {
	cfg := "../../examples/config.json"
	cases := [][]string{
		{},
		{"nope"},
		{"run", "-config", cfg},
		{"run", "-config", cfg, "-url", "http://x", "-snapshot", "f"},
		{"run", "-config", cfg, "-snapshot", "f", "-interval", "10ms"},
		{"run", "-config", cfg, "-snapshot", "f", "-format", "xml"},
		{"run", "-config", "missing.json", "-snapshot", "f"},
		{"run", "-config", cfg, "-url", "ftp://x/m"},
		{"run", "-config", cfg, "-snapshot", "missing.txt", "-once"},
		{"run", "-bogus"},
		{"plan", "-config", cfg, "-before", "a", "-after", "b"},
		{"plan", "-config", cfg, "-before", "../../examples/describe-before.txt", "-after", "missing", "-elapsed", "1m"},
		{"plan", "-config", cfg, "-before", "a", "-after", "b", "-elapsed", "1m", "-format", "xml"},
		{"plan", "-x"},
		{"demo", "-x"},
	}
	for _, args := range cases {
		var out bytes.Buffer
		if err := run(context.Background(), args, &out, &out); err == nil {
			t.Errorf("%v: accepted", args)
		}
	}
}

func TestDemoVerifies(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"demo"}, &out, &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	s := out.String()
	for _, want := range []string{"lag-based:", "static-2:", "static-32:", "verify: short_decisions=0 final_lag_ok=true max_wait_within_bound=true"} {
		if !strings.Contains(s, want) {
			t.Fatalf("demo output lacks %q:\n%s", want, s)
		}
	}
}
