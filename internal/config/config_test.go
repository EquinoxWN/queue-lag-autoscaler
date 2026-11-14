package config

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestShippedExamplesLoad(t *testing.T) {
	cfg, err := Load("../../examples/config.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Names(); len(got) != 2 || got[0] != "emails" || got[1] != "orders-workers" {
		t.Fatalf("names %v", got)
	}
	o := cfg.Policies["orders-workers"]
	if o.TargetDrain != 5*time.Minute || o.MaxWorkers != 10 || !o.CapToPartitions || cfg.Workers["orders-workers"] != 2 {
		t.Fatalf("orders policy %+v workers %d", o, cfg.Workers["orders-workers"])
	}
	if e := cfg.Policies["emails"]; e.WorkerRate != 10 || e.BusyLag != 100 {
		t.Fatalf("emails policy %+v", e)
	}
	if cfg.Smoothing != 0.5 {
		t.Fatalf("smoothing %v", cfg.Smoothing)
	}
	if _, err := Load("../../examples/ci-kafka.json"); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultsAndOverrides(t *testing.T) {
	cfg, err := Parse(strings.NewReader(`{"smoothing":1,"groups":{"g":{"target_drain":"30s","min_workers":1,"max_workers":3,"cap_to_partitions":false}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Smoothing != 1 || cfg.Policies["g"].CapToPartitions || cfg.Workers["g"] != 0 {
		t.Fatalf("got %+v", cfg)
	}
	cfg, err = Parse(strings.NewReader(`{"groups":{"g":{"target_drain":"30s","min_workers":1,"max_workers":3}}}`))
	if err != nil || cfg.Smoothing != 0.5 || !cfg.Policies["g"].CapToPartitions {
		t.Fatalf("defaults: %+v %v", cfg, err)
	}
}

func TestInvalidConfigs(t *testing.T) {
	g := func(body string) string { return `{"groups":{"g":{` + body + `}}}` }
	ok := `"target_drain":"1m","min_workers":1,"max_workers":3`
	cases := map[string]string{
		"empty":            ``,
		"not an object":    `[]`,
		"no groups":        `{"groups":{}}`,
		"unknown field":    `{"groups":{"g":{` + ok + `,"max_workerz":4}}}`,
		"unknown top":      `{"group":{}}`,
		"trailing data":    g(ok) + `{}`,
		"bad group name":   `{"groups":{"a b":{` + ok + `}}}`,
		"no target":        g(`"min_workers":1,"max_workers":3`),
		"bad duration":     g(`"target_drain":"soon","min_workers":1,"max_workers":3`),
		"target too short": g(`"target_drain":"10ms","min_workers":1,"max_workers":3`),
		"min zero":         g(`"target_drain":"1m","min_workers":0,"max_workers":3`),
		"max below min":    g(`"target_drain":"1m","min_workers":4,"max_workers":3`),
		"negative workers": g(ok + `,"workers":-1`),
		"huge workers":     g(ok + `,"workers":10001`),
		"negative rate":    g(ok + `,"worker_rate":-2`),
		"smoothing zero":   `{"smoothing":0,"groups":{"g":{` + ok + `}}}`,
		"smoothing high":   `{"smoothing":1.5,"groups":{"g":{` + ok + `}}}`,
		"wrong type":       g(`"target_drain":60,"min_workers":1,"max_workers":3`),
	}
	for name, in := range cases {
		if _, err := Parse(strings.NewReader(in)); err == nil {
			t.Errorf("%s: accepted %s", name, in)
		}
	}
}

func TestSizeAndGroupLimits(t *testing.T) {
	big := `{"groups":{"g":{"target_drain":"1m","min_workers":1,"max_workers":3}}}` + strings.Repeat(" ", MaxBytes)
	if _, err := Parse(strings.NewReader(big)); err == nil || !strings.Contains(err.Error(), "1 MiB") {
		t.Fatalf("oversized config: %v", err)
	}
	var b strings.Builder
	b.WriteString(`{"groups":{`)
	for i := range MaxGroups + 1 {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"g` + strings.Repeat("x", i%7) + strconv.Itoa(i) + `":{"target_drain":"1m","min_workers":1,"max_workers":2}`)
	}
	b.WriteString(`}}`)
	if _, err := Parse(strings.NewReader(b.String())); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("too many groups: %v", err)
	}
	if _, err := Load("does-not-exist.json"); err == nil {
		t.Fatal("missing file accepted")
	}
}
