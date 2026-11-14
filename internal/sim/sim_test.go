package sim

import (
	"testing"
	"time"
)

func TestDemoScenarioMeetsTheBound(t *testing.T) {
	cfg := Demo(true)
	r, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	bound := cfg.Policy.TargetDrain + 2*cfg.Interval + cfg.Startup
	if r.ShortDecisions != 0 || r.MaxWait > bound || float64(r.FinalLag) > cfg.Baseline*cfg.Interval.Seconds() {
		r.Timeline = nil
		t.Fatalf("report %+v (bound %v)", r, bound)
	}
	if r.Produced != r.Processed+r.FinalLag {
		t.Fatalf("produced %d != processed %d + lag %d", r.Produced, r.Processed, r.FinalLag)
	}
	if r.PeakWorkers <= cfg.InitialWorkers || r.Decisions != int(cfg.Duration/cfg.Interval) {
		t.Fatalf("peak %d decisions %d", r.PeakWorkers, r.Decisions)
	}
	static, err := Run(Demo(false))
	if err != nil {
		t.Fatal(err)
	}
	if static.MaxWait <= bound || static.Changes != 0 || static.Produced != r.Produced {
		static.Timeline = nil
		t.Fatalf("static run should miss the bound with the same load: %+v", static)
	}
	peak := Demo(false)
	peak.InitialWorkers = cfg.Policy.MaxWorkers
	p, err := Run(peak)
	if err != nil {
		t.Fatal(err)
	}
	if r.WorkerSeconds >= p.WorkerSeconds/2 {
		t.Fatalf("lag-based used %.0f worker-seconds, static peak %.0f", r.WorkerSeconds, p.WorkerSeconds)
	}
}

func TestDeterministicForASeed(t *testing.T) {
	a, _ := Run(Demo(true))
	b, _ := Run(Demo(true))
	if a.Produced != b.Produced || a.MaxWait != b.MaxWait || a.WorkerSeconds != b.WorkerSeconds || a.Changes != b.Changes {
		t.Fatalf("runs differ: max wait %v vs %v, worker-seconds %v vs %v", a.MaxWait, b.MaxWait, a.WorkerSeconds, b.WorkerSeconds)
	}
	c := Demo(true)
	c.Seed = 8
	other, _ := Run(c)
	if other.Produced == a.Produced {
		t.Fatal("another seed produced exactly the same load")
	}
}

func TestStartupDelaysScaleUp(t *testing.T) {
	cfg := Demo(true)
	cfg.Startup = 0
	fast, _ := Run(cfg)
	slow, _ := Run(Demo(true))
	if fast.MaxWait >= slow.MaxWait {
		t.Fatalf("no startup delay should not wait longer: %v vs %v", fast.MaxWait, slow.MaxWait)
	}
}

func TestConfigValidation(t *testing.T) {
	mods := map[string]func(*Config){
		"zero step":         func(c *Config) { c.Step = 0 },
		"interval < step":   func(c *Config) { c.Interval = c.Step / 2 },
		"interval not mult": func(c *Config) { c.Interval = 1500 * time.Millisecond },
		"short duration":    func(c *Config) { c.Duration = time.Second },
		"long duration":     func(c *Config) { c.Duration = 8 * 24 * time.Hour },
		"negative baseline": func(c *Config) { c.Baseline = -1 },
		"noise 1":           func(c *Config) { c.Noise = 1 },
		"negative startup":  func(c *Config) { c.Startup = -time.Second },
		"negative workers":  func(c *Config) { c.InitialWorkers = -1 },
		"bad burst":         func(c *Config) { c.Bursts = []Burst{{Start: 0, Duration: 0, Rate: 1}} },
		"bad policy":        func(c *Config) { c.Policy.MinWorkers = 0 },
		"bad partitions":    func(c *Config) { c.Partitions = 0 },
	}
	for name, mod := range mods {
		cfg := Demo(true)
		mod(&cfg)
		if _, err := Run(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
