// Command queue-lag-autoscaler recommends worker counts per consumer group from queue lag and processing rate.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/config"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/controller"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/lag"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/sim"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/source"
)

const usage = `usage:
  queue-lag-autoscaler run  -config FILE (-url URL | -snapshot FILE) [-format prom|describe] [-interval 15s] [-timeout 5s] [-once]
  queue-lag-autoscaler plan -config FILE -before FILE -after FILE -elapsed DURATION [-format describe|prom]
  queue-lag-autoscaler demo [-timeline]`

// main dispatches to a subcommand and exits non-zero on failure.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run parses the subcommand and its flags.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "run":
		return cmdRun(ctx, args[1:], stdout, stderr)
	case "plan":
		return cmdPlan(ctx, args[1:], stdout)
	case "demo":
		return cmdDemo(args[1:], stdout)
	}
	return fmt.Errorf("unknown command %q\n%s", args[0], usage)
}

// parser maps a -format value to a snapshot parser.
func parser(format string) (source.Parser, error) {
	switch format {
	case "prom":
		return lag.ParsePrometheus, nil
	case "describe":
		return lag.ParseDescribe, nil
	}
	return nil, fmt.Errorf("unknown -format %q (prom or describe)", format)
}

// cmdRun polls a source and prints one JSON line per group per tick.
func cmdRun(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "", "group policy file (JSON)")
	rawURL := fs.String("url", "", "metrics URL to poll (kafka_exporter /metrics)")
	snapshot := fs.String("snapshot", "", "file to re-read every tick instead of a URL")
	format := fs.String("format", "prom", "snapshot format: prom or describe")
	interval := fs.Duration("interval", 15*time.Second, "time between ticks")
	timeout := fs.Duration("timeout", 5*time.Second, "timeout per HTTP attempt")
	once := fs.Bool("once", false, "take one tick and exit (rates need two ticks)")
	maxFailures := fs.Int("max-failures", 5, "stop after this many consecutive failed ticks")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if (*rawURL == "") == (*snapshot == "") {
		return errors.New("run needs exactly one of -url or -snapshot")
	}
	if *interval < time.Second || *interval > time.Hour {
		return fmt.Errorf("-interval %v outside [1s, 1h]", *interval)
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	parse, err := parser(*format)
	if err != nil {
		return err
	}
	var src controller.Source = source.File{Path: *snapshot, Parse: parse}
	if *rawURL != "" {
		if src, err = source.NewHTTP(*rawURL, parse, *timeout); err != nil {
			return err
		}
	}
	ctl, err := controller.New(src, controller.Static(cfg.Workers), cfg.Policies, cfg.Smoothing, nil)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	emit := func(recs []controller.Recommendation) error {
		for _, r := range recs {
			if err := enc.Encode(r); err != nil {
				return err
			}
		}
		return nil
	}
	if *once {
		recs, err := ctl.Tick(ctx)
		if err != nil {
			return err
		}
		return emit(recs)
	}
	logErr := func(err error) {
		fmt.Fprintf(stderr, "%s tick failed: %v\n", time.Now().UTC().Format(time.RFC3339), err)
	}
	return ctl.Run(ctx, *interval, *maxFailures, emit, logErr)
}

// sequence returns each snapshot in turn, one per Offsets call.
type sequence struct {
	snaps [][]lag.Offset
	i     int
}

// Offsets returns the next snapshot.
func (s *sequence) Offsets(context.Context) ([]lag.Offset, error) {
	if s.i >= len(s.snaps) {
		return nil, errors.New("no more snapshots")
	}
	s.i++
	return s.snaps[s.i-1], nil
}

// cmdPlan computes one recommendation per group from two saved snapshots taken `elapsed` apart.
func cmdPlan(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("plan", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfgPath := fs.String("config", "", "group policy file (JSON)")
	before := fs.String("before", "", "first snapshot")
	after := fs.String("after", "", "second snapshot")
	elapsed := fs.Duration("elapsed", 0, "time between the two snapshots")
	format := fs.String("format", "describe", "snapshot format: describe or prom")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w\n%s", err, usage)
	}
	if *elapsed <= 0 || *elapsed > 24*time.Hour {
		return fmt.Errorf("-elapsed %v outside (0, 24h]", *elapsed)
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	parse, err := parser(*format)
	if err != nil {
		return err
	}
	seq := &sequence{}
	for _, p := range []string{*before, *after} {
		offs, err := source.File{Path: p, Parse: parse}.Offsets(ctx)
		if err != nil {
			return err
		}
		seq.snaps = append(seq.snaps, offs)
	}
	t0 := time.Now().UTC()
	clock := []time.Time{t0, t0.Add(*elapsed)}
	tick := 0
	now := func() time.Time { tick++; return clock[tick-1] }
	// plan reports the measured rates directly, so smoothing is off (alpha 1)
	ctl, err := controller.New(seq, controller.Static(cfg.Workers), cfg.Policies, 1, now)
	if err != nil {
		return err
	}
	if _, err := ctl.Tick(ctx); err != nil {
		return err
	}
	recs, err := ctl.Tick(ctx)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return nil
}

// cmdDemo runs the simulated bursty load with and without lag-based scaling and verifies the outcome.
func cmdDemo(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	timeline := fs.Bool("timeline", false, "print every controller tick, not only changes")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w\n%s", err, usage)
	}
	cfg := sim.Demo(true)
	fmt.Fprintf(stdout, "scenario: %d partitions, %.0f msg/s per worker, baseline %.0f msg/s +-%.0f%%, %d bursts, %v simulated\n",
		cfg.Partitions, cfg.WorkerRate, cfg.Baseline, cfg.Noise*100, len(cfg.Bursts), cfg.Duration)
	for _, b := range cfg.Bursts {
		fmt.Fprintf(stdout, "burst: at %v for %v at %.0f msg/s\n", b.Start, b.Duration, b.Rate)
	}
	fmt.Fprintf(stdout, "policy: target_drain %v, workers %d..%d, busy_lag %d, tick %v, worker startup %v, start with %d workers\n",
		cfg.Policy.TargetDrain, cfg.Policy.MinWorkers, cfg.Policy.MaxWorkers, cfg.Policy.BusyLag, cfg.Interval, cfg.Startup, cfg.InitialWorkers)
	started := time.Now()
	scaled, err := sim.Run(cfg)
	if err != nil {
		return err
	}
	static, err := sim.Run(sim.Demo(false))
	if err != nil {
		return err
	}
	peakCfg := sim.Demo(false)
	peakCfg.InitialWorkers = cfg.Policy.MaxWorkers
	peak, err := sim.Run(peakCfg)
	if err != nil {
		return err
	}
	wall := time.Since(started)
	last := -1
	for _, p := range scaled.Timeline {
		if *timeline || p.Desired != last {
			fmt.Fprintf(stdout, "t=%-6v arrival=%7.1f/s lag=%6d workers=%2d desired=%2d basis=%s\n",
				p.T, p.Arrival, p.Lag, p.Workers, p.Desired, p.Basis)
			last = p.Desired
		}
	}
	line := func(name string, r sim.Report) {
		fmt.Fprintf(stdout, "%s: produced=%d processed=%d final_lag=%d max_lag=%d wait_p50=%v wait_p99=%v wait_max=%v worker_minutes=%.1f peak_workers=%d changes=%d\n",
			name, r.Produced, r.Processed, r.FinalLag, r.MaxLag, r.P50, r.P99, r.MaxWait.Round(time.Second), r.WorkerSeconds/60, r.PeakWorkers, r.Changes)
	}
	line("lag-based", scaled)
	line(fmt.Sprintf("static-%d", cfg.InitialWorkers), static)
	line(fmt.Sprintf("static-%d", peakCfg.InitialWorkers), peak)
	bound := cfg.Policy.TargetDrain + 2*cfg.Interval + cfg.Startup
	finalOK := float64(scaled.FinalLag) <= cfg.Baseline*cfg.Interval.Seconds()
	waitOK := scaled.MaxWait <= bound
	fmt.Fprintf(stdout, "verify: short_decisions=%d final_lag_ok=%t max_wait_within_bound=%t (bound = target %v + 2 ticks + startup = %v) wall=%v\n",
		scaled.ShortDecisions, finalOK, waitOK, cfg.Policy.TargetDrain, bound, wall.Round(time.Millisecond))
	if scaled.ShortDecisions != 0 || !finalOK || !waitOK {
		return errors.New("demo verification failed")
	}
	return nil
}
