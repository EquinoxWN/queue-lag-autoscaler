// Package config loads the per-group scaling policies from a JSON file.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/lag"
	"github.com/EquinoxWN/queue-lag-autoscaler/internal/scale"
)

// MaxBytes caps the config file size.
const MaxBytes = 1 << 20

// MaxGroups caps the number of groups in one file.
const MaxGroups = 1000

// Group is one consumer group's entry in the file.
type Group struct {
	TargetDrain     string  `json:"target_drain"`
	MinWorkers      int     `json:"min_workers"`
	MaxWorkers      int     `json:"max_workers"`
	Workers         int     `json:"workers"`
	WorkerRate      float64 `json:"worker_rate"`
	BusyLag         int64   `json:"busy_lag"`
	CapToPartitions *bool   `json:"cap_to_partitions"`
}

// File is the whole config document.
type File struct {
	Smoothing *float64         `json:"smoothing"`
	Groups    map[string]Group `json:"groups"`
}

// Config is the validated configuration.
type Config struct {
	Smoothing float64
	Policies  map[string]scale.Policy
	Workers   map[string]int // current worker count per group (static in M1)
}

// Names returns the configured group names in order.
func (c Config) Names() []string {
	out := make([]string, 0, len(c.Policies))
	for n := range c.Policies {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Load reads and validates a config file.
func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-only file, nothing to flush
	return Parse(f)
}

// Parse decodes and validates a config document, refusing unknown fields.
func Parse(r io.Reader) (Config, error) {
	data, err := io.ReadAll(io.LimitReader(r, MaxBytes+1))
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if len(data) > MaxBytes {
		return Config{}, errors.New("config larger than 1 MiB")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var file File
	if err := dec.Decode(&file); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if dec.More() {
		return Config{}, errors.New("decode config: trailing data after the JSON object")
	}
	cfg := Config{Smoothing: 0.5, Policies: map[string]scale.Policy{}, Workers: map[string]int{}}
	if file.Smoothing != nil {
		cfg.Smoothing = *file.Smoothing
	}
	if !(cfg.Smoothing > 0 && cfg.Smoothing <= 1) {
		return Config{}, fmt.Errorf("smoothing %v outside (0, 1]", cfg.Smoothing)
	}
	if len(file.Groups) == 0 {
		return Config{}, errors.New("config has no groups")
	}
	if len(file.Groups) > MaxGroups {
		return Config{}, fmt.Errorf("config has %d groups, limit %d", len(file.Groups), MaxGroups)
	}
	for name, g := range file.Groups {
		if !lag.ValidGroup(name) {
			return Config{}, fmt.Errorf("invalid group name %q", name)
		}
		p, err := g.policy()
		if err != nil {
			return Config{}, fmt.Errorf("group %s: %w", name, err)
		}
		if g.Workers < 0 || g.Workers > scale.MaxWorkers {
			return Config{}, fmt.Errorf("group %s: workers %d outside [0, %d]", name, g.Workers, scale.MaxWorkers)
		}
		cfg.Policies[name] = p
		cfg.Workers[name] = g.Workers
	}
	return cfg, nil
}

// policy converts and validates one group entry.
func (g Group) policy() (scale.Policy, error) {
	if g.TargetDrain == "" {
		return scale.Policy{}, errors.New("target_drain is required (for example \"2m\")")
	}
	target, err := time.ParseDuration(g.TargetDrain)
	if err != nil {
		return scale.Policy{}, fmt.Errorf("target_drain: %w", err)
	}
	p := scale.Policy{
		TargetDrain: target, MinWorkers: g.MinWorkers, MaxWorkers: g.MaxWorkers,
		WorkerRate: g.WorkerRate, BusyLag: g.BusyLag, CapToPartitions: true,
	}
	if g.CapToPartitions != nil {
		p.CapToPartitions = *g.CapToPartitions
	}
	return p, p.Validate()
}
