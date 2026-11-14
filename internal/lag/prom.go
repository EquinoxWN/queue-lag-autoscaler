package lag

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// Metric names exported by kafka_exporter (danielqsj/kafka_exporter).
const (
	MetricGroupOffset = "kafka_consumergroup_current_offset"
	MetricLogEnd      = "kafka_topic_partition_current_offset"
)

// ParsePrometheus reads kafka_exporter metrics in the Prometheus text format and joins group offsets with log-end offsets.
func ParsePrometheus(r io.Reader) ([]Offset, error) {
	data, err := readLimited(r)
	if err != nil {
		return nil, err
	}
	logEnd := map[Key]int64{}
	var groups []Offset
	for n, raw := range bytes.Split(data, []byte("\n")) {
		line := strings.TrimSpace(string(raw))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, _ := strings.Cut(line, "{")
		if name != MetricGroupOffset && name != MetricLogEnd {
			continue
		}
		labels, value, err := parseSample(rest)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n+1, err)
		}
		k, err := keyFrom(labels)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n+1, err)
		}
		if name == MetricLogEnd {
			if value < 0 {
				return nil, fmt.Errorf("line %d: negative log-end offset", n+1)
			}
			if _, dup := logEnd[k]; dup {
				return nil, fmt.Errorf("line %d: log-end offset for %s/%d listed twice", n+1, k.Topic, k.Partition)
			}
			logEnd[k] = value
			continue
		}
		groups = append(groups, Offset{Group: labels["consumergroup"], Topic: k.Topic, Partition: k.Partition, Committed: max(value, NoCommit)})
	}
	for i, o := range groups {
		end, ok := logEnd[Key{o.Topic, o.Partition}]
		if !ok {
			return nil, fmt.Errorf("group %s: no %s for %s/%d", o.Group, MetricLogEnd, o.Topic, o.Partition)
		}
		groups[i].LogEnd = end
	}
	if _, err := ByGroup(groups); err != nil {
		return nil, err
	}
	return groups, nil
}

// keyFrom reads the topic and partition labels.
func keyFrom(labels map[string]string) (Key, error) {
	p, err := strconv.ParseInt(labels["partition"], 10, 32)
	if err != nil {
		return Key{}, fmt.Errorf("partition label %q: %w", labels["partition"], err)
	}
	return Key{Topic: labels["topic"], Partition: int32(p)}, nil
}

// parseSample parses `label="v",...} value [timestamp]` into labels and an integral value.
func parseSample(s string) (map[string]string, int64, error) {
	labels := map[string]string{}
	i := 0
	for {
		for i < len(s) && (s[i] == ' ' || s[i] == ',') {
			i++
		}
		if i >= len(s) {
			return nil, 0, fmt.Errorf("unterminated label set")
		}
		if s[i] == '}' {
			i++
			break
		}
		eq := strings.IndexByte(s[i:], '=')
		if eq <= 0 || i+eq+1 >= len(s) || s[i+eq+1] != '"' {
			return nil, 0, fmt.Errorf("malformed label at %q", s[i:])
		}
		name := s[i : i+eq]
		i += eq + 2
		var val strings.Builder
		closed := false
		for i < len(s) {
			c := s[i]
			i++
			if c == '"' {
				closed = true
				break
			}
			if c == '\\' && i < len(s) {
				switch s[i] {
				case 'n':
					val.WriteByte('\n')
				default:
					val.WriteByte(s[i])
				}
				i++
				continue
			}
			val.WriteByte(c)
		}
		if !closed {
			return nil, 0, fmt.Errorf("unterminated label value for %s", name)
		}
		if _, dup := labels[name]; dup {
			return nil, 0, fmt.Errorf("label %s repeated", name)
		}
		labels[name] = val.String()
	}
	fields := strings.Fields(s[i:])
	if len(fields) < 1 || len(fields) > 2 {
		return nil, 0, fmt.Errorf("expected a value and an optional timestamp, got %q", s[i:])
	}
	f, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) || math.Abs(f) > 1<<62 {
		return nil, 0, fmt.Errorf("value %q is not an integral offset", fields[0])
	}
	return labels, int64(f), nil
}
