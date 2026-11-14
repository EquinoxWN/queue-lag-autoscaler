// Package lag holds consumer-group offsets per partition and the parsers that read them.
package lag

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
)

// MaxInput caps the bytes any parser reads from one snapshot.
const MaxInput = 8 << 20

// NoCommit marks a partition the group has never committed an offset for.
const NoCommit int64 = -1

var (
	topicRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)
	groupRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)
)

// Offset is one consumer group's position on one partition.
type Offset struct {
	Group     string
	Topic     string
	Partition int32
	Committed int64 // NoCommit when the group has no committed offset yet
	LogEnd    int64
}

// Key names one partition of one topic.
type Key struct {
	Topic     string
	Partition int32
}

// Lag returns the messages on the partition not yet committed by the group.
func (o Offset) Lag() int64 {
	start := max(o.Committed, 0)
	if o.LogEnd <= start {
		return 0
	}
	return o.LogEnd - start
}

// Validate checks names and offset ranges.
func (o Offset) Validate() error {
	switch {
	case !ValidGroup(o.Group):
		return fmt.Errorf("invalid group name %q", o.Group)
	case !topicRE.MatchString(o.Topic):
		return fmt.Errorf("invalid topic name %q", o.Topic)
	case o.Partition < 0:
		return fmt.Errorf("%s/%d: negative partition", o.Topic, o.Partition)
	case o.Committed < NoCommit:
		return fmt.Errorf("%s/%d: committed offset %d below -1", o.Topic, o.Partition, o.Committed)
	case o.LogEnd < 0:
		return fmt.Errorf("%s/%d: negative log-end offset %d", o.Topic, o.Partition, o.LogEnd)
	}
	return nil
}

// ValidGroup reports whether a consumer-group name is accepted.
func ValidGroup(name string) bool { return groupRE.MatchString(name) }

// Group is one consumer group's offsets keyed by partition.
type Group struct {
	Name       string
	Partitions map[Key]Offset
}

// Lag returns the group's total lag over all its partitions.
func (g Group) Lag() int64 {
	var total int64
	for _, o := range g.Partitions {
		total += o.Lag()
	}
	return total
}

// ByGroup validates offsets and groups them, refusing duplicate partitions.
func ByGroup(offsets []Offset) (map[string]Group, error) {
	out := map[string]Group{}
	for _, o := range offsets {
		if err := o.Validate(); err != nil {
			return nil, err
		}
		g, ok := out[o.Group]
		if !ok {
			g = Group{Name: o.Group, Partitions: map[Key]Offset{}}
			out[o.Group] = g
		}
		k := Key{o.Topic, o.Partition}
		if _, dup := g.Partitions[k]; dup {
			return nil, fmt.Errorf("group %s: partition %s/%d listed twice", o.Group, o.Topic, o.Partition)
		}
		g.Partitions[k] = o
	}
	return out, nil
}

// Sorted returns offsets ordered by group, topic and partition.
func Sorted(offsets []Offset) []Offset {
	out := append([]Offset(nil), offsets...)
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		return a.Partition < b.Partition
	})
	return out
}

// ErrTooLarge is returned when a snapshot exceeds MaxInput.
var ErrTooLarge = errors.New("snapshot larger than 8 MiB")
