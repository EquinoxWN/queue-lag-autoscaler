package lag

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// readLimited reads at most MaxInput bytes and fails beyond that.
func readLimited(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, MaxInput+1))
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	if len(data) > MaxInput {
		return nil, ErrTooLarge
	}
	return data, nil
}

// describeColumns are the header names ParseDescribe needs.
var describeColumns = []string{"GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET"}

// ParseDescribe reads the output of `kafka-consumer-groups.sh --describe` (one or all groups).
func ParseDescribe(r io.Reader) ([]Offset, error) {
	data, err := readLimited(r)
	if err != nil {
		return nil, err
	}
	var (
		cols map[string]int
		out  []Offset
	)
	for n, raw := range bytes.Split(data, []byte("\n")) {
		line := strings.TrimSpace(string(raw))
		lineNo := n + 1
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "Error"):
			return nil, fmt.Errorf("line %d: tool reported an error: %s", lineNo, line)
		case strings.HasPrefix(line, "Consumer group"), strings.HasPrefix(line, "Warning"):
			continue // "has no active members" and similar notes
		}
		fields := strings.Fields(line)
		if fields[0] == "GROUP" {
			if cols, err = headerColumns(fields); err != nil {
				return nil, fmt.Errorf("line %d: %w", lineNo, err)
			}
			continue
		}
		if cols == nil {
			return nil, fmt.Errorf("line %d: data before the GROUP header", lineNo)
		}
		o, err := describeRow(fields, cols)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		out = append(out, o)
	}
	if cols == nil {
		return nil, fmt.Errorf("no GROUP header found")
	}
	if _, err := ByGroup(out); err != nil {
		return nil, err
	}
	return out, nil
}

// headerColumns maps column names to positions and checks the required ones exist.
func headerColumns(fields []string) (map[string]int, error) {
	cols := map[string]int{}
	for i, f := range fields {
		cols[f] = i
	}
	for _, c := range describeColumns {
		if _, ok := cols[c]; !ok {
			return nil, fmt.Errorf("header lacks column %s", c)
		}
	}
	return cols, nil
}

// describeRow parses one data row and cross-checks the LAG column when present.
func describeRow(fields []string, cols map[string]int) (Offset, error) {
	get := func(name string) (string, bool) {
		i, ok := cols[name]
		if !ok || i >= len(fields) {
			return "", false
		}
		return fields[i], true
	}
	var o Offset
	for _, c := range describeColumns {
		if _, ok := get(c); !ok {
			return o, fmt.Errorf("row has %d fields, missing %s", len(fields), c)
		}
	}
	o.Group, _ = get("GROUP")
	o.Topic, _ = get("TOPIC")
	p, _ := get("PARTITION")
	part, err := strconv.ParseInt(p, 10, 32)
	if err != nil {
		return o, fmt.Errorf("partition %q: %w", p, err)
	}
	o.Partition = int32(part)
	cur, _ := get("CURRENT-OFFSET")
	if o.Committed, err = parseOffset(cur, true); err != nil {
		return o, fmt.Errorf("current offset: %w", err)
	}
	end, _ := get("LOG-END-OFFSET")
	if o.LogEnd, err = parseOffset(end, false); err != nil {
		return o, fmt.Errorf("log-end offset: %w", err)
	}
	if err := o.Validate(); err != nil {
		return o, err
	}
	if l, ok := get("LAG"); ok && l != "-" && o.Committed >= 0 {
		want, err := strconv.ParseInt(l, 10, 64)
		if err != nil {
			return o, fmt.Errorf("lag %q: %w", l, err)
		}
		if want != o.LogEnd-o.Committed {
			return o, fmt.Errorf("%s/%d: LAG column %d does not match %d-%d", o.Topic, o.Partition, want, o.LogEnd, o.Committed)
		}
	}
	return o, nil
}

// parseOffset parses a non-negative offset; "-" means no commit when allowed.
func parseOffset(s string, dashOK bool) (int64, error) {
	if s == "-" && dashOK {
		return NoCommit, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not an offset", s)
	}
	if v < 0 {
		return 0, fmt.Errorf("negative offset %d", v)
	}
	return v, nil
}
