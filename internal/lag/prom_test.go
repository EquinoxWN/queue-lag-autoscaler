package lag

import (
	"os"
	"strings"
	"testing"
)

func TestParsePrometheusFixture(t *testing.T) {
	f, err := os.Open("testdata/kafka_exporter.prom")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // test file
	offs, err := ParsePrometheus(f)
	if err != nil {
		t.Fatal(err)
	}
	g, err := ByGroup(offs)
	if err != nil {
		t.Fatal(err)
	}
	if len(g) != 2 {
		t.Fatalf("groups %v, want orders-workers and emails (the unconsumed topic has no group)", g)
	}
	if got := g["orders-workers"].Lag(); got != 5900 {
		t.Fatalf("orders lag %d, want 5900", got)
	}
	e := g["emails"].Partitions[Key{"emails", 0}]
	if e.Committed != NoCommit || e.LogEnd != 25 || e.Lag() != 25 {
		t.Fatalf("emails partition %+v: -1 must mean no commit and 2.5e+01 must parse as 25", e)
	}
}

func TestParsePrometheusLabelsAndTimestamps(t *testing.T) {
	in := `kafka_topic_partition_current_offset{topic="t",partition="0"} 10 1700000000000
kafka_consumergroup_current_offset{ consumergroup="g" , topic="t", partition="0", note="a \"quoted\", \\ value\n"} 4
`
	offs, err := ParsePrometheus(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(offs) != 1 || offs[0].Lag() != 6 {
		t.Fatalf("got %+v", offs)
	}
}

func TestParsePrometheusErrors(t *testing.T) {
	end := "kafka_topic_partition_current_offset{topic=\"t\",partition=\"0\"} 10\n"
	cases := map[string]string{
		"no log end":         `kafka_consumergroup_current_offset{consumergroup="g",topic="t",partition="0"} 4`,
		"duplicate log end":  end + end,
		"duplicate group":    end + strings.Repeat(`kafka_consumergroup_current_offset{consumergroup="g",topic="t",partition="0"} 4`+"\n", 2),
		"negative log end":   `kafka_topic_partition_current_offset{topic="t",partition="0"} -3`,
		"NaN":                `kafka_topic_partition_current_offset{topic="t",partition="0"} NaN`,
		"Inf":                `kafka_topic_partition_current_offset{topic="t",partition="0"} +Inf`,
		"fraction":           `kafka_topic_partition_current_offset{topic="t",partition="0"} 1.5`,
		"no value":           `kafka_topic_partition_current_offset{topic="t",partition="0"}`,
		"three fields":       `kafka_topic_partition_current_offset{topic="t",partition="0"} 1 2 3`,
		"unterminated set":   `kafka_topic_partition_current_offset{topic="t",partition="0" 1`,
		"unterminated value": `kafka_topic_partition_current_offset{topic="t,partition="0"} 1`,
		"unquoted value":     `kafka_topic_partition_current_offset{topic=t,partition="0"} 1`,
		"repeated label":     `kafka_topic_partition_current_offset{topic="t",topic="u",partition="0"} 1`,
		"bad partition":      `kafka_topic_partition_current_offset{topic="t",partition="zero"} 1`,
		"bad group":          end + `kafka_consumergroup_current_offset{consumergroup="a b",topic="t",partition="0"} 4`,
	}
	for name, in := range cases {
		if _, err := ParsePrometheus(strings.NewReader(in)); err == nil {
			t.Errorf("%s: accepted %q", name, in)
		}
	}
}

func TestParsePrometheusIgnoresOtherMetrics(t *testing.T) {
	in := "# just comments\nup 1\nkafka_consumergroup_lag{consumergroup=\"g\",topic=\"t\",partition=\"0\"} 5\n"
	offs, err := ParsePrometheus(strings.NewReader(in))
	if err != nil || len(offs) != 0 {
		t.Fatalf("got %v, %v; want no offsets and no error", offs, err)
	}
}
