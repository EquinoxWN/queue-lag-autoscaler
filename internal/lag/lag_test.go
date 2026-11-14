package lag

import (
	"os"
	"strings"
	"testing"
)

func TestOffsetLag(t *testing.T) {
	cases := []struct {
		committed, logEnd, want int64
	}{
		{0, 0, 0},
		{10, 10, 0},
		{10, 25, 15},
		{NoCommit, 40, 40}, // no commit: the whole log counts
		{50, 40, 0},        // committed past the end (topic recreated): no negative lag
	}
	for _, c := range cases {
		o := Offset{Group: "g", Topic: "t", Committed: c.committed, LogEnd: c.logEnd}
		if got := o.Lag(); got != c.want {
			t.Errorf("Lag(committed=%d, end=%d) = %d, want %d", c.committed, c.logEnd, got, c.want)
		}
	}
}

func TestValidateRejects(t *testing.T) {
	good := Offset{Group: "g-1.a_b", Topic: "orders", Partition: 0, Committed: NoCommit, LogEnd: 0}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid offset rejected: %v", err)
	}
	bad := map[string]Offset{
		"empty group":     {Group: "", Topic: "t"},
		"space in group":  {Group: "a b", Topic: "t"},
		"slash in topic":  {Group: "g", Topic: "a/b"},
		"long topic":      {Group: "g", Topic: strings.Repeat("x", 250)},
		"neg partition":   {Group: "g", Topic: "t", Partition: -1},
		"committed -2":    {Group: "g", Topic: "t", Committed: -2},
		"negative logend": {Group: "g", Topic: "t", LogEnd: -1},
	}
	for name, o := range bad {
		if err := o.Validate(); err == nil {
			t.Errorf("%s: accepted %+v", name, o)
		}
	}
}

func TestByGroupSumsAndRefusesDuplicates(t *testing.T) {
	offs := []Offset{
		{Group: "a", Topic: "t", Partition: 0, Committed: 5, LogEnd: 10},
		{Group: "a", Topic: "t", Partition: 1, Committed: 0, LogEnd: 7},
		{Group: "a", Topic: "u", Partition: 0, Committed: NoCommit, LogEnd: 3},
		{Group: "b", Topic: "t", Partition: 0, Committed: 10, LogEnd: 10},
	}
	g, err := ByGroup(offs)
	if err != nil {
		t.Fatal(err)
	}
	if len(g) != 2 || g["a"].Lag() != 15 || g["b"].Lag() != 0 || len(g["a"].Partitions) != 3 {
		t.Fatalf("unexpected grouping: %+v", g)
	}
	if _, err := ByGroup(append(offs, offs[1])); err == nil {
		t.Fatal("duplicate partition accepted")
	}
	if _, err := ByGroup([]Offset{{Group: "a", Topic: "bad topic"}}); err == nil {
		t.Fatal("invalid offset accepted")
	}
	if g, err := ByGroup(nil); err != nil || len(g) != 0 {
		t.Fatalf("empty input: %v %v", g, err)
	}
}

func TestSortedOrdersByGroupTopicPartition(t *testing.T) {
	in := []Offset{
		{Group: "b", Topic: "t", Partition: 0},
		{Group: "a", Topic: "u", Partition: 1},
		{Group: "a", Topic: "u", Partition: 0},
		{Group: "a", Topic: "t", Partition: 2},
	}
	got := Sorted(in)
	want := []string{"a/t/2", "a/u/0", "a/u/1", "b/t/0"}
	for i, o := range got {
		if s := o.Group + "/" + o.Topic + "/" + string(rune('0'+o.Partition)); s != want[i] {
			t.Fatalf("position %d: %s, want %s", i, s, want[i])
		}
	}
	if in[0].Group != "b" {
		t.Fatal("Sorted modified its input")
	}
}

func TestShippedExamplesParse(t *testing.T) {
	for _, p := range []string{"../../examples/describe-before.txt", "../../examples/describe-after.txt"} {
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		offs, err := ParseDescribe(f)
		_ = f.Close()
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		if len(offs) != 6 {
			t.Fatalf("%s: %d rows, want 6", p, len(offs))
		}
	}
}
