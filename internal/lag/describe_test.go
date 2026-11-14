package lag

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
)

const header = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n"

func TestParseDescribeExample(t *testing.T) {
	f, err := os.Open("../../examples/describe-before.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // test file
	offs, err := ParseDescribe(f)
	if err != nil {
		t.Fatal(err)
	}
	g, err := ByGroup(offs)
	if err != nil {
		t.Fatal(err)
	}
	if got := g["orders-workers"].Lag(); got != 11800 {
		t.Fatalf("orders lag %d, want 11800", got)
	}
	if got := g["emails"].Lag(); got != 20 {
		t.Fatalf("emails lag %d, want 20 (partition 1 has no commit, 20 messages)", got)
	}
	p1 := g["emails"].Partitions[Key{"emails", 1}]
	if p1.Committed != NoCommit || p1.LogEnd != 20 {
		t.Fatalf("dash offset parsed as %+v", p1)
	}
}

func TestParseDescribeColumnOrderFromHeader(t *testing.T) {
	// columns in another order, an extra column and no LAG column
	in := "GROUP PARTITION EXTRA LOG-END-OFFSET TOPIC CURRENT-OFFSET\ng1 3 x 50 orders 20\n"
	offs, err := ParseDescribe(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	want := Offset{Group: "g1", Topic: "orders", Partition: 3, Committed: 20, LogEnd: 50}
	if len(offs) != 1 || offs[0] != want {
		t.Fatalf("got %+v, want %+v", offs, want)
	}
}

func TestParseDescribeErrors(t *testing.T) {
	cases := map[string]string{
		"empty":              "",
		"no header":          "g t 0 1 2 1 - - -\n",
		"tool error":         "Error: Consumer group 'x' does not exist.\n",
		"missing column":     "GROUP TOPIC PARTITION LOG-END-OFFSET\n",
		"short row":          header + "g t 0 5\n",
		"bad partition":      header + "g t x 5 6 1 - - -\n",
		"negative partition": header + "g t -1 5 6 1 - - -\n",
		"bad offset":         header + "g t 0 five 6 1 - - -\n",
		"negative offset":    header + "g t 0 -5 6 11 - - -\n",
		"dash log end":       header + "g t 0 5 - - - - -\n",
		"lag mismatch":       header + "g t 0 5 9 3 - - -\n",
		"bad lag":            header + "g t 0 5 9 four - - -\n",
		"duplicate":          header + "g t 0 5 9 4 - - -\ng t 0 5 9 4 - - -\n",
		"bad topic":          header + "g t/x 0 5 9 4 - - -\n",
	}
	for name, in := range cases {
		if _, err := ParseDescribe(strings.NewReader(in)); err == nil {
			t.Errorf("%s: accepted %q", name, in)
		}
	}
}

func TestParseDescribeTooLarge(t *testing.T) {
	big := bytes.Repeat([]byte(" "), MaxInput+1)
	if _, err := ParseDescribe(bytes.NewReader(big)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
}

func TestParseDescribeSkipsNotesAndBlankLines(t *testing.T) {
	in := "\nConsumer group 'g' has no active members.\nWarning: something\n\n" + header + "g t 0 5 9 4 - - -\n\n"
	offs, err := ParseDescribe(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(offs) != 1 || offs[0].Lag() != 4 {
		t.Fatalf("got %+v", offs)
	}
}
