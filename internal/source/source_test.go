package source

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/lag"
)

const metrics = `kafka_topic_partition_current_offset{topic="t",partition="0"} 10
kafka_consumergroup_current_offset{consumergroup="g",topic="t",partition="0"} 4
`

// server answers with the given statuses in turn, then 200 with metrics.
func server(t *testing.T, statuses ...int) (*httptest.Server, *atomic.Int32) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := int(calls.Add(1))
		if n <= len(statuses) {
			w.WriteHeader(statuses[n-1])
			return
		}
		_, _ = w.Write([]byte(metrics))
	}))
	t.Cleanup(s.Close)
	return s, &calls
}

// fast returns a source with a short backoff for tests.
func fast(t *testing.T, url string) *HTTP {
	h, err := NewHTTP(url, lag.ParsePrometheus, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.Backoff = time.Millisecond
	return h
}

func TestHTTPSuccess(t *testing.T) {
	s, calls := server(t)
	offs, err := fast(t, s.URL).Offsets(context.Background())
	if err != nil || len(offs) != 1 || offs[0].Lag() != 6 || calls.Load() != 1 {
		t.Fatalf("offs %+v err %v calls %d", offs, err, calls.Load())
	}
}

func TestHTTPRetriesServerErrorsThenSucceeds(t *testing.T) {
	s, calls := server(t, http.StatusServiceUnavailable, http.StatusTooManyRequests)
	if _, err := fast(t, s.URL).Offsets(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls %d, want 3", calls.Load())
	}
}

func TestHTTPGivesUpAfterAttempts(t *testing.T) {
	s, calls := server(t, 500, 500, 500, 500)
	_, err := fast(t, s.URL).Offsets(context.Background())
	if err == nil || !strings.Contains(err.Error(), "3 attempt") || calls.Load() != 3 {
		t.Fatalf("err %v calls %d, want failure after exactly 3", err, calls.Load())
	}
}

func TestHTTPClientErrorIsNotRetried(t *testing.T) {
	s, calls := server(t, http.StatusNotFound)
	if _, err := fast(t, s.URL).Offsets(context.Background()); err == nil || calls.Load() != 1 {
		t.Fatalf("err %v calls %d, want one failed call", err, calls.Load())
	}
}

func TestHTTPParseErrorIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`kafka_topic_partition_current_offset{topic="t",partition="0"} NaN`))
	}))
	defer s.Close()
	if _, err := fast(t, s.URL).Offsets(context.Background()); err == nil || calls.Load() != 1 {
		t.Fatalf("err %v calls %d", err, calls.Load())
	}
}

func TestHTTPBodyLimit(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		chunk := []byte(strings.Repeat("# padding\n", 1024))
		for written := 0; written <= lag.MaxInput; written += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer s.Close()
	if _, err := fast(t, s.URL).Offsets(context.Background()); !errors.Is(err, lag.ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
}

func TestHTTPTimeout(t *testing.T) {
	release := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer s.Close()
	defer close(release)
	h := fast(t, s.URL)
	h.Client.Timeout = 50 * time.Millisecond
	start := time.Now()
	if _, err := h.Offsets(context.Background()); err == nil {
		t.Fatal("hanging server did not fail")
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("took %v; per-attempt timeout not applied", el)
	}
}

func TestHTTPContextCancelStopsBackoff(t *testing.T) {
	s, calls := server(t, 500, 500, 500)
	h := fast(t, s.URL)
	h.Backoff = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := h.Offsets(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatalf("err %v calls %d", err, calls.Load())
	}
}

func TestNewHTTPValidation(t *testing.T) {
	for _, u := range []string{"", "ftp://h/m", "http://", "file:///etc/passwd", "http://user:pw@h/m", "://bad"} {
		if _, err := NewHTTP(u, lag.ParsePrometheus, time.Second); err == nil {
			t.Errorf("url %q accepted", u)
		}
	}
	for _, d := range []time.Duration{0, -time.Second, 2 * time.Minute} {
		if _, err := NewHTTP("http://127.0.0.1:9308/metrics", lag.ParsePrometheus, d); err == nil {
			t.Errorf("timeout %v accepted", d)
		}
	}
}

func TestFileSource(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "m.prom")
	if err := os.WriteFile(p, []byte(metrics), 0o600); err != nil {
		t.Fatal(err)
	}
	offs, err := File{Path: p, Parse: lag.ParsePrometheus}.Offsets(context.Background())
	if err != nil || len(offs) != 1 {
		t.Fatalf("%v %v", offs, err)
	}
	if _, err := (File{Path: filepath.Join(dir, "missing"), Parse: lag.ParsePrometheus}).Offsets(context.Background()); err == nil {
		t.Fatal("missing file accepted")
	}
	if err := os.WriteFile(p, []byte("GROUP\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (File{Path: p, Parse: lag.ParseDescribe}).Offsets(context.Background()); err == nil || !strings.Contains(err.Error(), p) {
		t.Fatalf("parse error should name the file: %v", err)
	}
}
