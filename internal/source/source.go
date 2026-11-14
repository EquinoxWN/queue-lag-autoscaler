// Package source fetches offset snapshots over HTTP or from a file.
package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/EquinoxWN/queue-lag-autoscaler/internal/lag"
)

// Parser turns a snapshot into offsets.
type Parser func(io.Reader) ([]lag.Offset, error)

// HTTP polls a metrics endpoint with a per-attempt timeout and bounded retries.
type HTTP struct {
	URL      string
	Parse    Parser
	Client   *http.Client
	Attempts int           // total attempts per snapshot, at least 1
	Backoff  time.Duration // wait before the second attempt; doubles each time
}

// NewHTTP checks the URL and returns a source with a timeout per attempt and 3 attempts.
func NewHTTP(rawURL string, parse Parser, timeout time.Duration) (*HTTP, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("source url: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("source url %q must be http(s)://host/...", rawURL)
	}
	if u.User != nil {
		return nil, errors.New("source url must not carry credentials")
	}
	if timeout <= 0 || timeout > time.Minute {
		return nil, fmt.Errorf("timeout %v outside (0, 1m]", timeout)
	}
	return &HTTP{URL: u.String(), Parse: parse, Client: &http.Client{Timeout: timeout}, Attempts: 3, Backoff: 200 * time.Millisecond}, nil
}

// errPermanent marks failures that a retry cannot fix.
type errPermanent struct{ err error }

// Error returns the wrapped message.
func (e errPermanent) Error() string { return e.err.Error() }

// Unwrap exposes the wrapped error.
func (e errPermanent) Unwrap() error { return e.err }

// Offsets fetches and parses one snapshot, retrying network errors, 429 and 5xx with backoff.
func (h *HTTP) Offsets(ctx context.Context) ([]lag.Offset, error) {
	attempts := max(h.Attempts, 1)
	wait := h.Backoff
	var last error
	for i := range attempts {
		if i > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, fmt.Errorf("fetch %s: %w (last error: %v)", h.URL, ctx.Err(), last)
			case <-t.C:
			}
			wait *= 2
		}
		offs, err := h.once(ctx)
		if err == nil {
			return offs, nil
		}
		last = err
		var perm errPermanent
		if errors.As(err, &perm) || ctx.Err() != nil {
			break
		}
	}
	return nil, fmt.Errorf("fetch %s after %d attempt(s): %w", h.URL, attempts, last)
}

// once performs a single GET and parses the bounded body.
func (h *HTTP) once(ctx context.Context) ([]lag.Offset, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.URL, nil)
	if err != nil {
		return nil, errPermanent{err}
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // body fully read or abandoned; close error carries nothing
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)) // drain a little so the connection can be reused
		err := fmt.Errorf("status %s", resp.Status)
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return nil, err
		}
		return nil, errPermanent{err}
	}
	offs, err := h.Parse(resp.Body)
	if err != nil {
		if errors.Is(err, lag.ErrTooLarge) || !isNetErr(err) {
			return nil, errPermanent{err}
		}
		return nil, err
	}
	return offs, nil
}

// isNetErr reports whether a parse failed because the body could not be read.
func isNetErr(err error) bool {
	var ne interface{ Timeout() bool }
	return errors.As(err, &ne) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded)
}

// File reads a snapshot from a path on every call.
type File struct {
	Path  string
	Parse Parser
}

// Offsets opens the file and parses it.
func (f File) Offsets(context.Context) ([]lag.Offset, error) {
	fh, err := os.Open(f.Path)
	if err != nil {
		return nil, fmt.Errorf("open snapshot: %w", err)
	}
	defer fh.Close() //nolint:errcheck // read-only file
	offs, err := f.Parse(fh)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Path, err)
	}
	return offs, nil
}
