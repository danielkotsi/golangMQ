package bench

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"sync"
)

// csvColumns is the stable per-interval schema for Phase 4 golden diffing.
var csvColumns = []string{
	"ts_start_rfc3339",
	"window_ms",
	"published",
	"delivered",
	"consumed",
	"pub_rate_per_s",
	"delivered_rate_per_s",
	"consumed_rate_per_s",
	"avg_delivery_lat_us",
	"min_delivery_lat_us",
	"max_delivery_lat_us",
	"p50_delivery_lat_us",
	"p95_delivery_lat_us",
	"p99_delivery_lat_us",
	"avg_ack_lat_us",
	"duplicates",
	"drops_cumulative",
	"both_lat_p95_us",
	"cons_lat_p95_us",
}

// csvWriter serializes interval records behind a mutex so the sampler
// goroutine is the sole producer but the engine can flush safely after stop.
// The first write/flush error is retained and returned by every later call.
type csvWriter struct {
	mu  sync.Mutex
	w   *bufio.Writer
	n   int
	err error
}

func newCSVWriter(dst io.Writer) (*csvWriter, error) {
	cw := &csvWriter{w: bufio.NewWriter(dst)}
	if err := cw.writeHeader(); err != nil {
		return nil, err
	}
	// Flush the header immediately so open-success/write-failure destinations
	// (for example /dev/full) fail before workers start.
	if err := cw.flush(); err != nil {
		return nil, err
	}
	return cw, nil
}

func (c *csvWriter) writeHeader() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if _, err := c.w.WriteString(strings.Join(csvColumns, ",") + "\n"); err != nil {
		c.err = err
		return err
	}
	return nil
}

func (c *csvWriter) writeInterval(r intervalRecord) error {
	row := []string{
		r.TsStart.Format("2006-01-02T15:04:05Z07:00"),
		strconv.FormatFloat(r.WindowMs, 'f', 1, 64),
		strconv.FormatUint(r.Published, 10),
		strconv.FormatUint(r.Delivered, 10),
		strconv.FormatUint(r.Consumed, 10),
		strconv.FormatFloat(perSec(r.Published, r.WindowMs), 'f', 2, 64),
		strconv.FormatFloat(perSec(r.Delivered, r.WindowMs), 'f', 2, 64),
		strconv.FormatFloat(perSec(r.Consumed, r.WindowMs), 'f', 2, 64),
		strconv.FormatFloat(r.DelLat.Avg, 'f', 2, 64),
		strconv.FormatFloat(r.DelLat.Min, 'f', 2, 64),
		strconv.FormatFloat(r.DelLat.Max, 'f', 2, 64),
		strconv.FormatFloat(r.DelLat.P50, 'f', 2, 64),
		strconv.FormatFloat(r.DelLat.P95, 'f', 2, 64),
		strconv.FormatFloat(r.DelLat.P99, 'f', 2, 64),
		strconv.FormatFloat(r.AckAvgUs, 'f', 2, 64),
		strconv.FormatUint(r.Duplicates, 10),
		strconv.FormatUint(r.Drops, 10),
		strconv.FormatFloat(r.BothP95Us, 'f', 2, 64),
		strconv.FormatFloat(r.ConsP95Us, 'f', 2, 64),
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if _, err := c.w.WriteString(strings.Join(row, ",") + "\n"); err != nil {
		c.err = err
		return err
	}
	c.n++
	return nil
}

func (c *csvWriter) flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if err := c.w.Flush(); err != nil {
		c.err = err
		return err
	}
	return nil
}

// close flushes the buffer and drops the writer (engine calls it after the
// sampler goroutine has exited).
func (c *csvWriter) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.w == nil {
		return c.err
	}
	if c.err == nil {
		c.err = c.w.Flush()
	}
	c.w = nil
	return c.err
}

// perSec converts a window count into a messages-per-second rate. Zero-length
// windows (sub-ms final intervals) clamp to 0 rather than dividing by zero.
func perSec(n uint64, windowMs float64) float64 {
	if windowMs <= 0 {
		return 0
	}
	return float64(n) / (windowMs / 1000)
}
