package bench

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// shared assertions
// ---------------------------------------------------------------------------

func assertLatencyInvariants(t *testing.T, name string, l LatencySummary) {
	t.Helper()
	if l.Count == 0 {
		if l.Min != 0 || l.Avg != 0 || l.Max != 0 || l.P50 != 0 || l.P95 != 0 || l.P99 != 0 {
			t.Fatalf("%s latency has no samples but non-zero values: %+v", name, l)
		}
		return
	}
	if l.Min < 0 || l.Avg < 0 || l.Max < 0 || l.P50 < 0 || l.P95 < 0 || l.P99 < 0 {
		t.Fatalf("%s latency contains a negative value: %+v", name, l)
	}
	if l.Min > l.P50 || l.P50 > l.P95 || l.P95 > l.P99 || l.P99 > l.Max {
		t.Fatalf("%s percentiles are not monotonic (min=%v p50=%v p95=%v p99=%v max=%v)",
			name, l.Min, l.P50, l.P95, l.P99, l.Max)
	}
	if l.Avg < l.Min || l.Avg > l.Max {
		t.Fatalf("%s avg=%v outside [min=%v,max=%v]", name, l.Avg, l.Min, l.Max)
	}
}

// assertCleanRunInvariants encodes section 1 of the bulletproof testing plan for
// a completed, acking, non-duration, non-warmup run.
func assertCleanRunInvariants(t *testing.T, cfg *Config, s Summary) {
	t.Helper()

	wantPublished := uint64(cfg.Publishers * cfg.Msgs)
	if s.Published != wantPublished {
		t.Fatalf("published=%d want publishers*msgs=%d", s.Published, wantPublished)
	}
	if s.Consumed != wantPublished {
		t.Fatalf("consumed=%d want %d for a clean acking run", s.Consumed, wantPublished)
	}
	if s.Delivered != s.Consumed {
		t.Fatalf("delivered=%d consumed=%d want equal (no duplicates in clean run)", s.Delivered, s.Consumed)
	}
	if s.Duplicates != 0 {
		t.Fatalf("duplicates=%d want 0", s.Duplicates)
	}
	if s.Drops != 0 {
		t.Fatalf("drops=%d want 0", s.Drops)
	}
	if s.Consumed+s.Drops != s.Published {
		t.Fatalf("consumed(%d)+drops(%d) != published(%d)", s.Consumed, s.Drops, s.Published)
	}
	if s.Delivered < s.Consumed {
		t.Fatalf("delivered=%d < consumed=%d", s.Delivered, s.Consumed)
	}
	if s.Duplicates > s.Delivered {
		t.Fatalf("duplicates=%d > delivered=%d", s.Duplicates, s.Delivered)
	}

	var byConnPublished, byConnConsumed uint64
	for _, r := range s.ByConn {
		byConnPublished += r.Published
		byConnConsumed += r.Consumed
		if r.AvgDeliveryLatUs < 0 {
			t.Fatalf("by_conn[%d] avg delivery latency is negative: %v", r.Conn, r.AvgDeliveryLatUs)
		}
	}
	if byConnPublished != s.Published {
		t.Fatalf("sum(by_conn.published)=%d != published=%d", byConnPublished, s.Published)
	}
	if byConnConsumed != s.Consumed {
		t.Fatalf("sum(by_conn.consumed)=%d != consumed=%d", byConnConsumed, s.Consumed)
	}

	var byPubChPublished uint64
	for _, r := range s.ByPublishChannel {
		byPubChPublished += r.Published
	}
	if byPubChPublished != s.Published {
		t.Fatalf("sum(by_publish_channel.published)=%d != published=%d", byPubChPublished, s.Published)
	}

	var roleN uint64
	for _, r := range s.ByRole {
		roleN += r.N
	}
	if roleN != s.Delivered {
		t.Fatalf("sum(by_role.n)=%d != delivered=%d", roleN, s.Delivered)
	}

	assertLatencyInvariants(t, "delivery", s.DeliveryLatency)
	assertLatencyInvariants(t, "ack", s.AckLatency)

	if s.ConsumedRatePerSec < 0 {
		t.Fatalf("negative consumed rate: %v", s.ConsumedRatePerSec)
	}
	if s.ElapsedMs < 0 {
		t.Fatalf("negative elapsed_ms=%d", s.ElapsedMs)
	}
	if s.RunID == "" {
		t.Fatal("empty runid")
	}
}

// ---------------------------------------------------------------------------
// 1) core invariants on clean runs
// ---------------------------------------------------------------------------

func TestHarness_CleanRunInvariants(t *testing.T) {
	addr := startBroker(t)

	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{name: "single", mut: func(c *Config) {}},
		{
			name: "many conns channels publishers consumers",
			mut: func(c *Config) {
				c.Conns = 4
				c.Channels = 8
				c.Publishers = 8
				c.Consumers = 8
				c.Msgs = 100
			},
		},
		{
			name: "separated pub and consume conns",
			mut: func(c *Config) {
				c.Conns = 2
				c.PubConns = 1
				c.ConsConns = 1
				c.Channels = 2
				c.Publishers = 2
				c.Consumers = 2
				c.Msgs = 50
			},
		},
		{
			name: "many consumers on one channel",
			mut: func(c *Config) {
				c.Conns = 1
				c.Channels = 1
				c.Publishers = 4
				c.Consumers = 4
				c.Msgs = 50
			},
		},
		{
			name: "uneven capped packing",
			mut: func(c *Config) {
				c.Conns = 3
				c.Channels = 5
				c.ConnsDist = DistUneven
				c.MaxChannelsPerConn = 2
				c.Publishers = 3
				c.Consumers = 3
				c.Msgs = 30
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tinyRunCfg(t, addr)
			cfg.Report = 10 * time.Millisecond
			cfg.DrainTimeout = 10 * time.Second
			tc.mut(cfg)

			res, err := Run(context.Background(), cfg)
			if err != nil {
				t.Fatalf("clean run failed: %v", err)
			}
			assertCleanRunInvariants(t, cfg, res.Summary)
		})
	}
}

func TestHarness_CSVSchemaAndWindowSums(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Publishers = 4
	cfg.Consumers = 4
	cfg.Msgs = 100
	cfg.Report = 10 * time.Millisecond
	cfg.DrainTimeout = 10 * time.Second

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	raw, err := os.ReadFile(res.CSVPath)
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	rows, err := csv.NewReader(bytes.NewReader(raw)).ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("expected header + at least one data row, got %d rows", len(rows))
	}
	if got := rows[0]; !equalStrings(got, csvColumns) {
		t.Fatalf("csv header mismatch:\n got %q\nwant %q", got, csvColumns)
	}
	for i, row := range rows[1:] {
		if len(row) != len(csvColumns) {
			t.Fatalf("data row %d has %d columns, want %d: %q", i+1, len(row), len(csvColumns), row)
		}
		if row[0] == "" {
			t.Fatalf("data row %d has empty timestamp", i+1)
		}
	}

	col := func(row []string, i int) uint64 {
		t.Helper()
		v, err := strconv.ParseUint(row[i], 10, 64)
		if err != nil {
			t.Fatalf("parse column %s=%q: %v", csvColumns[i], row[i], err)
		}
		return v
	}

	var pub, del, cons, dup uint64
	var lastDrops uint64
	for i, row := range rows[1:] {
		pub += col(row, 2)
		del += col(row, 3)
		cons += col(row, 4)
		dup += col(row, 15)
		lastDrops = col(row, 16)

		window, err := strconv.ParseFloat(row[1], 64)
		if err != nil || window < 0 {
			t.Fatalf("row %d window_ms=%q is not a non-negative number", i+1, row[1])
		}
		for j := 8; j <= 13; j++ {
			v, err := strconv.ParseFloat(row[j], 64)
			if err != nil || v < 0 {
				t.Fatalf("row %d column %s=%q is not a non-negative latency", i+1, csvColumns[j], row[j])
			}
		}
		p50, _ := strconv.ParseFloat(row[11], 64)
		p95, _ := strconv.ParseFloat(row[12], 64)
		p99, _ := strconv.ParseFloat(row[13], 64)
		if !(p50 <= p95 && p95 <= p99) {
			t.Fatalf("row %d latency percentiles are not monotonic: p50=%v p95=%v p99=%v", i+1, p50, p95, p99)
		}
	}

	if pub != res.Summary.Published || del != res.Summary.Delivered || cons != res.Summary.Consumed || dup != res.Summary.Duplicates {
		t.Fatalf("CSV row sums do not match summary: csv=%d/%d/%d/%d summary=%d/%d/%d/%d",
			pub, del, cons, dup,
			res.Summary.Published, res.Summary.Delivered, res.Summary.Consumed, res.Summary.Duplicates)
	}
	if lastDrops != res.Summary.Drops {
		t.Fatalf("last CSV drops=%d != summary drops=%d", lastDrops, res.Summary.Drops)
	}
}

func TestHarness_FinalCSVRowWrittenWhenReportExceedsRun(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Msgs = 10
	cfg.Report = 5 * time.Second
	cfg.DrainTimeout = 2 * time.Second

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	raw, err := os.ReadFile(res.CSVPath)
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	rows, err := csv.NewReader(bytes.NewReader(raw)).ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("expected a final interval row even though -report > run duration, got %d rows", len(rows))
	}
}

func TestHarness_NoAckPrefetchStallIsConsistent(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Publishers = 1
	cfg.Consumers = 1
	cfg.Msgs = 100
	cfg.Ack = false
	cfg.Report = 50 * time.Millisecond
	cfg.DrainTimeout = 300 * time.Millisecond

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("no-ack run: %v", err)
	}
	s := res.Summary
	const prefetch = 10
	if s.Consumed > prefetch {
		t.Fatalf("no-ack consumed=%d exceeds broker prefetch %d", s.Consumed, prefetch)
	}
	if s.Drops != s.Published-s.Consumed {
		t.Fatalf("drops=%d want published(%d)-consumed(%d)", s.Drops, s.Published, s.Consumed)
	}
	if s.Consumed+s.Drops != s.Published {
		t.Fatalf("consumed(%d)+drops(%d) != published(%d)", s.Consumed, s.Drops, s.Published)
	}
	if s.Drops == 0 {
		t.Fatalf("expected a prefetch stall to leave drops > 0")
	}
	if s.AckLatency.Min != 0 || s.AckLatency.Avg != 0 || s.AckLatency.Max != 0 ||
		s.AckLatency.P50 != 0 || s.AckLatency.P95 != 0 || s.AckLatency.P99 != 0 {
		t.Fatalf("ack latency recorded without -ack: %+v", s.AckLatency)
	}
}

// TestFinding_Bench008_NoAckRecordsZeroAckLatencySamples captures the summary
// reporting ack_latency_us.count > 0 even when -ack=false. The worker calls
// recordAck(0) for every delivery regardless of w.ack, so the count claims ack
// samples exist while the rest of the ack histogram is zero.
func TestFinding_Bench008_NoAckRecordsZeroAckLatencySamples(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Publishers = 1
	cfg.Consumers = 1
	cfg.Msgs = 100
	cfg.Ack = false
	cfg.Report = 50 * time.Millisecond
	cfg.DrainTimeout = 300 * time.Millisecond

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("no-ack run: %v", err)
	}
	if n := res.Summary.AckLatency.Count; n != 0 {
		t.Fatalf("ack latency count=%d with -ack=false; want 0 samples (values are all zero)", n)
	}
}

func TestHarness_DurationStopsPublishersThenDrains(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Publishers = 2
	cfg.Consumers = 2
	cfg.Msgs = 100000
	cfg.Duration = 50 * time.Millisecond
	cfg.Report = 20 * time.Millisecond
	cfg.DrainTimeout = 10 * time.Second

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("duration run: %v", err)
	}
	s := res.Summary
	total := uint64(cfg.Publishers * cfg.Msgs)
	if s.Published == 0 {
		t.Fatalf("duration run published 0 messages")
	}
	if s.Published >= total {
		t.Fatalf("duration run published all %d messages; workload should have been cut off at %v", total, cfg.Duration)
	}
	if s.Consumed+s.Drops != s.Published {
		t.Fatalf("consumed(%d)+drops(%d) != published(%d)", s.Consumed, s.Drops, s.Published)
	}
	if s.Duplicates > s.Delivered {
		t.Fatalf("duplicates=%d > delivered=%d", s.Duplicates, s.Delivered)
	}
}

// ---------------------------------------------------------------------------
// 3) metamorphic transformations
// ---------------------------------------------------------------------------

func TestMetamorphic_AccountingPreserved(t *testing.T) {
	addr := startBroker(t)

	run := func(mut func(*Config)) Summary {
		t.Helper()
		cfg := tinyRunCfg(t, addr)
		cfg.Report = 10 * time.Millisecond
		cfg.DrainTimeout = 10 * time.Second
		mut(cfg)
		res, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		assertCleanRunInvariants(t, cfg, res.Summary)
		return res.Summary
	}

	compare := func(name string, a, b Summary) {
		t.Helper()
		if a.Published != b.Published || a.Delivered != b.Delivered ||
			a.Consumed != b.Consumed || a.Duplicates != b.Duplicates || a.Drops != b.Drops {
			t.Fatalf("%s changed accounting: a={pub:%d del:%d cons:%d dup:%d drops:%d} b={pub:%d del:%d cons:%d dup:%d drops:%d}",
				name,
				a.Published, a.Delivered, a.Consumed, a.Duplicates, a.Drops,
				b.Published, b.Delivered, b.Consumed, b.Duplicates, b.Drops)
		}
	}

	t.Run("publisher split", func(t *testing.T) {
		a := run(func(c *Config) {
			c.Publishers = 1
			c.Msgs = 200
		})
		b := run(func(c *Config) {
			c.Publishers = 2
			c.Msgs = 100
		})
		compare("1x200 vs 2x100", a, b)
	})

	t.Run("channels across connections", func(t *testing.T) {
		a := run(func(c *Config) {
			c.Conns = 1
			c.Channels = 4
			c.Publishers = 4
			c.Consumers = 4
			c.Msgs = 50
		})
		b := run(func(c *Config) {
			c.Conns = 4
			c.Channels = 4
			c.Publishers = 4
			c.Consumers = 4
			c.Msgs = 50
		})
		compare("1 conn x 4 channels vs 4 conns x 4 channels", a, b)
	})

	t.Run("consumer count", func(t *testing.T) {
		a := run(func(c *Config) {
			c.Publishers = 4
			c.Msgs = 50
			c.Consumers = 1
		})
		b := run(func(c *Config) {
			c.Publishers = 4
			c.Msgs = 50
			c.Consumers = 4
		})
		compare("1 consumer vs 4 consumers", a, b)
	})

	t.Run("report interval", func(t *testing.T) {
		a := run(func(c *Config) {
			c.Publishers = 2
			c.Consumers = 2
			c.Msgs = 100
			c.Report = 5 * time.Millisecond
		})
		b := run(func(c *Config) {
			c.Publishers = 2
			c.Consumers = 2
			c.Msgs = 100
			c.Report = 500 * time.Millisecond
		})
		compare("report 5ms vs 500ms", a, b)
	})
}

// ---------------------------------------------------------------------------
// 4) topology & configuration validation
// ---------------------------------------------------------------------------

func TestTopology_RoleCarvingUsesGlobalIndexOrder(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Conns = 2
	cfg.Channels = 6
	cfg.PubChannels = 2
	cfg.ConsChannels = 2

	topo, err := Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// The manual defines the canonical order as global channel index 0..5:
	// first -pub-channels become P, next -cons-channels become C, rest B.
	wantRole := map[int]Role{
		0: RolePublish,
		1: RolePublish,
		2: RoleConsume,
		3: RoleConsume,
		4: RoleBoth,
		5: RoleBoth,
	}
	for _, cp := range topo.Conns {
		for _, sl := range cp.Channels {
			if got, want := sl.Role, wantRole[sl.GlobalIndex]; got != want {
				t.Fatalf("global channel %d (conn %d ch %d) role=%s want %s",
					sl.GlobalIndex, sl.Conn, sl.Ch, got, want)
			}
		}
	}

	if got, want := globalIndices(topo.publishCapable), []int{0, 1, 4, 5}; !equalInts(got, want) {
		t.Fatalf("publish-capable global order=%v want %v", got, want)
	}
	if got, want := globalIndices(topo.consumeCapable), []int{2, 3, 4, 5}; !equalInts(got, want) {
		t.Fatalf("consume-capable global order=%v want %v", got, want)
	}
}

func TestTopology_ChannelsFewerThanConnsAllowed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Conns = 4
	cfg.Channels = 2

	topo, err := Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	got := perConnCounts(topo)
	want := []int{1, 1, 0, 0}
	if !equalInts(got, want) {
		t.Fatalf("per-conn channel counts=%v want %v", got, want)
	}
}

func TestTopology_CapabilityListsAndWorkerPlacement(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{
			name: "separated",
			mut: func(c *Config) {
				c.Conns = 2
				c.PubConns = 1
				c.ConsConns = 1
				c.Channels = 2 // conn0 P, conn1 C
				c.Publishers = 2
				c.Consumers = 2
			},
		},
		{
			name: "carved mixed",
			mut: func(c *Config) {
				c.Conns = 2
				c.Channels = 6
				c.PubChannels = 2
				c.ConsChannels = 2
				c.Publishers = 3
				c.Consumers = 3
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mut(cfg)
			topo, err := Build(cfg)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			for _, sl := range topo.publishCapable {
				if sl.Role == RoleConsume {
					t.Fatalf("consume-only global channel %d in publish-capable list", sl.GlobalIndex)
				}
			}
			for _, sl := range topo.consumeCapable {
				if sl.Role == RolePublish {
					t.Fatalf("publish-only global channel %d in consume-capable list", sl.GlobalIndex)
				}
			}
			var pubs, cons int
			for _, cp := range topo.Conns {
				for _, sl := range cp.Channels {
					pubs += len(sl.PubGoroutines)
					cons += len(sl.ConsGoroutines)
					if len(sl.PubGoroutines) > 0 && sl.Role == RoleConsume {
						t.Fatalf("publisher placed on consume-only global channel %d", sl.GlobalIndex)
					}
					if len(sl.ConsGoroutines) > 0 && sl.Role == RolePublish {
						t.Fatalf("consumer placed on publish-only global channel %d", sl.GlobalIndex)
					}
				}
			}
			if pubs != cfg.Publishers {
				t.Fatalf("placed %d publishers, want %d", pubs, cfg.Publishers)
			}
			if cons != cfg.Consumers {
				t.Fatalf("placed %d consumers, want %d", cons, cfg.Consumers)
			}
		})
	}
}

func TestConfig_ValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{name: "empty addr", mut: func(c *Config) { c.Addr = "" }},
		{name: "zero conns", mut: func(c *Config) { c.Conns = 0 }},
		{name: "negative pub conns", mut: func(c *Config) { c.PubConns = -1 }},
		{name: "negative cons conns", mut: func(c *Config) { c.ConsConns = -1 }},
		{name: "pub plus cons conns overflow", mut: func(c *Config) { c.PubConns = 1; c.ConsConns = 1 }},
		{name: "zero channels", mut: func(c *Config) { c.Channels = 0 }},
		{name: "negative pub channels", mut: func(c *Config) { c.PubChannels = -1 }},
		{name: "negative cons channels", mut: func(c *Config) { c.ConsChannels = -1 }},
		{name: "negative publishers", mut: func(c *Config) { c.Publishers = -1 }},
		{name: "negative consumers", mut: func(c *Config) { c.Consumers = -1 }},
		{name: "negative msgs", mut: func(c *Config) { c.Msgs = -1 }},
		{name: "size below header", mut: func(c *Config) { c.Size = headerLen - 1 }},
		{name: "zero report", mut: func(c *Config) { c.Report = 0 }},
		{name: "zero drain timeout", mut: func(c *Config) { c.DrainTimeout = 0 }},
		{name: "negative warmup", mut: func(c *Config) { c.Warmup = -time.Second }},
		{name: "negative duration", mut: func(c *Config) { c.Duration = -time.Second }},
		{name: "invalid conns distribution", mut: func(c *Config) { c.ConnsDist = "bogus" }},
		{name: "invalid channels distribution", mut: func(c *Config) { c.ChannelsDist = "bogus" }},
		{name: "invalid pub channels distribution", mut: func(c *Config) { c.PubChannelsDist = "bogus" }},
		{
			name: "role carving exceeds mixed slots",
			mut: func(c *Config) {
				c.Conns = 2
				c.Channels = 2
				c.PubChannels = 2
				c.ConsChannels = 1
			},
		},
		{
			name: "uneven channels cap overflow",
			mut: func(c *Config) {
				c.Conns = 2
				c.Channels = 5
				c.ConnsDist = DistUneven
				c.MaxChannelsPerConn = 2
			},
		},
		{
			name: "uneven consumers cap overflow",
			mut: func(c *Config) {
				c.Conns = 2
				c.Channels = 2
				c.Consumers = 5
				c.ChannelsDist = DistUneven
				c.MaxConsumersPerChannel = 2
			},
		},
		{
			name: "uneven publishers cap overflow",
			mut: func(c *Config) {
				c.Conns = 2
				c.Channels = 2
				c.Publishers = 5
				c.PubChannelsDist = DistUneven
				c.MaxPublishersPerChannel = 2
			},
		},
		{
			name: "consumers without consume-capable channel",
			mut: func(c *Config) {
				c.Conns = 1
				c.PubConns = 1
				c.Channels = 1
				c.Consumers = 1
			},
		},
		{
			name: "publishers without publish-capable channel",
			mut: func(c *Config) {
				c.Conns = 1
				c.ConsConns = 1
				c.Channels = 1
				c.Publishers = 1
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mut(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate accepted invalid config: %+v", cfg)
			}
		})
	}
}

func TestConfig_Warnings(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Publishers = 1000
	cfg.Msgs = 1000 // total > 65535
	cfg.Ack = false
	cfg.Prefetch = 5
	cfg.MaxChannelsPerConn = 2 // even axis: ignored

	warnings := strings.Join(cfg.Warnings(), "\n")
	for _, want := range []string{"delivery-tag", "will not ack", "-prefetch 5 ignored", "-max-channels-per-conn ignored"} {
		if !strings.Contains(warnings, want) {
			t.Fatalf("warnings missing %q:\n%s", want, warnings)
		}
	}
}

// ---------------------------------------------------------------------------
// 6) percentile / latency statistics
// ---------------------------------------------------------------------------

func TestPercentileAlgorithm(t *testing.T) {
	cases := []struct {
		samples []float64
		p       float64
		want    float64
	}{
		{[]float64{5}, 50, 5},
		{[]float64{5}, 99, 5},
		{[]float64{1, 2}, 50, 1},
		{[]float64{1, 2}, 95, 2},
		{[]float64{1, 2}, 99, 2},
		{[]float64{1, 2, 3, 4}, 25, 1},
		{[]float64{1, 2, 3, 4}, 50, 2},
		{[]float64{1, 2, 3, 4}, 75, 3},
		{[]float64{1, 2, 3, 4}, 100, 4},
		{[]float64{10, 20, 30, 40, 50}, 50, 30},
		{[]float64{10, 20, 30, 40, 50}, 90, 50},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 50, 5},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 95, 10},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 99, 10},
	}
	for _, tc := range cases {
		if got := percentile(tc.samples, tc.p); got != tc.want {
			t.Fatalf("percentile(%v, %v)=%v want %v", tc.samples, tc.p, got, tc.want)
		}
	}

	samples := make([]float64, 1000)
	for i := range samples {
		samples[i] = float64((i * 37) % 997)
	}
	sort.Float64s(samples)
	last := -1.0
	for p := 0.0; p <= 100; p++ {
		got := percentile(samples, p)
		if got < last {
			t.Fatalf("percentile is not monotonic at p=%v: %v < %v", p, got, last)
		}
		last = got
	}
}

func TestLatencySetSummaryAndReset(t *testing.T) {
	s := newLatencySet()
	for _, v := range []float64{4, 1, 3, 2} {
		s.add(v)
	}
	got := s.summary()
	want := LatencySummary{Count: 4, Min: 1, Avg: 2.5, Max: 4, P50: 2, P95: 4, P99: 4}
	if got != want {
		t.Fatalf("summary=%+v want %+v", got, want)
	}
	s.reset()
	if got := s.summary(); got.Count != 0 || got.Min != 0 || got.Max != 0 || got.P50 != 0 {
		t.Fatalf("summary after reset=%+v want zero", got)
	}
}

// ---------------------------------------------------------------------------
// 5) warmup findings
// ---------------------------------------------------------------------------

// TestFinding_Bench001_WarmupLongerThanWorkloadIsNotSilentlyZero captures the
// known warmup bug: when every message is published before -warmup expires,
// resetAll zeros the counters and the run succeeds with an all-zero summary.
// A fixed harness may either (a) return a clear error, or (b) measure the
// workload correctly. Silently reporting zero is never acceptable.
func TestFinding_Bench001_WarmupLongerThanWorkloadIsNotSilentlyZero(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Msgs = 10
	cfg.Warmup = 250 * time.Millisecond
	cfg.Report = 25 * time.Millisecond
	cfg.DrainTimeout = 2 * time.Second

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Logf("harness rejected warmup-longer-than-workload clearly: %v", err)
		return
	}
	if res.Summary.Published == 0 {
		t.Fatalf("warmup=%v exceeded the workload but Run returned success with an all-zero summary: %+v",
			cfg.Warmup, res.Summary)
	}
	if res.Summary.Published != uint64(cfg.Publishers*cfg.Msgs) {
		t.Fatalf("after successful warmup run published=%d want %d", res.Summary.Published, cfg.Publishers*cfg.Msgs)
	}
	if res.Summary.Consumed != res.Summary.Published || res.Summary.Drops != 0 {
		t.Fatalf("warmup run is not internally consistent: published=%d consumed=%d drops=%d",
			res.Summary.Published, res.Summary.Consumed, res.Summary.Drops)
	}
}

// TestFinding_Bench002_WarmupShorterThanWorkloadKeepsAccountingConsistent
// covers the other half of the warmup bug: because resetAll only zeroes the
// counters (not the in-flight messages, sentTimes or seen map), deliveries of
// pre-reset messages are counted as post-reset consumption. The drain decision
// then stops early and the engine closes clients while workers are still acking,
// producing "worker failures: connection closed" and/or consumed > published.
func TestFinding_Bench002_WarmupShorterThanWorkloadKeepsAccountingConsistent(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Publishers = 2
	cfg.Consumers = 2
	cfg.Msgs = 5000
	cfg.Warmup = 10 * time.Millisecond
	cfg.Report = 10 * time.Millisecond
	cfg.DrainTimeout = 2 * time.Second

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("warmup shorter than workload must complete cleanly, got error: %v", err)
	}
	s := res.Summary
	if s.Published == 0 {
		t.Fatalf("warmup-short run published 0; measured phase was lost")
	}
	if s.Consumed+s.Drops != s.Published {
		t.Fatalf("measured window is not conservative: published=%d consumed=%d drops=%d (consumed+drops=%d)",
			s.Published, s.Consumed, s.Drops, s.Consumed+s.Drops)
	}
	if s.Consumed > s.Published {
		t.Fatalf("consumed=%d exceeds measured published=%d: pre-warmup in-flight deliveries leaked into the measured window",
			s.Consumed, s.Published)
	}
	if s.Drops != 0 {
		t.Fatalf("clean acking warmup run reported drops=%d", s.Drops)
	}
}

// ---------------------------------------------------------------------------
// 7) identity, repeatability, delivery-tag wrap
// ---------------------------------------------------------------------------

func TestHarness_RepeatabilityRunIDAndDefaultCSVPath(t *testing.T) {
	addr := startBroker(t)

	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	tmp := t.TempDir()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWD) })

	ids := make(map[string]bool)
	var first Summary
	for i := 0; i < 3; i++ {
		cfg := DefaultConfig()
		cfg.Addr = addr
		cfg.Quiet = true
		cfg.Output = "" // exercise the default bench-<runid>.csv path
		cfg.Msgs = 50
		cfg.Report = 10 * time.Millisecond
		cfg.DrainTimeout = 5 * time.Second

		res, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if ids[res.RunID] {
			t.Fatalf("duplicate runid across sequential runs: %s", res.RunID)
		}
		ids[res.RunID] = true
		wantPath := "bench-" + res.RunID + ".csv"
		if res.CSVPath != wantPath {
			t.Fatalf("default CSV path=%q want %q", res.CSVPath, wantPath)
		}
		if _, err := os.Stat(wantPath); err != nil {
			t.Fatalf("default CSV file missing: %v", err)
		}
		if i == 0 {
			first = res.Summary
		} else if res.Summary.Published != first.Published ||
			res.Summary.Consumed != first.Consumed ||
			res.Summary.Drops != first.Drops ||
			res.Summary.Duplicates != first.Duplicates {
			t.Fatalf("run %d totals differ from first run: got %d/%d/%d/%d want %d/%d/%d/%d",
				i, res.Summary.Published, res.Summary.Consumed, res.Summary.Drops, res.Summary.Duplicates,
				first.Published, first.Consumed, first.Drops, first.Duplicates)
		}
	}
}

// TestFinding_Bench006_RunIDUniqueUnderConcurrentGeneration documents that
// newRunID is a bare UnixNano timestamp: concurrent callers routinely receive
// the same id. Concurrent harness runs (or concurrent processes on a coarse
// clock) can therefore target the same exchange/queue names, defeating the
// documented "unique fixture names" isolation guarantee.
func TestFinding_Bench006_RunIDUniqueUnderConcurrentGeneration(t *testing.T) {
	const (
		rounds = 10
		n      = 50000
	)
	total := rounds * n
	seen := make(map[string]int, total)
	for round := 0; round < rounds; round++ {
		ids := make(chan string, n)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ids <- newRunID()
			}()
		}
		close(start)
		wg.Wait()
		close(ids)
		for id := range ids {
			seen[id]++
		}
	}

	if len(seen) == total {
		return
	}

	var examples []string
	for id, count := range seen {
		if count > 1 && len(examples) < 5 {
			examples = append(examples, fmt.Sprintf("%s x%d", id, count))
		}
	}
	t.Fatalf("newRunID produced %d unique ids for %d concurrent calls (%d duplicate ids), examples: %v",
		len(seen), total, total-len(seen), examples)
}

func TestHarness_DeliveryTagWrapAccounting(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Publishers = 1
	cfg.Consumers = 1
	cfg.Msgs = 70000 // beyond the uint16 delivery-tag space
	cfg.Size = headerLen
	cfg.Report = 200 * time.Millisecond
	cfg.DrainTimeout = 30 * time.Second

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("large-tag run: %v", err)
	}
	assertCleanRunInvariants(t, cfg, res.Summary)
}

// ---------------------------------------------------------------------------
// 8) failure injection / error reporting
// ---------------------------------------------------------------------------

func TestHarness_BrokerUnavailableReturnsError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	cfg := DefaultConfig()
	cfg.Addr = addr
	cfg.Quiet = true
	cfg.Output = filepath.Join(t.TempDir(), "bench.csv")
	cfg.Msgs = 1
	cfg.DrainTimeout = time.Second

	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatalf("Run succeeded against an unreachable broker at %s", addr)
	}
}

// TestFinding_Bench004_CSVWriteErrorsMustBeReported requires Run to propagate
// CSV write/flush failures. /dev/full accepts opens but fails every write.
func TestFinding_Bench004_CSVWriteErrorsMustBeReported(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("/dev/full is not available on this platform")
	}
	probe, err := os.Create("/dev/full")
	if err != nil {
		t.Skipf("/dev/full cannot be opened: %v", err)
	}
	_ = probe.Close()

	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Output = "/dev/full"
	cfg.Msgs = 10
	cfg.Report = 10 * time.Millisecond
	cfg.DrainTimeout = time.Second

	res, err := Run(context.Background(), cfg)
	if err != nil {
		return // correct: the run reports the output failure
	}
	if res != nil {
		t.Fatalf("Run returned success with a CSV path whose every write fails; summary=%+v", res.Summary)
	}
}

// TestFinding_Bench007_ContextCancellationIsReported verifies the Run contract
// for context cancellation after workers have started. The current engine
// ignores ctx during the measured phase, closes the clients, and reports either
// success or a misleading "worker failures: connection closed".
func TestFinding_Bench007_ContextCancellationIsReported(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Publishers = 2
	cfg.Consumers = 2
	cfg.Msgs = 100000
	cfg.Report = 10 * time.Millisecond
	cfg.DrainTimeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	_, err := Run(ctx, cfg)
	if err == nil {
		t.Fatalf("Run returned success after its context was cancelled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v after cancellation; want an error wrapping context.Canceled", err)
	}
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func globalIndices(slots []*channelSlot) []int {
	out := make([]int, 0, len(slots))
	for _, sl := range slots {
		out = append(out, sl.GlobalIndex)
	}
	return out
}
