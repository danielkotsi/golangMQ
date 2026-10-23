package bench

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"GolangRabbitMQBroker/broker"
)

// startBroker boots an in-process broker on an ephemeral port, mirroring the
// Phase 1 harness (tests/helpers.go:startTestBroker).
func startBroker(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := broker.NewServer(ln.Addr().String(), broker.ServerConfig{
		ChannelMax:   10,
		FramesMax:    10372,
		HeartbeatSec: 10,
	})
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String()
}

func countRoles(t *Topology) (pub, cons, both int) {
	for c := range t.Conns {
		for _, sl := range t.Conns[c].Channels {
			switch sl.Role {
			case RolePublish:
				pub++
			case RoleConsume:
				cons++
			case RoleBoth:
				both++
			}
		}
	}
	return pub, cons, both
}

func perConnCounts(t *Topology) []int {
	out := make([]int, len(t.Conns))
	for i := range t.Conns {
		out[i] = len(t.Conns[i].Channels)
	}
	return out
}

func slotConsumers(t *Topology, conn, ch int) int {
	for c := range t.Conns {
		if t.Conns[c].Conn != conn {
			continue
		}
		for _, sl := range t.Conns[c].Channels {
			if sl.Ch == ch {
				return len(sl.ConsGoroutines)
			}
		}
	}
	return -1
}

func slotPublishers(t *Topology, conn, ch int) int {
	for c := range t.Conns {
		if t.Conns[c].Conn != conn {
			continue
		}
		for _, sl := range t.Conns[c].Channels {
			if sl.Ch == ch {
				return len(sl.PubGoroutines)
			}
		}
	}
	return -1
}

func tinyRunCfg(t *testing.T, addr string) *Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Addr = addr
	cfg.Quiet = true
	cfg.Output = filepath.Join(t.TempDir(), "run.csv")
	return cfg
}

// --- topology math ----------------------------------------------------------

func TestTopology_EvenChannelSpread(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Conns = 4
	cfg.Channels = 8

	topo, err := Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(topo.Conns) != 4 {
		t.Fatalf("expected 4 conns, got %d", len(topo.Conns))
	}
	for i, c := range perConnCounts(topo) {
		if c != 2 {
			t.Fatalf("conn %d has %d channels, want 2", i, c)
		}
	}
}

func TestTopology_RoleCarving(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Conns = 2
	cfg.Channels = 6
	cfg.PubChannels = 2
	cfg.ConsChannels = 2

	topo, err := Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	pub, cons, both := countRoles(topo)
	if pub != 2 || cons != 2 || both != 2 {
		t.Fatalf("role carving: got pub=%d cons=%d both=%d, want 2/2/2 (mixed slots in global order: first 2 P, next 2 C, rest B)", pub, cons, both)
	}
	if len(topo.publishCapable) != 4 || len(topo.consumeCapable) != 4 {
		t.Fatalf("capability lists: publish_capable=%d consume_capable=%d, want 4/4", len(topo.publishCapable), len(topo.consumeCapable))
	}
}

func TestTopology_ValidationErrors(t *testing.T) {
	t.Run("pub+cons conns exceed conns", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Conns = 2
		cfg.PubConns = 2
		cfg.ConsConns = 1
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error: pub-conns + cons-conns > conns")
		}
	})

	t.Run("role carving exceeds mixed slots", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Conns = 2
		cfg.Channels = 2 // 1 channel per conn, both conns mixed -> 2 mixed slots
		cfg.PubChannels = 2
		cfg.ConsChannels = 1
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error: carving 3 roles out of 2 mixed slots")
		}
	})

	t.Run("uneven cap overflow", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Conns = 2
		cfg.Channels = 5
		cfg.ConnsDist = DistUneven
		cfg.MaxChannelsPerConn = 2 // 2 conns x 2 = 4 < 5
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error: 5 channels cannot fit in 2 conns x cap 2")
		}
	})

	t.Run("fake distribution flag without preceding axis", func(t *testing.T) {
		var cur AxisCursor
		v := NewDistributionFlag(DefaultConfig(), &cur)
		if err := v.Set("even"); err == nil {
			t.Fatal("expected error: -distribution with no preceding axis")
		}
	})
}

// --- placement --------------------------------------------------------------

func TestTopology_Placement(t *testing.T) {
	t.Run("even 4 consumers over 2 consume-capable slots", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Conns = 2
		cfg.PubConns = 1
		cfg.ConsConns = 1
		cfg.Channels = 4 // conn0 {0,2} P, conn1 {1,3} C
		cfg.Consumers = 4

		topo, err := Build(cfg)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if len(topo.publishCapable) != 2 || len(topo.consumeCapable) != 2 {
			t.Fatalf("capability lists: publish=%d consume=%d, want 2/2", len(topo.publishCapable), len(topo.consumeCapable))
		}
		// Consumers land on conn1's channels, never on the publish-only conn0.
		for _, ch := range []int{0, 1} {
			if got := slotConsumers(topo, 0, ch); got != 0 {
				t.Fatalf("conn0 ch%d has %d consumers, want 0 (publish-only conn)", ch, got)
			}
		}
		for _, ch := range []int{0, 1} {
			if got := slotConsumers(topo, 1, ch); got != 2 {
				t.Fatalf("conn1 ch%d has %d consumers, want 2", ch, got)
			}
		}
	})

	t.Run("uneven no cap packs into slot 0", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Conns = 2
		cfg.Channels = 4
		cfg.Consumers = 4
		cfg.ChannelsDist = DistUneven // no cap -> all into the first consume-capable slot

		topo, err := Build(cfg)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		first := topo.consumeCapable[0]
		if len(first.ConsGoroutines) != 4 {
			t.Fatalf("slot 0 has %d consumers, want 4", len(first.ConsGoroutines))
		}
		for i := 1; i < len(topo.consumeCapable); i++ {
			if n := len(topo.consumeCapable[i].ConsGoroutines); n != 0 {
				t.Fatalf("slot %d has %d consumers, want 0", i, n)
			}
		}
	})

	t.Run("uneven capped packing", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Conns = 2
		cfg.Channels = 4
		cfg.Consumers = 4
		cfg.ChannelsDist = DistUneven
		cfg.MaxConsumersPerChannel = 2

		topo, err := Build(cfg)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		// Packing fills slot 0 to the cap, then slot 1.
		if got := len(topo.consumeCapable[0].ConsGoroutines); got != 2 {
			t.Fatalf("slot 0 has %d consumers, want 2", got)
		}
		if got := len(topo.consumeCapable[1].ConsGoroutines); got != 2 {
			t.Fatalf("slot 1 has %d consumers, want 2", got)
		}
	})
}

// --- engine runs ------------------------------------------------------------

func TestEngine_SameChannelMultiConsumer(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Conns = 1
	cfg.Channels = 1
	cfg.Consumers = 4 // 4 consumers, 1 consume-capable slot (F-004 tolerance)
	cfg.Publishers = 4
	cfg.Msgs = 50
	cfg.DrainTimeout = 5 * time.Second

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	s := res.Summary
	if s.Published != 200 {
		t.Fatalf("published=%d want 200", s.Published)
	}
	if s.Consumed != 200 || s.Delivered != 200 {
		t.Fatalf("consumed=%d delivered=%d want 200/200", s.Consumed, s.Delivered)
	}
	if s.Drops != 0 || s.Duplicates != 0 {
		t.Fatalf("drops=%d duplicates=%d want 0/0", s.Drops, s.Duplicates)
	}
}

func TestEngine_OneVsNChannelsPublish(t *testing.T) {
	addr := startBroker(t)

	run := func(conns, channels int) *Summary {
		t.Helper()
		cfg := tinyRunCfg(t, addr)
		cfg.Conns = conns
		cfg.Channels = channels
		cfg.Publishers = 4
		cfg.Consumers = 2
		cfg.Msgs = 30
		cfg.DrainTimeout = 5 * time.Second
		res, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatalf("run (conns=%d channels=%d): %v", conns, channels, err)
		}
		s := res.Summary
		if s.Drops != 0 || s.Published != 120 || s.Consumed != 120 {
			t.Fatalf("conns=%d channels=%d: published=%d consumed=%d drops=%d want 120/120/0",
				conns, channels, s.Published, s.Consumed, s.Drops)
		}
		return &s
	}

	single := run(1, 1)
	multi := run(4, 8)
	if len(single.ByPublishChannel) != 1 {
		t.Fatalf("1-conn run reports %d publish channels, want 1", len(single.ByPublishChannel))
	}
	if len(multi.ByPublishChannel) != 8 {
		t.Fatalf("4-conn run reports %d publish channels, want 8", len(multi.ByPublishChannel))
	}
	var singlePub, multiPub uint64
	for _, r := range single.ByPublishChannel {
		singlePub += r.Published
	}
	for _, r := range multi.ByPublishChannel {
		multiPub += r.Published
	}
	if singlePub != 120 || multiPub != 120 {
		t.Fatalf("by_publish_channel totals differ: single=%d multi=%d want 120/120", singlePub, multiPub)
	}
}

func TestEngine_MixedVsSeparated(t *testing.T) {
	addr := startBroker(t)

	run := func(cfg *Config) Summary {
		t.Helper()
		cfg.DrainTimeout = 5 * time.Second
		res, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if res.Summary.Consumed != res.Summary.Published || res.Summary.Drops != 0 {
			t.Fatalf("consumed=%d published=%d drops=%d", res.Summary.Consumed, res.Summary.Published, res.Summary.Drops)
		}
		return res.Summary
	}

	// Mixed: every conn hosts both publishing and consuming channels.
	mixed := tinyRunCfg(t, addr)
	mixed.Conns = 2
	mixed.Channels = 2
	mixed.Publishers = 2
	mixed.Consumers = 2
	mixed.Msgs = 40
	sMix := run(mixed)

	if sMix.ByRole["both"].N == 0 {
		t.Fatal("mixed run: no deliveries recorded under by_role.both")
	}
	if sMix.ByRole["cons"].N != 0 {
		t.Fatal("mixed run: role 'cons' should be empty (both channels are BOTH)")
	}
	if sMix.ByConn[0].Consumed == 0 || sMix.ByConn[1].Consumed == 0 {
		t.Fatalf("mixed run: both conns must consume, by_conn=%+v", sMix.ByConn)
	}

	// Separated: conn0 publish-only, conn1 consume-only.
	sep := tinyRunCfg(t, addr)
	sep.Conns = 2
	sep.PubConns = 1
	sep.ConsConns = 1
	sep.Channels = 2
	sep.Publishers = 2
	sep.Consumers = 2
	sep.Msgs = 40
	sSep := run(sep)

	if sSep.ByConn[0].Consumed != 0 {
		t.Fatalf("separated run: publish-only conn0 consumed %d, want 0 (consumers never on a publish-only conn)", sSep.ByConn[0].Consumed)
	}
	if sSep.ByConn[1].Consumed == 0 {
		t.Fatalf("separated run: consume-only conn1 consumed 0, want > 0")
	}
	if sSep.ByRole["cons"].N == 0 {
		t.Fatal("separated run: no deliveries recorded under by_role.cons")
	}
}

func TestCSVHeaderRowAndJSONTotals(t *testing.T) {
	addr := startBroker(t)
	cfg := tinyRunCfg(t, addr)
	cfg.Publishers = 2
	cfg.Consumers = 2
	cfg.Msgs = 25
	cfg.DrainTimeout = 5 * time.Second

	path := cfg.Output
	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.CSVPath != path {
		t.Fatalf("CSV path=%q want %q", res.CSVPath, path)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	r := csv.NewReader(bytes.NewReader(raw))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("expected header + >= 1 data row, got %d rows", len(rows))
	}
	if got, want := rows[0], csvColumns; len(got) != len(want) {
		t.Fatalf("csv header mismatch:\n got %q\nwant %q", got, want)
	} else {
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("csv column %d = %q want %q", i, got[i], want[i])
			}
		}
	}

	// JSON summary totals must match the published workload.
	b, err := json.Marshal(res.Summary)
	if err != nil {
		t.Fatalf("marshal summary: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}
	if got, _ := decoded["published"].(float64); int(got) != 50 {
		t.Fatalf("json published=%v want 50", decoded["published"])
	}
	if got, _ := decoded["consumed"].(float64); int(got) != 50 {
		t.Fatalf("json consumed=%v want 50", decoded["consumed"])
	}
	if got, _ := decoded["drops"].(float64); int(got) != 0 {
		t.Fatalf("json drops=%v want 0", decoded["drops"])
	}
	if res.Summary.Published != 50 || res.Summary.Consumed != 50 || res.Summary.Drops != 0 {
		t.Fatalf("summary totals: published=%d consumed=%d drops=%d want 50/50/0",
			res.Summary.Published, res.Summary.Consumed, res.Summary.Drops)
	}
}