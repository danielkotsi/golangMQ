package bench

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/danielkotsi/golangMQSDK/gomqSDK"
)

// Result carries the CSV path and the machine-readable aggregate summary.
type Result struct {
	RunID   string
	CSVPath string
	Summary Summary
}

// Summary is the one-line JSON aggregate contract for Phase 4 gates.
type Summary struct {
	RunID    string          `json:"runid"`
	Config   any             `json:"config"`
	Topology TopologySummary `json:"topology"`

	Published          uint64  `json:"published"`
	Delivered          uint64  `json:"delivered"`
	Consumed           uint64  `json:"consumed"`
	Duplicates         uint64  `json:"duplicates"`
	Drops              uint64  `json:"drops"`
	ElapsedMs          int64   `json:"elapsed_ms"`
	ConsumedRatePerSec float64 `json:"consumed_rate_per_s"`

	DeliveryLatency LatencySummary `json:"delivery_latency_us"`
	AckLatency      LatencySummary `json:"ack_latency_us"`

	ByRole           map[string]RoleRow `json:"by_role"`
	ByConn           []ByConnRow        `json:"by_conn"`
	ByPublishChannel []ByPubChRow       `json:"by_publish_channel"`
}

type TopologySummary struct {
	Conns          []TopoConnRow `json:"conns"`
	PublishCapable int           `json:"publish_capable"`
	ConsumeCapable int           `json:"consume_capable"`
}

type TopoConnRow struct {
	Channels int    `json:"channels"`
	Role     string `json:"role"`
}

type RoleRow struct {
	P95Us float64 `json:"p95_us"`
	N     uint64  `json:"n"`
}

type ByConnRow struct {
	Conn             int     `json:"conn"`
	Published        uint64  `json:"published"`
	Consumed         uint64  `json:"consumed"`
	PubRatePerSec    float64 `json:"pub_rate_per_s"`
	AvgDeliveryLatUs float64 `json:"avg_delivery_lat_us"`
}

type ByPubChRow struct {
	Conn       int     `json:"conn"`
	Ch         int     `json:"ch"`
	Published  uint64  `json:"published"`
	RatePerSec float64 `json:"rate_per_s"`
}

var (
	runIDSeq   atomic.Uint64
	runIDNonce = func() string {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return strconv.FormatUint(uint64(os.Getpid()), 36)
		}
		return hex.EncodeToString(b[:])
	}()
)

// newRunID mints a unique, short run identifier: base36 timestamp, an
// in-process atomic sequence, and a per-process random nonce. The sequence
// prevents concurrent collisions in one process; the nonce separates
// concurrent processes that share a coarse clock.
func newRunID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" +
		strconv.FormatUint(runIDSeq.Add(1), 36) + "-" + runIDNonce
}

// connectConfig reuses the locked params from the Phase 1 harness.
func connectConfig() gomqSDK.Config {
	return gomqSDK.Config{
		ClientName:   "benchmark",
		Username:     "daniel",
		Password:     "123456789",
		ChannelMax:   10,
		FrameMax:     10372,
		HeartbeatSec: 10,
	}
}

// Run executes the benchmark against cfg.Addr and returns the JSON summary.
// The plan (topology table, goroutine counts, capability lists) is printed to
// stderr so every run is self-documenting and auditable.
func Run(ctx context.Context, cfg *Config) (*Result, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	topo, err := Build(cfg)
	if err != nil {
		return nil, err
	}

	runid := newRunID()
	exchange := "bench.ex." + runid
	queue := "bench.q." + runid
	routingKey := "bench.rk." + runid

	openCtx, openCancel := context.WithTimeout(ctx, 10*time.Second)
	defer openCancel()

	// Setup client declares the uniquely-named fixture, then drops off. Unique
	// names mean no cross-run interference on a shared broker.
	setup, err := gomqSDK.Connect(cfg.Addr, connectConfig())
	if err != nil {
		return nil, fmt.Errorf("setup connect: %w", err)
	}
	setupCh, err := setup.OpenChannel(openCtx)
	if err != nil {
		_ = setup.Close()
		return nil, fmt.Errorf("setup channel: %w", err)
	}
	if _, err := setupCh.DeclareExchange(exchange, openCtx); err != nil {
		_ = setup.Close()
		return nil, fmt.Errorf("declare exchange %s: %w", exchange, err)
	}
	if _, err := setupCh.DeclareQueue(queue, openCtx, "", ""); err != nil {
		_ = setup.Close()
		return nil, fmt.Errorf("declare queue %s: %w", queue, err)
	}
	if err := setupCh.BindQueue(queue, exchange, routingKey, openCtx); err != nil {
		_ = setup.Close()
		return nil, fmt.Errorf("bind %s -> %s: %w", queue, exchange, err)
	}
	_ = setup.Close()

	total := cfg.Publishers * cfg.Msgs
	st := newStats(topo, total)

	printPlan(os.Stderr, runid, cfg, topo, st.total)

	clients := make([]*gomqSDK.Client, len(topo.Conns))
	for i := range topo.Conns {
		cl, err := gomqSDK.Connect(cfg.Addr, connectConfig())
		if err != nil {
			closeClients(clients)
			return nil, fmt.Errorf("connect conn %d: %w", i, err)
		}
		clients[i] = cl
		for _, sl := range topo.Conns[i].Channels {
			ch, err := cl.OpenChannel(openCtx)
			if err != nil {
				closeClients(clients)
				return nil, fmt.Errorf("open channel on conn %d: %w", i, err)
			}
			sl.client = ch
		}
	}

	csvPath, csvDst, csvFile, err := openCSV(cfg, runid)
	if err != nil {
		closeClients(clients)
		return nil, err
	}
	defer func() {
		if csvFile != nil {
			_ = csvFile.Close()
		}
	}()
	csv, err := newCSVWriter(csvDst)
	if err != nil {
		closeClients(clients)
		return nil, fmt.Errorf("write CSV %s: %w", csvPath, err)
	}

	seq := atomic.Uint64{}
	sentTimes := make([]atomic.Int64, total)
	seen := make([]atomic.Bool, total)

	errCh := make(chan error, cfg.Consumers+cfg.Publishers+1)
	var cwg sync.WaitGroup
	for _, sl := range topo.consumeCapable {
		for range sl.ConsGoroutines {
			deliveries, err := sl.client.Consume(queue, openCtx)
			if err != nil {
				closeClients(clients)
				cwg.Wait()
				return nil, fmt.Errorf("consume conn %d ch %d: %w", sl.Conn, sl.Ch, err)
			}
			cw := &consumeWorker{
				ch:         sl.client,
				deliveries: deliveries,
				slot:       sl,
				st:         st,
				sentTimes:  sentTimes,
				seen:       seen,
				ack:        cfg.Ack,
			}
			cwg.Add(1)
			go func(w *consumeWorker) {
				defer cwg.Done()
				if err := w.run(); err != nil {
					errCh <- err
				}
			}(cw)
		}
	}

	stopPub := make(chan struct{})
	var stopOnce sync.Once
	stopPubs := func() {
		stopOnce.Do(func() { close(stopPub) })
	}
	var pwg sync.WaitGroup
	for _, sl := range topo.publishCapable {
		for range sl.PubGoroutines {
			pw := &publishWorker{
				ch:         sl.client,
				slot:       sl,
				st:         st,
				seqGen:     &seq,
				sentTimes:  sentTimes,
				exchange:   exchange,
				routingKey: routingKey,
				size:       cfg.Size,
				msgs:       cfg.Msgs,
				stop:       stopPub,
			}
			pwg.Add(1)
			go func(w *publishWorker) {
				defer pwg.Done()
				if err := w.run(); err != nil {
					errCh <- err
				}
			}(pw)
		}
	}

	pubDone := make(chan struct{})
	go func() {
		pwg.Wait()
		close(pubDone)
	}()

	shutdown := func() {
		stopPubs()
		pwg.Wait()
		closeClients(clients)
		cwg.Wait()
	}

	// Warmup is a real phase boundary: publishers keep running, but the cutoff
	// is captured atomically with the counter reset. Messages allocated before
	// the cutoff remain warmup traffic even if they are published or delivered
	// afterwards. If the entire workload finished during warmup there can be no
	// measured work, so fail clearly instead of returning an all-zero success.
	if cfg.Warmup > 0 && total > 0 {
		timer := time.NewTimer(cfg.Warmup)
		select {
		case <-ctx.Done():
			timer.Stop()
			shutdown()
			return nil, fmt.Errorf("benchmark warmup interrupted: %w", ctx.Err())
		case <-pubDone:
			timer.Stop()
			shutdown()
			return nil, fmt.Errorf("warmup %v exceeded workload: all %d messages were published before measurement", cfg.Warmup, total)
		case <-timer.C:
		}
		cutoff := st.beginMeasured(&seq)
		if cutoff >= uint64(total) {
			shutdown()
			return nil, fmt.Errorf("warmup %v exceeded workload: all %d messages were published before measurement", cfg.Warmup, total)
		}
	}

	sctx, sstop := context.WithCancel(context.Background())
	defer sstop()

	phaseStart := time.Now()
	windowStart := phaseStart
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(cfg.Report)
		defer ticker.Stop()
	writeWindow := func(now time.Time) error {
		rec := st.sampleWindow(windowStart, now)
		if err := csv.writeInterval(rec); err != nil {
			return err
		}
		if err := csv.flush(); err != nil {
			return err
		}
		if !cfg.Quiet {
			logInterval(runid, rec)
		}
		windowStart = now
		return nil
	}
		for {
			select {
		case <-sctx.Done():
			now := time.Now()
			rec := st.sampleWindow(windowStart, now)
			_ = csv.writeInterval(rec)
			_ = csv.flush()
			if !cfg.Quiet {
				logInterval(runid, rec)
			}
			return
			case now := <-ticker.C:
				if err := writeWindow(now); err != nil {
					return
				}
			}
		}
	}()

	// Measured phase: with -duration the publishers stop at the deadline; the
	// run otherwise completes when every publisher goroutine has fired its msgs.
	if cfg.Duration > 0 {
		timer := time.NewTimer(cfg.Duration)
		select {
		case <-ctx.Done():
			timer.Stop()
			stopPubs()
			<-pubDone
		case <-timer.C:
			stopPubs()
			<-pubDone
		case <-pubDone:
			timer.Stop()
		}
	} else {
		select {
		case <-ctx.Done():
			stopPubs()
			<-pubDone
		case <-pubDone:
		}
	}

	// Drain: acking runs wait for the whole queue (warmup included) so client
	// shutdown cannot race an in-flight Ack. No-ack runs freeze at the prefetch
	// ceiling and time out by design.
	interrupted := ctx.Err() != nil
	drainStart := time.Now()
	if !interrupted && cfg.Consumers > 0 {
		needDrain := false
		if cfg.Ack {
			needDrain = st.allPublished.Load() > 0
		} else {
			needDrain = st.published.Load() > 0
		}
		if needDrain {
			deadline := time.Now().Add(cfg.DrainTimeout)
			for !st.drained(cfg.Ack) {
				if !time.Now().Before(deadline) {
					break
				}
				select {
				case <-ctx.Done():
					interrupted = true
				case <-time.After(10 * time.Millisecond):
				}
				if interrupted {
					break
				}
			}
			if !interrupted && !st.drained(cfg.Ack) {
				logf("benchmark %s: WARNING drain not complete after %v: consumed %d of %d published",
					runid, time.Since(drainStart), st.consumed.Load(), st.published.Load())
			}
		}
	}
	if !interrupted {
		if pub, con := st.published.Load(), st.consumed.Load(); pub > con {
			st.drops.Store(pub - con)
		}
	}

	shutdown()
	sstop()
	<-samplerDone

	close(errCh)
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	var csvErr error
	if csv != nil {
		csvErr = csv.close()
	}
	if csvFile != nil {
		if err := csvFile.Close(); csvErr == nil {
			csvErr = err
		}
		csvFile = nil
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("benchmark interrupted: %w", ctxErr)
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("worker failures: %w", errors.Join(errs...))
	}
	if csvErr != nil {
		return nil, fmt.Errorf("write CSV %s: %w", csvPath, csvErr)
	}

	return &Result{
		RunID:   runid,
		CSVPath: csvPath,
		Summary: buildSummary(runid, cfg, topo, st, phaseStart),
	}, nil
}

func openCSV(cfg *Config, runid string) (path string, dst io.Writer, file *os.File, err error) {
	if cfg.Output == "-" {
		return "-", os.Stdout, nil, nil
	}
	path = cfg.Output
	if path == "" {
		path = "bench-" + runid + ".csv"
	}
	f, err := os.Create(path)
	if err != nil {
		return "", nil, nil, fmt.Errorf("create CSV %s: %w", path, err)
	}
	return path, f, f, nil
}

func closeClients(clients []*gomqSDK.Client) {
	for _, c := range clients {
		if c != nil {
			_ = c.Close() // idempotent; unblocks the SDK read loop
		}
	}
}

// logf writes a benchmark line to stderr (mirrors the SDK's stderr logging so
// all diagnostic output stays on the same stream and stdout stays clean for
// CSV/JSON).
func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func logInterval(runid string, rec intervalRecord) {
	logf("benchmark %s: interval t=%s published=%d delivered=%d consumed=%d dup=%d",
		runid, rec.TsStart.Format("15:04:05.000"), rec.Published, rec.Delivered, rec.Consumed, rec.Duplicates)
}

func printPlan(w io.Writer, runid string, cfg *Config, t *Topology, total int) {
	fmt.Fprintf(w, "benchmark %s: conns=%d (pub=%d cons=%d mixed=%d) channels=%d publishers=%d consumers=%d msgs=%d total=%d size=%d ack=%v\n",
		runid, cfg.Conns, cfg.PubConns, cfg.ConsConns, cfg.Conns-cfg.PubConns-cfg.ConsConns,
		cfg.Channels, cfg.Publishers, cfg.Consumers, cfg.Msgs, total, cfg.Size, cfg.Ack)
	fmt.Fprintf(w, "benchmark %s: exchange=%s queue=%s routing_key=%s\n", runid, exchangeOf(runid), queueOf(runid), routingKeyOf(runid))
	for _, cp := range t.Conns {
		parts := make([]string, 0, len(cp.Channels))
		for _, sl := range cp.Channels {
			parts = append(parts, fmt.Sprintf("ch%d=%s(pub%d/cons%d)",
				sl.Ch, sl.Role.String(), len(sl.PubGoroutines), len(sl.ConsGoroutines)))
		}
		fmt.Fprintf(w, "benchmark %s: conn%d [%s] %d channel(s): %s\n",
			runid, cp.Conn, cp.Role, len(cp.Channels), strings.Join(parts, ", "))
	}
	pubs := make([]string, 0, len(t.publishCapable))
	for _, sl := range t.publishCapable {
		pubs = append(pubs, fmt.Sprintf("c%d.ch%d(%s)", sl.Conn, sl.Ch, sl.Role))
	}
	cons := make([]string, 0, len(t.consumeCapable))
	for _, sl := range t.consumeCapable {
		cons = append(cons, fmt.Sprintf("c%d.ch%d(%s)", sl.Conn, sl.Ch, sl.Role))
	}
	fmt.Fprintf(w, "benchmark %s: publish-capable(%d): %s\n", runid, len(t.publishCapable), strings.Join(pubs, ", "))
	fmt.Fprintf(w, "benchmark %s: consume-capable(%d): %s\n", runid, len(t.consumeCapable), strings.Join(cons, ", "))
}

func exchangeOf(runid string) string   { return "bench.ex." + runid }
func queueOf(runid string) string      { return "bench.q." + runid }
func routingKeyOf(runid string) string { return "bench.rk." + runid }

func summaryConfig(cfg *Config) map[string]any {
	return map[string]any{
		"addr":                       cfg.Addr,
		"conns":                      cfg.Conns,
		"pub-conns":                  cfg.PubConns,
		"cons-conns":                 cfg.ConsConns,
		"channels":                   cfg.Channels,
		"pub-channels":               cfg.PubChannels,
		"cons-channels":              cfg.ConsChannels,
		"conns-distribution":         string(cfg.ConnsDist),
		"channels-distribution":      string(cfg.ChannelsDist),
		"pub-channels-distribution":  string(cfg.PubChannelsDist),
		"max-channels-per-conn":      cfg.MaxChannelsPerConn,
		"max-consumers-per-channel":  cfg.MaxConsumersPerChannel,
		"max-publishers-per-channel": cfg.MaxPublishersPerChannel,
		"publishers":                 cfg.Publishers,
		"consumers":                  cfg.Consumers,
		"msgs":                       cfg.Msgs,
		"size":                       cfg.Size,
		"ack":                        cfg.Ack,
		"prefetch":                   cfg.Prefetch,
		"duration":                   cfg.Duration.String(),
		"warmup":                     cfg.Warmup.String(),
		"report":                     cfg.Report.String(),
		"drain-timeout":              cfg.DrainTimeout.String(),
		"output":                     cfg.Output,
		"quiet":                      cfg.Quiet,
	}
}

func buildSummary(runid string, cfg *Config, t *Topology, st *stats, phaseStart time.Time) Summary {
	elapsed := time.Since(phaseStart)
	pub, del, cons, dup, drops := st.totals()

	delSummary := st.delLatTot.summary()
	ackSummary := st.ackLatTot.summary()

	byRole := make(map[string]RoleRow, len(roleKeys))
	for _, k := range roleKeys {
		s := st.roleDelTot[k].summary()
		byRole[k] = RoleRow{P95Us: s.P95, N: s.Count}
	}

	elapsedSec := elapsed.Seconds()
	byConn := make([]ByConnRow, 0, len(st.perConn))
	for i := range st.perConn {
		cs := &st.perConn[i]
		cs.mu.Lock()
		cPub, cCons, cDel := cs.published, cs.consumed, cs.delivered
		avgDel := 0.0
		if cs.delLatCount > 0 {
			avgDel = cs.delLatSum / float64(cs.delLatCount)
		}
		cs.mu.Unlock()
		_ = cDel
		rate := 0.0
		if elapsedSec > 0 {
			rate = float64(cPub) / elapsedSec
		}
		byConn = append(byConn, ByConnRow{
			Conn:             i,
			Published:        cPub,
			Consumed:         cCons,
			PubRatePerSec:    rate,
			AvgDeliveryLatUs: avgDel,
		})
	}

	byPubCh := make([]ByPubChRow, 0, len(t.publishCapable))
	for idx, sl := range t.publishCapable {
		ps := &st.perPubCh[idx]
		ps.mu.Lock()
		pChPub := ps.published
		ps.mu.Unlock()
		rate := 0.0
		if elapsedSec > 0 {
			rate = float64(pChPub) / elapsedSec
		}
		byPubCh = append(byPubCh, ByPubChRow{
			Conn:       sl.Conn,
			Ch:         sl.Ch,
			Published:  pChPub,
			RatePerSec: rate,
		})
	}

	topoConns := make([]TopoConnRow, 0, len(t.Conns))
	for _, cp := range t.Conns {
		topoConns = append(topoConns, TopoConnRow{Channels: len(cp.Channels), Role: cp.Role.String()})
	}

	consRate := 0.0
	if elapsedSec > 0 {
		consRate = float64(cons) / elapsedSec
	}

	return Summary{
		RunID:  runid,
		Config: summaryConfig(cfg),
		Topology: TopologySummary{
			Conns:          topoConns,
			PublishCapable: len(t.publishCapable),
			ConsumeCapable: len(t.consumeCapable),
		},
		Published:          pub,
		Delivered:          del,
		Consumed:           cons,
		Duplicates:         dup,
		Drops:              drops,
		ElapsedMs:          elapsed.Milliseconds(),
		ConsumedRatePerSec: consRate,
		DeliveryLatency:    delSummary,
		AckLatency:         ackSummary,
		ByRole:             byRole,
		ByConn:             byConn,
		ByPublishChannel:   byPubCh,
	}
}
