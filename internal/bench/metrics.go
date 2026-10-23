package bench

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// roleKeys are the delivery-latency decomposition families.
var roleKeys = []string{"both", "cons"}

// LatencySummary is the aggregate latency shape used in CSV rows and the JSON
// summary. All values are microseconds.
type LatencySummary struct {
	Count uint64  `json:"-"`
	Min   float64 `json:"min"`
	Avg   float64 `json:"avg"`
	Max   float64 `json:"max"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
}

// latencySet accumulates latency samples with running min/max/sum for cheap
// averages plus the raw samples needed for percentiles. Each set has its own
// lock so the sampler can snapshot-and-reset the window sets without stalling
// the worker record path.
type latencySet struct {
	mu      sync.Mutex
	samples []float64
	count   uint64
	sum     float64
	min     float64
	max     float64
	has     bool
}

func newLatencySet() *latencySet {
	return &latencySet{samples: make([]float64, 0, 1024)}
}

func (s *latencySet) add(v float64) {
	s.mu.Lock()
	s.samples = append(s.samples, v)
	s.count++
	s.sum += v
	if !s.has || v < s.min {
		s.min = v
	}
	if !s.has || v > s.max {
		s.max = v
	}
	s.has = true
	s.mu.Unlock()
}

func (s *latencySet) summary() LatencySummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.summaryLocked()
}

func (s *latencySet) summaryLocked() LatencySummary {
	var out LatencySummary
	if !s.has {
		return out
	}
	out.Count = s.count
	out.Min, out.Max = s.min, s.max
	out.Avg = s.sum / float64(s.count)
	sorted := s.samples
	if len(sorted) > 1 && !sort.Float64sAreSorted(sorted) {
		sorted = append([]float64(nil), sorted...)
		sort.Float64s(sorted)
	}
	out.P50 = percentile(sorted, 50)
	out.P95 = percentile(sorted, 95)
	out.P99 = percentile(sorted, 99)
	return out
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// snapshotReset returns the set's summary and clears it (sampler window path).
func (s *latencySet) snapshotReset() LatencySummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.summaryLocked()
	s.resetLocked()
	return out
}

func (s *latencySet) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetLocked()
}

func (s *latencySet) resetLocked() {
	s.samples = s.samples[:0]
	s.count, s.sum, s.min, s.max = 0, 0, 0, 0
	s.has = false
}

type connStats struct {
	mu          sync.Mutex
	published   uint64
	consumed    uint64
	delivered   uint64
	delLatSum   float64
	delLatCount uint64
}

type pubChStats struct {
	mu        sync.Mutex
	published uint64
}

// stats is the whole-run + interval accumulator. Totals never reset (except at
// the warmup boundary); window counters are swapped to zero by the sampler at
// each interval.
type stats struct {
	// phaseMu makes the warmup/measured boundary atomic with respect to
	// record calls. Every record call takes RLock; beginMeasured takes Lock,
	// captures the sequence cutoff, and resets the measured counters while no
	// record call can be in flight.
	phaseMu   sync.RWMutex
	phaseFrom uint64

	published  atomic.Uint64
	delivered  atomic.Uint64
	consumed   atomic.Uint64
	duplicates atomic.Uint64
	drops      atomic.Uint64

	// all-time counters include warmup traffic and are never reset. They are
	// used only to decide when an acking run is safe to shut down.
	allPublished atomic.Uint64
	allDelivered atomic.Uint64
	allConsumed  atomic.Uint64
	allAcks      atomic.Uint64

	pubW  atomic.Uint64
	delW  atomic.Uint64
	consW atomic.Uint64
	dupW  atomic.Uint64

	delLatTot *latencySet
	ackLatTot *latencySet
	delLatWin *latencySet
	ackLatWin *latencySet

	roleDelTot map[string]*latencySet
	roleDelWin map[string]*latencySet

	perConn  []connStats
	perPubCh []pubChStats

	total int // publishers x msgs message budget
}

func newStats(t *Topology, total int) *stats {
	s := &stats{
		delLatTot:  newLatencySet(),
		ackLatTot:  newLatencySet(),
		delLatWin:  newLatencySet(),
		ackLatWin:  newLatencySet(),
		roleDelTot: make(map[string]*latencySet, len(roleKeys)),
		roleDelWin: make(map[string]*latencySet, len(roleKeys)),
		perConn:    make([]connStats, len(t.Conns)),
		perPubCh:   make([]pubChStats, len(t.publishCapable)),
		total:      total,
	}
	for _, k := range roleKeys {
		s.roleDelTot[k] = newLatencySet()
		s.roleDelWin[k] = newLatencySet()
	}
	return s
}

func (s *stats) recordPublish(sl *channelSlot, seq uint64, latUs float64) {
	s.allPublished.Add(1)

	s.phaseMu.RLock()
	defer s.phaseMu.RUnlock()
	if seq <= s.phaseFrom {
		return
	}

	s.published.Add(1)
	s.pubW.Add(1)

	cs := &s.perConn[sl.Conn]
	cs.mu.Lock()
	cs.published++
	cs.mu.Unlock()

	if sl.publishCapIdx >= 0 {
		ps := &s.perPubCh[sl.publishCapIdx]
		ps.mu.Lock()
		ps.published++
		ps.mu.Unlock()
	}
}

func (s *stats) recordDeliver(sl *channelSlot, seq uint64, delLatUs float64, dup bool) {
	s.allDelivered.Add(1)
	if !dup {
		s.allConsumed.Add(1)
	}

	s.phaseMu.RLock()
	defer s.phaseMu.RUnlock()
	if seq <= s.phaseFrom {
		return
	}

	s.delivered.Add(1)
	s.delW.Add(1)
	if dup {
		s.duplicates.Add(1)
		s.dupW.Add(1)
	} else {
		s.consumed.Add(1)
		s.consW.Add(1)
	}
	s.delLatTot.add(delLatUs)
	s.delLatWin.add(delLatUs)
	if k := sl.Role.key(); k != "" {
		s.roleDelTot[k].add(delLatUs)
		s.roleDelWin[k].add(delLatUs)
	}

	cs := &s.perConn[sl.Conn]
	cs.mu.Lock()
	cs.delivered++
	if !dup {
		cs.consumed++
	}
	cs.delLatSum += delLatUs
	cs.delLatCount++
	cs.mu.Unlock()
}

func (s *stats) recordAck(seq uint64, latUs float64) {
	s.allAcks.Add(1)

	s.phaseMu.RLock()
	defer s.phaseMu.RUnlock()
	if seq <= s.phaseFrom {
		return
	}
	s.ackLatTot.add(latUs)
	s.ackLatWin.add(latUs)
}

// drained reports whether a run has no more useful work in flight. Acking runs
// wait for every message (warmup included) to be consumed and acked before the
// clients are closed, so shutdown cannot race an in-flight Ack into a
// misleading "connection closed" worker error. No-ack runs can never drain the
// broker queue past its prefetch ceiling, so they use the measured counters.
func (s *stats) drained(ack bool) bool {
	if ack {
		return s.allConsumed.Load() >= s.allPublished.Load() &&
			s.allAcks.Load() >= s.allDelivered.Load()
	}
	return s.consumed.Load() >= s.published.Load()
}

// beginMeasured starts the measured phase. It captures the sequence cutoff
// atomically with respect to publishers and resets measured counters. Messages
// already allocated before the cutoff remain warmup traffic even if their
// publish/deliver records arrive after the boundary.
func (s *stats) beginMeasured(seq *atomic.Uint64) uint64 {
	s.phaseMu.Lock()
	defer s.phaseMu.Unlock()
	s.phaseFrom = seq.Load()
	s.resetAll()
	return s.phaseFrom
}

// totals snapshots the whole-run counters for the JSON summary.
func (s *stats) totals() (published, delivered, consumed, duplicates, drops uint64) {
	return s.published.Load(), s.delivered.Load(), s.consumed.Load(),
		s.duplicates.Load(), s.drops.Load()
}

// intervalRecord is one CSV row's worth of data.
type intervalRecord struct {
	TsStart    time.Time
	WindowMs   float64
	Published  uint64
	Delivered  uint64
	Consumed   uint64
	Duplicates uint64
	Drops      uint64
	DelLat     LatencySummary
	AckAvgUs   float64
	BothP95Us  float64
	ConsP95Us  float64
}

// sampleWindow reads and resets the per-interval accumulators. Drops are a
// running estimate of published - consumed (clamped); at the final post-drain
// row they equal the true loss.
func (s *stats) sampleWindow(ts, end time.Time) intervalRecord {
	rec := intervalRecord{
		TsStart:    ts,
		WindowMs:   float64(end.Sub(ts).Nanoseconds()) / 1e6,
		Published:  s.pubW.Swap(0),
		Delivered:  s.delW.Swap(0),
		Consumed:   s.consW.Swap(0),
		Duplicates: s.dupW.Swap(0),
		DelLat:     s.delLatWin.snapshotReset(),
	}
	if ack := s.ackLatWin.snapshotReset(); ack.Count > 0 {
		rec.AckAvgUs = ack.Avg
	}
	rec.BothP95Us = s.roleDelWin["both"].snapshotReset().P95
	rec.ConsP95Us = s.roleDelWin["cons"].snapshotReset().P95

	if pub, con := s.published.Load(), s.consumed.Load(); pub > con {
		rec.Drops = pub - con
	}
	return rec
}

// resetAll zeroes every measured-phase accumulator. Called by beginMeasured
// under phaseMu so warmup traffic is discarded without losing concurrent
// measured records.
func (s *stats) resetAll() {
	s.published.Store(0)
	s.delivered.Store(0)
	s.consumed.Store(0)
	s.duplicates.Store(0)
	s.drops.Store(0)
	s.pubW.Store(0)
	s.delW.Store(0)
	s.consW.Store(0)
	s.dupW.Store(0)
	for _, set := range []*latencySet{s.delLatTot, s.ackLatTot, s.delLatWin, s.ackLatWin} {
		set.reset()
	}
	for _, k := range roleKeys {
		s.roleDelTot[k].reset()
		s.roleDelWin[k].reset()
	}
	for i := range s.perConn {
		cs := &s.perConn[i]
		cs.mu.Lock()
		cs.published, cs.consumed, cs.delivered = 0, 0, 0
		cs.delLatSum, cs.delLatCount = 0, 0
		cs.mu.Unlock()
	}
	for i := range s.perPubCh {
		ps := &s.perPubCh[i]
		ps.mu.Lock()
		ps.published = 0
		ps.mu.Unlock()
	}
}
