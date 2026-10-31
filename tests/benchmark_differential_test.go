package tests

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"GolangRabbitMQBroker/internal/bench"
	"github.com/danielkotsi/golangMQSDK/gomqSDK"
	"github.com/danielkotsi/golangMQSDK/protocol"
)

const refHeaderLen = 16

var refRunCounter atomic.Uint64

type refScenario struct {
	Publishers int
	Consumers  int
	Msgs       int
	Size       int
	Ack        bool
}

type refDelivery struct {
	ch *gomqSDK.ClientChannel
	d  protocol.Deliver
}

type refOutcome struct {
	Published int
	Delivered int
	Unique    int
	Duplicate int
	Latencies []float64
}

func refSuffix() string {
	return fmt.Sprintf("differential.%d.%d", time.Now().UnixNano(), refRunCounter.Add(1))
}

func refBody(size int, seq uint64, sendNanos int64) []byte {
	body := make([]byte, size)
	binary.BigEndian.PutUint64(body[0:8], seq)
	binary.BigEndian.PutUint64(body[8:16], uint64(sendNanos))
	for i := refHeaderLen; i < len(body); i++ {
		body[i] = 0x2a
	}
	return body
}

func refDecode(body []byte) (seq uint64, sendNanos int64, err error) {
	if len(body) < refHeaderLen {
		return 0, 0, fmt.Errorf("body length %d below header length %d", len(body), refHeaderLen)
	}
	return binary.BigEndian.Uint64(body[0:8]), int64(binary.BigEndian.Uint64(body[8:16])), nil
}

func refPublishLoop(t *testing.T, p *gomqSDK.ClientChannel, exchange, routingKey string, n, size int, seq *atomic.Uint64, errs chan<- error) {
	t.Helper()
	for i := 0; i < n; i++ {
		s := seq.Add(1)
		body := refBody(size, s, time.Now().UnixNano())
		if err := p.Publish(protocol.Publish{Exchange: exchange, RoutingKey: routingKey, Body: body}); err != nil {
			errs <- fmt.Errorf("publish seq %d: %w", s, err)
			return
		}
	}
}

func refRunAck(t *testing.T, addr string, sc refScenario) refOutcome {
	t.Helper()

	suffix := refSuffix()
	routingKey := "test.key." + suffix

	control := newTestClient(t, addr)
	controlCh := control.openChannel("differential-declare")
	exchange, queue := declareFixture(t, controlCh, suffix)

	consumers := make([]worker, 0, sc.Consumers)
	for i := 0; i < sc.Consumers; i++ {
		tc := newTestClient(t, addr)
		ch := tc.openChannel("differential-consumer")
		deliveries, err := ch.Consume(queue, testCtx(t))
		if err != nil {
			t.Fatalf("consume on reference consumer %d: %v", i, err)
		}
		consumers = append(consumers, worker{ch: ch, deliveries: deliveries})
	}

	done := make(chan struct{})
	defer close(done)

	total := sc.Publishers * sc.Msgs
	merged := make(chan refDelivery, total)
	for _, w := range consumers {
		ch, deliveries := w.ch, w.deliveries
		go func(ch *gomqSDK.ClientChannel, deliveries <-chan protocol.Deliver) {
			for {
				select {
				case d, ok := <-deliveries:
					if !ok {
						return
					}
					select {
					case merged <- refDelivery{ch: ch, d: d}:
					case <-done:
						return
					}
				case <-done:
					return
				}
			}
		}(ch, deliveries)
	}

	seq := &atomic.Uint64{}
	errs := make(chan error, sc.Publishers)
	var pubWG sync.WaitGroup
	for i := 0; i < sc.Publishers; i++ {
		tc := newTestClient(t, addr)
		ch := tc.openChannel("differential-publisher")
		pubWG.Add(1)
		go func(ch *gomqSDK.ClientChannel) {
			defer pubWG.Done()
			refPublishLoop(t, ch, exchange, routingKey, sc.Msgs, sc.Size, seq, errs)
		}(ch)
	}
	pubWG.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("reference publisher: %v", err)
	}

	seen := make(map[uint64]bool, total)
	latencies := make([]float64, 0, total)
	delivered, duplicates := 0, 0
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	for len(seen) < total {
		select {
		case dd := <-merged:
			s, sendNanos, err := refDecode(dd.d.Body)
			if err != nil {
				t.Fatalf("decode delivery: %v", err)
			}
			delivered++
			if seen[s] {
				duplicates++
			} else {
				seen[s] = true
				latencies = append(latencies, float64(time.Now().UnixNano()-sendNanos)/1000)
			}
			if err := dd.ch.Ack(dd.d.DeliveryTag); err != nil {
				t.Fatalf("reference ack: %v", err)
			}
		case <-deadline.C:
			t.Fatalf("reference consumer timed out after %d/%d unique deliveries", len(seen), total)
		}
	}

	return refOutcome{
		Published: total,
		Delivered: delivered,
		Unique:    len(seen),
		Duplicate: duplicates,
		Latencies: latencies,
	}
}

func refRunNoAck(t *testing.T, addr string, sc refScenario) int {
	t.Helper()

	suffix := refSuffix()
	tc := newTestClient(t, addr)
	ch := tc.openChannel("differential-noack")
	exchange, queue := declareFixture(t, ch, suffix)
	routingKey := "test.key." + suffix
	deliveries, err := ch.Consume(queue, testCtx(t))
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	seq := &atomic.Uint64{}
	for i := 0; i < sc.Msgs; i++ {
		s := seq.Add(1)
		body := refBody(sc.Size, s, time.Now().UnixNano())
		if err := ch.Publish(protocol.Publish{Exchange: exchange, RoutingKey: routingKey, Body: body}); err != nil {
			t.Fatalf("publish seq %d: %v", s, err)
		}
	}

	consumed := 0
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		idle := time.NewTimer(500 * time.Millisecond)
		select {
		case <-deliveries:
			consumed++
			idle.Stop()
		case <-idle.C:
			return consumed
		case <-deadline.C:
			t.Fatalf("no-ack reference stall did not settle; consumed=%d", consumed)
		}
	}
}

func refPercentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int((p/100)*float64(len(sorted)) + 0.999999999) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func compareAckOutcome(t *testing.T, ref refOutcome, s bench.Summary) {
	t.Helper()

	if s.Published != uint64(ref.Published) {
		t.Fatalf("published harness=%d reference=%d", s.Published, ref.Published)
	}
	if s.Delivered != uint64(ref.Delivered) {
		t.Fatalf("delivered harness=%d reference=%d", s.Delivered, ref.Delivered)
	}
	if s.Consumed != uint64(ref.Unique) {
		t.Fatalf("consumed harness=%d reference unique=%d", s.Consumed, ref.Unique)
	}
	if s.Duplicates != uint64(ref.Duplicate) {
		t.Fatalf("duplicates harness=%d reference=%d", s.Duplicates, ref.Duplicate)
	}
	wantDrops := uint64(ref.Published - ref.Unique)
	if s.Drops != wantDrops {
		t.Fatalf("drops harness=%d reference=%d", s.Drops, wantDrops)
	}
	if s.Consumed+s.Drops != s.Published {
		t.Fatalf("harness consumed(%d)+drops(%d) != published(%d)", s.Consumed, s.Drops, s.Published)
	}

	if len(ref.Latencies) > 0 {
		sorted := append([]float64(nil), ref.Latencies...)
		sort.Float64s(sorted)
		p50 := refPercentile(sorted, 50)
		p95 := refPercentile(sorted, 95)
		p99 := refPercentile(sorted, 99)
		if !(p50 <= p95 && p95 <= p99) {
			t.Fatalf("reference latency percentiles not monotonic: p50=%v p95=%v p99=%v", p50, p95, p99)
		}
		if s.DeliveryLatency.Count != uint64(len(sorted)) {
			t.Fatalf("harness delivery latency count=%d reference=%d", s.DeliveryLatency.Count, len(sorted))
		}
		t.Logf("latency p50/p95/p99 harness=%.2f/%.2f/%.2f us reference=%.2f/%.2f/%.2f us",
			s.DeliveryLatency.P50, s.DeliveryLatency.P95, s.DeliveryLatency.P99, p50, p95, p99)
	}
}

func harnessRun(t *testing.T, addr string, sc refScenario) bench.Summary {
	t.Helper()

	cfg := bench.DefaultConfig()
	cfg.Addr = addr
	cfg.Conns = sc.Publishers
	cfg.Channels = sc.Consumers
	cfg.Publishers = sc.Publishers
	cfg.Consumers = sc.Consumers
	cfg.Msgs = sc.Msgs
	cfg.Size = sc.Size
	cfg.Ack = sc.Ack
	cfg.Output = t.TempDir() + "/differential.csv"
	cfg.Quiet = true
	cfg.Report = 10 * time.Millisecond
	if sc.Ack {
		cfg.DrainTimeout = 3 * time.Second
	} else {
		cfg.DrainTimeout = 300 * time.Millisecond
	}

	res, err := bench.Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("harness run: %v", err)
	}
	return res.Summary
}

func TestDifferential_AckSinglePublisherConsumer(t *testing.T) {
	addr := startTestBroker(t)
	sc := refScenario{Publishers: 1, Consumers: 1, Msgs: 100, Size: 128, Ack: true}

	ref := refRunAck(t, addr, sc)
	s := harnessRun(t, addr, sc)
	compareAckOutcome(t, ref, s)
}

func TestDifferential_AckMultiplePublishersConsumers(t *testing.T) {
	addr := startTestBroker(t)
	sc := refScenario{Publishers: 2, Consumers: 2, Msgs: 50, Size: 128, Ack: true}

	ref := refRunAck(t, addr, sc)
	s := harnessRun(t, addr, sc)
	compareAckOutcome(t, ref, s)
}

func TestDifferential_NoAckPrefetchStall(t *testing.T) {
	addr := startTestBroker(t)
	const prefetch = 10
	sc := refScenario{Publishers: 1, Consumers: 1, Msgs: 100, Size: 128, Ack: false}

	refConsumed := refRunNoAck(t, addr, sc)
	if refConsumed != prefetch {
		t.Fatalf("reference no-ack consumer got %d deliveries, want broker prefetch %d", refConsumed, prefetch)
	}

	s := harnessRun(t, addr, sc)
	if s.Published != uint64(sc.Msgs) {
		t.Fatalf("harness published=%d want %d", s.Published, sc.Msgs)
	}
	if s.Consumed != uint64(refConsumed) {
		t.Fatalf("consumed harness=%d reference=%d", s.Consumed, refConsumed)
	}
	wantDrops := uint64(sc.Msgs - refConsumed)
	if s.Drops != wantDrops {
		t.Fatalf("drops harness=%d reference=%d", s.Drops, wantDrops)
	}
	if s.Consumed+s.Drops != s.Published {
		t.Fatalf("harness consumed(%d)+drops(%d) != published(%d)", s.Consumed, s.Drops, s.Published)
	}
	if s.AckLatency.Min != 0 || s.AckLatency.Avg != 0 || s.AckLatency.Max != 0 ||
		s.AckLatency.P50 != 0 || s.AckLatency.P95 != 0 || s.AckLatency.P99 != 0 {
		t.Fatalf("harness recorded non-zero ack latency without -ack: %+v", s.AckLatency)
	}
}
