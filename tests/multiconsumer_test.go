package tests

import (
	"testing"
	"time"

	"github.com/danielkotsi/golangMQSDK/gomqSDK"
)

// Consuming the same queue requires one consumer per channel on this broker, so
// M consumers on one queue means M channels on one (or several) clients.

// TestMultiConsumer_ExactlyOnceAcrossWorkers publishes N messages to a queue
// drained by 3 workers (one per channel) and asserts the union of what they
// receive equals the published multiset: no loss, no duplication.
func TestMultiConsumer_ExactlyOnceAcrossWorkers(t *testing.T) {
	const workers = 3
	const n = 30

	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("mc-workers")

	exchange, queue := declareFixture(t, ch, "mc-workers")
	routingKey := "test.key.mc-workers"

	workerChs := make([]*gomqSDK.ClientChannel, workers)
	for i := 0; i < workers; i++ {
		wc := client.openChannel("mc-worker")
		if _, err := wc.Consume(queue, testCtx(t)); err != nil {
			t.Fatalf("worker %d consume: %v", i, err)
		}
		workerChs[i] = wc
	}

	// Publish all messages.
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		want = append(want, string(body(i)))
		publish(t, ch, exchange, routingKey, body(i))
	}

	// Drain each worker by reading deliveries round-robin and acking them.
	got := make([]string, 0, n)
	for len(got) < n {
		for _, wc := range workerChs {
			select {
			case d, ok := <-wc.Incoming:
				if !ok {
					t.Fatalf("worker channel closed at %d/%d deliveries", len(got), n)
				}
				got = append(got, string(d.Body))
				if err := wc.Ack(d.DeliveryTag); err != nil {
					t.Fatalf("ack: %v", err)
				}
			case <-time.After(defaultTimeout):
				t.Fatalf("timed out draining workers; got %d/%d", len(got), n)
			}
		}
	}

	assertBodiesMultiset(t, got, want)
}

// TestMultiConsumer_PrefetchRespected verifies a single consumer on a queue is
// only handed broker-default prefetch (10) messages until it acks: dispatch
// stalls at the ceiling.
func TestMultiConsumer_PrefetchRespected(t *testing.T) {
	const published = 25
	const prefetch = 10

	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("mc-prefetch")

	exchange, queue := declareFixture(t, ch, "mc-prefetch")
	routingKey := "test.key.mc-prefetch"

	wc := client.openChannel("mc-prefetch-consumer")
	if _, err := wc.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	// Publish more than the prefetch ceiling without acking anything yet.
	for i := 0; i < published; i++ {
		publish(t, ch, exchange, routingKey, body(i))
	}

	// Immediately-flowing window: only prefetch deliveries may arrive while
	// nothing is acked.
	burst := collectDeliveries(t, wc, prefetch)

	// No more than prefetch may have been delivered before any ack.
	assertNoDelivery(t, wc, prefetchWait(), "unacked messages must stop at the prefetch ceiling")

	// Ack the burst; the remainder must now flow.
	for _, d := range burst {
		if err := wc.Ack(d.DeliveryTag); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	rest := consumeAcked(t, wc, published-len(burst))
	if len(rest) != published-prefetch {
		t.Fatalf("expected %d remaining deliveries, got %d", published-prefetch, len(rest))
	}
}

func prefetchWait() time.Duration {
	return 700 * time.Millisecond
}

// TestMultiConsumer_Distribution verifies messages go to more than one worker
// when several consumers share a queue (documenting the non-deterministic but
// distributed dispatch).
func TestMultiConsumer_Distribution(t *testing.T) {
	const workers = 3
	const n = 45

	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("mc-distribution")

	exchange, queue := declareFixture(t, ch, "mc-distribution")
	routingKey := "test.key.mc-distribution"

	workerChs := make([]*gomqSDK.ClientChannel, workers)
	perWorker := make([]int, workers)
	for i := 0; i < workers; i++ {
		wc := client.openChannel("mc-worker")
		if _, err := wc.Consume(queue, testCtx(t)); err != nil {
			t.Fatalf("worker %d consume: %v", i, err)
		}
		workerChs[i] = wc
	}

	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		want = append(want, string(body(i)))
		publish(t, ch, exchange, routingKey, body(i))
	}

	got := make([]string, 0, n)
	for len(got) < n {
		for i, wc := range workerChs {
			select {
			case d, ok := <-wc.Incoming:
				if !ok {
					t.Fatalf("worker channel closed")
				}
				got = append(got, string(d.Body))
				perWorker[i]++
				if err := wc.Ack(d.DeliveryTag); err != nil {
					t.Fatalf("ack: %v", err)
				}
			case <-time.After(defaultTimeout):
				t.Fatalf("timed out draining workers; got %d/%d (per-worker %v)", len(got), n, perWorker)
			}
		}
	}

	assertBodiesMultiset(t, got, want)

	activeWorkers := 0
	for _, c := range perWorker {
		if c > 0 {
			activeWorkers++
		}
	}
	if activeWorkers < 2 {
		t.Fatalf("expected messages to spread across >1 worker, got per-worker counts %v", perWorker)
	}
}