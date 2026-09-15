package tests

import (
	"testing"
	"time"
)

// TestDisconnect_ConsumerUnackedRedelivered verifies that when a consumer goes
// away without acking its deliveries, those bodies are requeued and a new
// consumer on the same queue receives them.
func TestDisconnect_ConsumerUnackedRedelivered(t *testing.T) {
	const n = 5

	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("disc-unacked")

	exchange, queue := declareFixture(t, ch, "disc-unacked")
	routingKey := "test.key.disc-unacked"

	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	for i := 0; i < n; i++ {
		publish(t, ch, exchange, routingKey, body(i))
	}
	// Deliveries are received but deliberately NOT acked.
	held := collectDeliveries(t, ch, n)
	want := sliceBodies(held)

	// Disconnect the consumer outright (no ack). Any pending deliveries must
	// be requeued by the broker on connection teardown.
	if err := client.c.Close(); err != nil {
		t.Fatalf("close consumer: %v", err)
	}

	// A fresh consumer on the same queue must see exactly the unacked bodies.
	client2 := newTestClient(t, addr)
	ch2 := client2.openChannel("disc-unacked-2")
	if _, err := ch2.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume after disconnect: %v", err)
	}

	got := consumeAcked(t, ch2, n)
	assertBodiesMultiset(t, got, want)
}

// TestDisconnect_ProducerCloses verifies published messages are not lost when
// the producing connection is torn down: already-published messages remain
// queued for an attached consumer, and a second producer keeps working.
func TestDisconnect_ProducerCloses(t *testing.T) {
	const n = 5

	addr := startTestBroker(t)

	// A consumer attaches first.
	consumer := newTestClient(t, addr)
	cch := consumer.openChannel("disc-producer-cons")
	exchange, queue := declareFixture(t, cch, "disc-producer")
	routingKey := "test.key.disc-producer"
	if _, err := cch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	// A producer publishes a batch, then closes its connection immediately.
	producer := newTestClient(t, addr)
	pch := producer.openChannel("disc-producer")
	for i := 0; i < n; i++ {
		publish(t, pch, exchange, routingKey, body(i))
	}
	if err := producer.c.Close(); err != nil {
		t.Fatalf("close producer: %v", err)
	}

	// The batch must still be delivered to the (still-attached) consumer.
	got := consumeAcked(t, cch, n)
	assertBodiesMultiset(t, got, sliceBodiesRange(n))

	// A second producer publishes more; the consumer keeps receiving.
	producer2 := newTestClient(t, addr)
	p2ch := producer2.openChannel("disc-producer-2")
	for i := n; i < n+3; i++ {
		publish(t, p2ch, exchange, routingKey, body(i))
	}
	more := consumeAcked(t, cch, 3)
	assertBodiesMultiset(t, more, sliceBodiesRange(n+3)[n:])
}

// TestDisconnect_RedeliveryExactness verifies reconnect redelivery is exact:
// only the unacked bodies are redelivered, and bodies acked before disconnect
// are not.
func TestDisconnect_RedeliveryExactness(t *testing.T) {
	const n = 6
	const ackCount = 3 // ack the first 3, leave the rest unacked

	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("disc-exact")

	exchange, queue := declareFixture(t, ch, "disc-exact")
	routingKey := "test.key.disc-exact"

	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	for i := 0; i < n; i++ {
		publish(t, ch, exchange, routingKey, body(i))
	}
	deliveries := collectDeliveries(t, ch, n)

	// Ack the first ackCount; leave the rest unacked.
	for i := 0; i < ackCount; i++ {
		if err := ch.Ack(deliveries[i].DeliveryTag); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	unacked := sliceBodies(deliveries[ackCount:])

	if err := client.c.Close(); err != nil {
		t.Fatalf("close consumer: %v", err)
	}

	// Reconnect: exactly the unacked bodies must be redelivered; acked bodies
	// must not reappear.
	client2 := newTestClient(t, addr)
	ch2 := client2.openChannel("disc-exact-2")
	if _, err := ch2.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume after reconnect: %v", err)
	}

	redelivered := consumeAcked(t, ch2, n-ackCount)
	assertBodiesMultiset(t, redelivered, unacked)

	assertNoDelivery(t, ch2, 700*time.Millisecond, "acked bodies must not be redelivered")
}

// sliceBodiesRange returns string bodies for body(0)..body(end-1).
func sliceBodiesRange(end int) []string {
	out := make([]string, 0, end)
	for i := 0; i < end; i++ {
		out = append(out, string(body(i)))
	}
	return out
}