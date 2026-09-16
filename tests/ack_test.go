package tests

import (
	"testing"
	"time"
)

// TestAck_FreesPrefetchSlot verifies that a positive ack unblocks dispatch past
// the broker's prefetch ceiling (10).
func TestAck_FreesPrefetchSlot(t *testing.T) {
	const prefetch = 10

	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("ack-prefetch")

	exchange, queue := declareFixture(t, ch, "ack-prefetch")
	routingKey := "test.key.ack-prefetch"

	deliveries := consumeOnChannel(t, ch, queue)

	// Publish more than prefetch so the ceiling can actually be hit.
	const published = prefetch + 5
	for i := 0; i < published; i++ {
		publish(t, ch, exchange, routingKey, body(i))
	}

	// Without acking, only prefetch messages may be dispatched.
	held := collectDeliveries(t, deliveries, prefetch)
	if len(held) != prefetch {
		t.Fatalf("expected %d deliveries while unacked, got %d", prefetch, len(held))
	}
	assertNoDelivery(t, deliveries, 300*time.Millisecond, "no delivery beyond prefetch while unacked")

	// Ack the first held message; the next one must now be dispatched.
	if err := ch.Ack(held[0].DeliveryTag); err != nil {
		t.Fatalf("ack: %v", err)
	}
	next := collectDeliveries(t, deliveries, 1)
	if string(next[0].Body) != string(body(prefetch)) {
		t.Fatalf("body mismatch: got %q want %q", next[0].Body, body(prefetch))
	}
}

// TestAck_UnknownTagNoop verifies acking a never-issued tag is harmless and does
// not corrupt subsequent behaviour.
func TestAck_UnknownTagNoop(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("ack-unknown")

	exchange, queue := declareFixture(t, ch, "ack-unknown")
	routingKey := "test.key.ack-unknown"

	deliveries := consumeOnChannel(t, ch, queue)

	publish(t, ch, exchange, routingKey, body(1))
	got := collectDeliveries(t, deliveries, 1)

	// Acking a tag that was never issued must not panic or corrupt state.
	if err := ch.Ack(9999); err != nil {
		t.Fatalf("ack unknown tag: %v", err)
	}
	if err := ch.Ack(got[0].DeliveryTag); err != nil {
		t.Fatalf("ack valid tag: %v", err)
	}

	// Broker still dispatches normally afterwards.
	publish(t, ch, exchange, routingKey, body(2))
	after := collectDeliveries(t, deliveries, 1)
	if string(after[0].Body) != string(body(2)) {
		t.Fatalf("body mismatch: got %q want %q", after[0].Body, body(2))
	}
}

// TestNack_Requeue verifies a nack with requeue=true redelivers the message.
func TestNack_Requeue(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("nack-requeue")

	exchange, queue := declareFixture(t, ch, "nack-requeue")
	routingKey := "test.key.nack-requeue"

	deliveries := consumeOnChannel(t, ch, queue)

	publish(t, ch, exchange, routingKey, body(1))
	first := collectDeliveries(t, deliveries, 1)

	if err := ch.Nack(first[0].DeliveryTag, true); err != nil {
		t.Fatalf("nack: %v", err)
	}

	redelivered := collectDeliveries(t, deliveries, 1)
	if string(redelivered[0].Body) != string(body(1)) {
		t.Fatalf("body mismatch after requeue: got %q want %q", redelivered[0].Body, body(1))
	}
	// The preserved message keeps its original delivery tag when requeued.
	if redelivered[0].DeliveryTag != first[0].DeliveryTag {
		t.Fatalf("requeued message changed delivery tag: got %d want %d", redelivered[0].DeliveryTag, first[0].DeliveryTag)
	}
}

// TestNack_DeadLetter verifies a nack with requeue=false routes the message to
// the queue's configured dead-letter exchange.
func TestNack_DeadLetter(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("nack-dlx")

	ctx := testCtx(t)

	// Dead-letter exchange + queue.
	if _, err := ch.DeclareExchange("dlx", ctx); err != nil {
		t.Fatalf("declare dlx exchange: %v", err)
	}
	if _, err := ch.DeclareQueue("dlq", ctx, "", ""); err != nil {
		t.Fatalf("declare dlq queue: %v", err)
	}
	if err := ch.BindQueue("dlq", "dlx", "dead", ctx); err != nil {
		t.Fatalf("bind dlq: %v", err)
	}

	// Main exchange + queue with DLX configured.
	if _, err := ch.DeclareExchange("main", ctx); err != nil {
		t.Fatalf("declare main exchange: %v", err)
	}
	if _, err := ch.DeclareQueue("mainq", ctx, "dlx", "dead"); err != nil {
		t.Fatalf("declare main queue: %v", err)
	}
	if err := ch.BindQueue("mainq", "main", "key", ctx); err != nil {
		t.Fatalf("bind mainq: %v", err)
	}

	// Each channel carries at most one consumer, so the DLQ consumer lives on
	// its own channel.
	dlqCh := client.openChannel("nack-dlx-dlq")

	mainqDeliveries := consumeOnChannel(t, ch, "mainq")
	dlqDeliveries := consumeOnChannel(t, dlqCh, "dlq")

	publish(t, ch, "main", "key", body(42))
	d := collectDeliveries(t, mainqDeliveries, 1)

	if err := ch.Nack(d[0].DeliveryTag, false); err != nil {
		t.Fatalf("nack: %v", err)
	}

	dead := collectDeliveriesTimeout(t, dlqDeliveries, 1, defaultTimeout)
	if string(dead[0].Body) != string(body(42)) {
		t.Fatalf("dead-letter body mismatch: got %q want %q", dead[0].Body, body(42))
	}
}