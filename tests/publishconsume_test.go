package tests

import (
	"testing"
	"time"
)

func TestPublishConsume_RoundTrip(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("roundtrip")

	exchange, queue := declareFixture(t, ch, "roundtrip")
	routingKey := "test.key.roundtrip"

	publish(t, ch, exchange, routingKey, body(1))

	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	deliveries := collectDeliveries(t, ch, 1)
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(deliveries))
	}
	d := deliveries[0]
	if string(d.Body) != string(body(1)) {
		t.Fatalf("body mismatch: got %q want %q", d.Body, body(1))
	}
	if d.Exchange != exchange {
		t.Fatalf("exchange mismatch: got %q want %q", d.Exchange, exchange)
	}
	if d.RoutingKey != routingKey {
		t.Fatalf("routing key mismatch: got %q want %q", d.RoutingKey, routingKey)
	}
	if d.DeliveryTag != 1 {
		t.Fatalf("first delivery tag should be 1, got %d", d.DeliveryTag)
	}
}

func TestPublishConsume_MultipleMessages(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("multiple")

	exchange, queue := declareFixture(t, ch, "multiple")
	routingKey := "test.key.multiple"

	const n = 25
	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	// Consume first, then publish, so deliveries start flowing immediately.
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		want = append(want, string(body(i)))
		publish(t, ch, exchange, routingKey, body(i))
	}

	got := consumeAcked(t, ch, n)
	assertBodiesMultiset(t, got, want)
}

func TestPublishConsume_UnboundRoutingKey(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("unbound")

	exchange, queue := declareFixture(t, ch, "unbound")

	publish(t, ch, exchange, "no.such.routing.key", body(1))

	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	assertNoDelivery(t, ch, 400*time.Millisecond, "unbound routing key should drop the message")
}

func TestPublishConsume_UnknownExchange(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("unknown-exchange")

	exchange, queue := declareFixture(t, ch, "unknown-exchange")

	publish(t, ch, "no.such.exchange", "whatever", body(1))

	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	// Publishing to an unknown exchange must not deliver anything, and must not
	// disrupt the connection (subsequent valid operations still work).
	assertNoDelivery(t, ch, 400*time.Millisecond, "unknown exchange should drop the message")

	publish(t, ch, exchange, "test.key.unknown-exchange", body(2))
	deliveries := collectDeliveries(t, ch, 1)
	if string(deliveries[0].Body) != string(body(2)) {
		t.Fatalf("body mismatch: got %q want %q", deliveries[0].Body, body(2))
	}
}