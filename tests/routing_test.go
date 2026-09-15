package tests

import (
	"testing"
	"time"
)

func TestRouting_ExactMatch(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("routing-exact")

	exchange, queue := declareFixture(t, ch, "routing-exact")
	routingKey := "test.key.routing-exact"

	publish(t, ch, exchange, routingKey, body(1))
	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	deliveries := collectDeliveries(t, ch, 1)
	if string(deliveries[0].Body) != string(body(1)) {
		t.Fatalf("body mismatch: got %q want %q", deliveries[0].Body, body(1))
	}
	if deliveries[0].RoutingKey != routingKey {
		t.Fatalf("routing key mismatch: got %q want %q", deliveries[0].RoutingKey, routingKey)
	}
}

func TestRouting_NonMatchingKeyDropped(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("routing-dropped")

	exchange, queue := declareFixture(t, ch, "routing-dropped")

	// Same exchange, different routing key with no binding.
	publish(t, ch, exchange, "other.key", body(1))

	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	assertNoDelivery(t, ch, 400*time.Millisecond, "unbound key on a bound exchange should drop the message")
}

func TestRouting_MultipleBindingsSameQueue(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("routing-multi-bind")

	ctx := testCtx(t)
	exchange := "test.exchange.routing-multi-bind"
	queue := "test.queue.routing-multi-bind"

	if _, err := ch.DeclareExchange(exchange, ctx); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if _, err := ch.DeclareQueue(queue, ctx, "", ""); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	for _, rk := range []string{"key.one", "key.two"} {
		if err := ch.BindQueue(queue, exchange, rk, ctx); err != nil {
			t.Fatalf("bind %s: %v", rk, err)
		}
	}

	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	publish(t, ch, exchange, "key.one", body(1))
	publish(t, ch, exchange, "key.two", body(2))

	deliveries := collectDeliveries(t, ch, 2)
	assertBodiesMultiset(t, []string{string(deliveries[0].Body), string(deliveries[1].Body)}, []string{string(body(1)), string(body(2))})
}