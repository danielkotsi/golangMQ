package tests

import (
	"testing"
)

// TestHarnessSmoke verifies the harness plumbing end-to-end: broker boots on an
// ephemeral port, an SDK client connects, and a publish/consume round-trip works.
func TestHarnessSmoke(t *testing.T) {
	addr := startTestBroker(t)
	t.Log("broker started")

	client := newTestClient(t, addr)
	t.Log("client connected")

	ch := client.openChannel("smoke")
	t.Log("channel opened")

	exchange, queue := declareFixture(t, ch, "smoke")
	t.Log("exchange/queue/binding declared")

	routingKey := "test.key.smoke"
	publish(t, ch, exchange, routingKey, body(1))
	t.Log("published message")

	_, err := ch.Consume(queue, testCtx(t))
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	t.Log("consumer registered")

	deliveries := collectDeliveriesTimeout(t, ch, 1, defaultTimeout)
	t.Log("deliveries collected")
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(deliveries))
	}
	if string(deliveries[0].Body) != string(body(1)) {
		t.Fatalf("body mismatch: got %q want %q", deliveries[0].Body, body(1))
	}
	if deliveries[0].Exchange != exchange {
		t.Fatalf("exchange mismatch: got %q want %q", deliveries[0].Exchange, exchange)
	}
	if deliveries[0].RoutingKey != routingKey {
		t.Fatalf("routing key mismatch: got %q want %q", deliveries[0].RoutingKey, routingKey)
	}
}