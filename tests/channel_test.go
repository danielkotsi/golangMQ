package tests

import (
	"context"
	"testing"
	"time"

	"github.com/danielkotsi/golangMQSDK/protocol"
)

// TestChannel_MultipleChannelsOverOneConnection opens two channels on a single
// Client; each channel owns its own queue and consumer, and deliveries are
// routed to the correct channel independently.
func TestChannel_MultipleChannelsOverOneConnection(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)

	chA := client.openChannel("chan-a")
	chB := client.openChannel("chan-b")

	exA, qA := declareFixture(t, chA, "chan-a")
	exB, qB := declareFixture(t, chB, "chan-b")

	if _, err := chA.Consume(qA, testCtx(t)); err != nil {
		t.Fatalf("consume chan-a: %v", err)
	}
	if _, err := chB.Consume(qB, testCtx(t)); err != nil {
		t.Fatalf("consume chan-b: %v", err)
	}

	const n = 5
	for i := 0; i < n; i++ {
		publish(t, chA, exA, "test.key.chan-a", body(i))
		publish(t, chB, exB, "test.key.chan-b", body(100+i))
	}

	gotA := consumeAcked(t, chA, n)
	gotB := consumeAcked(t, chB, n)

	wantA := make([]string, 0, n)
	wantB := make([]string, 0, n)
	for i := 0; i < n; i++ {
		wantA = append(wantA, string(body(i)))
		wantB = append(wantB, string(body(100+i)))
	}
	assertBodiesMultiset(t, gotA, wantA)
	assertBodiesMultiset(t, gotB, wantB)
}

// TestChannel_CloseRequeuesUnacked verifies that closing a channel unregisters
// its consumer and re-enqueues its unacked pending messages, observable by a
// different channel consuming the same queue.
//
// Note: the broker replies with channel.close-ok (fixed per F-003), so
// ClientChannel.Close resolves on the response path rather than the context
// deadline; the requeue below is asserted as well.
func TestChannel_CloseRequeuesUnacked(t *testing.T) {
	const n = 4

	addr := startTestBroker(t)
	client := newTestClient(t, addr)

	ch := client.openChannel("chan-close")
	exchange, queue := declareFixture(t, ch, "chan-close")
	routingKey := "test.key.chan-close"

	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	for i := 0; i < n; i++ {
		publish(t, ch, exchange, routingKey, body(i))
	}
	// Deliveries are received but deliberately NOT acked, so they sit in the
	// consumer's pending set.
	held := collectDeliveries(t, ch, n)
	if len(held) != n {
		t.Fatalf("expected %d deliveries, got %d", n, len(held))
	}

	// Close the channel. The broker requeues held (unacked) deliveries and
	// replies with channel.close-ok (F-003), so Close must resolve on the
	// response path, not the context deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	if err := ch.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A different channel on the same client consumes the same queue: the
	// re-enqueued bodies must arrive.
	ch2 := client.openChannel("chan-close-2")
	if _, err := ch2.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume after close: %v", err)
	}
	got := collectDeliveriesTimeout(t, ch2, n, defaultTimeout)

	want := make([]string, 0, n)
	for _, d := range held {
		want = append(want, string(d.Body))
	}
	assertBodiesMultiset(t, sliceBodies(got), want)
}

// sliceBodies projects a slice of deliveries onto their string bodies.
func sliceBodies(deliveries []protocol.Deliver) []string {
	out := make([]string, 0, len(deliveries))
	for _, d := range deliveries {
		out = append(out, string(d.Body))
	}
	return out
}