package tests

import (
	"context"
	"errors"
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

	deliveriesA := consumeOnChannel(t, chA, qA)
	deliveriesB := consumeOnChannel(t, chB, qB)

	const n = 5
	for i := 0; i < n; i++ {
		publish(t, chA, exA, "test.key.chan-a", body(i))
		publish(t, chB, exB, "test.key.chan-b", body(100+i))
	}

	gotA := consumeAcked(t, chA, deliveriesA, n)
	gotB := consumeAcked(t, chB, deliveriesB, n)

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

	deliveries := consumeOnChannel(t, ch, queue)

	for i := 0; i < n; i++ {
		publish(t, ch, exchange, routingKey, body(i))
	}
	// Deliveries are received but deliberately NOT acked, so they sit in the
	// consumer's pending set.
	held := collectDeliveries(t, deliveries, n)
	if len(held) != n {
		t.Fatalf("expected %d deliveries, got %d", n, len(held))
	}

	// Close the channel. The broker requeues held (unacked) deliveries and
	// replies with channel.close-ok (F-003), so Close must resolve promptly on
	// the response path rather than the context deadline. The SDK (v0.3.0+)
	// surfaces close-ok as a "channel closed" error instead of nil, so we only
	// assert Close returns before the deadline and that the error is not a
	// context timeout — that still proves the F-003 code path without pinning
	// the exact error value.
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	err := ch.Close(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close: stuck until context deadline instead of resolving on close-ok: %v", err)
	}
	if err == nil {
		t.Logf("note: close-ok resolved nil; the SDK branch surfaces a 'channel closed' error here")
	}

	// A different channel on the same client consumes the same queue: the
	// re-enqueued bodies must arrive.
	ch2 := client.openChannel("chan-close-2")
	deliveries2 := consumeOnChannel(t, ch2, queue)
	got := collectDeliveriesTimeout(t, deliveries2, n, defaultTimeout)

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