package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/danielkotsi/golangMQSDK/protocol"
)

// TestFindingF004_ConsumeReturnsDistinctChannels asserts that two Consume calls
// on the same channel return distinct delivery channels, one per consumer.
//
// FAIL condition (current behaviour): the SDK's Consume always returns the
// channel's single shared ch.Incoming, so both calls return the same channel
// object (chanA == chanB).
func TestFindingF004_ConsumeReturnsDistinctChannels(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("f004")
	ctx := testCtx(t)

	exA, qA := declareFixture(t, ch, "a")
	exB, qB := declareFixture(t, ch, "b")

	chanA, err := ch.Consume(qA, ctx)
	if err != nil {
		t.Fatalf("consume %s: %v", qA, err)
	}
	chanB, err := ch.Consume(qB, ctx)
	if err != nil {
		t.Fatalf("consume %s: %v", qB, err)
	}

	if chanA == chanB {
		t.Fatal("F-004: two Consume calls on the same channel returned identical channels; expected distinct per-consumer channels")
	}

	publish(t, ch, exA, "test.key.a", []byte("msg-A"))
	publish(t, ch, exB, "test.key.b", []byte("msg-B"))

	gotA := <-chanA
	gotB := <-chanB

	if string(gotA.Body) != "msg-A" {
		t.Errorf("consumer A got %q, want %q", gotA.Body, "msg-A")
	}
	if string(gotB.Body) != "msg-B" {
		t.Errorf("consumer B got %q, want %q", gotB.Body, "msg-B")
	}
}

// TestFindingF004_DeliveriesRoutedToCorrectConsumer asserts that n messages
// published to queue A arrive only on consumer A's channel, and likewise for
// B. The two consumers must not share a delivery stream.
//
// FAIL condition (current behaviour): both consumers read from ch.Incoming, so
// messages destined for queue A can be consumed by either consumer's read
// goroutine — there is no routing.
func TestFindingF004_DeliveriesRoutedToCorrectConsumer(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("f004")
	ctx := testCtx(t)

	exA, qA := declareFixture(t, ch, "a")
	exB, qB := declareFixture(t, ch, "b")

	chanA, err := ch.Consume(qA, ctx)
	if err != nil {
		t.Fatalf("consume %s: %v", qA, err)
	}
	chanB, err := ch.Consume(qB, ctx)
	if err != nil {
		t.Fatalf("consume %s: %v", qB, err)
	}

	if chanA == chanB {
		t.Skip("F-004: channels not distinct; delivery routing cannot be tested until per-consumer channels are implemented")
	}

	const n = 5
	for i := range n {
		publish(t, ch, exA, "test.key.a", []byte(fmt.Sprintf("a-%d", i)))
		publish(t, ch, exB, "test.key.b", []byte(fmt.Sprintf("b-%d", i)))
	}

	var mu sync.Mutex
	gotA := make(map[string]int)
	gotB := make(map[string]int)

	collect := func(c <-chan protocol.Deliver, dst map[string]int) {
		for range n {
			select {
			case d := <-c:
				mu.Lock()
				dst[string(d.Body)]++
				mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); collect(chanA, gotA) }()
	go func() { defer wg.Done(); collect(chanB, gotB) }()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	for i := range n {
		key := fmt.Sprintf("a-%d", i)
		if gotA[key] != 1 {
			t.Errorf("consumer A got %d copies of %q, want 1", gotA[key], key)
		}
		if gotB[key] != 0 {
			t.Errorf("consumer B received %q (belongs to consumer A)", key)
		}
	}

	for i := range n {
		key := fmt.Sprintf("b-%d", i)
		if gotB[key] != 1 {
			t.Errorf("consumer B got %d copies of %q, want 1", gotB[key], key)
		}
		if gotA[key] != 0 {
			t.Errorf("consumer A received %q (belongs to consumer B)", key)
		}
	}
}

// TestFindingF004_ConsumerTagStampedPerConsumer asserts every delivery carries a
// non-empty consumer tag, that the tag is stable per consumer, and that the two
// consumers on one channel have distinct tags. This is the wire-level guarantee
// that lets a client tell deliveries apart (Deliver.ConsumerTag).
func TestFindingF004_ConsumerTagStampedPerConsumer(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("f004-tags")
	ctx := testCtx(t)

	exA, qA := declareFixture(t, ch, "tags-a")
	exB, qB := declareFixture(t, ch, "tags-b")

	chanA, err := ch.Consume(qA, ctx)
	if err != nil {
		t.Fatalf("consume %s: %v", qA, err)
	}
	chanB, err := ch.Consume(qB, ctx)
	if err != nil {
		t.Fatalf("consume %s: %v", qB, err)
	}

	const n = 3
	for i := 0; i < n; i++ {
		publish(t, ch, exA, "test.key.tags-a", []byte(fmt.Sprintf("a-%d", i)))
		publish(t, ch, exB, "test.key.tags-b", []byte(fmt.Sprintf("b-%d", i)))
	}

	var tagA, tagB string
	for i := 0; i < n; i++ {
		da := <-chanA
		db := <-chanB

		if da.ConsumerTag == "" {
			t.Fatalf("delivery %d to consumer A missing consumer tag", i)
		}
		if db.ConsumerTag == "" {
			t.Fatalf("delivery %d to consumer B missing consumer tag", i)
		}
		if tagA == "" {
			tagA = da.ConsumerTag
		} else if da.ConsumerTag != tagA {
			t.Fatalf("consumer A tag changed: %q -> %q", tagA, da.ConsumerTag)
		}
		if tagB == "" {
			tagB = db.ConsumerTag
		} else if db.ConsumerTag != tagB {
			t.Fatalf("consumer B tag changed: %q -> %q", tagB, db.ConsumerTag)
		}
	}

	if tagA == tagB {
		t.Fatalf("two consumers on one channel share tag %q; expected distinct per-consumer tags", tagA)
	}
}

// TestFindingF004_NackRedeliversOnSameConsumer asserts that a nack with
// requeue=true on consumer A's delivery redelivers only on consumer A's channel:
// the requeued message goes to the front of queue A, whose only consumer is A,
// and never leaks onto consumer B's channel.
func TestFindingF004_NackRedeliversOnSameConsumer(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("f004-nack")
	ctx := testCtx(t)

	exA, qA := declareFixture(t, ch, "nack-a")
	_, qB := declareFixture(t, ch, "nack-b")

	chanA, err := ch.Consume(qA, ctx)
	if err != nil {
		t.Fatalf("consume %s: %v", qA, err)
	}
	chanB, err := ch.Consume(qB, ctx)
	if err != nil {
		t.Fatalf("consume %s: %v", qB, err)
	}

	publish(t, ch, exA, "test.key.nack-a", []byte("nack-me"))

	d := <-chanA
	if string(d.Body) != "nack-me" {
		t.Fatalf("consumer A got %q, want %q", d.Body, "nack-me")
	}
	if d.ConsumerTag == "" {
		t.Fatalf("delivery missing consumer tag")
	}

	if err := ch.Nack(d.DeliveryTag, true); err != nil {
		t.Fatalf("nack: %v", err)
	}

	redel := <-chanA
	if string(redel.Body) != "nack-me" {
		t.Fatalf("redelivered body mismatch: got %q want %q", redel.Body, "nack-me")
	}
	if redel.DeliveryTag != d.DeliveryTag {
		t.Fatalf("requeued delivery changed tag: got %d want %d", redel.DeliveryTag, d.DeliveryTag)
	}
	if redel.ConsumerTag != d.ConsumerTag {
		t.Fatalf("redelivered on a different consumer: got %q want %q", redel.ConsumerTag, d.ConsumerTag)
	}

	assertNoDeliveryOnChan(t, chanB, 300*time.Millisecond, "nacked delivery must not leak onto consumer B's channel")
}

// TestFindingF004_CloseClosesPerConsumerChans asserts that closing the client
// closes every per-consumer channel (and the shared Incoming fallback), so a
// blocked consumer unblocks rather than hanging forever.
func TestFindingF004_CloseClosesPerConsumerChans(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("f004-close")
	ctx := testCtx(t)

	exA, qA := declareFixture(t, ch, "close-a")
	exB, qB := declareFixture(t, ch, "close-b")

	chanA, err := ch.Consume(qA, ctx)
	if err != nil {
		t.Fatalf("consume %s: %v", qA, err)
	}
	chanB, err := ch.Consume(qB, ctx)
	if err != nil {
		t.Fatalf("consume %s: %v", qB, err)
	}

	publish(t, ch, exA, "test.key.close-a", []byte("msg-A"))
	publish(t, ch, exB, "test.key.close-b", []byte("msg-B"))
	<-chanA
	<-chanB

	if err := client.c.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}

	assertChanClosed(t, chanA)
	assertChanClosed(t, chanB)
	assertChanClosed(t, ch.Incoming)
}

// assertNoDeliveryOnChan requires that nothing arrives on deliveries within wait;
// it fails the test if any delivery shows up or the channel closes.
func assertNoDeliveryOnChan(t *testing.T, deliveries <-chan protocol.Deliver, wait time.Duration, desc string) {
	t.Helper()

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case d, ok := <-deliveries:
		if ok {
			t.Fatalf("unexpected delivery (%q): %s", d.Body, desc)
		}
		t.Fatalf("delivery channel closed: %s", desc)
	case <-timer.C:
	}
}

// assertChanClosed fails the test unless deliveries has been closed (read
// returns ok=false) within defaultTimeout.
func assertChanClosed(t *testing.T, deliveries <-chan protocol.Deliver) {
	t.Helper()

	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()

	select {
	case d, ok := <-deliveries:
		if ok {
			t.Fatalf("expected channel closed, got delivery %q", d.Body)
		}
	case <-timer.C:
		t.Fatal("expected delivery channel to close")
	}
}
