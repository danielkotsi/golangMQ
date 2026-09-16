package tests

import (
	"fmt"
	"sync"
	"testing"

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
