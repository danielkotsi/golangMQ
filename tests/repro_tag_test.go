package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/danielkotsi/golangMQSDK/protocol"
)

// TestRepro_ConsumerTagCollisionAcrossClients reproduces the Phase 2 multi-conn
// consumer-tag collision: two clients open their first channel (channel id 1 on
// each) and consume the same queue. The SDK mints the tag "client-1-consumer-1"
// on both, so the broker's per-queue consumer registry collapses them.
func TestRepro_ConsumerTagCollisionAcrossClients(t *testing.T) {
	addr := startTestBroker(t)

	c1 := newTestClient(t, addr)
	c2 := newTestClient(t, addr)

	setup := newTestClient(t, addr)
	sc := setup.openChannel("repro-setup")
	exchange, queue := declareFixture(t, sc, "repro-tag")
	routingKey := "test.key.repro-tag"

	ch1 := c1.openChannel("repro-consumer-1")
	ch2 := c2.openChannel("repro-consumer-2")

	del1 := consumeOnChannel(t, ch1, queue)
	del2 := consumeOnChannel(t, ch2, queue)

	t.Logf("delivery channels distinct: %v", del1 != del2)

	const n = 100
	for i := 0; i < n; i++ {
		publish(t, sc, exchange, routingKey, body(i))
	}

	var wg sync.WaitGroup
	var got1, got2 int
	var mu sync.Mutex
	count := func(dels <-chan protocol.Deliver, dst *int) {
		defer wg.Done()
		for {
			select {
			case d, ok := <-dels:
				if !ok {
					return
				}
				mu.Lock()
				*dst++
				mu.Unlock()
				_ = d
			case <-time.After(defaultTimeout):
				return
			}
		}
	}
	wg.Add(2)
	go count(del1, &got1)
	go count(del2, &got2)
	wg.Wait()

	t.Logf("n=%d delivered to consumer-1=%d consumer-2=%d", n, got1, got2)
	if got1 == 0 || got2 == 0 {
		t.Fatalf("one consumer starved: got1=%d got2=%d (tag collision over the same queue)", got1, got2)
	}
}