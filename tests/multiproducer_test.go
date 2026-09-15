package tests

import (
	"sync"
	"testing"

	"github.com/danielkotsi/golangMQSDK/gomqSDK"
	"github.com/danielkotsi/golangMQSDK/protocol"
)

// TestMultiProducer_ConcurrentProducersOneConsumer verifies that N concurrently
// publishing producers do not lose or corrupt messages: every published message
// is delivered exactly once to a single consumer.
func TestMultiProducer_ConcurrentProducersOneConsumer(t *testing.T) {
	const producers = 4
	const perProducer = 20

	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("mp-producers")

	exchange, queue := declareFixture(t, ch, "mp-producers")
	routingKey := "test.key.mp-producers"

	// A single consumer drains the queue on its own channel.
	consumerCh := client.openChannel("mp-consumer")
	if _, err := consumerCh.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	// N concurrent producers, one channel each (publishing needs no consume).
	want := make([]string, 0, producers*perProducer)
	for p := 0; p < producers; p++ {
		for i := 0; i < perProducer; i++ {
			want = append(want, string(body(p*perProducer+i)))
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, producers)
	for p := 0; p < producers; p++ {
		pc := client.openChannel("mp-producer")
		wg.Add(1)
		go func(p int, pc *gomqSDK.ClientChannel) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := pc.Publish(protocol.Publish{
					Exchange:   exchange,
					RoutingKey: routingKey,
					Body:       body(p*perProducer + i),
				}); err != nil {
					errCh <- err
					return
				}
			}
		}(p, pc)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("publish: %v", err)
	}

	// All published deliveries must arrive at the single consumer.
	got := consumeAcked(t, consumerCh, producers*perProducer)
	assertBodiesMultiset(t, got, want)
}

// TestMultiProducer_NoConsumerThenAttach verifies that messages published while
// a queue has no consumer are retained and delivered once a consumer attaches.
func TestMultiProducer_NoConsumerThenAttach(t *testing.T) {
	const producers = 3
	const perProducer = 15

	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("mp-no-consumer")

	exchange, queue := declareFixture(t, ch, "mp-no-consumer")
	routingKey := "test.key.mp-no-consumer"

	want := make([]string, 0, producers*perProducer)
	for p := 0; p < producers; p++ {
		pc := client.openChannel("mp-producer")
		for i := 0; i < perProducer; i++ {
			msg := body(p*perProducer + i)
			want = append(want, string(msg))
			publish(t, pc, exchange, routingKey, msg)
		}
	}

	// No consumer has been registered yet: nothing is lost, messages are queued.
	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	got := consumeAcked(t, ch, producers*perProducer)
	assertBodiesMultiset(t, got, want)
}

// TestMultiProducer_ConcurrentGoroutinePublish hammers a single channel from
// many goroutines to expose any race or serialization bug in the SDK/broker
// publish path. Delivery set must be complete and uncorrupted.
func TestMultiProducer_ConcurrentGoroutinePublish(t *testing.T) {
	const goroutines = 8
	const perGoroutine = 30

	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("mp-goroutine")

	exchange, queue := declareFixture(t, ch, "mp-goroutine")
	routingKey := "test.key.mp-goroutine"

	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				if err := ch.Publish(protocol.Publish{
					Exchange:   exchange,
					RoutingKey: routingKey,
					Body:       body(g*perGoroutine + i),
				}); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("publish from goroutine: %v", err)
	}

	want := make([]string, 0, goroutines*perGoroutine)
	for i := 0; i < goroutines*perGoroutine; i++ {
		want = append(want, string(body(i)))
	}

	got := consumeAcked(t, ch, goroutines*perGoroutine)
	assertBodiesMultiset(t, got, want)
}

// TestMultiProducer_UnknownExchangeConcurrent verifies that a bad publish among
// good ones never blocks or corrupts subsequent deliveries.
func TestMultiProducer_UnknownExchangeConcurrent(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("mp-unknown-ex")

	exchange, queue := declareFixture(t, ch, "mp-unknown-ex")
	routingKey := "test.key.mp-unknown-ex"

	if _, err := ch.Consume(queue, testCtx(t)); err != nil {
		t.Fatalf("consume: %v", err)
	}

	publish(t, ch, exchange, routingKey, body(1))
	publish(t, ch, "no.such.exchange", "whatever", body(2))
	publish(t, ch, exchange, routingKey, body(3))

	got := consumeAcked(t, ch, 2)
	assertBodiesMultiset(t, got, []string{string(body(1)), string(body(3))})

	// The connection is still fully functional afterwards.
	publish(t, ch, exchange, routingKey, body(4))
	d := consumeAcked(t, ch, 1)
	assertBodiesMultiset(t, d, []string{string(body(4))})
}