package tests

import (
	"GolangRabbitMQBroker/broker"
	"context"
	"fmt"
	"net"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/danielkotsi/golangMQSDK/gomqSDK"
	"github.com/danielkotsi/golangMQSDK/protocol"
)

const (
	defaultTimeout   = 5 * time.Second
	defaultPollEvery = 50 * time.Millisecond
)

// testClient wraps a connected SDK client with test convenience methods.
type testClient struct {
	t *testing.T
	c *gomqSDK.Client
}

// startTestBroker boots an in-process broker on an ephemeral port and returns
// the host:port address clients should dial. The broker is shut down when the
// test finishes.
func startTestBroker(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on ephemeral port: %v", err)
	}

	cfg := broker.ServerConfig{
		ChannelMax:   10,
		FramesMax:    10372,
		HeartbeatSec: 10,
	}
	srv := broker.NewServer(ln.Addr().String(), cfg)

	t.Cleanup(func() {
		_ = ln.Close()
	})

	go func() {
		_ = srv.Serve(ln)
	}()

	addr := ln.Addr().String()
	t.Logf("broker listening on %s", addr)
	return addr
}

// newTestClient connects an SDK client to addr and registers cleanup to close
// it when the test finishes.
func newTestClient(t *testing.T, addr string) *testClient {
	t.Helper()

	cfg := gomqSDK.Config{
		ClientName:   "test-client",
		Username:     "daniel",
		Password:     "123456789",
		ChannelMax:   10,
		FrameMax:     10372,
		HeartbeatSec: 10,
	}

	c, err := gomqSDK.Connect(addr, cfg)
	if err != nil {
		t.Fatalf("connect to %s: %v", addr, err)
	}

	t.Cleanup(func() {
		_ = c.Close()
	})

	return &testClient{t: t, c: c}
}

// openChannel opens a channel on the client, failing the test on error.
func (tc *testClient) openChannel(taskName string) *gomqSDK.ClientChannel {
	tc.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	ch, err := tc.c.OpenChannel(ctx)
	if err != nil {
		tc.t.Fatalf("open channel: %v", err)
	}
	return ch
}

// testCtx returns a context that is automatically cancelled when the test
// finishes.
func testCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	t.Cleanup(cancel)
	return ctx
}

// declareFixture declares a uniquely-named exchange, queue, and binding derived
// from suffix. It returns the exchange and queue names.
func declareFixture(t *testing.T, ch *gomqSDK.ClientChannel, suffix string) (exchange, queue string) {
	t.Helper()

	ctx := context.Background()
	exchange = "test.exchange." + suffix
	queue = "test.queue." + suffix
	routingKey := "test.key." + suffix

	if _, err := ch.DeclareExchange(exchange, ctx); err != nil {
		t.Fatalf("declare exchange %s: %v", exchange, err)
	}
	if _, err := ch.DeclareQueue(queue, ctx, "", ""); err != nil {
		t.Fatalf("declare queue %s: %v", queue, err)
	}
	if err := ch.BindQueue(queue, exchange, routingKey, ctx); err != nil {
		t.Fatalf("bind queue %s to %s: %v", queue, exchange, err)
	}
	return exchange, queue
}

// assertEventually polls cond until it returns true or timeout elapses, failing
// the test with desc on timeout. Use this instead of raw time.Sleep so async
// delivery is waited for deterministically.
func assertEventually(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(defaultPollEvery)
	}
	t.Fatalf("condition not met within %s: %s", timeout, desc)
}

// consumeOnChannel starts consuming queue on ch and returns the per-consumer
// delivery channel, failing the test on error. With the v0.3.0 SDK the channel
// returned by Consume (keyed by consumer tag) is the only place deliveries land;
// ch.Incoming is only a legacy fallback.
func consumeOnChannel(t *testing.T, ch *gomqSDK.ClientChannel, queue string) <-chan protocol.Deliver {
	t.Helper()

	deliveries, err := ch.Consume(queue, testCtx(t))
	if err != nil {
		t.Fatalf("consume %s: %v", queue, err)
	}
	return deliveries
}

// collectDeliveries reads n deliveries from the consumer's delivery channel with
// an overall timeout, failing the test if the channel closes or the timeout
// elapses before all n arrive.
func collectDeliveries(t *testing.T, deliveries <-chan protocol.Deliver, n int) []protocol.Deliver {
	t.Helper()
	return collectDeliveriesTimeout(t, deliveries, n, defaultTimeout)
}

// collectDeliveriesTimeout is collectDeliveries with a caller-supplied timeout.
func collectDeliveriesTimeout(t *testing.T, deliveries <-chan protocol.Deliver, n int, timeout time.Duration) []protocol.Deliver {
	t.Helper()

	out := make([]protocol.Deliver, 0, n)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for len(out) < n {
		select {
		case d, ok := <-deliveries:
			if !ok {
				t.Fatalf("delivery channel closed after %d/%d deliveries", len(out), n)
			}
			out = append(out, d)
		case <-deadline.C:
			t.Fatalf("timed out after %s waiting for %d deliveries; got %d", timeout, n, len(out))
		}
	}
	return out
}

// consumeAcked reads n deliveries from the consumer's channel, acking each on
// ch as it arrives (so the broker keeps dispatching past the prefetch ceiling),
// and returns the bodies.
func consumeAcked(t *testing.T, ch *gomqSDK.ClientChannel, deliveries <-chan protocol.Deliver, n int) []string {
	t.Helper()

	bodies := make([]string, 0, n)
	for len(bodies) < n {
		got := collectDeliveriesTimeout(t, deliveries, 1, defaultTimeout)
		bodies = append(bodies, string(got[0].Body))
		if err := ch.Ack(got[0].DeliveryTag); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	return bodies
}

// assertNoDelivery requires that nothing arrives on deliveries within wait; it
// fails the test if any delivery shows up. Use it to assert messages are dropped
// (unbound routing key, unknown exchange, etc.).
func assertNoDelivery(t *testing.T, deliveries <-chan protocol.Deliver, wait time.Duration, desc string) {
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

// worker ties a delivery stream to the channel that carries it, so callers can
// ack/nack each delivery on the correct channel.
type worker struct {
	ch         *gomqSDK.ClientChannel
	deliveries <-chan protocol.Deliver
}

// workerDelivery pairs a delivery with the worker channel it arrived on, so
// callers can ack/nack it on the correct channel.
type workerDelivery struct {
	ch *gomqSDK.ClientChannel
	d  protocol.Deliver
}

// mergeWorkerDeliveries fans in the per-consumer delivery streams of several
// workers into a single channel of deliveries, tagging each with its source
// worker. A strict round-robin reader would stall as soon as any single
// worker's stream runs dry (the distribution of messages across workers is
// uneven by design), so multi-consumer drains read from the merged stream
// instead. The returned channel closes once every source stream has closed.
func mergeWorkerDeliveries(workers ...worker) <-chan workerDelivery {
	out := make(chan workerDelivery, 100)

	var wg sync.WaitGroup
	wg.Add(len(workers))
	for _, w := range workers {
		go func(w worker) {
			defer wg.Done()
			for d := range w.deliveries {
				out <- workerDelivery{ch: w.ch, d: d}
			}
		}(w)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// publish sends a single message over ch to the given routing key.
func publish(t *testing.T, ch *gomqSDK.ClientChannel, exchange, routingKey string, body []byte) {
	t.Helper()

	if err := ch.Publish(protocol.Publish{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Body:       body,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// body returns a deterministic payload for message i.
func body(i int) []byte {
	return []byte(fmt.Sprintf("msg-%d", i))
}

// assertBodiesMultiset fails the test if got and want do not contain exactly
// the same bodies (order-independent comparison).
func assertBodiesMultiset(t *testing.T, got, want []string) {
	t.Helper()

	gotSorted := append([]string(nil), got...)
	wantSorted := append([]string(nil), want...)
	sort.Strings(gotSorted)
	sort.Strings(wantSorted)

	if !slices.Equal(gotSorted, wantSorted) {
		t.Fatalf("delivered bodies mismatch:\n got: %v\nwant: %v", got, want)
	}
}