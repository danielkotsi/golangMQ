# Step 2 — Define metrics

> Part of [METRICS_PLAN.md](../METRICS_PLAN.md) §2.
> Execution order: [Step 1](01-add-prometheus-client.md) → **you are here** → [Step 3.1](03-1-thread-metrics.md) → [Step 3.2](03-2-hook-points.md) → [Step 4](04-expose-metrics.md) → [Step 5](05-verification.md)

## Goal

Create `broker/metrics.go` with the `Metrics` struct, every counter, the per-server
registry, and the gauge collector skeleton. **No instrumentation yet** — this step
only has to compile.

## New file: `broker/metrics.go` (package `broker`)

Chosen as package `broker` (not a separate `metrics/` package) so instrumentation
calls stay local and no import cycle is possible.

### 2.1 Counter inventory (15 counters)

| Metric | Labels | Meaning |
|---|---|---|
| `golangmq_publish_total` | `exchange` | Publish accepted for a known exchange |
| `golangmq_publish_unroutable_total` | `exchange` | Publish matched 0 queues (silent drop today) |
| `golangmq_publish_errors_total` | — | Publish rejected (exchange not found) |
| `golangmq_enqueue_total` | `queue` | Message appended to a queue buffer |
| `golangmq_deliver_total` | `queue` | `basic.deliver` written to consumer OK |
| `golangmq_deliver_errors_total` | `queue` | Deliver write failed (message re-enqueued) |
| `golangmq_ack_total` | `queue` | Ack processed |
| `golangmq_nack_total` | `queue`, `requeue` (`"true"/"false"`) | Nack processed |
| `golangmq_dead_letter_total` | `queue` | Nack w/o requeue → DLX republish executed |
| `golangmq_drop_total` | `queue` | Nack w/o requeue and **no DLX** → discarded |
| `golangmq_connections_opened_total` | — | Connection registered |
| `golangmq_connections_closed_total` | — | Connection cleaned up |
| `golangmq_handshake_failures_total` | — | `RunHandshake` failed |
| `golangmq_consumers_registered_total` | `queue` | Consumer registered |
| `golangmq_consumers_unregistered_total` | `queue` | Consumer removed |

### 2.2 Gauge inventory (6 gauges, snapshot collector)

Do **not** maintain depth gauges with Inc/Dec at every mutation site — there are
4 re-enqueue paths (deliver-error `queue.go:123`, nack-requeue `channel.go:132`,
disconnect-requeue `queue.go:61`, plus `enqueue`) and they would drift. Instead a
custom `prometheus.Collector` snapshots live state on every scrape:

| Metric | Source |
|---|---|
| `golangmq_queue_messages` | `len(q.messages)` per queue |
| `golangmq_queue_consumers` | `len(q.consumers)` per queue |
| `golangmq_consumer_inflight` | `c.inflight` (labels: `queue`, `consumer`) |
| `golangmq_queues` | `len(b.queues)` |
| `golangmq_exchanges` | `len(b.exchanges)` |
| `golangmq_connections` | `len(s.connections)` |

### 2.3 Struct sketch

```go
// broker/metrics.go
type Metrics struct {
    Registry *prometheus.Registry

    Published             *prometheus.CounterVec // exchange
    PublishUnroutable     *prometheus.CounterVec // exchange
    PublishErrors         prometheus.Counter
    Enqueued              *prometheus.CounterVec // queue
    Delivered             *prometheus.CounterVec // queue
    DeliverErrors         *prometheus.CounterVec // queue
    Acked                 *prometheus.CounterVec // queue
    Nacked                *prometheus.CounterVec // queue, requeue
    DeadLettered          *prometheus.CounterVec // queue
    Dropped               *prometheus.CounterVec // queue
    ConnectionsOpened     prometheus.Counter
    ConnectionsClosed     prometheus.Counter
    HandshakeFailures     prometheus.Counter
    ConsumersRegistered   *prometheus.CounterVec // queue
    ConsumersUnregistered *prometheus.CounterVec // queue
}

func NewMetrics() *Metrics {
    reg := prometheus.NewRegistry()
    reg.MustRegister(
        collectors.NewGoCollector(),
        collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
        // all counters above...
        // stateCollector placeholder (Step 3.2) — can be registered empty here
    )
    // ...
}
```

In this step the `stateCollector` (gauges) can be a stub whose `Describe` sends the
descriptors and whose `Collect` does nothing — it gets its snapshot logic in
[Step 3.2](03-2-hook-points.md).

**Lock-order rule (document as a comment in `metrics.go`):** the collector takes
`Server.mu → Broker.mu → Queue.mu`, and **never holds `Queue.mu` while calling back
into `Broker`**. No existing path acquires these in reverse.

## Files touched

| File | Change |
|---|---|
| `broker/metrics.go` | **new** — struct, `NewMetrics`, counter defs, collector stub |

## Done when

- [ ] `go build ./...` passes
- [ ] `go vet ./...` clean
- [ ] `go test ./tests/` still 30/30 PASS (nothing wired in yet)

## Next

→ [Step 3.1 — Thread `*Metrics` through construction](03-1-thread-metrics.md)
