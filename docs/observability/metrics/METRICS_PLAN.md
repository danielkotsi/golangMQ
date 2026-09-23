# Prometheus Metrics — Execution Plan (Goal 1)

**Goal:** Instrument the broker with the important metrics and expose them on an HTTP
`/metrics` endpoint that Prometheus can scrape.

**Pipeline:** Add Prometheus Go client → Define metrics → Instrument broker → Expose `/metrics`.

**Step files (one per execution step):**

| Step | File |
|---|---|
| 1. Add the Prometheus Go client | [`steps/01-add-prometheus-client.md`](steps/01-add-prometheus-client.md) |
| 2. Define metrics | [`steps/02-define-metrics.md`](steps/02-define-metrics.md) |
| 3.1 Thread `*Metrics` through construction | [`steps/03-1-thread-metrics.md`](steps/03-1-thread-metrics.md) |
| 3.2 Hook points (publish → deliver → ack/nack → connections/consumers) | [`steps/03-2-hook-points.md`](steps/03-2-hook-points.md) |
| 4. Expose `/metrics` | [`steps/04-expose-metrics.md`](steps/04-expose-metrics.md) |
| 5. Verification (smoke) | [`steps/05-verification.md`](steps/05-verification.md) |

---

## 0. Current state (baseline)

| Aspect | Status |
|---|---|
| Dependencies | Only `golangMQSDK v0.3.2` — no Prometheus, no HTTP server anywhere |
| Counters / stats | None (`sync/atomic`, `expvar`, `prometheus` — zero matches) |
| TCP broker | `:5672`, custom `GOMQ/1` JSON protocol |
| Entry point | `cmd/broker/main.go` — builds `ServerConfig`, calls `server.ListenAndServe()` |
| Docs layout | `docs/testing_benchmarking/{PLAN,REPORT}.md` (moved as part of this work) |

**Natural hook points already identified:**

| Event | Location |
|---|---|
| Publish accepted / unroutable / exchange-not-found | `broker/broker.go:54` (`Broker.Publish`) |
| Message enqueued (queue depth grows) | `broker/queue.go:134` (`Queue.enqueue`) |
| Message delivered to consumer | `broker/queue.go:108` (successful `WriteEnvelope` in `dispatchLoop`) |
| Deliver failed → re-enqueue | `broker/queue.go:120` (error branch in `dispatchLoop`) |
| Ack processed | `broker/broker.go:116` (success return in `Broker.Ack`) |
| Nack (requeue / dead-letter / drop) | `broker/channel.go:130-140` (`HandleNack`) |
| Connection opened / closed | `broker/server.go:51-64` (`Server.HandleConnection` + defer) |
| Consumer registered / unregistered | `broker/broker.go:91` / `broker/queue.go:50` |
| Structural gauges (depth, counts) | `Broker.queues`, `Broker.exchanges`, `Server.connections`, `Queue.messages`, `Queue.consumers` |

---

## 1. Add the Prometheus Go client

```bash
go get github.com/prometheus/client_golang@latest
go mod tidy
```

- This becomes the **first non-SDK dependency** — acceptable, it is the de-facto standard.
- Use the **custom-registry pattern**, not the global default registry: each `Server`
  owns its own `*prometheus.Registry`. Rationale: the test harness
  (`tests/helpers.go: startTestBroker`) boots many brokers in one process; a shared
  default registry would aggregate their metrics and make future metric assertions
  flaky. A per-server registry also lets `Server` own its own HTTP handler.
- Register the free collectors alongside ours:
  - `collectors.NewGoCollector()` (goroutines, heap, GC — for broker self-monitoring)
  - `collectors.NewProcessCollector(...)` (RSS, CPU, open FDs)

**Deliverable:** `go.mod` lists `github.com/prometheus/client_golang`; `go build ./...` passes.

---

## 2. Define metrics

**New file: `broker/metrics.go`** (package `broker` — keeps instrumentation calls
local and avoids a new package cycle).

Naming: all metrics prefixed `golangmq_`. Label cardinality is bounded by
**declared queues/exchanges and active connections only** — never label by
routing key, body, or consumer tag on high-churn events.

### 2.1 Counters

| Metric | Labels | Meaning | Incremented at |
|---|---|---|---|
| `golangmq_publish_total` | `exchange` | Publish calls accepted for a known exchange | `broker.go` `Publish` after exchange lookup succeeds |
| `golangmq_publish_unroutable_total` | `exchange` | Publish matched 0 queues (silent drop by design today) | `broker.go:63` when `len(queues)==0` |
| `golangmq_publish_errors_total` | — | Publish rejected (exchange not found) | `broker.go:59-61` |
| `golangmq_enqueue_total` | `queue` | Messages appended to a queue buffer | `queue.go` `enqueue` |
| `golangmq_deliver_total` | `queue` | `basic.deliver` written to a consumer successfully | `queue.go` after successful `WriteEnvelope` |
| `golangmq_deliver_errors_total` | `queue` | Deliver write failed (message re-enqueued) | `queue.go:120` error branch |
| `golangmq_ack_total` | `queue` | Acks processed | `broker.go:116` before `return nil` |
| `golangmq_nack_total` | `queue`, `requeue` (`"true"/"false"`) | Nacks processed | `channel.go` `HandleNack` after locating the pending message |
| `golangmq_dead_letter_total` | `queue` | Nack without requeue, DLX republish executed | `channel.go:134-139` |
| `golangmq_drop_total` | `queue` | Nack without requeue and **no DLX** → message discarded | `channel.go` else-branch when `dlx == ""` (currently a silent data-loss path — worth visibility) |
| `golangmq_connections_opened_total` | — | Connections accepted and registered | `server.go:55-56` |
| `golangmq_connections_closed_total` | — | Connections cleaned up | `server.go:57-64` defer |
| `golangmq_handshake_failures_total` | — | `RunHandshake` failed | `server.go:67-70` |
| `golangmq_consumers_registered_total` | `queue` | Consumer registered | `broker.go:91` |
| `golangmq_consumers_unregistered_total` | `queue` | Consumer removed (channel cleanup / disconnect) | `queue.go:50` `unregisterConsumer` |

### 2.2 Gauges — via a custom `prometheus.Collector`

Do **not** maintain depth gauges with Inc/Dec at every mutation site (there are 4
re-enqueue paths: deliver-error `queue.go:123`, nack-requeue `channel.go:132`,
disconnect-requeue `queue.go:61`, plus `enqueue` — too easy to drift). Instead,
implement a small collector that snapshots live state on every scrape:

| Metric | Source |
|---|---|
| `golangmq_queue_messages` | `len(q.messages)` per queue |
| `golangmq_queue_consumers` | `len(q.consumers)` per queue |
| `golangmq_consumer_inflight` | `c.inflight` (labels: `queue`, `consumer`) — bounded by active consumers; series vanish on scrape after unregister |
| `golangmq_queues` | `len(b.queues)` |
| `golangmq_exchanges` | `len(b.exchanges)` |
| `golangmq_connections` | `len(s.connections)` |

**Lock-order rule (must document in `metrics.go`):** the collector takes
`Server.mu → Broker.mu → Queue.mu`, and **never holds `Queue.mu` while calling back
into `Broker`**. No existing code path acquires these in reverse, so scrape-time
collection cannot deadlock.

### 2.3 Struct type (sketch)

```go
// broker/metrics.go
type Metrics struct {
    Registry *prometheus.Registry

    Published            *prometheus.CounterVec // exchange
    PublishUnroutable    *prometheus.CounterVec // exchange
    PublishErrors        prometheus.Counter
    Enqueued             *prometheus.CounterVec // queue
    Delivered            *prometheus.CounterVec // queue
    DeliverErrors        *prometheus.CounterVec // queue
    Acked                *prometheus.CounterVec // queue
    Nacked               *prometheus.CounterVec // queue, requeue
    DeadLettered         *prometheus.CounterVec // queue
    Dropped              *prometheus.CounterVec // queue
    ConnectionsOpened    prometheus.Counter
    ConnectionsClosed    prometheus.Counter
    HandshakeFailures    prometheus.Counter
    ConsumersRegistered   *prometheus.CounterVec // queue
    ConsumersUnregistered *prometheus.CounterVec // queue
}

func NewMetrics() *Metrics { /* NewCounterVec/NewCounter + registry.Register(...) + Go/Process collectors */ }
```

**Deliverable:** `metrics.go` compiles; registry ready; no instrumentation yet
(`go build ./...`, `go vet ./...` clean).

---

## 3. Instrument the broker

### 3.1 Thread `*Metrics` through construction

| File | Change |
|---|---|
| `broker/server.go` | `Server` gains `metrics *Metrics`; `NewServer` calls `NewMetrics()` and registers the queue-state collector (collector needs a back-reference to `s`) |
| `broker/broker.go` | `Broker` gains `metrics *Metrics`; `NewBroker()` signature updated (or `NewBrokerWithMetrics(m)`), set from `NewServer` |
| `broker/queue.go` | `Queue` reads metrics from the broker when created in `Broker.DeclareQueue` — no signature change to `NewQueue` callers beyond broker internals |
| `broker/channel.go` | No new field needed — uses `ch.broker.metrics` |
| `broker/connection.go` | No new field needed — connection already holds `*Server` |

`tests/helpers.go` calls `broker.NewServer(addr, cfg)` — keep that signature
unchanged so all 30 existing tests keep passing untouched.

### 3.2 Hook-point edits (exact)

1. **`broker/broker.go` `Publish` (~line 54)**
   - exchange not found → `PublishErrors.Inc()` before returning error
   - `len(queues) == 0` → `PublishUnroutable.WithLabelValues(exchange).Inc()`
   - after lookup → `Published.WithLabelValues(exchange).Inc()`
2. **`broker/queue.go` `enqueue` (~line 134)** → `Enqueued.WithLabelValues(q.name).Inc()`
3. **`broker/queue.go` `dispatchLoop`**
   - after successful `WriteEnvelope` → `Delivered.WithLabelValues(q.name).Inc()`
   - `if err != nil` branch (~line 120) → `DeliverErrors.WithLabelValues(q.name).Inc()`
4. **`broker/broker.go` `Ack`** → on success (line 116) `Acked.WithLabelValues(queueName).Inc()`
5. **`broker/channel.go` `HandleNack`** (after locating pending message, ~line 128)
   - always: `Nacked.WithLabelValues(queue, strconv.FormatBool(requeue)).Inc()`
   - `requeue` branch: nothing extra (collector reflects new depth)
   - DLX branch (~line 139): `DeadLettered.WithLabelValues(queue).Inc()` (the republish itself also bumps `Publish`/`Enqueued` for the DLX target — correct, no double counting of the original queue)
   - no-requeue + no-DLX: `Dropped.WithLabelValues(queue).Inc()`
6. **`broker/server.go` `HandleConnection`**
   - after map insert (~line 55) → `ConnectionsOpened.Inc()`
   - inside defer (~line 57) → `ConnectionsClosed.Inc()`
   - handshake error branch (~line 67) → `HandshakeFailures.Inc()`
7. **`broker/broker.go` `RegisterConsumer`** (after `registerConsumer`, ~line 91) →
   `ConsumersRegistered.WithLabelValues(queue).Inc()`
8. **`broker/queue.go` `unregisterConsumer`** (after existence check) →
   `ConsumersUnregistered.WithLabelValues(q.name).Inc()`
9. **`broker/metrics.go`** → implement `Describe`/`Collect` snapshotting gauges from §2.2

**Rules:** metric updates happen **outside** held locks where the value is already
known (Prometheus vectors are concurrency-safe) — except the collector, which takes
locks only to read state. No logging changes, no behavior changes: instrumentation
is increment-only.

**Deliverable:** `go build ./... && go vet ./... && go test -race ./tests/ -count=1`
→ **30/30 still PASS** (instrumentation must not alter semantics).

---

## 4. Expose `/metrics`

### 4.1 Handler

Add to `broker/server.go` (or `metrics.go`):

```go
func (s *Server) MetricsHandler() http.Handler {
    return promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{})
}
```

### 4.2 Startup — `cmd/broker/main.go`

- Add `flag` package (first use in the repo):
  - `-addr` (default `:5672`) — broker listen address
  - `-metrics-addr` (default `:9090`) — metrics listen address; empty string disables
- Start a second listener in a goroutine **before** `server.ListenAndServe()`:

```go
if *metricsAddr != "" {
    go func() {
        mux := http.NewServeMux()
        mux.Handle("/metrics", server.MetricsHandler())
        log.Printf("metrics listening on %s/metrics", *metricsAddr)
        log.Println(http.ListenAndServe(*metricsAddr, mux))
    }()
}
```

Metrics on a **separate port** from `:5672` (the broker port speaks `GOMQ/1`, not HTTP).

### 4.3 Deployment surface

| File | Change |
|---|---|
| `Dockerfile` | `EXPOSE 5672 9090` |
| `deployment.yaml` | add `containerPort: 9090`; add scrape annotations `prometheus.io/scrape: "true"`, `prometheus.io/port: "9090"`, `prometheus.io/path: "/metrics"` |
| `service.yaml` | only if Prometheus runs outside the cluster — otherwise pod annotations suffice |

**Deliverable:** broker runs, `curl localhost:9090/metrics` returns exposition text.

---

## 5. Verification

```bash
# 1. Static + correctness (must stay 30/30)
go build ./... && go vet ./...
go test -race ./tests/ -count=1

# 2. Run broker + generate traffic
go run ./cmd/broker &
go run ./exampleClients/publisher   # declares exchange/queue, publishes 3 msgs
go run ./exampleClients/consumer    # consumes + nacks (exercises nack/drop paths)

# 3. Scrape and validate
curl -s localhost:9090/metrics | grep golangmq_
curl -s localhost:9090/metrics | promtool check metrics   # optional, zero errors expected
```

**Expected series after the smoke run (minimum):**
`golangmq_publish_total`, `golangmq_enqueue_total`, `golangmq_deliver_total`,
`golangmq_ack_total`, `golangmq_nack_total{requeue=...}`, `golangmq_queue_messages`,
`golangmq_queues`, `golangmq_connections`, plus `go_*` / `process_*` collectors.

**Acceptance criteria:**

- [ ] `prometheus/client_golang` in `go.mod`; custom per-server registry
- [ ] All counters from §2.1 defined and hooked at the listed locations
- [ ] Gauges from §2.2 served by the snapshot collector (no drift on re-enqueue paths)
- [ ] `GET /metrics` on `-metrics-addr` returns Prometheus exposition format
- [ ] `go test -race ./tests/` still 30/30 PASS
- [ ] No behavior/protocol changes — increments only
- [ ] K8s manifests expose/scrape the metrics port

---

## 6. Out of scope for Goal 1 (feed later goals)

- Histograms (publish/deliver latency, message-size buckets)
- Alert rules & Grafana dashboards
- Structured logging / correlation IDs (tracing)
- Integration with the benchmark harness (`cmd/benchmark/`, Phase 2 of the testing plan)
- Fixing the latent issues the metrics will *reveal* (silent unroutable drops,
  no-requeue-no-DLX drops, `uint16` delivery-tag wraparound)

**Suggested execution order:** §1 → §2 (compiles) → §3.1 (threads through, tests green)
→ §3.2 hooks one-by-one (build+test after each cluster: publish path → deliver path →
ack/nack → connections/consumers) → §4 → §5 smoke.

Each step is expanded into its own file under [`steps/`](steps/) — start at
[Step 1](steps/01-add-prometheus-client.md) and follow the `Next` links.
