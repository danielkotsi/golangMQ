# Step 3.2 — Hook points, one cluster at a time

> Part of [METRICS_PLAN.md](../METRICS_PLAN.md) §3.2.
> Execution order: [Step 1](01-add-prometheus-client.md) → [Step 2](02-define-metrics.md) → [Step 3.1](03-1-thread-metrics.md) → **you are here** → [Step 4](04-expose-metrics.md) → [Step 5](05-verification.md)

## Goal

Add the actual increments at the hook points. Work in **4 clusters**; after each
cluster run `go build ./... && go vet ./... && go test -race ./tests/ -count=1`
(must stay 30/30) before moving on.

## Rules for every hook

- **Increment-only.** No behavior, logging, or protocol changes.
- Prometheus vectors are concurrency-safe — call `.Inc()` **outside** held locks
  wherever the value is already known. The only component that takes locks is the
  gauge collector, and only to read.
- Never panic on a nil metric during development — if you hit nil, wiring from
  [Step 3.1](03-1-thread-metrics.md) is incomplete.

---

## Cluster A — publish path

### A1. `broker/broker.go` `Publish` (~line 54)

```go
func (b *Broker) Publish(exchangeName, routingKey string, body []byte) error {
    b.mu.Lock()
    ex, ok := b.exchanges[exchangeName]
    b.mu.Unlock()

    if !ok {
        b.metrics.PublishErrors.Inc()                    // NEW
        return fmt.Errorf("exchange %q not found", exchangeName)
    }

    b.metrics.Published.WithLabelValues(exchangeName).Inc() // NEW

    queues := ex.getQueues(routingKey)
    if len(queues) == 0 {                                // NEW
        b.metrics.PublishUnroutable.WithLabelValues(exchangeName).Inc()
    }
    for _, q := range queues {
        // ... unchanged
    }
    return nil
}
```

### A2. `broker/queue.go` `enqueue` (~line 134)

```go
func (q *Queue) enqueue(msg Message) {
    q.metrics.Enqueued.WithLabelValues(q.name).Inc()     // NEW
    q.mu.Lock()
    q.messages = append(q.messages, msg)
    q.mu.Unlock()
    q.cond.Signal()
}
```

**Verify after cluster A:** build + vet + `go test -race ./tests/ -count=1`.

---

## Cluster B — deliver path

### B1. `broker/queue.go` `dispatchLoop` — success (after `WriteEnvelope`, ~line 108)

```go
    err := consumer.channel.conn.WriteEnvelope(/* ... */)
    if err == nil {                                      // NEW (restructure if/else)
        q.metrics.Delivered.WithLabelValues(q.name).Inc()
    }
```

### B2. `broker/queue.go` `dispatchLoop` — error branch (~line 120)

```go
    if err != nil {
        q.metrics.DeliverErrors.WithLabelValues(q.name).Inc()  // NEW
        log.Println("deliver error, re-enqueueing:", err)
        // ... existing re-enqueue unchanged
    }
```

**Verify after cluster B:** build + vet + race tests.

---

## Cluster C — ack / nack path

### C1. `broker/broker.go` `Ack` — success return (line 116)

```go
            c.mu.Unlock()
            q.mu.Unlock()
            q.cond.Signal()
            b.metrics.Acked.WithLabelValues(queueName).Inc()  // NEW
            return nil
```

### C2. `broker/channel.go` `HandleNack` (after locating pending msg, ~line 128)

```go
        if !ok {
            continue
        }

        queue := consumer.queue.name                              // NEW (for labels)
        b := ch.broker                                            // NEW (shorthand)

        b.metrics.Nacked.WithLabelValues(queue,
            strconv.FormatBool(requeue)).Inc()                    // NEW — always

        if requeue {
            // depth restored — collector reflects it; nothing extra
            consumer.queue.mu.Lock()
            consumer.queue.messages = append([]Message{msg}, consumer.queue.messages...)
            consumer.queue.mu.Unlock()
        } else if consumer.queue.dlx != "" {
            b.metrics.DeadLettered.WithLabelValues(queue).Inc()   // NEW
            // existing DLX republish unchanged — it also bumps Publish/Enqueued
            // for the DLX target, which is correct (no double count on original)
            // ...
        } else {
            b.metrics.Dropped.WithLabelValues(queue).Inc()        // NEW — silent data loss path
        }
```

Add `"strconv"` to the import block of `channel.go`.

**Verify after cluster C:** build + vet + race tests.

---

## Cluster D — connections / consumers

### D1. `broker/server.go` `HandleConnection`

```go
    s.mu.Lock()
    s.connections[conn] = struct{}{}
    s.mu.Unlock()
    s.metrics.ConnectionsOpened.Inc()                        // NEW
    defer func() {
        s.metrics.ConnectionsClosed.Inc()                    // NEW
        conn.shutdown()
        // ... existing cleanup unchanged
    }()

    err := conn.RunHandshake()
    if err != nil {
        s.metrics.HandshakeFailures.Inc()                    // NEW
        log.Println(err)
        return
    }
```

### D2. `broker/broker.go` `RegisterConsumer` (after `registerConsumer`, ~line 91)

```go
    q.registerConsumer(c)
    ch.consumers[c.tag] = c
    b.metrics.ConsumersRegistered.WithLabelValues(queue).Inc()   // NEW
    return c.tag, nil
```

### D3. `broker/queue.go` `unregisterConsumer` (after existence check, ~line 50)

```go
    delete(q.consumers, tag)
    q.metrics.ConsumersUnregistered.WithLabelValues(q.name).Inc()  // NEW
```

### D4. `broker/metrics.go` — finish the state collector

Replace the stub from Step 2 with the real `Describe`/`Collect` that snapshots
gauges from §2.2 of the plan:

- `golangmq_queue_messages{queue}` ← `len(q.messages)`
- `golangmq_queue_consumers{queue}` ← `len(q.consumers)`
- `golangmq_consumer_inflight{queue,consumer}` ← `c.inflight`
- `golangmq_queues`, `golangmq_exchanges`, `golangmq_connections`

Respect the lock-order rule: `Server.mu → Broker.mu → Queue.mu`, never hold
`Queue.mu` while calling back into `Broker`.

**Verify after cluster D:** build + vet + race tests.

---

## Files touched (whole step)

| File | Hooks |
|---|---|
| `broker/broker.go` | A1 publish, C1 ack, D2 consumer register |
| `broker/queue.go` | A2 enqueue, B1/B2 deliver, D3 consumer unregister |
| `broker/channel.go` | C2 nack/dead-letter/drop (+`strconv` import) |
| `broker/server.go` | D1 connections/handshake |
| `broker/metrics.go` | D4 gauge collector |

## Done when

- [ ] All 15 counters hooked at their listed locations
- [ ] State collector serves all 6 gauges
- [ ] `go build ./... && go vet ./...` clean after **each** cluster
- [ ] `go test -race ./tests/ -count=1` → **30/30 PASS** after each cluster

## Next

→ [Step 4 — Expose `/metrics`](04-expose-metrics.md)
