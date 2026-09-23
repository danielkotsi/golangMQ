# Step 5 — Verification (smoke)

> Part of [METRICS_PLAN.md](../METRICS_PLAN.md) §5.
> Execution order: [Step 1](01-add-prometheus-client.md) → [Step 2](02-define-metrics.md) → [Step 3.1](03-1-thread-metrics.md) → [Step 3.2](03-2-hook-points.md) → [Step 4](04-expose-metrics.md) → **you are here (last)**

## Goal

Prove end-to-end that real broker traffic produces the expected series and that no
existing behavior regressed.

## 1. Static + correctness

```bash
go build ./... && go vet ./...
go test -race ./tests/ -count=1      # must stay 30/30 PASS
```

## 2. Run broker + generate traffic

```bash
go run ./cmd/broker &                # starts :5672 + :9090/metrics

go run ./exampleClients/publisher    # declares exchange/queue, publishes 3 msgs
go run ./exampleClients/consumer     # consumes + nacks every 3rd (exercises nack/requeue paths)
```

## 3. Scrape and validate

```bash
curl -s localhost:9090/metrics | grep golangmq_
curl -s localhost:9090/metrics | promtool check metrics   # optional; expect zero errors
```

## Expected series after the smoke run (minimum)

| Category | Series |
|---|---|
| Publish | `golangmq_publish_total`, `golangmq_enqueue_total` |
| Deliver | `golangmq_deliver_total` |
| Ack/Nack | `golangmq_ack_total`, `golangmq_nack_total{requeue=...}` |
| Gauges | `golangmq_queue_messages`, `golangmq_queues`, `golangmq_connections` |
| Free collectors | `go_*`, `process_*` |

## Acceptance criteria (Goal 1 complete)

- [ ] `prometheus/client_golang` in `go.mod`; custom **per-server** registry
- [ ] All 15 counters from the plan defined and hooked at their listed locations
- [ ] All 6 gauges served by the snapshot collector (no drift on the 4 re-enqueue paths)
- [ ] `GET /metrics` on `-metrics-addr` returns Prometheus exposition format
- [ ] `go test -race ./tests/` still **30/30 PASS**
- [ ] No behavior/protocol changes — increments only
- [ ] K8s manifests expose/scrape the metrics port

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `nil` pointer in a hook | [Step 3.1](03-1-thread-metrics.md) wiring incomplete — a component holds no `*Metrics` |
| Metrics from several test brokers mixed together | global default registry used instead of per-server `NewMetrics()` |
| Gauge value drifts after nack/disconnect requeues | depth maintained with Inc/Dec instead of the snapshot collector |
| `:9090` connection refused in tests | tests must not start the HTTP server — only `cmd/broker/main.go` does |
