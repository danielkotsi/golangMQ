# Step 1 — Add the Prometheus Go client

> Part of [METRICS_PLAN.md](../METRICS_PLAN.md) §1.
> Execution order: **you are here** → [Step 2](02-define-metrics.md) → [Step 3.1](03-1-thread-metrics.md) → [Step 3.2](03-2-hook-points.md) → [Step 4](04-expose-metrics.md) → [Step 5](05-verification.md)

## Goal

Add `github.com/prometheus/client_golang` as the first non-SDK dependency and fix the
registry strategy before any code is written.

## Commands

```bash
go get github.com/prometheus/client_golang@latest
go mod tidy
```

## Decisions locked in this step

1. **Custom per-server registry, not the global default registry.**
   Each `broker.Server` will own its own `*prometheus.Registry`.
   Rationale: `tests/helpers.go: startTestBroker` boots many brokers in one process;
   a shared default registry would aggregate their metrics and make future metric
   assertions flaky. It also lets `Server` own its own HTTP handler later.

2. **Register the free collectors alongside ours** (inside the registry, Step 2):
   - `collectors.NewGoCollector()` — goroutines, heap, GC
   - `collectors.NewProcessCollector(...)` — RSS, CPU, open FDs

3. **Naming/label policy** (applies from Step 2 on):
   - Prefix every metric with `golangmq_`
   - Label cardinality bounded only by declared queues/exchanges and live consumers —
     **never** label by routing key, body, or connection

## Files touched

| File | Change |
|---|---|
| `go.mod` | `require github.com/prometheus/client_golang v1.x` |
| `go.sum` | new hashes |

No `.go` source changes in this step.

## Done when

- [ ] `github.com/prometheus/client_golang` listed in `go.mod`
- [ ] `go build ./...` passes
- [ ] `go test ./tests/` still 30/30 PASS

## Next

→ [Step 2 — Define metrics](02-define-metrics.md)
