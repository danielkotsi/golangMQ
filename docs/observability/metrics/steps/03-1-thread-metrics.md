# Step 3.1 — Thread `*Metrics` through construction

> Part of [METRICS_PLAN.md](../METRICS_PLAN.md) §3.1.
> Execution order: [Step 1](01-add-prometheus-client.md) → [Step 2](02-define-metrics.md) → **you are here** → [Step 3.2](03-2-hook-points.md) → [Step 4](04-expose-metrics.md) → [Step 5](05-verification.md)

## Goal

Give every component that will be instrumented access to the same `*Metrics` instance,
without changing any external signatures and **without adding a single hook yet**.
After this step everything compiles, all 30 tests pass, and Step 3.2 can go
hook-by-hook.

## Edits

| File | Change |
|---|---|
| `broker/server.go` | `Server` gains field `metrics *Metrics`; `NewServer` calls `NewMetrics()`; registers the state collector (collector gets a back-reference to `s`) |
| `broker/broker.go` | `Broker` gains field `metrics *Metrics`; `NewBroker()` takes `m *Metrics` (internal callers only) and stores it; set from `NewServer` |
| `broker/queue.go` | `Queue` gains field `metrics *Metrics`; populated from the broker in `Broker.DeclareQueue` when it constructs `NewQueue` — `NewQueue` itself can read it off the broker it is given, or take it as a param (internal only) |
| `broker/channel.go` | **no field** — uses existing `ch.broker.metrics` |
| `broker/connection.go` | **no field** — connection already holds `*Server` (`s.metrics`) |

### Construction chain after the edit

```
NewServer(addr, cfg)
  ├─ s.metrics = NewMetrics()          // registry + counters + collectors
  ├─ s.Broker  = NewBroker(s.metrics)  // broker holds the same *Metrics
  └─ (collector back-ref = s)

DeclareQueue(name, ...)
  └─ NewQueue(..., b.metrics)          // queue holds the same *Metrics
```

### Hard constraint

`tests/helpers.go` calls `broker.NewServer(addr, cfg)` — **that signature must not
change**. Only internal constructors (`NewBroker`, `NewQueue`) may gain parameters.
This keeps all 30 existing tests untouched.

## Sanity check during this step

Every `metrics` field read should be non-nil once `NewServer` has run. If any test
constructs `Broker`/`Queue` directly (none do today — they all go through
`NewServer`), guard with a nil-check or route them through `NewMetrics()` too.

## Files touched

| File | Change |
|---|---|
| `broker/server.go` | field + `NewServer` wiring |
| `broker/broker.go` | field + `NewBroker` signature |
| `broker/queue.go` | field + `NewQueue`/`DeclareQueue` wiring |

## Done when

- [ ] `go build ./...` passes
- [ ] `go vet ./...` clean
- [ ] `go test -race ./tests/ -count=1` → **30/30 PASS**
- [ ] Zero hook calls added (diff should be wiring only)

## Next

→ [Step 3.2 — Hook points, one cluster at a time](03-2-hook-points.md)
