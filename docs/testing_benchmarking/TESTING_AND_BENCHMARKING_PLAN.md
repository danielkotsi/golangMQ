# GolangMQ — Testing & Benchmarking Plan

## Goal

Build a full testing and benchmarking suite for GolangMQ, progressing in phases from
correctness (integration tests) through to a reusable performance harness, load
scenarios, and finally regression testing.

The recommended progression is the industry-standard one: **establish behavioral
correctness first**, then measure, then lock the invariants down against future
regressions.

## Repo context (current state)

- **Zero tests and zero benchmarks exist** today — a clean slate.
- The broker is a custom JSON-over-TCP broker (`GOMQ/1` protocol) in `/broker`,
  built only on the Go standard library.
- Server code is **cleanly constructible**: `broker.NewServer(addr, cfg)` exists, so
  tests can boot an in-process broker on an ephemeral port (`127.0.0.1:0`) with no
  Docker/K8s dependency.
- Clients talk to it over the real TCP protocol via the external
  `github.com/danielkotsi/golangMQSDK`.
- Known latent issues (expected to surface during load testing — do NOT preemptively
  fix as part of the test plan):
  - `uint16` DeliveryTag has no wraparound handling (>65535 msgs).
  - Consumer selection is non-deterministic (Go map iteration in
    `queue.go:selectConsumer`).
  - Heartbeat is negotiated in the handshake but never implemented.

---

## Phase 1 — Test Infrastructure & Integration Tests (correctness)

Decisions: separate `tests/` directory, stdlib `testing` + table-driven, black-box
only (over the wire).

### 1a. Shared harness — `tests/helpers.go`

- `startTestBroker(t)` — boot `broker.NewServer` on `127.0.0.1:0`, run the accept
  loop in a goroutine, register `t.Cleanup` for shutdown.
- `newTestClient(t, addr)` — wraps `gomqSDK.Connect` + `OpenChannel` with sane
  timeouts; registers `t.Cleanup` to close.
- `assertEventually(t, timeout, desc, cond)` — poll for async delivery conditions
  instead of flaky sleeps; fails with `desc` on timeout.
- `declareFixture(t, ch)` — declare exchange + queue + binding with unique names per
  test.

Each integration test boots its own broker instance for isolation.

### 1b. Test suites

Each suite lives in its own file under `tests/` and is table-driven.

| File | Cases |
|------|-------|
| `publishconsume_test.go` | basic round-trip (publish 1, consume 1, body integrity); multiple messages (count, order, body fidelity); publish to unbound routing key |
| `multiproducer_test.go` | N concurrent publishers → expected total consumed; no loss/corruption (collect set of bodies, assert exact match) |
| `multiconsumer_test.go` | 1 queue, N consumers → all messages delivered exactly once; prefetch respected per-consumer; round-robin distribution across workers |
| `ack_test.go` | ack positive frees dispatch slot (deliver beyond prefetch after ack); ack with unknown tag is no-op; nack+requeue (redelivered); nack+dead-letter (DLX route asserted) |
| `disconnect_test.go` | consumer receives unacked msgs then closes connection → reconnect, verify msgs redelivered; producer disconnects cleanly → no corruption, remaining consumers unaffected |
| `routing_test.go` | exact routing-key match; publish to non-bound key dropped |
| `channel_test.go` | multiple channels over one connection, independent queues/consumers; channel close cleanup |

### Verification

- `go build ./...`
- `go test ./...`
- `go vet ./...`
- `go test -race ./tests/`

---

## Phase 2 — Performance Harness (reusable CLI)

A new `cmd/benchmark/` using stdlib `flag`.

### Flags

- `-addr` — broker address
- `-publishers int` — number of concurrent publishers
- `-consumers int` — number of concurrent consumers
- `-msgs int` — messages per publisher
- `-size int` — payload size in bytes
- `-prefetch int` — consumer prefetch
- `-ack bool` — enable acks
- `-duration time.Duration` — run cap
- `-report time.Duration` — reporting interval
- `-warmup` — warmup period before measuring

### Flow

1. Declare an isolated exchange/queue/binding.
2. Spawn N publisher goroutines + M consumer goroutines via the SDK.
3. Consumers count and optionally ack.
4. Controller samples per-interval throughput.

### Output

- Aggregated: msgs/sec, total count, avg/min/max latency, drops.
- Per-interval series written to CSV for later plotting/regression.

---

## Phase 3 — Load Scenarios

A `cmd/benchmark` subcommand (or scenario config) driving the Phase 2 engine.

### Shapes

- 1:1
- Fanout (1 publisher : N consumers)
- N:1
- N:M

### Stress axes

- High publish rate
- Large payloads (KB/MB)
- Ack-pressure saturation (low prefetch)
- Connect/disconnect churn
- Sustained run to expose the `uint16` delivery-tag wraparound and the missing
  heartbeat

### Goal

Characterize throughput/latency curves and record a **baseline snapshot** of
numbers. This is the "performance harness taking arguments." The snapshot becomes
the reference for Phase 4.

---

## Phase 4 — Regression Testing

### Baseline capture

Record the Phase 3 scenario outputs (msgs/sec, latency percentiles) as committed
golden files / CSV.

### Regression gates (two complementary layers)

1. **`testing.Benchmark` functions** on hot paths (publish, dispatch, ack) for
   micro-level drift detection.
2. **Scenario runner** that re-runs Phase 3 scenarios in CI and fails when
   throughput drops or latency crosses a threshold (e.g., >10–15%) vs baseline.

### Correctness regression

Phase 1 integration tests run on every change as the safety net.

### CI wiring (optional)

- `make test`
- `make bench`
- `make regression`
- GitHub Actions workflow running the above.

---

## Suggested execution order

1. Build the Phase 1 harness (`tests/helpers.go`) and confirm the ephemeral-addr
   exposure in the broker (may need to expose the bound listener address).
2. Add the Phase 1 integration suites; get `go build ./...` + `go test -race ./...`
   green.
3. Build the Phase 2 harness.
4. Add Phase 3 load scenarios.
5. Add Phase 4 regression gates.
