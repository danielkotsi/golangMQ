# Phase 1 — Integration Test Spec

This is the detailed, actionable specification for **Phase 1** of the testing plan
(see `docs/TESTING_AND_BENCHMARKING_PLAN.md`). Phase 1 covers the shared test
harness and the correctness-focused integration test suites, all running **black-box
over the real TCP wire protocol** against an in-process broker.

## 0. Decisions (from planning)

- **Location:** a new top-level `tests/` package (true end-to-end, not in the
  `broker` package).
- **Framework:** stdlib `testing` only, table-driven, with hand-written helpers.
  No new external test dependencies (keeps `go.mod` minimal).
- **Scope:** black-box only — no access to unexported broker internals; everything
  is asserted through observable client behavior (deliveries, acks, redelivery).
- **Isolation:** each test boots its **own broker instance** on an ephemeral port,
  so broker state (queues/exchanges) never leaks between tests.

## 1. Requirements (prerequisites — all complete)

All production prerequisites for Phase 1 are implemented and wired:

| ID | Repo | Requirement | Status |
|----|------|-------------|--------|
| A1 | Broker | Expose bound listener for ephemeral-port serving (`Server.Serve(ln)`) | ✅ Done |
| B1 | SDK | Exported `Client.Close()` | ✅ Done (SDK v0.2.0) |
| B2 | SDK | Exported `ClientChannel.Close()` + `handleChannelCloseOK` | ✅ Done (SDK v0.2.0) |
| B3 | SDK | Graceful use-after-close (unblock pending requests) | ✅ Done (SDK v0.2.0) |

**SDK wiring:** `go.mod` requires `github.com/danielkotsi/golangMQSDK v0.2.0`.
`go build ./...` and `go vet ./...` pass. No `replace` directive needed.

## 2. Shared harness — `tests/helpers.go`

Package-scoped helpers used by every suite.

### `startTestBroker(t) string`
- Boots a single in-process broker using the listener-exposure change (A1) on an
  ephemeral port.
- Registers `t.Cleanup` to shut it down.
- Returns the `host:port` the test connects to.
- Uses a per-test unique broker so state is isolated.
- `ServerConfig`: reuse defaults (ChannelMax 10, FramesMax 10372, HeartbeatSec 10).

### `newTestClient(t, addr) *testClient`
- Wraps `gomqSDK.Connect(addr, Config{...})` with a short connect timeout.
- Registers `t.Cleanup` to call `Close()` (requires B1).
- Returns a small struct capturing `*gomqSDK.Client` plus:
  - `openChannel(ctx)` — wraps `OpenChannel`.
- Config fields are arbitrary (broker does not enforce auth today); use constant
  values like the examples (`Username: "daniel"`, etc.).

### `declareFixture(t, ch, suffix) (exchange, queue string)`
- Declares a uniquely-named exchange, queue, and binding (suffix per test to avoid
  name collisions if any state is accidentally shared).
- Returns the names so tests can publish/consume with them.

### `assertEventually(t, timeout, desc string, cond func() bool)`
- Polls `cond` until it returns true or `timeout` elapses (5s default, 50ms
  interval).
- `t.Fatalf(desc)` on timeout — no raw `time.Sleep` in tests.

### `collectDeliveries(t, ch, n) []protocol.Deliver`
- Reads `n` deliveries from `ch.Incoming` with an overall deadline.
- Returns them so tests can assert count/order/body.
- A lower-level variant reads until a predicate (for disconnect/redelivery cases).

### `randBody(i)` / payload helpers
- Deterministic payload content for body-integrity assertions (e.g.
  `fmt.Sprintf("msg-%d", i)` marshalled as JSON bytes, matching example style).

## 3. Test suites (`tests/*_test.go`)

Each suite is table-driven where the table varies scenario parameters (payloads,
counts, concurrency). All assertions use stdlib helpers + the harness.

### 3.1 `publishconsume_test.go` ✅
- **Basic publish/consume round-trip:** publish 1 message, consume 1, assert body
  bytes match exactly and delivered `Exchange`/`RoutingKey` match what was published.
- **Multiple messages:** publish N messages to one routing key, consume N, assert
  exact count and the **set** of received bodies equals the published set (order is
  not guaranteed due to map-iteration dispatch; assert as a multiset).
- **Publish to unbound routing key:** publish to a key with no binding; assert no
  delivery within timeout.
- **Publish to non-existent exchange:** assert the SDK surfaces an error and no
  delivery occurs.

### 3.2 `multiproducer_test.go` ✅
- **N concurrent producers, one consumer:** N publisher goroutines each publish a
  distinct set of K messages to one queue; one consumer drains. Assert total = N×K
  and the received multiset equals the union of all published sets (no loss, no
  corruption).
- **N concurrent producers, no consumer:** publish N×K with no consumer registered,
  then attach a consumer and drain — verifies queued messages are retained and
  delivered later.

### 3.3 `multiconsumer_test.go` ⚠️ (partially blocked by F-002)
- **1 queue, M consumers:** M consumers on the same queue; publish N messages.
  Assert every message is delivered **exactly once** across all consumers (union of
  received sets equals published multiset; no duplicates).
- **Prefetch respected:** with broker default prefetch (10, a broker-side constant,
  not client-settable via SDK today), publish more than prefetch messages to a single
  consumer **without acking**; assert the consumer receives at most `prefetch`
  messages until acks are sent. Validates dispatch stalls at the prefetch ceiling.
- **Distribution across workers:** multiple consumers with acks; assert messages are
  spread across consumers (at least >1 consumer receives messages), documenting the
  non-deterministic-but-distributed behavior.

> Status note: `TestMultiConsumer_PrefetchRespected` passes. The
> exactly-once and distribution tests fail because `dispatchLoop` and
> `HandleAck`/`HandleNack` race on `inflightTags`/`pendingMessages` with no
> shared lock — tracked as **F-002** in `reports/findings/F-002.md`. These two tests act
> as intentional-failing regression guards (Option C) until the broker is fixed.

### 3.4 `ack_test.go` ✅
- **Ack frees prefetch slot:** single consumer hits the prefetch ceiling (messages
  held unacked); publish more; assert no further delivery until an ack is sent, then
  assert the next message arrives.
- **Ack with unknown tag is no-op:** ack a delivery tag never issued; assert no panic
  and no corruption (a subsequent valid delivery/ack still works).
- **Nack + requeue:** consumer nacks with `requeue=true`; assert the same body is
  redelivered after re-dispatch.
- **Nack + dead-letter:** set up a queue with a DLX + DLQ (as in the example
  publisher). Nack with `requeue=false`; assert the message is delivered on the
  dead-letter queue instead.

### 3.5 `disconnect_test.go` ✅  ← needs SDK `Client.Close()` (B1)
- **Consumer disconnect with unacked messages:** consumer receives messages and does
  **not** ack them, then calls `Close()` (B1). Reconnect a new consumer on the same
  queue; assert the previously-unacked bodies are redelivered.
- **Producer disconnect:** a producer publishes a batch then calls `Close()`. Assert
  already-published messages are still delivered/consumed correctly (no corruption,
  no partial frames), and a second producer continues to work.
- **Consumer reconnect / redelivery exactness:** assert the redelivered set equals the
  unacked set (no loss); messages already acked before disconnect are **not**
  redelivered.

> Status note: all three pass. On connection teardown the broker's
> `Connection.cleanup()` → `Channel.cleanup()` → `Queue.unregisterConsumer()`
> re-enqueues the consumer's `pendingMessages` with their original delivery
> tags, which is exactly what these tests assert.

### 3.6 `routing_test.go` ✅
- **Exact routing-key match:** publish to bound key → delivered.
- **Non-matching key on same exchange:** publish to a different key with no binding →
  dropped (no delivery).
- **Multiple bindings, same queue:** queue bound to two keys on one exchange (as the
  example does with `email.sent`/`email.something`); publish to each key → all
  delivered to the same queue.

### 3.7 `channel_test.go` ✅  ← needs SDK `ClientChannel.Close()` (B2)
- **Multiple channels over one connection:** open >1 channel on a single `Client`;
  each channel opens its own queue/consumer; assert independent delivery per channel.
- **Channel close cleanup:** open a channel, register a consumer, close the channel;
  assert its consumer is unregistered and its unacked pending messages are
  re-enqueued (observable by a different channel consuming them).

> Status note: both pass. The close-cleanup test verifies the requeue (which the
> broker does perform), but `ClientChannel.Close()` only returns via its context
> deadline because the broker never replies `channel.close-ok` — tracked as
> **F-003** in `reports/findings/F-003.md`.

## 4. Verification (definition of done for Phase 1)

From the repo root:

```
go build ./...
go vet ./...
go test ./...            # all packages
go test -race ./tests/   # race detector over the integration suites
```

- All tests must pass repeatedly (run each suite a few times to catch flakiness from
  async timing / map-iteration order).
- No new external dependencies added to `go.mod`.
- The only production change in the broker repo is A1 (listener exposure); the SDK
  changes (B1–B3) live in the SDK repo.

**Verification status (as-implemented, blocked by findings):**

- `go build ./...` ✅ and `go vet ./...` ✅ pass.
- `go test ./...` passes for the broker package; `go test ./tests/` is **not**
  green by design: `TestFindingF001_SecondConsumeOnSameChannel` (F-001) and the two
  multi-consumer tests (F-002) fail intentionally as regression guards.
- `go test -race ./tests/` **cannot pass** — the F-002 data race is flagged
  deterministically.
- Full-suite stability is compromised by F-002: under concurrent load the race
  escalates to `fatal error: concurrent map read and map write`, aborting the whole
  test process roughly one run in two. Even the "passing" suites (publish/consume,
  ack, channel, disconnect, multi-producer) may be killed by it. This blocks the
  "all tests pass repeatedly" DOD until the broker is fixed (see `reports/findings/F-002.md`).

Isolated suites run cleanly: e.g. `go test ./tests/ -run 'Test(Ack|Nack|Channel|Disconnect)'`
passes 8/8 without crashing.

## 5. Out of scope (deferred to later phases)

- Performance harness (`cmd/benchmark`) — Phase 2.
- Load scenarios and baselines — Phase 3.
- Regression gates + CI — Phase 4.
- Fixing latent broker issues (delivery-tag wraparound, non-deterministic selection,
  missing heartbeat) — logged, not fixed here.

## 6. Execution order

1. ~~Implement missing requirements (A1, B1–B3)~~ — **done** (SDK v0.2.0 wired).
2. ~~Build the harness `tests/helpers.go`~~ — **done** (all helpers in place).
3. ~~Add suites in order: publish/consume → routing → ack → multi-producer →
   multi-consumer → channel → disconnect~~ — **done** (all 7 suites written;
   3 suites carry intentional-failing guards blocked by F-001/F-002).
4. ~~Run the verification commands in §4~~ — **done** (see §4 status; `-race` and
   full-suite stability blocked by F-002).
5. Confirm `go.mod` still has no new dependencies — **done**.
