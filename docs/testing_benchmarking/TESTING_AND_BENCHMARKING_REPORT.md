# GolangMQ — Testing & Benchmarking Report

Single overall report for everything that happened on the `dkotsi/integrationTests`
branch, ready to be merged to `main`. Companion to
`docs/TESTING_AND_BENCHMARKING_PLAN.md` (the plan this work implements) — Phase 1
of that plan is complete.

## 1. What this branch did

Phase 1 of the testing plan: **correctness-first integration testing** of the
GolangMQ broker as a **black box over the real TCP wire protocol**, plus the
broker/protocol fixes the testing uncovered. Starting state as described in the
plan — zero tests existed; the broker (custom JSON-over-TCP, `GOMQ/1`) was a
clean slate.

Five issues were found, documented, fixed, and locked in with regression
guards (F-001 … F-004.1). Final state on this branch:

- **Production code** (`broker/`, `protocol/`) — 4 targeted fixes (see §3).
- **Test suite** (`tests/`) — a shared harness + 7 integration suites + finding
  regression guards.
- **SDK dependency** — bumped to `github.com/danielkotsi/golangMQSDK v0.3.2`.
- **Docs** — kept: the plan and this report. The per-finding detail files
  (`reports/findings/*`, `reports/changes/*`) and the old
  `docs/PHASE1_INTEGRATION_TESTS.md` were consolidated into this single report.

## 2. The shared test harness — `tests/helpers.go`

Black-box only: no access to unexported broker internals; every assertion goes
through observable client behaviour (deliveries, acks, redelivery). Each test
boots its **own in-process broker** on an ephemeral port so no state leaks
between tests. Pure stdlib `testing` — no new external dependencies.

| Helper | Purpose |
|--------|---------|
| `startTestBroker(t)` | Boot `broker.NewServer` on `127.0.0.1:0`, serve in a goroutine, `t.Cleanup` shutdown; returns `host:port`. Uses requirement A1 (`Server.Serve(ln)`). |
| `newTestClient(t, addr)` | Wraps `gomqSDK.Connect` + `openChannel` with sane timeouts; `t.Cleanup` closes. |
| `declareFixture(t, ch, suffix)` | Unique exchange/queue/binding names per test. |
| `assertEventually(t, timeout, desc, cond)` | Poll `cond` (50 ms) until true or fatal timeout — no raw sleeps. |
| `consumeOnChannel(t, ch, queue)` | `ch.Consume(...)` returning the **per-consumer** `<-chan protocol.Deliver`, failing the test on error. |
| `collectDeliveries(T)(t, deliveries, n)` | Read exactly `n` deliveries from the per-consumer chan under a deadline. |
| `consumeAcked(t, ch, deliveries, n)` | Drain `n` deliveries, ack each one, return bodies. |
| `assertNoDelivery(t, deliveries, wait, desc)` | Assert nothing arrives within `wait`. |
| `mergeWorkerDeliveries(workers...)` | Fan M consumer streams into one merged stream tagged by source (for multi-consumer drain). |
| `assertBodiesMultiset(t, got, want)` | Order-independent body comparison. |
| `body(i)`, `publish(t, ...)` | Deterministic payload (`msg-N`) + publish helper. |

## 3. Test suites — `tests/*_test.go`

All seven Phase-1 suites, plus regression guards. Each is table-driven and uses
the harness.

| File / suite | Cases | Status |
|--------------|-------|--------|
| `harness_smoke_test.go` | boot broker, connect, open channel | ✅ |
| `publishconsume_test.go` | round-trip (body/exchange/routing-key match); N messages as multiset; publish to unbound key dropped; unknown exchange errors | ✅ |
| `routing_test.go` | exact routing-key match; non-matching key dropped; multiple bindings → same queue | ✅ |
| `ack_test.go` | ack frees prefetch slot; ack unknown tag is no-op; nack+requeue redelivered; nack → dead-letter queue | ✅ |
| `multiproducer_test.go` | N concurrent producers → exact total, multiset equality; queued-without-consumer then attach; concurrent goroutine publish; unknown-exchange concurrency | ✅ |
| `multiconsumer_test.go` | 1 queue / M consumers → exactly-once across workers; prefetch ceiling respected; distribution across workers | ✅ |
| `channel_test.go` | multiple channels over one connection; channel-close requeues unacked msgs | ✅ |
| `disconnect_test.go` | consumer disconnect → unacked redelivered; producer close → already-published still delivered; redelivery exactness (acked msgs not redelivered) | ✅ |
| `finding_f001_test.go` | F-001 guard: second consume on same channel is accepted and both work (Option B) | ✅ |
| `finding_f004_test.go` | 5 F-004 guards: distinct chans; per-consumer routing; stable/distinct ConsumerTag; nack redelivers only on same consumer; close closes all chans | ✅ |

## 4. Findings found and addressed (F-001 … F-004.1)

The testing plan explicitly expected a few latent issues to surface; instead the
integration suite surfaced **five concrete defects**, all of which were then fixed
and regression-guarded.

### F-001 — Channels silently overwrite consumers

- **Found:** registering a second consumer on an already-consuming channel
  silently dropped the first. The channel stored consumers in a map keyed by the
  client-sent tag, which was empty (`""`) for every SDK consumer; the
  auto-generated tag was never written back, so the second `Consume` overwrote
  the first under the empty key. Result: silent data loss (e.g. dead-letter
  consumer never saw its messages), misrouted acks/nacks.
- **Fix (Option B — support multiple consumers per channel):** `RegisterConsumer`
  now keys `ch.consumers` by the consumer's **actual** tag and returns
  `(string, error)`; duplicate explicit tags are rejected; `HandleConsume`
  propagates registration errors and echoes the resolved tag in `basic.consume-ok`.
- **Guard:** `TestFindingF001_SecondConsumeOnSameChannel` — a second consume is
  accepted and a `nack(requeue=true)` on the first consumer's delivery is handled
  and redelivered end to end.

### F-002 — Data race in consumer bookkeeping

- **Found:** `dispatchLoop` (goroutine 1) and `HandleAck`/`HandleNack`
  (connection goroutine) mutated the same `Consumer` maps (`inflight`,
  `inflightTags`, `pendingMessages`) with no shared lock. `go test -race`
  flagged a genuine `DATA RACE`; under full-suite concurrency it escalated to
  `fatal error: concurrent map read and map write`, aborting the whole test
  process ~1 run in 2. A lost ack pinned a consumer at the prefetch ceiling
  forever → delivery stalled and the multi-consumer suite could never pass.
- **Fix (recommended Option B — per-consumer mutex):** each `Consumer` owns a
  `sync.Mutex` guarding its bookkeeping; every touch-point locks it, with a
  documented lock order (`q.mu → c.mu`; ack/nack never hold `c.mu` while
  acquiring `q.mu`) that rules out deadlock. A separate harness fix
  (`mergeWorkerDeliveries`) repaired the multi-consumer drain loop that was
  stalling independently of the race.
- **Guard:** the multi-consumer exactly-once and distribution suites now pass
  repeatedly under `-race`.

### F-003 — Broker never replies `channel.close-ok`

- **Found:** the broker processed `channel.close` teardown but never replied
  `channel.close-ok`, so `ClientChannel.Close()` always returned via its context
  deadline (a protocol violation on every channel close), and the SDK's
  `handleChannelCloseOK` (which removes the channel and closes `Incoming`) never
  ran, leaving stale client-side state.
- **Fix:** `Connection.ChannelClose` now writes `channel.close-ok` after
  `ch.cleanup()`; added the missing `protocol.ChannelCloseOK` struct. No SDK
  change was needed.
- **Guard:** `TestChannel_CloseRequeuesUnacked` now asserts `Close()` resolves
  nil on the response path (previously tolerated `ctx.Err()`).

### F-004 — Multiple consumers per channel not usable end to end

- **Found:** F-001 fixed the bookkeeping, but the feature still didn't work end
  to end — the wire discarded the consumer's identity. `protocol.Deliver` had no
  `consumer_tag`; the SDK's `Consume` always returned the channel's single shared
  `Incoming`; its `route` funneled every `basic.deliver` into that one channel.
  Two legitimately-registered consumers received a merged, indistinguishable
  stream.
- **Fix (broker + SDK):** broker — added `ConsumerTag` (`omitempty`) to
  `protocol.Deliver` and `dispatchLoop` stamps the selected consumer's tag. SDK
  (`v0.3.2`) — `Consume` sends a client-authoritative tag and returns a
  **per-consumer channel**, `route` forwards by tag (legacy `Incoming` fallback),
  close cleans up all per-consumer chans. Works when both sides are new; any
  mixed-version mixture degrades gracefully to the old shared-`Incoming`
  behaviour.
- **Guards:** 5 new tests (`tests/finding_f004_test.go`) — distinct channels,
  per-consumer routing, stable/distinct tags, nack-redelivery locality, and
  channel-close cleanup.

### F-004.1 — Pre-queued messages silently fall back to `Incoming`

- **Found (during the F-004 PR):** the first SDK cut registered the per-consumer
  entry only on `basic.consume-ok`. Because the broker's `dispatchLoop` runs on a
  separate goroutine, `basic.deliver` can arrive **before** `basic.consume-ok`;
  the entry didn't exist yet, so pre-queued messages silently fell back to the
  unread `Incoming` chan. `TestMultiProducer_NoConsumerThenAttach` flaked ~1 in 30
  under `-race`.
- **Fix (SDK v0.3.2):** `Consume` now **pre-registers** the per-consumer chan
  under the client-minted tag *before* sending `basic.consume`, cleaning up on
  every failure path. Also fixed a send-on-closed-channel panic race in
  `Client.Close` (readLoop is stopped before delivery chans close). Broker was
  correct — no broker change.
- **Guard:** the previously-flaky pre-queued-delivery suites run clean
  `-count=10` under `-race`.

## 5. What changed (production + dependency)

| File | Change |
|------|--------|
| `broker/server.go` | A1: exposed `Server.Serve(ln)` for ephemeral-port in-process serving (no behaviour change to `ListenAndServe`). |
| `broker/broker.go` | F-001: `RegisterConsumer` keys by actual tag, returns `(string, error)`, rejects duplicate tags. |
| `broker/channel.go` | F-001: `HandleConsume` propagates errors + echoes resolved tag; F-002: `HandleAck`/`HandleNack` lock `consumer.mu`. |
| `broker/consumer.go` | F-002: added per-consumer `sync.Mutex`. |
| `broker/queue.go` | F-002: `selectConsumer`, `dispatchLoop`, `unregisterConsumer` lock `consumer.mu`; F-004: `dispatchLoop` stamps `ConsumerTag`. |
| `broker/connection.go` | F-003: `ChannelClose` replies `channel.close-ok`. |
| `protocol/events.go` | F-003: `ChannelCloseOK`; F-004: `Deliver.ConsumerTag`. |
| `go.mod` / `go.sum` | SDK `v0.1.0 → v0.2.0 → v0.3.2` (B1–B3 + F-004/F-004.1). No other dependencies. |

## 6. Verification

From the repo root, on the final branch state against the **published**
SDK v0.3.2 (no `replace`):

- `go build ./...` ✅
- `go vet ./...` ✅
- `go test ./...` ✅ (broker/protocol packages compile; `tests/` green)
- `go test ./tests/` — **30/30 PASS**, 0 fail, 0 skip ✅
- `go test -race ./tests/ -count=10` — **300/300 PASS**, 0 race, 0 panic ✅

All DOD criteria from the Phase-1 plan (§4 of the plan) pass, including the
"tests pass repeatedly" and race-detector requirements. The three
intentional-failure guards that existed mid-branch (F-001, F-002) are now green
regression guards.

## 7. Out of scope (deferred — later phases of the plan)

- **Phase 2** — reusable performance harness (`cmd/benchmark/`, stdlib `flag`).
- **Phase 3** — load scenarios and baseline snapshot (incl. sustained runs to
  expose the `uint16` delivery-tag wraparound and the unimplemented heartbeat —
  latent issues deliberately not preemptively fixed).
- **Phase 4** — regression gates + CI (`make test/bench/regression`, GitHub
  Actions).
- **Non-deterministic consumer selection** (Go map-iteration order in
  `selectConsumer`) — latent, deferred.
- **Delivery-tag ambiguity across queues on one channel** and the unused
  broker-side `Consumer.ch` field — logged, deferred.