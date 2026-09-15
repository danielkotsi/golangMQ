# Changes on branch `integrationTests`

What has been changed in the current branch so far, relative to the state it
inherited from `main`.

## 1. Production code

### `broker/server.go`
- Split `ListenAndServe` so the in-process listener loop is available as
  `Server.Serve(ln net.Listener) error`. This is requirement **A1** from
  `docs/PHASE1_INTEGRATION_TESTS.md`: it lets tests boot a broker in-process on
  an ephemeral port (`net.Listen("tcp", "127.0.0.1:0")`) and serve it directly.
  No behaviour change for the existing `ListenAndServe` path.

### `go.mod` / `go.sum`
- SDK bumped from `github.com/danielkotsi/golangMQSDK v0.1.0` to **v0.2.0**
  (requirements B1–B3: exported `Client.Close()`, exported
  `ClientChannel.Close()` + `handleChannelCloseOK`, graceful use-after-close).
- No other dependencies added.

## 2. Phase-1 integration suites — `tests/`

New top-level black-box test package exercising the broker over the real TCP
wire protocol. Each test boots its own in-process broker on an ephemeral port
(no state shared between tests). All stdlib `testing`, no new test deps.

| File | Suite (per `docs/PHASE1_INTEGRATION_TESTS.md`) | Status |
|------|-----------------------------------------------|--------|
| `tests/helpers.go` | shared harness: `startTestBroker`, `newTestClient`, `openChannel`, `declareFixture`, `assertEventually`, `collectDeliveries(Timeout)`, `consumeAcked`, `assertNoDelivery`, `publish`, `body`, `assertBodiesMultiset` | ✅ |
| `tests/harness_smoke_test.go` | boot broker, connect, open channel | ✅ builds |
| `tests/publishconsume_test.go` | 3.1 publish/consume round-trips | ✅ passes |
| `tests/routing_test.go` | 3.6 routing-key/binding behaviour | ✅ passes |
| `tests/ack_test.go` | 3.4 ack frees prefetch, unknown-tag no-op, nack requeue, nack dead-letter | ✅ passes |
| `tests/multiproducer_test.go` | 3.2 concurrent producers, no-consumer retention, concurrent goroutine publish | ✅ passes |
| `tests/multiconsumer_test.go` | 3.3 exactly-once / distribution / prefetch | ⚠️ prefetch ✅; exactly-once + distribution **intentionally failing** (blocked by F-002) |
| `tests/channel_test.go` | 3.7 multiple channels on one connection, channel-close requeue | ✅ passes |
| `tests/disconnect_test.go` | 3.5 consumer disconnect redelivery, producer close, redelivery exactness | ✅ passes |
| `tests/finding_f001_test.go` | F-001 regression guard: second consume on same channel | ⚠️ **intentionally failing** by design |

### Deliberately failing regression guards
- `TestFindingF001_SecondConsumeOnSameChannel` — fails on current broker;
  passes once F-001 (Option A or B) is implemented.
- `TestMultiConsumer_ExactlyOnceAcrossWorkers` and
  `TestMultiConsumer_Distribution` — fail on current broker due to the F-002
  data race; pass once the consumer bookkeeping is synchronized.

These three are kept red on purpose (Option C) to guard against regressions and
to document the open broker bugs.

## 3. Docs — `docs/`

- `docs/PHASE1_INTEGRATION_TESTS.md` — the full Phase-1 spec, updated with
  completion/status notes and the verification-status section.
- `docs/TESTING_AND_BENCHMARKING_PLAN.md` — the overall plan this phase
  implements.

## 4. Reports — `reports/`

Reorganized into two directories:

- `reports/findings/F-001.md` — channels silently overwrite consumers
  (one-consumer-per-channel enforced by accident). Status: PARTIAL.
- `reports/findings/F-002.md` — data race: `HandleAck`/`HandleNack` race
  `dispatchLoop` on `inflightTags`/`pendingMessages`/`inflight`. Escalates to
  `fatal error: concurrent map read and map write` under full-suite load.
  Status: OPEN. Blocks suite 3.3 and `go test -race`.
- `reports/findings/F-003.md` — broker never replies `channel.close-ok`, so
  `ClientChannel.Close()` always resolves via its context deadline. Status:
  OPEN (cleanup side works).
- `reports/README.md` — index of both report areas.

## 5. Verification summary

- `go build ./...` ✅
- `go vet ./...` ✅
- `go test ./broker/ ./protocol/` ✅ (no test files)
- `go test ./tests/` — 3 intentional failures (see guards above).
- `go test -race ./tests/` — **cannot pass**: F-002 is a real data race flagged
  by the race detector, and under full-suite concurrency the same race aborts
  the whole test process roughly one run in two.
- `go.mod` unchanged apart from the SDK v0.2.0 bump.

Nothing else on the branch was touched.