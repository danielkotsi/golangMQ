# BENCH-002: Warmup shorter than the workload corrupts accounting and drain

- **Status:** Fixed
- **Severity:** High
- **Component:** `internal/bench/engine.go`, `internal/bench/metrics.go`
- **Failing test:** `internal/bench/harness_bulletproof_test.go::TestFinding_Bench002_WarmupShorterThanWorkloadKeepsAccountingConsistent`

## Reproduction

```bash
go test ./internal/bench \
  -run TestFinding_Bench002_WarmupShorterThanWorkloadKeepsAccountingConsistent \
  -count=1 -v
```

Test configuration: `2 publishers`, `2 consumers`, `msgs=5000`,
`warmup=10ms`, `report=10ms`, `drain-timeout=2s`.

## Expected

A short warmup must produce a clean measured window:

- `Run` returns no error for a healthy acking workload.
- `consumed <= published` in the measured window.
- `consumed + drops == published`.
- `drops == 0` for a clean acking run.
- No worker/connection failures caused by the harness itself.

## Observed

The current implementation fails in more than one way and is timing-dependent.

Latest run:

```text
measured window is not conservative:
published=9838 consumed=9901 drops=0 (consumed+drops=9901)
```

Earlier repetitions of the same probe also produced:

```text
worker failures: connection closed
connection closed
```

and successful-looking runs with inflated consumption, for example:

```text
published=9306 consumed=9535
published=9262 consumed=9343
published=9337 consumed=9424
published=9338 consumed=9678
```

In other iterations 4 of 8 runs ended in worker failures; the remaining successful
runs all had `consumed > published`.

## Root cause

Warmup is implemented as a counter reset, not as a phase boundary.

- Publishers and consumers start before the warmup timer
  (`internal/bench/engine.go`).
- `st.resetAll()` (`internal/bench/metrics.go:279`) zeroes counters and latency
  accumulators but does not clear:
  - messages already in flight at the broker,
  - `sentTimes` for pre-reset messages,
  - the `seen` identity map,
  - consumer prefetch/in-flight state.
- After the reset, deliveries of pre-reset messages are counted as new measured
  consumption, so `consumed` can exceed post-reset `published`.
- Drain compares post-reset `published` with the polluted `consumed`
  (`internal/bench/engine.go:310-330`), so it can stop early.
- The engine then closes clients while consume workers are still trying to ack;
  those workers report `connection closed`, which becomes
  `worker failures: connection closed`.

## Impact

- The measured summary is not a valid measurement of the configured workload.
- Invariant `consumed + drops == published` can be violated.
- Consumption can exceed publication.
- Runs are non-repeatable: the same flags produce different totals.
- The harness can blame workers for failures it caused by closing clients early.

## Suggested fix direction

Make warmup a real phase boundary rather than a counter wipe:

- Start measured workers only after warmup, or quiesce/drain warmup traffic before
  resetting.
- Reset all per-message identity and in-flight tracking together with counters.
- Ensure drain is based on a phase-aware ledger so pre-reset messages cannot count
  as measured consumption.
- Do not close clients while workers still have acks to complete; separate
  cancellation from normal drain.

## Resolution

Fixed in `internal/bench/metrics.go` and `internal/bench/engine.go`. Warmup now
captures a message-sequence cutoff atomically with the counter reset. Messages
allocated before the cutoff stay excluded even when their publish/deliver records
arrive after the boundary. Draining for acking runs also waits for all-time
consumption and acks before clients are closed, so shutdown cannot race an
in-flight Ack into a connection error.
