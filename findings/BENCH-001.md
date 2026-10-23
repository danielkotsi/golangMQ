# BENCH-001: Warmup longer than the workload silently returns an all-zero success

- **Status:** Open
- **Severity:** High
- **Component:** `internal/bench/engine.go`
- **Failing test:** `internal/bench/harness_bulletproof_test.go::TestFinding_Bench001_WarmupLongerThanWorkloadIsNotSilentlyZero`

## Reproduction

```bash
go test ./internal/bench \
  -run TestFinding_Bench001_WarmupLongerThanWorkloadIsNotSilentlyZero \
  -count=1 -v
```

Test configuration: `1 publisher`, `1 consumer`, `msgs=10`, `warmup=250ms`,
`report=25ms`, `drain-timeout=2s`.

## Expected

For a positive workload, `Run` must not return success with an all-zero summary.
If the workload completes before the warmup boundary, the harness must either:

1. return a clear error explaining that no measured work remains, or
2. measure the completed workload correctly (without silently discarding it).

## Observed

`Run` returned `err=nil` with a non-nil result whose counters were all zero:

```text
warmup=250ms exceeded the workload but Run returned success with an all-zero summary
Published:0 Delivered:0 Consumed:0 Duplicates:0 Drops:0 ElapsedMs:0
```

The CSV/output path was still produced, so callers receive a valid-looking
successful run with no data.

## Root cause

`Run` starts publishers and consumers, waits for `-warmup`, then unconditionally
calls `st.resetAll()` (`internal/bench/engine.go:234-247`). There is no check that
publishers are still running or that any workload remains after the reset. If the
fixed `publishers x msgs` workload finishes inside the warmup window, the reset
discards every counter and the measured phase observes no new traffic.

Related warmup implementation:

- `internal/bench/metrics.go:277-307` — `resetAll` zeroes counters and latency sets.
- `internal/bench/engine.go:310-330` — drain uses the post-reset
  `st.published.Load()`; zero published skips drain and succeeds.

## Impact

- A benchmark can report zero throughput as a successful result.
- CI or comparison tooling can accept the output as a valid measurement.
- The failure is not visible in the JSON summary or exit status.
- Short smoke workloads and large warmups are especially likely to trigger it.

## Suggested fix direction

Define and enforce the warmup contract at the boundary:

- Detect that all publishers completed before the warmup timer fires and return a
  descriptive error, or redesign the run so the measured phase performs work
  after the reset.
- Do not treat zero published messages from a positive configured workload as a
  successful clean run.
- Consider a phase barrier that starts measurement only after all pre-measurement
  activity is quiescent.

The failing test accepts either a clear error or a correct nonzero measurement;
it rejects only the silent all-zero success.
