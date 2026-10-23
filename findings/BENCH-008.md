# BENCH-008: No-ack runs record zero-valued ack-latency samples

- **Status:** Fixed
- **Severity:** Low
- **Component:** `internal/bench/worker.go`, `internal/bench/metrics.go`
- **Failing test:** `internal/bench/harness_bulletproof_test.go::TestFinding_Bench008_NoAckRecordsZeroAckLatencySamples`

## Reproduction

```bash
go test ./internal/bench \
  -run TestFinding_Bench008_NoAckRecordsZeroAckLatencySamples \
  -count=1 -v
```

Test configuration: `1 publisher`, `1 consumer`, `msgs=100`, `-ack=false`,
`drain-timeout=300ms`.

## Expected

With `-ack=false`, no acknowledgements occur, so the ack-latency histogram should
have zero samples. All ack latency values should be zero and the sample count
should be zero. The plan says ack latency should only be recorded when acking
(`docs/BENCHMARK_HARNESS_BULLETPROOF_TESTING_PLAN.md`, latency checks).

## Observed

The summary reports `ack_latency_us.count = 10` while every value is zero:

```text
ack latency count=10 with -ack=false; want 0 samples (values are all zero)
AckLatency{Count:10 Min:0 Avg:0 Max:0 P50:0 P95:0 P99:0}
```

The count equals the prefetch-limited delivery count, not the number of acks.

## Root cause

`consumeWorker.run` always calls `recordAck`, even when acking is disabled:

```go
var ackLatUs float64
if w.ack {
    t0 := time.Now()
    if err := w.ch.Ack(d.DeliveryTag); err != nil {
        return err
    }
    ackLatUs = float64(time.Since(t0).Nanoseconds()) / 1000
}
w.st.recordAck(ackLatUs)
```

(`internal/bench/worker.go:109-117`). `recordAck` adds the value to both latency
sets (`internal/bench/metrics.go:226-229`), so each no-ack delivery contributes a
zero sample.

This was discovered by the differential no-ack test; a dedicated finding test was
added so the differential suite itself can remain a passing reference check.

## Impact

- The summary claims ack samples exist for a run that never acked.
- `Count` consumers/analysts may infer ack activity or plot an ack histogram.
- The value fields are zero, so CSV ack averages may look plausible while the
  sample count is semantically wrong.
- This is a reporting/semantics defect, not a data-path failure.

## Suggested fix direction

- Call `recordAck` only inside the `if w.ack` branch.
- Or add a separate delivery-without-ack path in stats that does not touch ack
  latency.
- Assert `AckLatency.Count == 0` in no-ack tests in addition to checking the
  value fields.

## Resolution

Fixed in `internal/bench/worker.go`. `recordAck` is now called only inside the
`-ack` branch, so no-ack runs report zero ack-latency samples.
