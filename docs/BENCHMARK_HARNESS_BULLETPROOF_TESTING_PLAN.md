# Benchmark Harness Bulletproof Testing Plan

## Purpose

This document defines a systematic, defensive approach to validating the benchmarking harness when there is **no single expected numeric outcome**. The goal is to validate behavior through invariants, differential comparison, metamorphic properties, and failure-mode observation rather than hardcoded golden numbers in most cases.

This plan is written specifically for the harness in `cmd/benchmark/` and `internal/bench/` against the in-repo broker (`broker/`) and SDK (`github.com/danielkotsi/golangMQSDK`).

## Known Pitfalls To Account For

| Issue | Location | Why it matters |
|---|---|---|
| Warmup timer-based reset | `internal/bench/engine.go:237-247` | If workload completes during `-warmup`, `resetAll()` zeros everything, `totalPublished` becomes 0, drain is skipped, and runs can produce all-zero summaries or errors. |
| Drain logic uses post-reset totals | `internal/bench/engine.go:310-330` | Drain decision depends on counters after warmup reset. |
| Consumer selection is non-deterministic | `broker/queue.go:selectConsumer()` (map iteration) | Per-consumer counts will vary; only aggregates are stable. |
| Prefetch hardcoded to 10 | broker + warnings in CLI | No-ack runs stall at prefetch 10. |
| Delivery tags are uint16 | protocol/broker | Totals > 65535 can wrap; benchmark warns but identity is tracked separately by header sequence. |
| Clients closed on error paths | `engine.go`, workers | Abrupt shutdown can produce "connection closed"/"worker failures" before summary is written. |
| `-output -` mixes streams | `cmd/benchmark/main.go`, `engine.go` | CSV goes to stdout when `-output -`, followed by JSON summary on stdout (mixed). |
| Order-sensitive `-distribution` | `cmd/benchmark/main.go`, `internal/bench/config.go` | Easy to misconfigure axes. |

---

## 1) Core Invariants (Must Hold For Valid Runs)

These should hold for any "clean" run (no errors, no forced timeouts).

| Invariant | How to verify | Notes |
|---|---|---|
| `published >= 0` | JSON summary | `published = publishers × msgs` in normal completion (no early stop). |
| `delivered >= consumed` (in general) | JSON | Duplicates mean delivered can exceed consumed in count of deliveries, but consumed is unique count. |
| `consumed + drops == published` (after drain/completion) | JSON | At end of run, undelivered = drops. |
| `duplicates <= delivered` | JSON | Must be non-negative. |
| `drops >= 0` | JSON | No negative drops. |
| If `-ack=true`, inflight should drain to 0 within `-drain-timeout` | logs/JSON | If not, expect drops/warning. |
| If `-ack=false`, expect prefetch-limited consumption | JSON | Should see ~prefetch per consumer and remaining as drops/stall. |
| CSV header matches `csvColumns` exactly | parse CSV | Schema stability is required. |
| Every row has correct column count | parse CSV | No malformed rows. |
| `runid` consistent across summary/filenames/exchange/queue names | JSON + plan logs | Unique per run. |
| Publish-capable set subset logic correct | topology in summary/plan | `P ∪ B` only. |
| Consume-capable set subset logic correct | topology in summary/plan | `C ∪ B` only. |
| Consumers never placed on publish-only connections/channels | plan logs + `by_conn` | Must be zero consumption on publish-only conns. |

---

## 2) Differential Testing (Against A Reference Implementation)

Since there's no single golden number, compare the harness against a **direct, minimal reference client** using the same broker/SDK.

### Setup
- Start broker on ephemeral port
- Use same credentials/config as harness (`daniel/123456789`, ChannelMax 10, FrameMax 10372, HeartbeatSec 10)
- For each scenario, run reference test and harness with identical parameters

### What to compare
| Property | Reference | Tolerance/Rule |
|---|---|---|
| Total published | Count publishes attempted | Should equal `publishers×msgs` (or what harness reports) |
| Total consumed (unique) | Track seen seqs in consumer | Must equal harness `consumed` if no drops |
| Drops | `published - consumed_unique` | Must match harness `drops` |
| Duplicates | Count of redeliveries seen | Must match harness `duplicates` |
| Delivery latency distribution shape | Compute from same header timestamps | Compare p50/p95/p99 within reasonable variance (see repeatability). For N>=50 very close; for N>=100 should be very consistent. |
| Ack latency (if acking) | Time around Ack() | Compare similarly |

### Implementation notes for reference test
- Stamp `[seq][sendnanos]` in body (same as harness) to compute client-side latency identically
- Use `atomic.Uint64` for seq allocation
- Track `seen` map/array to detect duplicates
- Mirror drain: wait until all expected messages consumed or timeout
- Do not use harness code paths

---

## 3) Metamorphic Testing (Properties That Must Hold Under Transformations)

These are powerful when absolute values are unknown.

| Transformation | Property | Test idea |
|---|---|---|
| Double publishers, same total msgs per pub? Or double total | `published` scales linearly | `publishers 1,msgs 100` vs `publishers 2,msgs 50` → same `published`, similar total throughput characteristics. |
| Move channels across connections (same counts) | Totals unchanged | `conns 1,channels 4` vs `conns 4,channels 4` (even) → `published/consumed/drops` identical for same workload; `by_conn`/`by_publish_channel` differ. |
| Split vs mixed roles | Accounting unchanged | Mixed vs separated with same total publishers/consumers/msgs → totals same, role distribution differs as expected. |
| Add more consumers on same channel | Totals unchanged, load distribution changes | More consumers should still consume all messages (if acking, no prefetch deadlock beyond normal behavior). |
| Even vs uneven distribution (packing) | Totals unchanged, placement differs | Worker placement changes but message accounting must be identical. |
| Increase `-report` interval | Totals unchanged | CSV row count changes, summary totals identical. |
| `-ack true` vs behavior | With ack, no prefetch stall | Without ack on finite run, should hit prefetch limit and show drops/stall. |
| Idempotent close/restart | Each run independent | Different `runid`, no cross-run leakage. |

---

## 4) Topology & Configuration Validation

### Topology math
| Test | Expected |
|---|---|
| `conns=4, channels=8` even | 2 channels per conn |
| `conns=2, channels=6, pub-channels=2, cons-channels=2` mixed | 2P/2C/2B total, publish-capable 4, consume-capable 4 |
| `pub-conns + cons-conns > conns` | Validation error |
| `pub-channels + cons-channels > mixed slots` | Validation error |
| `channels > conns * max-channels-per-conn` with uneven | Validation error |
| `-distribution` with no preceding axis | Parse error (exit 2) |
| `-max-*-per-*` with even distribution | Warning, ignored |
| `channels < conns` even | Some conns get 0 channels (allowed, idle sockets) |

### Placement correctness
| Test | Expected |
|---|---|
| Publish-only conn has no consumers | `slotConsumers` 0, `by_conn[pubconn].Consumed == 0` |
| Consume-only conn has no publishers placed on it? | Publishers only on publish-capable slots; verify plan |
| Uneven no-cap packing | All workers on first slot |
| Uneven capped packing | Fill to cap then next slot |
| Global index order respected | Role carving follows global order as documented |

---

## 5) Warmup, Timing, Drain & The Known Bug

This is critical given the bug we identified.

| Scenario | Method | Expected/Check |
|---|---|---|
| `-warmup 0` | Baseline | Normal behavior, all metrics collected from start. |
| `-warmup < workload duration` | Normal case | Warmup traffic discarded, measured phase starts after reset. `phaseStart` after reset is correct for measured interval. Summary reflects only measured phase. |
| `-warmup > workload duration` | **Bug scenario** | If workload finishes during warmup: currently can produce zeros or "worker failures: connection closed". This MUST be caught. After fix, should either error clearly, or measure correctly. Document expected behavior. |
| `-warmup` with no consumers | Edge case | Check behavior. |
| `-warmup` with `-duration` set | Interaction | Both can apply; need to understand semantics. |
| `-duration 0` | Run to completion | Waits for all publishers done. |
| `-duration > 0` | Timed run | Publishers stop at deadline, then drain. |
| Drain timeout exceeded (no-ack) | We tested | Warning logged, `drops = published - consumed`, run completes (exit 0) with partial results. |
| Drain completes early | Fast run | Should not wait full timeout. |

**Test procedure for bug:**
```bash
# Should not silently produce zeros
/tmp/benchmark -warmup 500ms -msgs 10 -conns 1 -channels 1 -publishers 1 -consumers 1 -quiet
# Compare to warmup 0
/tmp/benchmark -warmup 0 -msgs 10 -conns 1 -channels 1 -publishers 1 -consumers 1 -quiet
```
Compare JSON outputs. Also check CSV - if warmup bug hits, might be empty/minimal or run fails.

---

## 6) Latency & Statistics Validation

### Methodology
- Use header timestamps for delivery latency (same as harness)
- Compute percentiles with same algorithm: sort, `idx = ceil(p/100*n)-1`, clamp to [0, n-1]
- Validate on synthetic data first (unit-test style) to be sure

### Checks
| Check | Method |
|---|---|
| Percentile algorithm matches | Create known array, compute expected, compare |
| Monotonicity | `p50 <= p95 <= p99` (and >= min, <= max) |
| Averages reasonable | `avg` between min and max |
| Latencies non-negative | All latency values >= 0 |
| Ack latency only when acking | If `-ack=false`, ack latencies should be 0 |
| Per-role latencies populated correctly | `by_role` has entries for roles that received deliveries |
| Windowed sampling | Each interval row sums correctly to totals over time |
| Final interval written on stop | CSV has header + data rows |

### Synthetic validation
Create a quick test script that feeds known latency samples into the same percentile function logic as `internal/bench/metrics.go` to verify `percentile()` is correct.

---

## 7) Concurrency, Races & Stress

| Test | What to look for |
|---|---|
| `go test -race ./internal/bench ./tests` | Must pass with no races |
| Multiple publishers (N>1) | No lost messages, no seq collisions, totals correct |
| Multiple consumers (N>1) on same channel | All messages consumed (with ack), distribution non-deterministic but sum correct |
| High concurrency | `-conns 4 -channels 8 -publishers 8 -consumers 8 -msgs 1000` |
| Channel-per-publisher patterns | Various topologies |
| Rapid sequential runs | No resource leaks, ports freed, files cleaned up |
| Large message count | Test accounting holds (watch for uint16 tag issues if >65535) |

---

## 8) Failure Injection & Error Reporting

| Scenario | How | Expected |
|---|---|---|
| Broker not running | Point to wrong port | Connection error, exit 1, clear error message |
| Broker killed mid-run | Kill broker process during publish/consume | Worker errors collected, run fails with "worker failures" or connection errors |
| Invalid flags | Bad values | Exit 2 for parse errors, exit 1 for validation errors |
| Network issues | Hard to simulate locally | SDK errors propagate |
| Queue full/prefetch stall | No-ack mode | Detected, warning logged, drops counted |
| Drain timeout | Set very short timeout with no-ack | Times out, logs warning, reports drops |

---

## 9) Repeatability Testing

| Test | Method | Acceptance |
|---|---|---|
| Same config 3-5 times | Run identically | `published/consumed/drops` identical each time |
| Latency variance | Compare p50/p95/p99 across runs | Should be in same ballpark (varies with system load) |
| Different runids | Verify uniqueness | All runids different |
| No cross-contamination | Check exchange/queue names unique | Old resources cleaned up or just unused (unique names prevent issues) |

---

## 10) Concrete Test Matrix

### Minimal smoke (must pass)
```bash
# Build
go build ./... && go vet ./...

# Tests
go test ./internal/bench/ ./tests/
go test -race ./internal/bench/ ./tests/

# Basic runs
/tmp/benchmark -warmup 0 -conns 1 -channels 1 -publishers 1 -consumers 1 -msgs 100 -quiet
/tmp/benchmark -warmup 0 -conns 2 -pub-conns 1 -cons-conns 1 -channels 2 -publishers 2 -consumers 2 -msgs 100 -quiet
/tmp/benchmark -warmup 0 -conns 1 -channels 1 -publishers 4 -consumers 4 -msgs 50 -quiet
```

### Differential test script outline
```go
// For each scenario:
// 1. Run reference client directly against broker
// 2. Run harness with same config
// 3. Parse both results
// 4. Assert invariants hold
// 5. Assert totals match
// 6. Compare latency distributions within tolerance
```

### Edge case battery
```bash
# Warmup bug test
/tmp/benchmark -warmup 1s -msgs 5  # tiny workload
/tmp/benchmark -warmup 0 -msgs 5   # baseline

# No-ack
/tmp/benchmark -ack=false -msgs 100 -drain-timeout 2s

# Uneven
/tmp/benchmark -conns 3 -distribution uneven -max-channels-per-conn 2 -channels 5 -publishers 0 -consumers 0

# Output to stdout
/tmp/benchmark -msgs 10 -output - 2>&1 | head -30

# Many channels on one conn
/tmp/benchmark -conns 1 -channels 10 -publishers 2 -consumers 2 -msgs 100
```

---

## 11) Checklist For "Bulletproof" Validation

- [ ] **Invariants**: All invariants from section 1 pass on clean runs
- [ ] **Differential**: Reference implementation matches harness for 1pub/1cons, Npub/Ncons, multi-channel cases
- [ ] **Metamorphic**: All transformations preserve accounting invariants
- [ ] **Topology**: Role carving, placement, validation errors all correct
- [ ] **Warmup bug**: Identified and tested; behavior documented/fixed if possible
- [ ] **Latency stats**: Percentile algorithm verified correct
- [ ] **Concurrency**: Race tests pass, no deadlocks observed
- [ ] **Failures**: Error cases produce sensible exit codes and messages
- [ ] **Repeatability**: Totals stable across runs
- [ ] **CSV/JSON schema**: Stable and parseable
- [ ] **Drain behavior**: Correct for ack and no-ack modes
- [ ] **No cross-run interference**: Unique fixtures work
- [ ] **Documentation**: Manual matches actual behavior

---

## 12) Recommendations

1. **Fix or guard the warmup bug**: Either (a) track pre-reset totals, (b) don't reset if publishers already finished, (c) start workers after warmup, or (d) make warmup only apply to long-running duration-based runs. At minimum, detect and error/warn clearly.
2. **Add validation for summary completeness**: If run errors out, don't try to write partial JSON in confusing ways.
3. **Consider making warmup semantics explicit**: Document the bug or fix it. The current behavior is surprising.
4. **Add unit tests for percentile function**: Test edge cases (n=1, n=2, even/odd).
5. **Add integration test for differential comparison**: Automate the reference-vs-harness check for a few scenarios.
6. **Be explicit about client-side latency**: Document clearly that latencies are measured client-side.

---

## Appendix: Quick Commands

```bash
# Start broker
go run ./cmd/broker &

# Build tools
go build -o /tmp/benchmark ./cmd/benchmark
go build -o /tmp/benchbroker ./cmd/broker

# Run full test suite
go test -count=1 ./... && go test -count=1 -race ./...

# Check for races more specifically
go test -race -count=1 ./internal/bench/
```
