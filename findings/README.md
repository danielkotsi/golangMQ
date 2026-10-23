# Benchmark Harness Findings

Date: 2026-10-23
Scope: `cmd/benchmark/`, `internal/bench/`, and differential coverage in `tests/`
Reference: `docs/BENCHMARK_HARNESS_BULLETPROOF_TESTING_PLAN.md`

All findings below have regression coverage and have been fixed. The finding tests
that originally reproduced these defects now pass against the corrected harness.

## Summary

| ID | Severity | Area | Regression test | Status |
|---|---|---|---|---|
| BENCH-001 | High | Warmup longer than workload | `TestFinding_Bench001_WarmupLongerThanWorkloadIsNotSilentlyZero` | Fixed |
| BENCH-002 | High | Warmup accounting / drain | `TestFinding_Bench002_WarmupShorterThanWorkloadKeepsAccountingConsistent` | Fixed |
| BENCH-003 | Medium | Topology global-index order | `TestTopology_RoleCarvingUsesGlobalIndexOrder` | Fixed |
| BENCH-004 | High | CSV write/flush errors | `TestFinding_Bench004_CSVWriteErrorsMustBeReported` | Fixed |
| BENCH-005 | Medium | CLI stdout stream mixing | `TestFinding_Bench005_OutputDashMustNotMixStreams` | Fixed |
| BENCH-006 | Low-Medium | Concurrent run-ID uniqueness | `TestFinding_Bench006_RunIDUniqueUnderConcurrentGeneration` | Fixed |
| BENCH-007 | Medium | Context cancellation reporting | `TestFinding_Bench007_ContextCancellationIsReported` | Fixed |
| BENCH-008 | Low | No-ack ack-latency sample count | `TestFinding_Bench008_NoAckRecordsZeroAckLatencySamples` | Fixed |

## Test suite state

Baseline and final verification used Go `go1.26.5-X:nodwarf5 linux/amd64`.

| Command | Result |
|---|---|
| `go build ./...` | Pass |
| `go vet ./...` | Pass |
| `go test ./... -count=1` | Pass |
| `go test -race ./... -count=1` | Pass (`internal/bench` 20.998s, `tests` 10.568s) |
| `go test ./internal/bench -count=1` | Pass (all finding and harness tests) |
| `go test -race ./internal/bench -count=1` | Pass, no `DATA RACE` reported |
| `go test ./cmd/benchmark -count=1` | Pass, including BENCH-005 |

All eight finding tests now pass:

- BENCH-001 through BENCH-004, BENCH-006 through BENCH-008 in `internal/bench`.
- BENCH-005 in `cmd/benchmark`.

The differential suite (`tests/benchmark_differential_test.go`) passes. It runs a
direct SDK reference client, tracks unique sequence numbers independently, and
compares publish/consume/duplicate/drop accounting for acking and no-ack runs.

## Verify fixes

```bash
go test ./internal/bench \
  -run 'TestFinding_Bench|TestTopology_RoleCarvingUsesGlobalIndexOrder' \
  -count=1 -v

go test ./cmd/benchmark -run TestFinding_Bench005 -count=1 -v
```

Run the reliable, non-finding coverage:

```bash
go test ./tests -count=1
go test ./internal/bench -run 'TestHarness_|TestMetamorphic_|TestTopology_Channels|TestTopology_Capability|TestConfig_|TestPercentile|TestLatencySet' -count=1
```

## Added test files

- `internal/bench/harness_bulletproof_test.go` — invariants, metamorphic,
  topology/config, latency, warmup, identity, failure-injection and finding tests.
- `cmd/benchmark/main_test.go` — in-process CLI tests and BENCH-005.
- `tests/benchmark_differential_test.go` — independent SDK reference comparison.

## Findings index

- [`BENCH-001.md`](BENCH-001.md) — long warmup silently zeroes a completed run.
- [`BENCH-002.md`](BENCH-002.md) — short warmup corrupts accounting and drain.
- [`BENCH-003.md`](BENCH-003.md) — role carving ignores documented global order.
- [`BENCH-004.md`](BENCH-004.md) — CSV writes/flushes fail silently.
- [`BENCH-005.md`](BENCH-005.md) — `-output -` mixes CSV and JSON on stdout.
- [`BENCH-006.md`](BENCH-006.md) — concurrent `newRunID` collisions.
- [`BENCH-007.md`](BENCH-007.md) — cancellation reported as connection failure.
- [`BENCH-008.md`](BENCH-008.md) — no-ack run records zero-valued ack samples.
