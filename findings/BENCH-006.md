# BENCH-006: `newRunID` collides under concurrent generation

- **Status:** Fixed
- **Severity:** Low-Medium
- **Component:** `internal/bench/engine.go`
- **Failing test:** `internal/bench/harness_bulletproof_test.go::TestFinding_Bench006_RunIDUniqueUnderConcurrentGeneration`

## Reproduction

```bash
go test ./internal/bench \
  -run TestFinding_Bench006_RunIDUniqueUnderConcurrentGeneration \
  -count=1 -v
```

The test runs several synchronized rounds of concurrent `newRunID` calls and
fails if any two IDs are equal. Recent results:

```text
normal: 499821 unique ids for 500000 calls (179 duplicates)
race:   499989 unique ids for 500000 calls (11 duplicates)
```

Earlier 20k/50k probes also produced duplicate IDs in multiple trials.

## Expected

Concurrent callers must receive distinct run IDs. The manual states every run gets
a unique `runid` used in fixture and CSV names (`docs/benchmark-manual.md:40`).

## Observed

Duplicate IDs are routine under concurrent generation. Example duplicates:

```text
dmcajdl2vuhm x2
dmcajdjb89jc x2
dmcajdoe0yqh x2
dmcajdnrnwys x2
dmcajdk7rcw9 x2
```

The test is intentionally synchronized so the failure is reliable rather than a
rare scheduling artifact.

## Root cause

`newRunID` is a bare wall-clock timestamp:

```go
func newRunID() string {
    return strconv.FormatInt(time.Now().UnixNano(), 36)
}
```

(`internal/bench/engine.go:78-80`). Multiple goroutines or processes can observe
the same nanosecond value. There is no atomic counter, lock, random component, or
process-unique prefix.

## Impact

- Concurrent harness runs can use the same exchange, queue, routing key and CSV
  path.
- Cross-run traffic can contaminate results, and output files can overwrite each
  other.
- The documented isolation guarantee ("unique fixture names") is not met.
- The impact is lower for strictly sequential CLI usage, but the harness engine is
  callable as a library and may be run concurrently.

## Suggested fix direction

Generate IDs with multiple entropy sources, for example:

- an atomic per-process sequence combined with timestamp, plus
- random bytes or a process ID, and
- collision retry if resources must be globally unique.

Keep IDs filesystem-safe and reasonably short. Add a concurrency test to the
production package so future changes cannot regress uniqueness.

## Resolution

Fixed in `internal/bench/engine.go`. Run IDs now combine a base36 timestamp, a
process-wide atomic sequence, and a per-process random nonce, preventing collisions
for concurrent callers and concurrent processes.
