# BENCH-004: CSV write and flush errors are silently ignored

- **Status:** Fixed
- **Severity:** High
- **Component:** `internal/bench/csv.go`, `internal/bench/engine.go`
- **Failing test:** `internal/bench/harness_bulletproof_test.go::TestFinding_Bench004_CSVWriteErrorsMustBeReported`

## Reproduction

```bash
go test ./internal/bench \
  -run TestFinding_Bench004_CSVWriteErrorsMustBeReported \
  -count=1 -v
```

The test uses `/dev/full`, which opens successfully but fails every write, as the
configured `-output` path. It skips only when `/dev/full` is unavailable.

## Expected

The harness must report an output failure when interval/header/flush writes fail.
`Run` should return an error (or a non-nil error with no successful result), not a
successful summary that implies the CSV was written.

## Observed

`Run` returned `err=nil` and a normal successful result:

```text
Run returned success with a CSV path whose every write fails
Published:10 Delivered:10 Consumed:10 Drops:0
ElapsedMs:10
```

The benchmark summary was fully populated even though the configured CSV
destination could not accept any data.

## Root cause

The CSV layer discards write and flush errors:

- `csvWriter.writeHeader` calls `c.w.WriteString(...)` but does not inspect the
  returned error (`internal/bench/csv.go:47-55`).
- `csvWriter.writeInterval` does the same (`internal/bench/csv.go:57-96`).
- `newCSVWriter` cannot return an error and calls `writeHeader` unconditionally
  (`internal/bench/csv.go:41-44`).
- The sampler ignores flush errors:
  `_ = csv.flush()` in `internal/bench/engine.go:263-268`.
- The engine ignores the final close error:
  `_ = csv.close()` in `internal/bench/engine.go:347`.
- `openCSV` only detects failures at open/create time, not write time
  (`internal/bench/engine.go:357-371`).

`bufio.Writer` records the first write error internally, but no layer ever checks
or propagates it.

## Impact

- Data loss is invisible: callers receive a successful exit/result with missing or
  truncated CSV.
- CI pipelines and benchmark comparisons can trust a file that was never written.
- This also affects `-output -` when stdout is broken or closed early.
- Failures may only surface much later when a consumer tries to parse the file.

## Suggested fix direction

- Make CSV writer methods return errors and retain the first write error.
- Check `flush`/`close` errors, including the final sampler flush.
- Have `Run` return the output error and skip a success summary.
- Add tests for header-write failure, interval-write failure, flush failure, and
  close failure (not only open failure).

## Resolution

Fixed in `internal/bench/csv.go` and `internal/bench/engine.go`. The CSV writer
retains the first write/flush error, the header is flushed immediately, sampler
write failures stop sampling, and `Run` returns the close/flush error instead of
a success result.
