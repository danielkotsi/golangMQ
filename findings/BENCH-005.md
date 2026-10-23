# BENCH-005: `-output -` mixes CSV and JSON in one stdout stream

- **Status:** Fixed
- **Severity:** Medium
- **Component:** `cmd/benchmark/main.go`, `internal/bench/engine.go`
- **Failing test:** `cmd/benchmark/main_test.go::TestFinding_Bench005_OutputDashMustNotMixStreams`

## Reproduction

```bash
go test ./cmd/benchmark \
  -run TestFinding_Bench005_OutputDashMustNotMixStreams \
  -count=1 -v
```

Test invocation: `-output - -msgs 10 -report 10ms -quiet` against an in-process
ephemeral broker.

## Expected

A single stdout stream must be safely consumable as one machine-readable format.
With `-output -` the CSV document goes to stdout. The JSON summary must not be
appended to the same stream, or the CLI must emit a documented, parseable envelope
for both formats.

The manual says the harness emits CSV and a single JSON summary
(`docs/benchmark-manual.md:5-6`), with the JSON summary on stdout
(`docs/benchmark-manual.md:33-36`). `-output -` is documented as CSV to stdout,
so silently concatenating both is not a valid CSV or JSON stream.

## Observed

stdout contained the CSV header, a CSV data row, and then the JSON summary:

```text
ts_start_rfc3339,window_ms,published,...
2026-10-23T14:28:49Z,10.6,10,10,10,...
{"runid":"dmcaeuaa58nc","config":{...},...}
```

The finding test fails with:

```text
stdout mixes CSV and JSON in one stream
```

## Root cause

Two independent writers target stdout:

- `openCSV` returns `os.Stdout` when `cfg.Output == "-"`
  (`internal/bench/engine.go:357-361`).
- The CLI always prints the JSON summary with `fmt.Println(string(data))`
  (`cmd/benchmark/main.go:88-93`), regardless of the output destination.

The engine comment claims stdout stays clean for CSV/JSON
(`internal/bench/engine.go:380-384`), but it does not reserve or separate the two
documents.

## Impact

- `benchmark -output - | jq` fails because the stream is not pure JSON.
- `benchmark -output - > run.csv` produces a corrupt CSV file.
- Piped/automated consumers receive a format that cannot be parsed without
  custom splitting.
- The documented stdout contract is ambiguous and currently violated.

## Suggested fix direction

Choose and document one contract, then enforce it:

- When `-output -`, write JSON summary to stderr (or a file), keeping stdout pure
  CSV; or
- Keep stdout pure JSON and require a real path for CSV; or
- Emit a single documented envelope (for example JSON with embedded CSV records).

The test currently accepts any outcome except both formats appearing together on
stdout, so it supports whichever clean contract is chosen.

## Resolution

Fixed in `cmd/benchmark/main.go`. With `-output -`, the JSON summary is written to
stderr while stdout remains a pure CSV stream. File-backed CSV runs still write
the JSON summary to stdout.
