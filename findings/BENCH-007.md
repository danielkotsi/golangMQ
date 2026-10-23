# BENCH-007: Context cancellation is reported as a connection failure

- **Status:** Fixed
- **Severity:** Medium
- **Component:** `internal/bench/engine.go`
- **Failing test:** `internal/bench/harness_bulletproof_test.go::TestFinding_Bench007_ContextCancellationIsReported`

## Reproduction

```bash
go test ./internal/bench \
  -run TestFinding_Bench007_ContextCancellationIsReported \
  -count=1 -v
```

Test configuration: `2 publishers`, `2 consumers`, `msgs=100000`,
cancellation after `30ms`.

## Expected

After the caller cancels the context, `Run` should return an error that wraps
`context.Canceled` (so `errors.Is(err, context.Canceled)` is true). A cancellation
is not a broker or worker fault. Any drain warning should reflect the actual
cancellation path, not a fabricated full timeout.

## Observed

`Run` returned a misleading worker failure instead of cancellation:

```text
Run returned worker failures: connection closed
connection closed after cancellation; want an error wrapping context.Canceled
```

An earlier probe at `50ms` also logged:

```text
benchmark dmcaegr16wy9: WARNING drain not complete after 5s:
consumed 1048 of 2225 published
```

while the run itself bailed immediately on cancellation. The warning text claimed
the full 5s drain timeout even though the wait was cut short by `ctx.Done()`.

## Root cause

The measured phase does not propagate cancellation cleanly:

- Setup/consume contexts use `context.Background()` (`openCtx`), not the caller's
  context.
- During the measured phase, `ctx.Done()` closes `stopPub` and exits the wait
  (`internal/bench/engine.go:280-309`).
- Drain detects cancellation but sets `deadline = time.Now()` to bail out, then
  logs the configured timeout rather than elapsed time
  (`internal/bench/engine.go:310-327`).
- The engine immediately calls `closeClients`, then waits for consume workers.
- Workers blocked on ack/delivery observe closed connections and send
  `connection closed` to `errCh`.
- The engine processes `errCh` before checking `ctx.Err()`, so the returned error
  is `worker failures: connection closed`
  (`internal/bench/engine.go:333-349`).

The caller therefore cannot distinguish intentional cancellation from a real
worker failure.

## Impact

- Orchestrators cannot use `errors.Is(err, context.Canceled)` to classify
  intentional shutdown.
- Cleanup-induced worker errors masquerade as benchmark failures.
- Logs report a timeout that did not elapse, complicating diagnosis.
- Cancellation may hide the true partial-run status and CSV state.

## Suggested fix direction

- Thread the caller context through setup, SDK operations, and shutdown where
  supported.
- After cancelling/shutting down workers, check `ctx.Err()` and return it (wrapped
  with run context) before aggregating derivative connection/close errors.
- Suppress or annotate worker errors that are consequences of cancellation.
- Log actual elapsed drain time, not the configured timeout, when bailing out.
- Define whether a cancelled run returns partial results; test that contract.

## Resolution

Fixed in `internal/bench/engine.go`. Setup derives from the caller context,
cancellation stops publishers, skips drain, and shuts workers down before the
engine returns an error wrapping `context.Canceled`. Cancellation is checked before
derivative worker/close errors, and drain warnings now report actual elapsed time.
