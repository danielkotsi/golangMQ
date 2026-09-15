# Reports

Index for the reports kept in this repository.

| Area | Purpose | Location |
|------|---------|----------|
| Findings | Broker issues discovered by exercising it through the Phase-1 integration suites (each in its own file: finding, behaviour, why, why change, how to counter) | [`findings/`](findings/) |
| Changes | What has been changed in the current branch so far | [`changes/`](changes/) |

## Findings Index

| ID | Title | Status | File |
|----|-------|--------|------|
| F-001 | Channels silently overwrite consumers — one consumer per channel is enforced by accident, not by design | PARTIAL (tests work around it; broker guard not implemented) | [`findings/F-001.md`](findings/F-001.md) |
| F-002 | Data race in consumer bookkeeping — ack/nack racing `dispatchLoop` silently loses acks and stalls delivery | FIXED (per-consumer mutex + multi-consumer test drain; see [`changes/f002.md`](changes/f002.md)) | [`findings/F-002.md`](findings/F-002.md) |
| F-003 | Broker never replies `channel.close-ok` — `ClientChannel.Close()` always times out | OPEN (cleanup works; handshake never completes) | [`findings/F-003.md`](findings/F-003.md) |

## Changes Index

| Report | File |
|--------|------|
| Phase-1 test suite + findings (integrationTests branch) | [`changes/integrationTests.md`](changes/integrationTests.md) |
| F-002 fix — consumer-bookkeeping data race + multi-consumer test drain (fix/finding-002 branch) | [`changes/f002.md`](changes/f002.md) |