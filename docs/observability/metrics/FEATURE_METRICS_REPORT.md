# Feature Report — Prometheus Metrics (Goal 1)

**Branch:** `feature/metrics` → **PR:** [#6](https://github.com/danielkotsi/golangMQ/pull/6) (base: `observability`, synced with `main` first)
**Scope:** 4 commits, 20 files (`git diff --name-status main feature/metrics`)
**Status:** Implemented, build/vet/test green; **1 blocker + 5 nits** open from PR review (§4).

---

## 1. What was done

Executed the plan in [`METRICS_PLAN.md`](METRICS_PLAN.md) step by step; each step
is documented under [`steps/`](steps/) and was built + tested before moving on.

| Step | Commit(s) | Outcome |
|---|---|---|
| [01 — Add Prometheus client](steps/01-add-prometheus-client.md) | `723c79e` | `github.com/prometheus/client_golang v1.24.1` added as first non-SDK dependency; `go` directive bumped `1.22.2 → 1.25.0` (required by client_golang). Custom per-server registry decided. |
| [02 — Define metrics](steps/02-define-metrics.md) | `723c79e` | New `broker/metrics.go`: `Metrics` struct, 15 `golangmq_*` counters, per-server `prometheus.Registry`, Go/Process collectors, `stateCollector` stub. |
| [03.1 — Thread `*Metrics`](steps/03-1-thread-metrics.md) | `1896c3b` | `NewServer → NewBroker → NewQueue` wiring; `NewServer(addr, cfg)` external signature unchanged (tests untouched); `stateCollector` gets server back-ref. |
| [03.2 — Hook points](steps/03-2-hook-points.md) | `1896c3b` | All 15 counters hooked (publish / deliver / ack / nack / connections / consumers) + real `stateCollector.Collect` snapshotting 6 gauges. |
| [04 — Expose `/metrics`](steps/04-expose-metrics.md) | `1896c3b`, `fca012d` | `MetricsHandler()`, `-addr`/`-metrics-addr` flags (separate port), `Dockerfile EXPOSE 5672 9090`, `deployment.yaml` scrape annotations, local `prometheus.yml`. |
| [05 — Verification](steps/05-verification.md) | — | Manual smoke done; automated metric assertions **not** yet added (see §4, issue 6). |
| Metrics execution plan docs | `55b0244` | `METRICS_PLAN.md` + 6 step files. |
| Local Prometheus scrape config | `fca012d` | `prometheus.yml` for the dev demo. |

### Instrumentation summary

- **15 counters** (`golangmq_*`): publish accepted/unroutable/errors, enqueue,
  deliver/deliver-errors, ack, nack (`queue`,`requeue`), dead-letter, drop,
  connections opened/closed, handshake failures, consumers registered/unregistered.
- **6 gauges** via `stateCollector` snapshot (no Inc/Dec depth drift):
  `queue_messages`, `queue_consumers`, `consumer_inflight`, `queues`,
  `exchanges`, `connections`.
- **Lock order** (documented in `broker/metrics.go`):
  collector `Server.mu → Broker.mu → Queue.mu → Consumer.mu`; all metric
  `.Inc()` calls happen outside held locks.

---

## 2. Verification performed

| Check | Result |
|---|---|
| `go build ./... && go vet ./...` | clean |
| `gofmt -l broker/ cmd/` | clean |
| `go test -race ./tests/ -count=1` | **30/30 PASS** (after every step) |
| Smoke: `go run ./cmd/broker -metrics-addr=:9091` + `curl` | exposition text returned (`golangmq_*`, `go_*`, `process_*`) |
| `prometheus.yml` + `prom/prometheus` container | targets endpoint responds; demo documented below |

**Plan §5 acceptance criteria** (from [`METRICS_PLAN.md`](../METRICS_PLAN.md)):

| Criterion | Status |
|---|---|
| `prometheus/client_golang` in `go.mod`; custom per-server registry | ✅ |
| All §2.1 counters defined + hooked | ✅ |
| §2.2 gauges served by snapshot collector | ✅ |
| `GET /metrics` returns exposition format | ✅ |
| `go test -race ./tests/` 30/30 | ✅ |
| No behavior/protocol changes — increments only | ✅ |
| K8s manifests expose/scrape metrics port | ✅ (annotations + containerPort; see issue 1 re: image) |
| Automated metric assertions in tests | ❌ not in Goal 1 scope (issue 6) |

---

## 3. Files changed (20)

```
Dockerfile                                 broker/broker.go
broker/channel.go                          broker/metrics.go  (new)
broker/queue.go                            broker/server.go
cmd/broker/main.go                         deployment.yaml
go.mod                                     go.sum
prometheus.yml (new)
docs/observability/metrics/METRICS_PLAN.md
docs/observability/metrics/steps/{01,02,03-1,03-2,04,05}.md
docs/TESTING_AND_BENCHMARKING_{PLAN,REPORT}.md  → docs/testing_benchmarking/ (rename, see issue 2)
```

---

## 4. Known issues from PR review — **to fix later**

PR #6 review verdict: *“Approachable, approve with 1 blocker + nits.”* All findings
reproduced/verified against the branch before listing them here.

### Blocker

**1. `Dockerfile` broken by this PR — builder image vs `go` directive**
- `Dockerfile:1` → `FROM golang:1.22-alpine AS builder`, but `go.mod:3` now says
  `go 1.25.0` (bumped in Step 1 because client_golang v1.24.1 requires Go 1.25).
  Go 1.22 **refuses to build** a module declaring `go >= 1.25.0`, so
  `docker build` fails.
- `deployment.yaml:26` still points at `danielkotsi/golangmq:latest` (an older
  pre-metrics image), so the k8s artifact doesn't match this branch either.
- **Fix:** bump the builder to `golang:1.25-alpine` (or later), rebuild and push
  the image. PR body previously deferred this to “a separate fix”; reviewer wants
  it **in this PR** before merge.

### Nits

**2. Out-of-scope file rename in `55b0244`**
- The metrics-plan commit also moved
  `docs/TESTING_AND_BENCHMARKING_{PLAN,REPORT}.md` → `docs/testing_benchmarking/`.
  Rename detection hides it (`R100`, `0/0` insertions/deletes), so it doesn't show
  in the 20-file stat. Documented as intended in plan §0, but it is scope creep and
  can surface as conflicts at merge.
- **Fix:** either call it out in the PR description, or split it into its own
  commit/PR.

**3. `prometheus.yml` target `:9091` vs broker default `:9090`**
- `prometheus.yml` scrapes `host.docker.internal:9091` to match the manual demo
  (`-metrics-addr=:9091`, chosen so Prometheus' UI can keep `:9090`). A
  **default-run** broker (`go run ./cmd/broker`) listens on `:9090` and gets **no
  scrape** from this config.
- **Fix:** document the required flag prominently, or add a second job/default
  port, or change broker default metrics port.

**4. `NewBroker` / `NewQueue` nil-`*Metrics` footgun**
- Both are exported and take a `*Metrics` parameter (`broker/broker.go:15`,
  `broker/queue.go:32`). A `nil` passed there later nil-derefs in
  `enqueue`/`dispatchLoop`. No current caller passes `nil` (all routes go through
  `NewServer`), so it is latent only.
- **Fix:** unexport them, guard with a no-op registry, or assert non-nil at
  construction.

**5. `deployment.yaml` missing trailing newline**
- File ends at `containerPort: 9090` with no final `\n` (cosmetic; `xxd` confirms).

**6. Step 5 verification not fully closed**
- PR body/plan mark [05 — Verification](steps/05-verification.md) as “next”.
  Plan §5 acceptance checkboxes aren't fully closed: only a **manual smoke** was
  run — no test in `tests/` asserts on metric values yet. Acceptable for Goal 1
  scope, but should be tracked.
- **Fix:** add a small test that scrapes `MetricsHandler()` (or gathers the
  registry) after a publish/consume cycle and asserts `golangmq_publish_total`
  etc. increased; then tick the §5 checkboxes and mark Step 5 done.

---

## 5. Out of scope for Goal 1 (from plan §6)

- Histograms (publish/deliver latency, message-size buckets)
- Alert rules & Grafana dashboards
- Structured logging / correlation IDs
- Integration with the benchmark harness
- Fixing latent issues metrics will *reveal* (silent unroutable drops,
  no-requeue-no-DLX drops, `uint16` delivery-tag wraparound)
- K8s Prometheus operator/in-cluster deployment (annotations are in place)