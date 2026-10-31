# Observability Status (Updated)

This document summarizes what is implemented, what is partially done, and what is missing for a clean Prometheus + Grafana stack. It also includes a **separate section for Docker/Compose requirements**.

## 1) What is done

### Metrics (broker)
- Prometheus client added; metrics registry exposed via HTTP (`/metrics`) on configurable port (default `:9090`, can run on `:9091` for local demos).
- Counters: `golangmq_publish_total{exchange}`, `golangmq_publish_unroutable_total{exchange}`, `golangmq_publish_errors_total`, `golangmq_enqueue_total{queue}`, `golangmq_deliver_total{queue}`, `golangmq_deliver_errors_total{queue}`, `golangmq_ack_total{queue}`, `golangmq_nack_total{queue,requeue}`, `golangmq_dead_letter_total{queue}`, `golangmq_drop_total{queue}`, `golangmq_connections_opened_total`, `golangmq_connections_closed_total`, `golangmq_handshake_failures_total`, `golangmq_consumers_registered_total{queue}`, `golangmq_consumers_unregistered_total{queue}`.
- Gauges (via custom collector): `golangmq_queue_messages{queue}`, `golangmq_queue_consumers{queue}`, `golangmq_consumer_inflight{queue,consumer}`, `golangmq_queues`, `golangmq_exchanges`, `golangmq_connections`.
- Go/process collectors registered (`prometheus/client_golang` collectors).
- State collector uses documented lock order (`Server.mu → Broker.mu → Queue.mu`) to avoid scrape-time deadlocks.
- Wired into broker/server flow (publish/consume/ack/nack/declare/register/unregister).

### Prometheus
- `prometheus.yml` present with scrape config for broker (`host.docker.internal:9091` in current config).
- Scrape interval/eval interval set to 1s for responsive local dashboards.

### Grafana
- Provisioning structure present: `grafana/provisioning/datasources/`, `grafana/provisioning/dashboards/`.
- Dashboard JSON present: `grafana/dashboards/golangmq-overview.json` (overview with connections/queues/exchanges/consumers, rates, queue depth, ack/nack, errors/DLQ/drops, consumers+inflight, connections+errors).
- Plan/docs under `docs/observability/grafana/` with step-by-step instructions.

### Benchmark integration
- Benchmark creates uniquely named queues/exchanges per run (`bench.ex.<runid>`, `bench.q.<runid>`) making metrics traceable per run.
- Benchmark runner supports reusing existing broker or starting fresh; results include run metadata.

## 2) What we've seen as missing / gaps

| Gap | Severity | Notes / evidence |
|---|---|---|
| **Total backlog confusion** | High | `golangmq_queue_messages` only counts undispatched; inflight is separate. Easy to misread stalls (e.g. no-ack leaves messages "stuck" in inflight then requeued on disconnect). Need a "Total Backlog = queue_messages + sum(consumer_inflight)" panel/query. |
| **Message age / oldest message** | High | No gauge for oldest message age/age seconds. Can't tell how long messages have been queued when consumers are stuck at prefetch. |
| **Broker-side latency histograms** | High | Latency is measured by the benchmark externally. No broker-side histograms (publish->enqueue, enqueue->deliver, ack latency) for in-situ observation. |
| **Queue/exchange filtering in dashboard** | Medium | Dashboard shows all queues/exchanges; benchmark runs create many `bench.q.<runid>` series causing noise. Need template variables / filtering. |
| **Runtime visibility in dashboard** | Medium | Go/process collectors registered but dashboard doesn't visualize goroutines, heap alloc, GC pause duration, CPU. |
| **Consumer utilization** | Medium | No panel showing inflight/prefetch utilization per consumer to detect saturation. |
| **Queue length limits / backpressure visibility** | Medium | No metrics for queue max length, dropped due to limits, or publisher blocking duration. |
| **Metrics tests** | Medium | FEATURE_METRICS_REPORT notes no test in `tests/` asserts metric values yet (Step 5 not fully closed). |
| **Docker Compose for full stack** | High | No `docker-compose.yml` in repo root. Running broker+Prometheus+Grafana requires manual `docker run`/commands. |
| **Port/default alignment** | Medium | `prometheus.yml` scrapes `:9091`; broker defaults to `:9090`. Need clear docs or defaults aligned. |
| **Image/version alignment** | Medium | `Dockerfile`/`deployment.yaml` noted in report (Go version/image considerations). |
| **Queue/exchange deletion metrics** | Low | No delete/purge APIs exist; no metrics for them. |

## 3) Separate section: Docker parts missing for clean docker compose

To run a clean end-to-end stack (`broker`, `prometheus`, `grafana`) with provisioning and networking, the following are missing or need to be added.

### Missing files/artifacts
| Item | Status | Notes |
|---|---|---|
| `docker-compose.yml` (repo root) | **Missing** | Orchestrate broker, Prometheus, Grafana with volumes/networks. |
| `.dockerignore` | **Missing** (likely) | Keep images small; avoid copying `bin/`, `results/`, `.git`. |
| `grafana/provisioning/dashboards/dashboards.yml` | **Check/ensure present** | Plan expects it; verify it's committed and mounted. |
| `grafana/provisioning/datasources/datasource.yml` | **Check/ensure present** | Prometheus datasource provisioning. |
| Prometheus config mounted in compose | **Missing wiring** | Need to mount `./prometheus.yml` into Prometheus container. |
| Grafana provisioning/dashboards mounted | **Missing wiring** | Mount `./grafana/provisioning` and `./grafana/dashboards`. |
| Broker metrics port exposure | **Needs wiring** | Expose metrics port (e.g. `9090` or `9091`) and ensure broker runs with metrics enabled. |
| Network/host resolution | **Needs config** | On Linux, `host.docker.internal` may not resolve; use `host-gateway` or service names depending on setup. |
| Healthchecks/dependencies | **Missing** | Add healthchecks so Grafana waits for Prometheus/broker. |
| Environment for broker metrics addr | **Missing** | Broker may need flag/env for metrics listen address. |

### Suggested minimal docker-compose.yml (template)
```yaml
version: '3.8'
services:
  broker:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["./broker", "-metrics-addr=:9090"]  # or adjust to flags
    ports:
      - "5672:5672"
      - "9090:9090"
    restart: unless-stopped

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
    ports:
      - "9090:9090"  # avoid conflict with broker; use different host port or same only if one
    depends_on:
      - broker
    extra_hosts:
      - "host.docker.internal:host-gateway"

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
    environment:
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
      - GF_AUTH_DISABLE_LOGIN_FORM=false
    depends_on:
      - prometheus
```

Notes for the template above:
- Port conflicts: if broker uses `:9090` for metrics, don't map Prometheus to host `:9090` too. Map Prometheus to `:9091` or use different ports.
- `host.docker.internal` requires `extra_hosts` on Linux or Docker Desktop; adjust `prometheus.yml` targets accordingly (use `broker:9090` if on same network).
- Broker entrypoint/flags may need verification against current `cmd/broker/main.go`.
- Provisioning paths must match what Grafana expects.

### Quick verification checklist for compose
- [ ] `docker compose up` brings all services healthy
- [ ] `curl localhost:<broker-metrics>/metrics` returns Prometheus exposition
- [ ] Prometheus targets show broker UP
- [ ] Grafana loads datasource and dashboard automatically
- [ ] Traffic from benchmark/example clients updates panels
- [ ] Volumes/provisioning are read-only where appropriate

## 4) Recommended next steps
1. **Fix observability confusion**: Add "Total Backlog" panel (queue_messages + sum consumer_inflight) and consider oldest-message-age metric.
2. **Reduce noise**: Add Grafana template variables for queue/exchange; document filtering by `bench.q.<runid>`.
3. **Add docker-compose.yml**: Create root compose file with correct ports/networking and mount provisioning/config.
4. **Align defaults/ports**: Decide broker metrics default port and update `prometheus.yml`/docs consistently.
5. **Close Step 5**: Add a basic metrics assertion test in `tests/` against the registry/handler.
