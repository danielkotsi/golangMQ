# Observability Status (Updated)

This document summarizes what is implemented, what is partially done, and what is missing for a clean Prometheus + Grafana stack. It also includes a **separate section for the Docker Compose setup**.

## 1) What is done

### Metrics (broker)
- Prometheus client added; metrics registry exposed via HTTP (`/metrics`) on configurable port (default `:9090`, can run on `:9091` for local demos).
- Counters: `golangmq_publish_total{exchange}`, `golangmq_publish_unroutable_total{exchange}`, `golangmq_publish_errors_total`, `golangmq_enqueue_total{queue}`, `golangmq_deliver_total{queue}`, `golangmq_deliver_errors_total{queue}`, `golangmq_ack_total{queue}`, `golangmq_nack_total{queue,requeue}`, `golangmq_dead_letter_total{queue}`, `golangmq_drop_total{queue}`, `golangmq_connections_opened_total`, `golangmq_connections_closed_total`, `golangmq_handshake_failures_total`, `golangmq_consumers_registered_total{queue}`, `golangmq_consumers_unregistered_total{queue}`.
- Gauges (via custom collector): `golangmq_queue_messages{queue}`, `golangmq_queue_consumers{queue}`, `golangmq_consumer_inflight{queue,consumer}`, `golangmq_queues`, `golangmq_exchanges`, `golangmq_connections`.
- Go/process collectors registered (`prometheus/client_golang` collectors).
- State collector uses documented lock order (`Server.mu → Broker.mu → Queue.mu`) to avoid scrape-time deadlocks.
- Wired into broker/server flow (publish/consume/ack/nack/declare/register/unregister).

### Prometheus
- `prometheus.yml` present; scrapes the broker at `broker:9090` (compose service DNS).
- Scrape interval/eval interval set to 1s for responsive local dashboards.

### Grafana
- Provisioning structure present: `grafana/provisioning/datasources/`, `grafana/provisioning/dashboards/`.
- Datasource points at `http://prometheus:9090` (compose service DNS).
- Dashboard JSON present: `grafana/dashboards/golangmq-overview.json` (overview with connections/queues/exchanges/consumers, rates, queue depth, ack/nack, errors/DLQ/drops, consumers+inflight, connections+errors).
- Plan/docs under `docs/observability/grafana/` with step-by-step instructions.

### Docker Compose
- Root `docker-compose.yml` orchestrates broker + Prometheus + Grafana: provisioning mounted, healthchecks + `depends_on: service_healthy` ordering, named volumes for Prometheus/Grafana data, aligned ports and a shared compose network.
- `Dockerfile` builder bumped to `golang:1.25-alpine` to match `go.mod` (`go 1.25.0`); module download layer added for build caching.
- `.dockerignore` added so the build context excludes `.git`, `bin/`, `results/`, `*.csv`, docs and the compose-mounted config/provisioning.
- No `host.docker.internal` needed: all services talk over the compose network (`prometheus` → `broker:9090`, `grafana` → `prometheus:9090`).

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
| **Docker Compose for full stack** | Done | Root `docker-compose.yml` orchestrates broker + Prometheus + Grafana with provisioning, healthchecks and volumes. |
| **Port/default alignment** | Done | Broker metrics stay `:9090` inside the container, published to host `:9091`; Prometheus UI keeps `:9090`. `prometheus.yml` targets `broker:9090`. |
| **Image/version alignment** | Done | `Dockerfile` builder bumped to `golang:1.25-alpine`; `.dockerignore` added. |
| **Queue/exchange deletion metrics** | Low | No delete/purge APIs exist; no metrics for them. |

## 3) Docker Compose

`docker-compose.yml` (repo root) runs the full stack. Services and ports:

| Service | Image | Host ports | Notes |
|---|---|---|---|
| `broker` | built from `./Dockerfile` | `5672` (GOMQ/1), `9091`→`9090` (metrics) | `-metrics-addr=:9090`; healthcheck hits `/metrics`. |
| `prometheus` | `prom/prometheus:latest` | `9090` | Mounts `./prometheus.yml`; scrapes `broker:9090`; config/data volumes. |
| `grafana` | `grafana/grafana:latest` | `3000` | Mounts `./grafana/provisioning` + `./grafana/dashboards`; anonymous viewer. |

```bash
docker compose up -d --build      # build the broker image and start everything
docker compose ps                 # all services should report healthy
docker compose down               # stop (add -v to also drop the data volumes)
```

Notes:
- Prometheus UI keeps `:9090`; the broker's metrics endpoint is published on host `:9091` to avoid the clash.
- Provisioning paths match what Grafana expects (`/etc/grafana/provisioning`, `/var/lib/grafana/dashboards`).
- Kubernetes manifests (`deployment.yaml`, `service.yaml`, `kind-config.yaml`) remain out of scope for this stack.

### Quick verification checklist for compose
- [ ] `docker compose up -d --build` brings all services healthy
- [ ] `curl localhost:9091/metrics` returns Prometheus exposition
- [ ] Prometheus targets show the broker UP (`curl -s 'localhost:9090/api/v1/targets'`)
- [ ] Grafana loads datasource + dashboard automatically (`localhost:3000`)
- [ ] Traffic from benchmark/example clients updates panels
- [ ] Volumes/provisioning are read-only where appropriate

## 4) Recommended next steps
1. **Fix observability confusion**: Add "Total Backlog" panel (queue_messages + sum consumer_inflight) and consider an oldest-message-age metric.
2. **Reduce noise**: Add Grafana template variables for queue/exchange; document filtering by `bench.q.<runid>`.
3. **Close Step 5**: Add a basic metrics assertion test in `tests/` against the registry/handler.
