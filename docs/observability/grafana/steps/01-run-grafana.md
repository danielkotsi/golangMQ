# Step 1 — Run Grafana (docker)

> Part of [GRAFANA_PLAN.md](../GRAFANA_PLAN.md) §3.1.
> Execution order: **you are here** → [Step 2](02-datasource-provisioning.md) → [Step 3](03-dashboard-provisioning.md) → [Step 4](04-explore-choose-save-queries.md) → [Step 5](05-verification.md)

## Goal

Run Prometheus and Grafana side by side in docker so Grafana can consume the broker
metrics. Grafana runs stateless (`--rm`); its configuration lives in `./grafana/`.

## Environment facts

- Linux host → containers reach the host's broker via `host.docker.internal`
  with `--add-host host.docker.internal:host-gateway` (Prometheus already relies
  on this in `prometheus.yml`).
- Neither Prometheus nor Grafana is currently running (`docker ps` empty).
- `prom/prometheus` image present; Grafana image **not** yet pulled.

## Commands

### 1. Grafana image (first run only)

```bash
docker pull grafana/grafana:latest
```

### 2. Run Prometheus (as today)

```bash
docker run -d --rm --name prometheus -p 9090:9090 \
  -v "$(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml" \
  --add-host host.docker.internal:host-gateway \
  prom/prometheus
```

### 3. Run Grafana

```bash
docker run -d --rm --name grafana -p 3000:3000 \
  -e GF_AUTH_ANONYMOUS_ENABLED=true \
  -e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer \
  -e GF_SECURITY_ADMIN_USER=admin \
  -e GF_SECURITY_ADMIN_PASSWORD=admin \
  -v "$(pwd)/grafana/provisioning:/etc/grafana/provisioning" \
  -v "$(pwd)/grafana/dashboards:/var/lib/grafana/dashboards" \
  --add-host host.docker.internal:host-gateway \
  grafana/grafana:latest
```

> Order matters only because both use `--rm`: stop/remove an old container with
> `docker rm -f prometheus grafana` before re-running.

### 4. Run the broker (metrics on :9091)

```bash
go run ./cmd/broker -metrics-addr=:9091 &
```

## Files touched

| File | Change |
|---|---|
| (none yet) | containers only — config lands in Steps 2–3 |

## Done when

- [ ] `curl -s localhost:9090/-/healthy` → `Prometheus is Healthy.`
- [ ] `docker logs grafana` shows provisioning dirs mounted without error
- [ ] `curl -s localhost:3000/api/health` → `"database":"ok"`
- [ ] `curl -s localhost:9091/metrics | grep golangmq_` → series present

## Next

→ [Step 2 — Provision the Prometheus datasource](02-datasource-provisioning.md)