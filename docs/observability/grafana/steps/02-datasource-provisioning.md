# Step 2 — Provision the Prometheus datasource

> Part of [GRAFANA_PLAN.md](../GRAFANA_PLAN.md) §3.2.
> Execution order: [Step 1](01-run-grafana.md) → **you are here** → [Step 3](03-dashboard-provisioning.md) → [Step 4](04-explore-choose-save-queries.md) → [Step 5](05-verification.md)

## Goal

Make Grafana aware of the Prometheus instance via provisioning, so the datasource
exists on every container start without manual setup.

## New file: `grafana/provisioning/datasources/prometheus.yml`

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://host.docker.internal:9090
    isDefault: true
    editable: true
```

Notes:

- `access: proxy` → Grafana proxies queries server-side; no browser CORS issues.
- `url` points from inside the Grafana container back to the host's Prometheus
  `:9090` via `host.docker.internal` (needs the `--add-host` from Step 1).
- `isDefault: true` → panels/dashboards use it automatically.
- `editable: true` → still tweakable from the UI while we curate queries.

## Files touched

| File | Change |
|---|---|
| `grafana/provisioning/datasources/prometheus.yml` | **new** — datasource manifest |

## Done when

- [ ] Datasource appears in UI: *Configuration → Data sources → Prometheus* (green ✓)
- [ ] *Explore* returns series: `golangmq_*`, `go_*`, `process_*`
- [ ] No manual datasource creation needed on restart (provisioned)

## Next

→ [Step 3 — Provision dashboards as code](03-dashboard-provisioning.md)