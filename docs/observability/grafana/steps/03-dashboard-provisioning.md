# Step 3 — Provision dashboards as code

> Part of [GRAFANA_PLAN.md](../GRAFANA_PLAN.md) §3.3.
> Execution order: [Step 1](01-run-grafana.md) → [Step 2](02-datasource-provisioning.md) → **you are here** → [Step 4](04-explore-choose-save-queries.md) → [Step 5](05-verification.md)

## Goal

Wire a file-based dashboard provider and commit a starter dashboard skeleton, proving
that "the queries we save" === "the JSON we commit under `grafana/dashboards/`".

## New file: `grafana/provisioning/dashboards/dashboards.yml`

```yaml
apiVersion: 1

providers:
  - name: golangmq
    orgId: 1
    folder: GolangMQ
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    options:
      path: /var/lib/grafana/dashboards
```

Notes:

- `type: file` + `options.path` → every `*.json` in the mounted folder is imported.
- `updateIntervalSeconds: 10` → editing the JSON on disk **auto-reloads** the
  dashboard without restarting Grafana (fast explore → export → iterate loop).
- `folder: GolangMQ` → dashboards group under one dashboard folder.
- `disableDeletion: false` → safe to delete stray dashboards while iterating.

## New file: `grafana/dashboards/golangmq-overview.json` (starter skeleton)

A minimal dashboard so the pipeline is provably live **before** we choose the real
panels in [Step 4](04-explore-choose-save-queries.md). It carries one placeholder
panel wired to the provisioned Prometheus datasource. Template:

```json
{
  "title": "GolangMQ Overview",
  "uid": "golangmq-overview",
  "schemaVersion": 39,
  "version": 1,
  "tags": ["golangmq"],
  "timezone": "browser",
  "refresh": "10s",
  "panels": [
    {
      "type": "timeseries",
      "title": "Publish rate (placeholder — replace in Step 4)",
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "targets": [
        {
          "expr": "rate(golangmq_publish_total[1m])",
          "legendFormat": "{{exchange}}",
          "refId": "A"
        }
      ]
    }
  ]
}
```

> Datasource `uid: "prometheus"` must match `uid` in
> `grafana/provisioning/datasources/prometheus.yml`. If none is set there, Grafana
> auto-generates one from the datasource `name`; prefer setting
> `uid: prometheus` explicitly in the datasource manifest (Step 2) to make the
> dashboard JSON deterministic.

## Files touched

| File | Change |
|---|---|
| `grafana/provisioning/dashboards/dashboards.yml` | **new** — file provider |
| `grafana/dashboards/golangmq-overview.json` | **new** — starter skeleton |

## Done when

- [ ] *Dashboards → GolangMQ → GolangMQ Overview* appears (no manual import)
- [ ] Dashboard loads data from the Prometheus datasource
- [ ] Editing the JSON on disk (e.g. panel title) shows up within ~10s, no restart
- [ ] Anonymous (no login) view of the dashboard works

## Next

→ [Step 4 — Explore, choose, save the queries](04-explore-choose-save-queries.md)