# Step 5 — Verification (end-to-end smoke)

> Part of [GRAFANA_PLAN.md](../GRAFANA_PLAN.md) §3.5.
> Execution order: [Step 1](01-run-grafana.md) → [Step 2](02-datasource-provisioning.md) → [Step 3](03-dashboard-provisioning.md) → [Step 4](04-explore-choose-save-queries.md) → **you are here (last)**

## Goal

Prove the whole pipeline — broker → Prometheus → Grafana → live dashboard — with
real traffic, and confirm the provisioning config reloads from the repo.

## 1. Component health

```bash
curl -s localhost:9091/metrics | grep -c '^golangmq_'     # broker series
curl -s localhost:9090/-/healthy                          # Prometheus Healthy
curl -s localhost:3000/api/health                         # Grafana db ok

# Prometheus is actually scraping the broker:
curl -s 'localhost:9090/api/v1/targets' | grep -A2 '$golangmq' | head
```

## 2. Datasource

- UI: *Configuration → Data sources → Prometheus* → green ✓
- *Explore* runs a query and returns `golangmq_*` series.

## 3. Dashboard is provisioned + live

- *Dashboards → GolangMQ → GolangMQ Overview* present without manual import.
- Open in an incognito window (anonymous `Viewer`) — no login needed.
- **Live check:** run traffic, watch a panel move:

```bash
go run ./exampleClients/publisher   # run a few times
go run ./exampleClients/consumer
```

Refresh the dashboard → publish/deliver/ack panels change.

## 4. Provisioning reload (config-as-code)

Edit `grafana/dashboards/golangmq-overview.json` (e.g. change a panel title); within
~10s the dashboards reflects it with **no container restart**. Revert after testing.

## Acceptance criteria (Goal 2 complete)

- [ ] Grafana container runs on `:3000`; provisioning folders mounted
- [ ] Prometheus datasource provisioned, default, green ✓
- [ ] `grafana/provisioning/{datasources,dashboards}/*.yml` committed + loading
- [ ] Starter `grafana/dashboards/golangmq-overview.json` committed + auto-imported
- [ ] Anonymous viewer can open dashboard without login; admin can edit
- [ ] Dashboard JSON edits on disk auto-reload
- [ ] End-to-end live smoke: broker traffic moves at least one panel
- [ ] No Go source changes, no k8s changes

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Grafana can't reach Prometheus | Datasource `url` host wrong; `--add-host host.docker.internal:host-gateway` missing |
| Dashboard not imported | `dashboards.yml` `options.path` doesn't match the mounted folder |
| Panels show "No data" | Prometheus isn't scraping the broker (check targets); broker not on `:9091` |
| `host.docker.internal` not resolving | Linux docker without the `--add-host` flag |
| JSON edits don't reload | `updateIntervalSeconds` too high, or file is being watched by another process |