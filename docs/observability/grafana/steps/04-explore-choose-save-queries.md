# Step 4 — Explore, choose, save the queries

> Part of [GRAFANA_PLAN.md](../GRAFANA_PLAN.md) §3.4.
> Execution order: [Step 1](01-run-grafana.md) → [Step 2](02-datasource-provisioning.md) → [Step 3](03-dashboard-provisioning.md) → **you are here** → [Step 5](05-verification.md)

## Goal

Lock in the **repeatable workflow** we use whenever we choose which metrics to put
on the dashboard. The specific panel set is deliberately **not** decided in this
plan — this step is the process, the metric choice happens per iteration.

## The loop

```
Start traffic → Explore queries → agree on panels → Export JSON
→ overwrite grafana/dashboards/golangmq-overview.json → commit
```

## Steps

### 1. Generate traffic

```bash
go run ./cmd/broker -metrics-addr=:9091 &   # if not already running
go run ./exampleClients/publisher
go run ./exampleClients/consumer
```

### 2. Explore

Grafana → *Explore* (left sidebar), datasource `Prometheus`. Iterate live PromQL
against the `golangmq_*` series. The datasource is `editable`, so ad-hoc queries
are cheap to try.

### 3. Build a scratch dashboard

Create a new **un-provisioned** dashboard (not under `GolangMQ` folder) so it is
fully editable in the UI. Add the panels we agree on, set sensible time range and
refresh.

### 4. Export → commit

- *Share → Export → Save JSON to file* (or copy JSON, or use the Provisioning
  *Export* action).
- Overwrite `grafana/dashboards/golangmq-overview.json`, keeping the `uid`
  `golangmq-overview` stable so the provisioned dashboard keeps its identity.
- The provider's `updateIntervalSeconds: 10` reloads it automatically.
- Delete the scratch dashboard; the committed JSON is now **the saved Grafana
  configuration**.

### 5. Commit to the repo

```bash
git add grafana/dashboards/golangmq-overview.json
git commit -m "chore(grafana): add golangmq overview dashboard panels"
```

## Candidate queries — examples only (not finalized)

| Panel idea | Example query |
|---|---|
| Publish / deliver / ack rates | `rate(golangmq_publish_total[1m])`, `rate(golangmq_deliver_total[1m])`, `rate(golangmq_ack_total[1m])` |
| Queue depth heat (current) | `golangmq_queue_messages` (gauge) |
| Consumers per queue | `golangmq_queue_consumers` (gauge) |
| Queue growth during load | `rate(golangmq_enqueue_total[1m])` vs `rate(golangmq_deliver_total[1m])` |
| Connections over time | `golangmq_connections`; open/closed as `rate(golangmq_connections_opened_total[1m])` |
| Silent drops visibility | `golangmq_publish_unroutable_total`, `golangmq_drop_total` |

These are the *menu*; Step 4 of a later run picks the winners.

## Done when

- [ ] Workflow documented above is repeatable in ~5 minutes
- [ ] At least one real panel replaces the placeholder
- [ ] Committed JSON reloads into the provisioned dashboard automatically
- [ ] Scratch dashboard removed after export

## Next

→ [Step 5 — Verification](05-verification.md)