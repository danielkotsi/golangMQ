# Grafana Dashboards — Execution Plan (Goal 2)

**Goal:** Hook Grafana up to the existing Prometheus instance so the `golangmq_*`
metrics have live representations, and establish the workflow for choosing which
queries we want to save to the Grafana configuration.

**Pipeline:** Run Grafana → Provision Prometheus datasource → Provision dashboards
as code → Explore queries → Save the first chosen panels → Verify live.

> **Metric selection is intentionally deferred.** This plan builds the pipeline and
> the "explore → export → commit" workflow. Which specific metrics/queries we want
> to represent is decided later, inside that workflow.

> **Superseded by `docker-compose.yml` (recommended).** The repo now ships a root
> [`docker-compose.yml`](../../../docker-compose.yml) that runs broker + Prometheus +
> Grafana together, fully provisioned — see
> [README › Observability](../../../README.md#observability-prometheus--grafana).
> The manual `docker run` steps below are kept for the host-run-broker variant; that
> flow reaches the broker over `host.docker.internal` and therefore needs a local
> `prometheus.yml` override (target `host.docker.internal:9091`), because the committed
> `prometheus.yml` now targets the compose service `broker:9090`.

---

## 0. Current state (baseline)

| Aspect | Status |
|---|---|
| Broker metrics | 15 `golangmq_*` counters + 6 gauges on `/metrics` (Goal 1, done) |
| Prometheus | Runs in docker (`prom/prometheus`), scrapes `host.docker.internal:9091`, UI on `:9090` |
| Broker | Started locally via `go run ./cmd/broker -metrics-addr=:9091` (9091 so Prometheus keeps 9090) |
| Grafana | Not deployed yet — this plan |
| Kubernetes | **Out of scope** for now — local docker only |

**Environment facts verified:**

- `prom/prometheus` image present locally; no Grafana image yet (pulled in Step 1).
- No containers currently running; Prometheus must be (re)started in Step 1.
- Linux host → the broker runs on the docker host, so Prometheus/Grafana containers
  reach it via `host.docker.internal` with `--add-host host.docker.internal:host-gateway`
  (this is how the existing `prometheus.yml` already targets `:9091`).

**Target architecture:**

```
localhost:5672   broker (GOMQ/1)            go run ./cmd/broker -metrics-addr=:9091
localhost:9091   broker /metrics  ─┐
                                  │ scrape
localhost:9090   Prometheus (docker) ─┘ targets host.docker.internal:9091
                                  │ datasource
localhost:3000   Grafana (docker)   ─┘ url http://host.docker.internal:9090
```

---

## 1. Key decisions (locked)

1. **Deployment:** local docker next to the existing Prometheus demo. No k8s
   (explicitly out of scope for now).

2. **Auth — lean dev setup:**
   - Anonymous access **enabled with `Viewer` role** → anyone can watch dashboards
     without logging in (the "live representation" consumption path).
   - Default `admin/admin` remains for curation/editing during query selection.

3. **Query persistence — provisioning-as-code:**
   - Datasource and dashboards are committed to the repo:
     - `grafana/provisioning/datasources/prometheus.yml`
     - `grafana/provisioning/dashboards/dashboards.yml`
     - `grafana/dashboards/*.json` (the actual chosen queries, as panels)
   - Grafana mounts these folders at container start; the dashboard file provider
     has a short `updateIntervalSeconds` so edited JSON reloads without restarts.
   - **The "which queries we save" question === which panels we commit into
     `grafana/dashboards/`.** Nothing is stored only inside Grafana's DB.

4. **Orchestration:** a root `docker-compose.yml` now runs the full stack
   (broker + Prometheus + Grafana) with provisioning and healthchecks. The manual
   `docker run` commands in this plan remain as the host-run-broker variant.

5. **No Go code changes** in this step — ops config + docs only.

---

## 2. Files to add (all new)

```
grafana/
  provisioning/
    datasources/prometheus.yml     # Prometheus datasource → http://host.docker.internal:9090
    dashboards/dashboards.yml      # file provider → /var/lib/grafana/dashboards
  dashboards/
    golangmq-overview.json         # starter dashboard skeleton (panels chosen later)
docs/observability/grafana/
  GRAFANA_PLAN.md                  # this file
  steps/
    01-run-grafana.md              # pull image, run container, env flags, mounts
    02-datasource-provisioning.md  # datasources/prometheus.yml + UI verification
    03-dashboard-provisioning.md   # dashboards.yml provider + starter JSON
    04-explore-choose-save-queries.md  # the "we decide metrics later" workflow
    05-verification.md             # end-to-end smoke
```

No existing files change. `deployment.yaml`/`service.yaml`/`kind-config.yaml`
touch nothing — k8s deferred.

---

## 3. Step breakdown

### 3.1 — Run Grafana

- `docker pull grafana/grafana:latest` (needed; not pulled yet).
- Run with:
  - `-p 3000:3000` (Grafana UI), `--name grafana`, `--rm`
  - `GF_AUTH_ANONYMOUS_ENABLED=true`, `GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer`
  - `GF_SECURITY_ADMIN_USER=admin`, `GF_SECURITY_ADMIN_PASSWORD=admin`
  - `-v ./grafana/provisioning:/etc/grafana/provisioning`
  - `-v ./grafana/dashboards:/var/lib/grafana/dashboards`
  - `--add-host host.docker.internal:host-gateway`
- Also (re)start Prometheus the same way it runs today: mount `prometheus.yml`,
  `-p 9090:9090`, same `--add-host`.
- **Lean container:** stateless `--rm`; the repo folders are the source of truth.

### 3.2 — Provision the Prometheus datasource

`grafana/provisioning/datasources/prometheus.yml`:

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

Verify in UI: *Configuration → Data sources → Prometheus* shows reachable (green ✓),
and *Explore* returns series (`golangmq_*`, `go_*`, `process_*`).

### 3.3 — Provision dashboards as code

`grafana/provisioning/dashboards/dashboards.yml`:

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

Commit a **starter** `grafana/dashboards/golangmq-overview.json` — a minimal
dashboard skeleton so the mechanism is provably working before we decide real
panels (see template in the step file).

### 3.4 — Explore, choose, save (metric decision happens here, later)

The workflow we lock in now, used when we pick the actual metrics:

1. Start broker + traffic (`exampleClients/publisher`, `exampleClients/consumer`).
2. In Grafana *Explore*, iterate PromQL against the live `golangmq_*` series.
3. Build the working dashboard as a **scratch (un-provisioned) dashboard** so it
   is fully editable in the UI.
4. When we agree on a set of panels → **Export JSON → overwrite
   `grafana/dashboards/golangmq-overview.json`** → the provider auto-reloads it.
5. Delete the scratch dashboard; the committed file is now "the queries we saved".

Candidate query ideas documented as **examples only** (not finalized):
`rate(golangmq_publish_total[1m])`, `rate(golangmq_deliver_total[1m])`,
`golangmq_queue_messages`, `golangmq_consumers`, connection open/close counts.

### 3.5 — Verification

- Broker: `curl localhost:9091/metrics` returns exposition text.
- Prometheus: `curl localhost:9090/-/healthy` up; targets show the broker UP.
- Grafana: `curl localhost:3000/api/health` OK; dashboard auto-imports; anonymous
  view shows it without a login.
- Live check: panels move while traffic flows (publish → deliver → ack), mirroring
  the Goal-1 smoke style.

---

## 4. Acceptance criteria (Goal 2 complete)

- [ ] `grafana/grafana` container runs on `:3000`; provisioning folders mounted
- [ ] Prometheus datasource provisioned, default, green ✓ in the UI
- [ ] `grafana/provisioning/{datasources,dashboards}/*.yml` committed and loading
- [ ] Starter `grafana/dashboards/golangmq-overview.json` committed and auto-imported
- [ ] Anonymous viewer can open the dashboard without login; admin can edit
- [ ] Dashboard JSON edits on disk auto-reload (`updateIntervalSeconds`)
- [ ] End-to-end live smoke: broker traffic moves at least one panel
- [ ] No Go source changes, no k8s changes

---

## 5. Documented workflow for "which queries to save"

After this plan, choosing metrics is a repeatable 5-minute loop:

> Explore → agree → Export JSON → overwrite `grafana/dashboards/*.json` →
> commit. The committed JSON is the saved Grafana configuration.

---

## 6. Out of scope for now

- Deciding the actual metrics/panels (explicitly later — the loop in §5/Step 4)
- Alert rules & threshold notifications
- Kubernetes deployment of Grafana/Prometheus
- Auth beyond anonymous-viewer + default admin
- Persisting Grafana's internal DB between runs (stateless `--rm` container)
- Histograms / latency panels (metrics don't exist yet)

**Suggested execution order:** §3.1 → §3.2 → §3.3 (mechanism proven) → §3.5 smoke
→ then start the §3.4 loop for real metric selection.

Each step is expanded into its own file under [`steps/`](steps/) — start at
[Step 1](steps/01-run-grafana.md).