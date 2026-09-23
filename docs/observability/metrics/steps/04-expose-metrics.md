# Step 4 — Expose `/metrics`

> Part of [METRICS_PLAN.md](../METRICS_PLAN.md) §4.
> Execution order: [Step 1](01-add-prometheus-client.md) → [Step 2](02-define-metrics.md) → [Step 3.1](03-1-thread-metrics.md) → [Step 3.2](03-2-hook-points.md) → **you are here** → [Step 5](05-verification.md)

## Goal

Serve the registry on an HTTP `/metrics` endpoint Prometheus can scrape, and expose
the port in the deployment manifests.

## 4.1 Handler — `broker/server.go` (or `broker/metrics.go`)

```go
func (s *Server) MetricsHandler() http.Handler {
    return promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{})
}
```

Imports: `net/http`, `github.com/prometheus/client_golang/prometheus/promhttp`.

## 4.2 Startup — `cmd/broker/main.go`

First use of `flag` in the repo:

```go
package main

import (
    "GolangRabbitMQBroker/broker"
    "flag"
    "log"
    "net/http"
)

func main() {
    addr := flag.String("addr", ":5672", "broker listen address")
    metricsAddr := flag.String("metrics-addr", ":9090", "metrics listen address (empty disables)")
    flag.Parse()

    serverconfig := &broker.ServerConfig{
        ChannelMax:   10,
        FramesMax:    10372,
        HeartbeatSec: 10,
    }
    server := broker.NewServer(*addr, *serverconfig)

    if *metricsAddr != "" {
        go func() {
            mux := http.NewServeMux()
            mux.Handle("/metrics", server.MetricsHandler())
            log.Printf("metrics listening on %s/metrics", *metricsAddr)
            log.Println(http.ListenAndServe(*metricsAddr, mux))
        }()
    }

    log.Printf("MQ server started on %s", *addr)
    if err := server.ListenAndServe(); err != nil {
        log.Println(err)
        return
    }
}
```

Metrics live on a **separate port** — `:5672` speaks `GOMQ/1`, not HTTP.
Default metrics port `:9090`, disable with `-metrics-addr=""`.

## 4.3 Deployment surface

| File | Change |
|---|---|
| `Dockerfile` | `EXPOSE 5672 9090` |
| `deployment.yaml` | add `containerPort: 9090`; add pod annotations `prometheus.io/scrape: "true"`, `prometheus.io/port: "9090"`, `prometheus.io/path: "/metrics"` |
| `service.yaml` | only if Prometheus runs outside the cluster — otherwise pod annotations suffice |

## Files touched

| File | Change |
|---|---|
| `broker/server.go` (or `metrics.go`) | `MetricsHandler()` |
| `cmd/broker/main.go` | flags + metrics goroutine |
| `Dockerfile` | `EXPOSE 5672 9090` |
| `deployment.yaml` | containerPort + scrape annotations |
| `service.yaml` | optional NodePort/annotations |

## Done when

- [ ] `go build ./...` passes
- [ ] Broker boots and logs `metrics listening on :9090/metrics`
- [ ] `curl -s localhost:9090/metrics` returns Prometheus exposition text
- [ ] `go test -race ./tests/ -count=1` → **30/30 PASS** (tests don't start the HTTP server — no port conflicts)

## Next

→ [Step 5 — Verification](05-verification.md)
