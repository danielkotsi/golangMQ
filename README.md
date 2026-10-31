

https://github.com/user-attachments/assets/ce08a0ca-10ec-47f4-8597-9ec4440dd6c8

# GolangMQ

A lightweight, in-memory message broker written in Go, implementing a custom JSON protocol over TCP with channel multiplexing inspired by AMQP 0-9-1.

## Features

- **Channel multiplexing** — multiple logical channels per TCP connection, identified by uint16 channel IDs
- **Exchange/queue routing** — declare exchanges, queues, and bind them with routing keys
- **Ack/Nack support** — consumer acknowledgment with requeue or dead-letter routing
- **Dead-letter queues** — nack'd messages route to a DLX exchange and DLQ 
- **Prefetch-based flow control** — inflight message tracking per consumer
- **Prometheus metrics & Grafana dashboards** — built-in observability (see below)

## Architecture

```
┌─────────────────────────────────────┐
│  TCP Connection (net.Conn)           │
├─────────────────────────────────────┤
│  JSON + Newline Framing              │
│  (protocol/coder-decoder.go)        │
├─────────────────────────────────────┤
│  Connection (Envelope Dispatch)      │
│  └── Route to Channel (via uint16)  │
├─────────────────────────────────────┤
│  Channel Handlers                    │
│  (Publish, Consume, Ack, Declare)   │
├─────────────────────────────────────┤
│  Broker Core                          │
│  (Exchanges → Queues → Consumers)    │
└─────────────────────────────────────┘
```

Each TCP connection supports multiple logical channels. Frames are wrapped in an `Envelope` containing a `ChannelID` and `RequestID`, allowing the server to demultiplex and correlate requests/responses over a single socket. A dedicated write-pump goroutine serializes all outgoing frames, eliminating mutex contention on writes.

## Quick Start

### 1. Start the broker

```bash
go run ./cmd/broker
```

You should see:

```
MQ server started on :5672
```

### 2. Publish messages

In a second terminal:

```bash
go run exampleClients/publisher/main.go
```

This declares an `emails` exchange, an `email_queue` with a dead-letter exchange (`dlx`), binds them with routing key `email.sent`, and publishes 3 messages.

### 3. Consume messages

In a third terminal:

```bash
go run exampleClients/consumer/main.go
```

Three workers start:
- **Worker A** and **Worker B** consume from `email_queue` and ack messages
- **Worker C** consumes from `dlq` (dead-letter queue)

Every 3rd message is nack'd without requeue, routing it to the DLQ where Worker C picks it up.

### 4. Publish more messages

Go back to the publisher terminal and run it again:

```bash
go run exampleClients/publisher/main.go
```

Watch the consumer terminal — messages flow through, nack'd ones land in the DLQ. Run the publisher as many times as you like to keep feeding messages.

## SDK Usage

The example clients use the [Go SDK](https://github.com/danielkotsi/golangMQSDK) - A client library to use the broker
```go
import "github.com/danielkotsi/golangMQSDK/gomqSDK"
import "github.com/danielkotsi/golangMQSDK/protocol"
```

**Connect:**

```go
cfg := gomqSDK.Config{
    ClientName:   "my-client",
    Username:     "daniel",
    Password:     "123456789",
    ChannelMax:   10,
    FrameMax:     10372,
    HeartbeatSec: 10,
}

c, err := gomqSDK.Connect("localhost:5672", cfg)
```

**Open a channel:**

```go
channel, err := c.OpenChannel(ctx)
```

**Declare exchange and queue:**

```go
channel.DeclareExchange("emails")
channel.DeclareQueue("email_queue", ctx, "dlx", "dead_emails")
channel.BindQueue("email_queue", "emails", "email.sent", ctx)
```

**Publish:**

```go
err = channel.Publish(protocol.Publish{
    Exchange:   "emails",
    RoutingKey: "email.sent",
    Body:       body,
})
```

**Consume:**

```go
incoming, err := channel.Consume("email_queue", ctx)
```

## Observability (Prometheus & Grafana)

The broker exposes Prometheus metrics and ships with Grafana provisioning for live monitoring.

### Quick start (docker compose)

Bring the whole stack up — broker, Prometheus, and Grafana, fully provisioned:

```bash
docker compose up -d --build
```

- **Broker** (GOMQ/1): `localhost:5672`
- **Broker metrics** (ad-hoc curl): `http://localhost:9091/metrics`
- **Prometheus**: http://localhost:9090 — scrapes the broker at `broker:9090` over the compose network
- **Grafana**: http://localhost:3000 — dashboard "GolangMQ / GolangMQ Overview", anonymous viewer access

Tear down with `docker compose down` (add `-v` to also drop the Prometheus/Grafana data volumes). See [`docs/observability/OBSERVABILITY_STATUS.md`](docs/observability/OBSERVABILITY_STATUS.md) for the full setup and known gaps.

- **Metrics endpoint**: Broker exposes `/metrics` (configurable via flags/env). Key metrics include `golangmq_publish_total`, `golangmq_enqueue_total`, `golangmq_deliver_total`, `golangmq_ack_total`, `golangmq_nack_total`, `golangmq_dead_letter_total`, `golangmq_drop_total`, `golangmq_queue_messages`, `golangmq_queue_consumers`, `golangmq_consumer_inflight`, plus connection/consumer counters and Go/process collectors.
- **Prometheus**: `prometheus.yml` scrapes the broker at `broker:9090` inside the compose network.
- **Grafana**: Dashboards and provisioning live under `grafana/` (`grafana/provisioning/`, `grafana/dashboards/`). The overview dashboard tracks connections/queues/exchanges/consumers, publish/deliver rates, queue depth, ack/nack rates, errors/DLQ/drops, consumers/inflight, and connections/errors.
- **Docs**: See [`docs/observability/`](docs/observability/) for plans, step-by-step setup, and [`docs/observability/OBSERVABILITY_STATUS.md`](docs/observability/OBSERVABILITY_STATUS.md) for current status and known gaps.

## Benchmarking

### Benchmark CLI
The repo includes a purpose-built benchmarking CLI at `cmd/benchmark` with a topology-aware harness (`internal/bench/`). It supports:
- Deterministic topology resolution (connections/channels/roles, publish/consume-capable sets)
- Order-sensitive distribution flags (`-conns`, `-channels`, `-pub-channels` with `-distribution even|uneven` and caps)
- Publisher/consumer goroutines, message size/ack control, warmup, duration-based runs with drain
- Per-interval CSV sampling and JSON summary output

### Running scenarios
Use the reusable runner script:

```bash
# Run all scenarios from a scenarios JSON
./scripts/run_baseline.sh scenarios/baseline.json

# Run a specific scenario
./scripts/run_baseline.sh scenarios/baseline.json -n single-conn-baseline

# Reuse an existing broker or use a custom address
./scripts/run_baseline.sh scenarios/baseline.json --addr 127.0.0.1:5672
./scripts/run_baseline.sh scenarios/baseline.json --no-start-broker
```

Scenario definitions live in `scenarios/` (e.g. `scenarios/baseline.json`, `scenarios/scenario.json`). See [`scenarios/README.md`](scenarios/README.md) and [`scenarios/baseline-scenarios.md`](scenarios/baseline-scenarios.md) for details on flags and topology rules.

### Results location
Results are saved under:
```
results/baselines/<git-sha-short>/<scenario-set>/<timestamp>/
├── manifest.json   # commit, env, broker config, scenarios file, filter, broker reuse info
├── <scenario>.csv  # interval samples (per -report)
└── <scenario>.json # pretty-printed summary (totals, rates, latencies, topology)
```
Each run creates a new timestamped directory. The runner can reuse an existing broker (and will not kill it unless it started it).

### Current broker performance (1M msg, 3 fresh-broker runs)
Based on [`docs/benchmark-1m-3runs-experiment.md`](docs/benchmark-1m-3runs-experiment.md) (all scenarios: published=consumed=1,000,000, drops=0, duplicates=0):

| Scenario | Topology (conns/pubs/cons) | Msgs/publisher | Mean throughput (msg/s) | Rate CV | Delivery p95 | Notes |
|---|---|---|---|---|---|---|
| `01-baseline` | 1 mixed / 1 / 1 | 1,000,000 | ~9,145 | 3.4% | 64.0–72.3 s | Reference (single consumer bottleneck) |
| `02-publishers-up` | 1 mixed / 64 / 1 | 15,625 | ~9,893 | 3.9% | 83.7–91.0 s | Consumer bottleneck confirmed (more pubs don't help) |
| `03-consumers-up` | 1 mixed / 1 / 64 | 1,000,000 | ~16,310 | 1.1% | 0.15–0.67 s | Publisher bottleneck confirmed (queue stays short) |
| `04-both-up` | 1 mixed / 64 / 64 | 15,625 | ~17,362 | 0.7% | 43.3–45.5 s | Shared single connection/channel contention |
| `05-publisher-conns-up` | 16 pub + 1 cons / 64 / 64 | 15,625 | ~18,508 | 0.7% | 47.0–48.8 s | Extra pub conns help little (consumer conn limits) |
| `06-consumer-conns-up` | 1 pub + 16 cons / 64 / 64 | 15,625 | ~32,770 | 4.2% | 14.8–15.3 s | Consumer connections help significantly |
| `07-both-conns-up` | 16 pub + 16 cons / 64 / 64 | 15,625 | ~29,625 | 4.9% | 27.2–30.4 s | Broker contention (single exchange/queue) reduces gains |

**Bottlenecks verified:** single consumer is the limit when overloaded (02), single publisher limits when consumers are plentiful (03), one consumer connection is a major constraint (05), and adding connections on both sides creates contention on the single queue/dispatch path (07 drops vs 06). Delivery p95 includes queue wait time (backlog), not just broker latency.

## Ongoing Work & Planned Improvements

- **Standardize baseline scenarios for CI regression tests**: Turn the experiment scenarios into stable, reusable baselines in `scenarios/` and wire them into CI with regression gates (e.g. fail if throughput drops >10–15%, with wider tolerances for noisy p95/ack metrics).
- **Cover untested dimensions**: The 1M experiment did **not** vary the **number of queues**. Testing multiple queues/routing patterns may reveal different bottlenecks and behaviors (dispatch fairness, lock contention).
- **Explore sustained-duration benchmarks**: Add rate-limited publishers and `-duration`-based sustained runs to expose publisher saturation more clearly (some bursty cases hide it).
- **Observability polish**: Add "Total Backlog = queue_messages + sum(consumer_inflight)" panels, message age/oldest-message metrics, queue/exchange template variables, and Go runtime panels. See [`docs/observability/OBSERVABILITY_STATUS.md`](docs/observability/OBSERVABILITY_STATUS.md).
- **Clean Docker Compose stack**: ✅ Root `docker-compose.yml` runs broker, Prometheus, and Grafana together with provisioning, healthchecks, and aligned ports/network. See [`docs/observability/OBSERVABILITY_STATUS.md`](docs/observability/OBSERVABILITY_STATUS.md) for the remaining observability gaps.
- **Metrics test coverage**: Add tests that assert metric values increase after publish/consume cycles to close observability verification.
- **Handle tag space & edge cases**: Continue investigating uint16 delivery-tag wraparound behavior under high backlog (warned by benchmark in large runs).

## Tests

```bash
go test ./...
go test -race ./...
```
