# Benchmark Experiment: 1M Messages, 3 Fresh-Broker Runs

## Purpose

This experiment establishes a repeatable baseline for regression testing the broker under
a constant workload while varying topology. The goal is to see which side becomes the
bottleneck as publishers, consumers, and their dedicated connections are scaled.

## Scenario matrix

All scenarios use the same workload: **1,000,000 total messages**, **64-byte payloads**,
acks enabled, `-duration 0` (run to completion), `-warmup 0s`, `-report 1s`, and
`-drain-timeout 5m`.

| Scenario | Connections | Publishers | Consumers | Msgs/publisher | Total msgs | Expected bottleneck |
|---|---:|---:|---:|---:|---:|---|
| `01-baseline` | 1 mixed | 1 | 1 | 1,000,000 | 1,000,000 | Reference |
| `02-publishers-up` | 1 mixed | 64 | 1 | 15,625 | 1,000,000 | Consumer |
| `03-consumers-up` | 1 mixed | 1 | 64 | 1,000,000 | 1,000,000 | Publisher |
| `04-both-up` | 1 mixed | 64 | 64 | 15,625 | 1,000,000 | Single connection/channel |
| `05-publisher-conns-up` | 16 pub + 1 cons | 64 | 64 | 15,625 | 1,000,000 | Consumer connection |
| `06-consumer-conns-up` | 1 pub + 16 cons | 64 | 64 | 15,625 | 1,000,000 | Publisher connection |
| `07-both-conns-up` | 16 pub + 16 cons | 64 | 64 | 15,625 | 1,000,000 | Broker queue/dispatch |

The scenario definitions live in [`scenarios/scenario.json`](../scenarios/scenario.json).

## Method

For each of the 3 benchmark runs:

1. A **fresh broker instance** was started on `127.0.0.1:5672`.
2. The full scenario set was run sequentially against that broker.
3. The broker was stopped before the next run began.

The runner records `broker_started_by_us=true` in each `manifest.json`, with a different
broker PID per run. No benchmark was run in parallel with another.

Command used for each run:

```bash
./scripts/run_baseline.sh scenarios/scenario.json --addr 127.0.0.1:5672
```

Results are stored under:

- Run 1: `results/baselines/e9fbdb1/scenario/20261031-121452`
- Run 2: `results/baselines/e9fbdb1/scenario/20261031-122231`
- Run 3: `results/baselines/e9fbdb1/scenario/20261031-123013`

### Correctness checks

Across all 21 scenario runs (7 scenarios × 3 runs):

- `published = consumed = 1,000,000`
- `drops = 0`
- `duplicates = 0`

The workload exceeds the broker's uint16 delivery-tag space, so the benchmark emitted its
wrap warning. Despite multiple tag wraps, no drops or duplicate deliveries were observed.

## Results

### Table 1: All 3 runs

| Scenario | Run | Elapsed (s) | Throughput (msg/s) | Delivery p50 (s) | Delivery p95 (s) | Ack p95 (µs) |
|---|---:|---:|---:|---:|---:|---:|
| 01-baseline | 1 | 104.59 | 9,561 | 44.14 | 64.05 | 186 |
| 01-baseline | 2 | 113.41 | 8,818 | 48.38 | 72.27 | 195 |
| 01-baseline | 3 | 110.42 | 9,056 | 44.42 | 66.93 | 195 |
| 02-publishers-up | 1 | 96.62 | 10,349 | 48.41 | 83.67 | 143 |
| 02-publishers-up | 2 | 106.25 | 9,412 | 53.52 | 90.96 | 161 |
| 02-publishers-up | 3 | 100.82 | 9,918 | 51.13 | 87.13 | 147 |
| 03-consumers-up | 1 | 61.71 | 16,204 | 0.021 | 0.147 | 2,056 |
| 03-consumers-up | 2 | 60.38 | 16,563 | 0.030 | 0.154 | 2,325 |
| 03-consumers-up | 3 | 61.87 | 16,163 | 0.037 | 0.667 | 2,221 |
| 04-both-up | 1 | 57.52 | 17,384 | 27.68 | 44.81 | 1,489 |
| 04-both-up | 2 | 57.17 | 17,491 | 27.24 | 43.34 | 2,092 |
| 04-both-up | 3 | 58.10 | 17,211 | 28.48 | 45.51 | 1,370 |
| 05-publisher-conns-up | 1 | 53.84 | 18,573 | 27.41 | 47.04 | 1,507 |
| 05-publisher-conns-up | 2 | 54.55 | 18,330 | 27.81 | 48.75 | 1,254 |
| 05-publisher-conns-up | 3 | 53.70 | 18,622 | 25.39 | 47.86 | 1,352 |
| 06-consumer-conns-up | 1 | 32.42 | 30,843 | 8.65 | 15.28 | 65 |
| 06-consumer-conns-up | 2 | 29.92 | 33,422 | 8.17 | 14.95 | 62 |
| 06-consumer-conns-up | 3 | 29.37 | 34,045 | 7.93 | 14.84 | 62 |
| 07-both-conns-up | 1 | 36.11 | 27,691 | 18.16 | 30.43 | 76 |
| 07-both-conns-up | 2 | 32.11 | 31,142 | 16.23 | 27.17 | 65 |
| 07-both-conns-up | 3 | 33.28 | 30,043 | 16.52 | 27.85 | 71 |

### Table 2: Consistency across runs

| Scenario | Mean rate (msg/s) | Rate CV | Delivery p95 range (s) |
|---|---:|---:|---:|
| 01-baseline | 9,145 | 3.4% | 64.0–72.3 |
| 02-publishers-up | 9,893 | 3.9% | 83.7–91.0 |
| 03-consumers-up | 16,310 | 1.1% | 0.15–0.67 |
| 04-both-up | 17,362 | 0.7% | 43.3–45.5 |
| 05-publisher-conns-up | 18,508 | 0.7% | 47.0–48.8 |
| 06-consumer-conns-up | 32,770 | 4.2% | 14.8–15.3 |
| 07-both-conns-up | 29,625 | 4.9% | 27.2–30.4 |

## Evaluation

### Repeatability and regression gates

CV is the run-to-run variation as a percentage of the average. A 0.7–4.9% CV means the
three runs produced similar throughput, so differences between future runs are likely
real. That supports a regression gate such as "fail if throughput drops more than
10–15%." Ack p95 varies more because it measures individual ack round-trips, and with many
consumers sharing a connection, scheduling noise affects it more. Scenarios 03 and 04 are
especially noisy, so use a wider tolerance (roughly ±25–30%) or compare medians instead of
raw p95 values.

### Delivery latency is queue wait, not per-message broker latency

The harness records when a message is published and when it is consumed. If publishers
produce faster than consumers drain, messages sit in the broker queue, so the reported p95
includes that waiting time. In scenarios 01 and 02, one consumer drains slowly while
publishers dump work quickly, so a message can wait tens of seconds behind hundreds of
thousands of others. It does **not** mean one message took 60 seconds to travel through the
broker. In scenario 06, 16 consumer connections drain faster, so the backlog — and
therefore the p95 — is smaller (~15s). The values are internally consistent: lower consume
capacity leads to a longer backlog and higher p95.

### Scenario 02: consumer bottleneck confirmed

Baseline gets ~9.1k msg/s. Adding 64 publishers does not make the single consumer any
faster, so throughput barely moves (~9.9k msg/s, about 8% better). It just fills the queue
faster, which is why delivery p95 gets worse (68s → 87s). The consumer side is the limit.

### Scenario 03: publisher bottleneck confirmed

Keeping one publisher but giving it 64 consumers means consumers can drain faster than the
publisher can produce. The queue stays short, so delivery p95 falls to 0.15–0.67s.
Throughput rises to ~16.3k msg/s and is now limited by the single publisher. This is the
clean publisher-side bottleneck.

### Scenario 05: publisher connections do not help

Scenario 04 already has 64 publishers and 64 consumers on one connection. Scenario 05
gives publishers 16 separate connections but leaves consumers on one connection.
Throughput only rises about 7% (~18.5k vs ~17.4k msg/s). Since the publisher side was not
the limiting side, giving it more connections adds little; the single consumer connection
still controls the drain rate.

### Scenario 06: consumer connections matter, but it is not a publisher bottleneck

Giving consumers 16 connections raises throughput to ~32.8k msg/s, about 1.8× scenario 05.
That confirms consumer connections were limiting. However, the single publisher connection
still pushes 1M messages fast enough, so it is not the bottleneck here. The remaining limit
is consumer/broker dispatch, not publisher capacity.

### Scenario 07: broker contention

Scenario 06 reaches ~32.8k msg/s with 16 consumer connections. Scenario 07 also gives
publishers 16 connections, but throughput drops to ~29.6k msg/s and p95 rises from ~15s to
~28.5s. All traffic still goes through one exchange/queue, so extra connections add
goroutines, syscalls, and locking contention without adding parallel queue capacity. More
connections make the broker work harder, not faster. This points at the broker's
single-queue/dispatch path as the bottleneck.

### Regression recommendation

Keep 01, 02, 04, 05, 06, and 07 as baseline scenarios because they stress different parts
of the system: consumer limits, publisher limits, connection limits, and broker contention.
If a publisher-connection bottleneck is required explicitly, scenario 03 is the clean case.
A multi-publisher single-connection case like 06 hides publisher saturation because
publishing finishes in a burst. To expose it, use a sustained `-duration` run or a
rate-limited publisher so the publisher must keep producing for the whole measurement
window instead of dumping everything up front.
