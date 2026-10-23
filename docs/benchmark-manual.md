# benchmark — usage manual

`cmd/benchmark` is the Phase 2 performance harness for the in-repo broker. It opens a
self-contained topology of **connections → channels → publishers/consumers**, runs a
publish→consume workload against a live broker, and emits per-interval **CSV** plus a
single **JSON** summary.

Everything about a run is governed by command-line flags, so this manual is organized
around them: concepts, flags, topology math, examples, outputs, and known limits.

---

## 1. Quick start

Build both binaries (optionally to `/tmp` so the roast doesn't stay in the workspace):

```
go build -o /tmp/benchbroker ./cmd/broker
go build -o /tmp/benchmark ./cmd/benchmark
```

Start the broker on the default port, then run the harness:

```
/tmp/benchbroker -port 5672 &            # broker listens on 127.0.0.1:5672
/tmp/benchmark                            # one conn, one channel, 1 pub + 1 cons, 1000 msgs
```

The default run:
- opens 1 connection with 1 channel that is **both** publish- and consume-capable,
- registers 1 publisher and 1 consumer,
- publishes `1 × 1000 = 1000` messages of 128 bytes and consumes (and acks) them all,
- writes `bench-<runid>.csv` in the current directory and prints the JSON summary to stdout.

Exit code `0` means the run completed; inspect stderr for the planned topology and any
warnings, and stdout for the machine-readable summary. With `-output -`, CSV owns stdout
and the JSON summary is written to stderr instead, so stdout stays a single CSV stream.

### Reading a run's identity

Every run gets a unique `runid` (base36 timestamp, in-process sequence, and process nonce,
e.g. `dlmyb6i5lbet-1-6d75f302317701ec`). It names the CSV
(`bench-<runid>.csv`), the exchange/queue/routing-key used on the broker
(`bench.ex.<runid>`, `bench.q.<runid>`, `bench.rk.<runid>`), and appears as
`summary.runid`. Unique fixture names mean runs on a shared broker cannot interfere.

---

## 2. Core concepts

### Connections, channels, roles

- **`-conns`** — number of TCP connections (SDK clients). One connection is one socket;
  per-connection accounting is "honest" about socket count.
- **`-channels`** — **total** number of SDK channels across *all* connections. Channels are
  spread onto connections by the conns distribution (see below). Every channel gets a
  deterministic **global index** `0..channels-1`; that index is the single source of
  ordering for everything downstream (role carving, worker placement, per-publish-channel
  metrics).
- Each connection has a **role**:
  - **Publish-only** (`-pub-conns` count, the first connections) — every channel is `P`.
  - **Consume-only** (`-cons-conns` count, the next connections) — every channel is `C`.
  - **Mixed** (any remaining connections) — channels are carved by `-pub-channels` /
    `-cons-channels` into `P`, `C`, and `B` (both).
- A channel is **publish-capable** if `P ∪ B`; **consume-capable** if `C ∪ B`. With all
  role flags at their defaults everything is `B`.

Role resolution is deterministic:
1. channels → connections placement (conns distribution);
2. `-pub-conns`' channels become `P`, `-cons-conns`' channels become `C`;
3. on mixed connections only, walk slots in global-index order: first `-pub-channels`
   become `P`, next `-cons-channels` become `C`, the rest `B`;
4. `publish-capable = P ∪ B`, `consume-capable = C ∪ B`, each in global-index order.

### Distribution — why `-distribution` is order-sensitive

There are three independent placement axes, and one shared flag:

```
-conns         -distribution even|uneven        governs channel→connection placement
-channels      -distribution even|uneven        governs consumer→channel placement
-pub-channels  -distribution even|uneven        governs publisher→channel placement
```

`-distribution` reads the **command-line position** of the axis flag that *immediately
precedes* it. The harness logs the effective topology so you can verify what you meant:

```
/tmp/benchmark -conns 2 -distribution uneven -channels 4 -publishers 0 -consumers 0
#              ^conns axis   ^binds to conns — then -channels sets a fresh axis
```

`even`:
- channels: channel `i` → conn `i % conns` (round-robin; when `channels < conns` some
  conns hold zero channels and exist as idle sockets);
- workers: worker `j` → slot `j % count`.

`uneven`:
- without a cap: pack everything into slot 0;
- with a cap `C`: fill slot 0 to `C`, then slot 1, …; **validation error** if it cannot
  fit (e.g. `channels > conns × C`).

Caps are three separate flags, one per axis:
`-max-channels-per-conn`, `-max-consumers-per-channel`, `-max-publishers-per-channel`.
A cap given while its axis is `even` prints a warning and is ignored.

---

## 3. All flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-addr` | `127.0.0.1:5672` | broker `host:port` |
| **Topology** | | |
| `-conns` *(axis)* | `1` | total connections |
| `-pub-conns` | `0` | connections dedicated to publishing (all their channels are `P`) |
| `-cons-conns` | `0` | connections dedicated to consuming (all their channels are `C`) |
| `-channels` *(axis)* | `1` | total channels across all connections |
| `-pub-channels` *(axis)* | `0` | publish-only slots carved from mixed-conn channels |
| `-cons-channels` | `0` | consume-only slots carved from mixed-conn channels |
| `-distribution` | `even` | placement mode `even\|uneven`; binds to the axis flag immediately before it |
| `-max-channels-per-conn` | `0` | cap for the uneven conns axis |
| `-max-consumers-per-channel` | `0` | cap for the uneven channels axis |
| `-max-publishers-per-channel` | `0` | cap for the uneven pub-channels axis |
| **Workload** | | |
| `-publishers` | `1` | publisher goroutines (fire `Publish` on their channel) |
| `-consumers` | `1` | consumer goroutines |
| `-msgs` | `1000` | messages per publisher; total = `publishers × msgs` |
| `-size` | `128` | payload bytes; **must be ≥ 16** (identity header) |
| `-ack` | `true` | consumers ack after recording each delivery |
| `-prefetch` | `10` | forward-compat only — broker hardcodes 10; warns if ≠ 10 |
| **Run / measurement** | | |
| `-duration` | `0s` | `0` = run to completion; `>0` = publishers stop at the deadline, then drain |
| `-warmup` | `0s` | traffic counted but excluded from statistics |
| `-report` | `1s` | sampler cadence (one CSV row per interval) |
| `-drain-timeout` | `30s` | max wait for the queue to empty after publishing stops |
| `-output` | `""` | CSV path (default `bench-<runid>.csv`; `-` = stdout) |
| `-quiet` | `false` | suppress per-interval log lines |

*(axis)* marks the flags that record the cursor used by the next `-distribution`.

---

## 4. Typical invocations

Start the broker, then pick a scenario.

**Mixed — publish and consume on the same connections**

```
/tmp/benchmark -conns 4 -channels 4 -publishers 2 -consumers 2 -msgs 5000 -ack -report 1s
```

4 connections, 1 channel each, all both. Publishers land on slots 0 and 1 (conn0, conn1);
consumers likewise. Expect `published == consumed == 10000`, `drops == 0`.

**Clearly separated connections**

```
/tmp/benchmark -conns 4 -pub-conns 2 -cons-conns 2 -channels 8 -publishers 4 -consumers 4 -msgs 10000
```

conn0–conn1 publish only, conn2–conn3 consume only. Consumers never appear on a
publish-only conn (the harness rejects it during placement).

**Channel-level split inside mixed connections**

```
/tmp/benchmark -conns 2 -channels 6 -pub-channels 2 -cons-channels 2 -publishers 2 -consumers 2
```

On each mixed conn, channels 0–1 become `P`, 2–3 `C`, rest `B`.

**Hammer publish on ONE channel vs across N**

```
/tmp/benchmark -conns 1 -channels 1 -publishers 8 -msgs 5000   # one channel, one socket
/tmp/benchmark -conns 4 -channels 8 -publishers 8 -msgs 5000   # spread across 4 sockets
```

Compare `summary.by_publish_channel[].rate_per_s` and the p95 delivery latency: publishing
on one channel serializes behind a single socket, so the multi-socket run shows the ceiling.

**Uneven packing**

```
/tmp/benchmark -conns 2 -distribution uneven -max-channels-per-conn 3 -channels 5 -publishers 0 -consumers 0
```

Ram all 5 channels onto conn0 first (3) then conn1 (2). Flip cap placement off to pack
everything on one connection:

```
/tmp/benchmark -conns 2 -distribution uneven -channels 5 -publishers 0 -consumers 0
```

**No-ack prefetch stall probe**

```
/tmp/benchmark -ack=false -msgs 50
```

Each consumer freezes at the broker's fixed prefetch of 10, so ~10 messages get consumed
and remaining are reported as not drained. This is an observability probe, not a
throughput run — the harness says so loudly.

**Timed run with warmup**

```
/tmp/benchmark -duration 30s -warmup 5s -report 1s
```

Publishers run until the 30s deadline, then the engine drains the queue. Warmup is a phase
boundary keyed by message sequence: traffic published before the boundary is excluded from
the summary and CSV, and the sampler starts *after* the boundary. If the whole workload
finishes during warmup, there is no measured work left and `Run` fails clearly instead of
returning an all-zero success.

---

## 5. Outputs

### stderr

- The **plan**: a one-line summary and a per-connection channel/worker table, plus the
  publish-capable / consume-capable lists (e.g. `c0.ch0(both)`). Verify this matches your
  intent — especially after order-sensitive `-distribution` usage.
- **Warnings** (prefixed `WARNING:`): total > 65535 delivery-tag space, `-ack=false`
  stall probe, `-prefetch` ≠ 10, and caps ignored under an `even` axis.
- Per-interval log lines (suppressed with `-quiet`).
- The final `run <runid> complete: published=N consumed=N drops=N elapsed=..ms csv=..`
  line.
- With `-output -`, the JSON summary (normally on stdout) is also written to stderr so
  stdout remains pure CSV.

### CSV (default `bench-<runid>.csv`, or `-output -` to stdout)

One row per sampler interval (plus a header); the exact columns are the **stable schema**:

```
ts_start_rfc3339, window_ms, published, delivered, consumed,
pub_rate_per_s, delivered_rate_per_s, consumed_rate_per_s,
avg_delivery_lat_us, min_delivery_lat_us, max_delivery_lat_us,
p50_delivery_lat_us, p95_delivery_lat_us, p99_delivery_lat_us,
avg_ack_lat_us, duplicates, drops_cumulative, both_lat_p95_us, cons_lat_p95_us
```

Latencies are in microseconds and measured client-side (see Limitations). `drops` is
cumulative; `duplicates` are deliveries whose sequence number was seen twice.

### JSON summary (stdout; stderr with `-output -`)

| Field | Meaning |
|-------|---------|
| `runid` | unique run identifier |
| `config` | the effective configuration |
| `topology` | per-conn role/channels, `publish_capable`, `consume_capable` counts |
| `published` / `delivered` / `consumed` | totals (consumed = unique messages recorded) |
| `duplicates` / `drops` | duplicate deliveries / undelivered messages |
| `elapsed_ms` | wall-clock run time |
| `consumed_rate_per_s` | overall consume rate |
| `delivery_latency_us` / `ack_latency_us` | `{min, max, avg, p50, p95, p99}` in µs |
| `by_role` | keyed `both` / `cons`, `{p95_us, n}` of delivery latency |
| `by_conn` | per-connection `{conn, published, consumed, pub_rate_per_s, avg_delivery_lat_us}` |
| `by_publish_channel` | per publish-capable channel `{conn, ch, published, rate_per_s}` |

`by_conn` / `by_role` / `by_publish_channel` are how you compare topologies (e.g. the
"consumers must never appear on a publish-only conn" check).

### Exit codes

| Code | Meaning |
|------|---------|
| `0` | success (run completed; totals may still show drops) |
| `1` | configuration validation error or runtime/engine error |
| `2` | command-line parse error (unknown flag, bad value, `-distribution` without a preceding axis) |

---

## 6. Common pitfalls

- **`-distribution` placement.** It binds to the axis flag parsed *immediately before it*.
  `-conns 2 -channels 4 -distribution uneven` makes the **channels** axis uneven, not
  conns — put `-distribution` right after the axis you mean:
  `-conns 2 -distribution uneven -channels 4`.
- **`-distribution` with no preceding axis** is a parse error (exit 2): `-distribution even`
  as the first flag fails.
- **Caps need `uneven`.** A `-max-*` cap on an `even` axis warps a warning and is ignored;
  the fit check only runs under `uneven` packing.
- **`-channels` counts across all connections.** `-conns 4 -channels 4` gives **one** channel
  per connection, not four per connection.
- **Validation errors beat warnings.** Overflow of connections, carving, or cap packing
  exits 1 *before* anything connects.

---

## 7. Known limitations (inherited, intentional)

- **Prefetch is not client-settable** — no `basic.qos`; the broker fixed prefetch at 10.
- **No-ack stall** — each consumer freezes at prefetch 10; a stall probe only.
- **uint16 delivery-tag wraparound** — totals > 65535 warn; benchmark bodies carry their own
  64-bit sequence for duplicate/drop detection.
- **Non-deterministic consumer selection** — the broker picks consumers by map iteration;
  per-consumer counts are uneven by design. Aggregate assertions only.
- **Fire-and-forget ack/confirm** — latency numbers are client-side, reproducible, relative.
- **No heartbeat** — fine locally; traffic keeps connections alive.