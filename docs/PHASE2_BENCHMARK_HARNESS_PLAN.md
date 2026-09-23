# Phase 2 — Benchmark Harness Implementation Plan (final design)

## Status: Phase 1 verified complete

Checked against `docs/TESTING_AND_BENCHMARKING_PLAN.md` (Phase 1):

| Plan item | Evidence | Status |
|-----------|----------|--------|
| `tests/helpers.go` harness | `startTestBroker`, `newTestClient`, `assertEventually`, `declareFixture` all present; plus extras (`consumeOnChannel`, `collectDeliveries`, `consumeAcked`, `assertNoDelivery`, `mergeWorkerDeliveries`, `publish`, `assertBodiesMultiset`). Ephemeral-addr exposure: harness listens on `127.0.0.1:0` itself and hands the bound addr to `broker.NewServer` — no broker change needed. | ✅ |
| `publishconsume_test.go` | round-trip, multi-msg, unbound key, unknown exchange | ✅ |
| `multiproducer_test.go` | concurrent producers, exact multiset, no-consumer attach, unknown exchange | ✅ |
| `multiconsumer_test.go` | exactly-once, prefetch respected, distribution | ✅ |
| `ack_test.go` | ack frees slot, unknown-tag no-op, nack+requeue, nack+DLX | ✅ |
| `disconnect_test.go` | consumer redelivery, producer close, redelivery exactness | ✅ |
| `routing_test.go` | exact match, non-matching dropped, multiple bindings | ✅ |
| `channel_test.go` | multi-channel over one connection, close requeue | ✅ |
| Extras | `harness_smoke_test.go`, `finding_f001_test.go`, `finding_f004_test.go` | ✅ |

Verification commands all green:

```
go build ./...                 # ok
go vet ./...                   # ok
go test ./...                  # ok  GolangRabbitMQBroker/tests 3.4s
go test -race ./tests/         # ok  GolangRabbitMQBroker/tests 4.7s
```

Phase 1 is **complete**. This document is the implementation plan for Phase 2.

---

# Phase 2 — Reusable performance harness

Build `cmd/benchmark/`: an argument-driven CLI that exercises the broker **black-box over
the wire** via `github.com/danielkotsi/golangMQSDK v0.3.2`, models a role-partitioned
connection/channel topology, and reports per-interval CSV series plus an aggregated JSON
summary. The engine lives in `internal/bench` so Phase 3 scenarios and Phase 4 regression
drive it as a function, not via CLI parsing.

## Design decisions (final, locked)

1. **Topology = connections → channels, with roles.** A connection is `publish-only`,
   `consume-only`, or `mixed`. On mixed connections a channel is `publish-only`,
   `consume-only`, or `both`. `publish-capable = P∪B`; `consume-capable = C∪B`.
2. **Publishers are not entities.** There is no "publisher" registration in the protocol —
   any channel can be hammered with `basic.publish`. A publisher is just a goroutine that
   calls `Publish` repeatedly on the channel it was handed. Consumers *are* registered
   (unique tags on a channel, multi-consumer per channel supported since F-004).
3. **`-channels`/`-pub-channels`/`-cons-channels` are TOTALS across the topology** (not
   "per connection"). The conns-axis distribution decides how those totals land on each
   connection. This makes `-max-channels-per-conn` a meaningful cap.
4. **`-distribution even|uneven` is order-sensitive.** It binds to whichever countable
   axis immediately precedes it on the command line: `-conns`, `-channels`,
   `-pub-channels`, `-routing-keys`, `-queues`. `even` = deterministic balanced
   placement; `uneven` = packing (no cap → everything into slot 0; with cap → fill slot 0
   to the cap, then slot 1, …). `-exchanges` is **count-only** — it deliberately has NO
   distribution axis (an exchange is stressed with 1 exchange; see decision 10).
5. **Message loads are deterministic.** `even` assigns whole goroutines round-robin, so the
   message total per channel is exactly `(goroutines on channel) × -msgs` — no scheduling
   jitter in the per-channel totals. `uneven` packs the same way → one channel carries the
   drift difference.
6. **Message identity header.** Each body's first 16 bytes carry
   `[8-byte BE monotonic seq][8-byte BE send-time unixnanos]`, so wire payload length stays
   exactly `-size`. A global `atomic.Uint64` minted before publish gives order-independent
   identity across N publishers, redelivery, and multi-consumer interleaving.
7. **`-ack` defaults to `true`.** Dispatch is gated on `consumer.inflight < prefetch`
   (`broker/queue.go:76`, `broker/consumer.go:33`); inflight only drops on ack/nack. No-ack
   freezes each consumer at the broker's fixed prefetch (10) — a stall experiment, not a
   throughput one.
8. **One connection is one socket.** All channels on a conn share its write pump + TCP
   socket (`client.go:writePump`). Multi-channel publishing only parallelizes across
   `-conns`. Metrics split per connection and per publish-channel to make this visible.
9. **CSV + JSON outputs.** CSV = per-interval series (plotting / Phase 4 golden files);
   JSON summary on stdout = machine-readable aggregates for Phase 4 gates. Both include the
   per-role, per-connection, and per-publish-channel decomposition.
10. **Messaging objects are count + a bipartite binding.** `-exchanges`, `-routing-keys`,
    `-queues` define the messaging topology. Only the **count** of exchanges matters
    (stressing an exchange = run with `-exchanges 1`; key `k` derives onto exchange
    `k % exchanges`). Keys and queues form a **bipartite binding graph** with two
    independently-dialed degrees: the **key-degree** (how many queues a key fans into;
    `-distribution` after `-routing-keys`, cap `-max-queues-per-key`) and the
    **queue-degree** (how many keys feed a queue; `-distribution` after `-queues`, cap
    `-max-keys-per-queue`). No single "fanout" flag.
11. **Conflicting degree totals soften loudly.** When the two degree targets disagree
    (`Σd_k ≠ Σe_q`), the axis with the larger total keeps its degrees; the smaller side
    scales up proportionally, re-checked against its cap; any residual clamp/adjustment
    emits an **explicit warning** (stderr + run header + `softened_bindings` in the JSON).
    Realization is deterministic (index-ordered greedy fill).
12. **Accounting is per key and per queue.** Publishing to key `k` produces one copy per
    bound queue, so `consumed_total = Σ_k published_k × (queues bound to k)`. Duplicate
    detection is per-queue (same seq redelivered on the *same* queue).

## Verified broker/SDK facts the design depends on

- `ch.Publish` is **fire-and-forget** (`channels.go:305`) — no confirm; drops are measured
  as `total − consumed` after drain.
- `ch.Ack`/`ch.Nack` are **fire-and-forget** (reqID 0) — ack latency is client-side.
- `ch.Consume` returns a **per-consumer** delivery channel (`channels.go:344`); broker
  stamps `ConsumerTag` per delivery (`broker/queue.go:112-118`), SDK routes by tag.
- Delivery tags are `uint16` (`protocol/events.go:84`), never wrapped. Total
  `publishers × msgs` above 65535 risks corrupting dispatch → warning (Phase 3 will
  deliberately exceed this).
- Client connect params to reuse (`tests/helpers.go:newTestClient`): `Username "daniel"`,
  `Password "123456789"`, `ChannelMax 10`, `FrameMax 10372`, `HeartbeatSec 10`.

## Package layout

```
cmd/benchmark/
  main.go                 # flag parsing, order-sensitive distribution binding, validation, wiring
internal/bench/
  config.go               # Config struct, flag.Value helpers, Validate()
  topology.go             # role resolution + even/uneven placement (conns/channels/workers) + key↔queue binding graph
  engine.go               # run(ctx, cfg): build topology, workers, drain, Summary
  worker.go               # publishWorker, consumeWorker, message header encode/decode
  metrics.go              # sampler, latency stats, per-role/conn/publish-channel split
  csv.go                  # CSV writer + JSON summary encoder
  bench_smoke_test.go     # boots in-process broker, tiny runs, asserts topology + counts
```

`internal/bench` instead of flat `cmd` so the smoke test and Phase 3 call the engine as a
function.

## Config & flags

### Topology

| Flag | Default | Meaning |
|------|---------|---------|
| `-conns` | `1` | total connections |
| `-pub-conns` | `0` | connections dedicated to publishing (their channels are all `P`) |
| `-cons-conns` | `0` | connections dedicated to consuming (their channels are all `C`) |
| `-channels` | `1` | **total** channels across all connections |
| `-pub-channels` | `0` | publish-only channel slots carved from mixed-conn slots |
| `-cons-channels` | `0` | consume-only channel slots carved from mixed-conn slots |

`mixed-conns = conns − pub-conns − cons-conns` (must be ≥ 0); the remainder of mixed slots
is `both`. With all role flags at default, every channel is `both` and can both publish and
consume.

**Channel indexing (single source of order):** every channel has a **global index**
`0..channels-1` assigned *during* channel→conn placement. The map from global index to
conn/ch is exactly what the conns-axis distribution produces (see below). All later steps —
role carving, worker placement, per-publish-channel metrics — iterate slots in **global
index order** (`conn` ascending, then channel index within the conn is NOT used for
ordering; the global index wins).

**Role resolution (deterministic):**
1. Channel→conn placement first (see distribution below); record each channel's global index.
2. Pub-conns' channels → `P`; cons-conns' channels → `C` (regardless of `-pub-channels` /
   `-cons-channels`, which only affect mixed conns).
3. On **mixed conns only**: walk their slots in global index order; stamp the first
   `-pub-channels` as `P`, the next `-cons-channels` as `C`, the rest as `B`.
4. `publish-capable = P∪B` (in global index order); `consume-capable = C∪B` (same order).

**Validation:** `pub-conns + cons-conns ≤ conns`;
`pub-channels + cons-channels ≤` (total channel slots on mixed conns).

**Edge case — unbalanced `even`:** when `channels < conns`, round-robin `i % conns`
guarantees each conn holds `floor` or `ceil` channels, no conn exceeds `ceil`, and some
conns may hold **zero** channels. Those conns still exist as SDK clients (open, idle) but
host no channels; this is intentional — it keeps "one conn = one socket" accounting honest.

### Messaging objects

| Flag | Default | Meaning |
|------|---------|---------|
| `-exchanges` | `1` | **count only**; key `k` derives onto exchange `k % exchanges`. No distribution axis. |
| `-routing-keys` | `1` | distinct routing keys, named `bench.rk.<runid>.<k>` |
| `-queues` | `1` | queues, named `bench.q.<runid>.<j>` |
| `-max-queues-per-key` | `0` | key-axis cap: max queues one key may bind (needs `uneven`); `0` = no limit |
| `-max-keys-per-queue` | `0` | queue-axis cap: max keys that may feed one queue (needs `uneven`); `0` = no limit |

Declared named exports `bench.ex.<runid>.<e>` for `e in 0..exchanges-1`. Exchanges are
**never distributed** — count only. A run with `-exchanges 1` is the exchange stress case
(all keys land on one exchange).

### Binding — bipartite key↔queue graph

Two degrees, each dialed by its own `-distribution`:

- **key-degree** `d_k` = number of queues key `k` binds to (how many queues one key fans
  into) — `-distribution` after `-routing-keys`, cap `-max-queues-per-key`.
- **queue-degree** `e_q` = number of keys that bind queue `q` (how many keys feed one
  queue) — `-distribution` after `-queues`, cap `-max-keys-per-queue`.

**Target degree sequences (deterministic):**
- key-degree `even`: distribute the `-queues` slots as evenly as possible over the
  `-routing-keys`, each key ≥ 1 while `queues ≥ routing-keys`:
  `d_k = floor(Q/R) + (k < Q mod R ? 1 : 0)`, so `Σd_k = Q`.
- queue-degree `even`: symmetric: `e_q = floor(R/Q) + (q < R mod Q ? 1 : 0)`, `Σe_q = R`.
- key-degree `uneven` (no cap): key 0 binds **all** `Q` queues, keys 1..R-1 bind none
  ("one key, many queues").
- key-degree `uneven` with cap `C`: key 0 binds up to `C`, then key 1, …; **error** if
  `Q > C × R` (can't fit).
- queue-degree `uneven` (no cap): queue 0 is fed by **all** `R` keys ("one queue, many
  keys").
- queue-degree `uneven` with cap `C`: queue 0 bound by up to `C` keys, then queue 1, …;
  **error** if `R > C × Q`.

Any degree flagged for a cap while its axis is `even` → warning + ignored (same as
topology caps).

**Adaptive softening** when `Σd_k ≠ Σe_q`:
1. The axis with the larger target total keeps its degrees exactly.
2. The smaller side scales up proportionally (deterministic rounding), re-clamped by its cap.
3. Residual mismatches are absorbed by deterministic rightmost-first reduction on the
   primary side.
4. Every step that alters a target emits an explicit `WARN` (stderr + run header +
   `softened_bindings` array in JSON).

Even/even with `routing-keys == queues` yields the invariant 1:1 graph
(`d_k = e_q = 1`) with **no warnings**.

**Realization (topology.go):** index-ordered greedy fill over the two degree sequences
(Havel–Hakimi-style, ties broken by index) producing `Σd_k` edges `(k,q)`; verify both
degree sequences post-hoc; failure = validation error naming the constraint clash.

### Distribution (order-sensitive)

| Flag | Binds after | Governs | Cap (needs `uneven`) |
|------|-------------|---------|----------------------|
| `-distribution even\|uneven` | `-conns` | channel→connection placement | `-max-channels-per-conn` |
| `-distribution even\|uneven` | `-channels` | consumer→channel placement | `-max-consumers-per-channel` |
| `-distribution even\|uneven` | `-pub-channels` | publisher→channel placement | `-max-publishers-per-channel` |
| `-distribution even\|uneven` | `-routing-keys` | key-degree: queues a key binds | `-max-queues-per-key` |
| `-distribution even\|uneven` | `-queues` | queue-degree: keys feeding a queue | `-max-keys-per-queue` |

`-exchanges` is **not** an axis — count only (decision 10).

The **same** `-distribution` flag is registered once; a custom `flag.Value` is used. Because
stdlib `flag` processes argv in order, each `-distribution` occurrence reads `lastAxis`,
which `-conns` / `-channels` / `-pub-channels` / `-routing-keys` / `-queues` set when they
are parsed.

**Mechanics:**
- `-distribution` with no preceding axis flag (`-conns`/`-channels`/`-pub-channels`/
  `-routing-keys`/`-queues`) → error.
- `even`: round-robin (channel `i` → conn `i % conns`; worker `j` → slot `j % count`;
  degree targets as above for keys/queues).
- `uneven`, no cap: pack everything into slot 0 (for keys/queues: one key binds all queues
  / one queue is fed by all keys).
- `uneven` with cap `C`: fill slot 0 to `C`, then slot 1, …; **validation error** if it
  cannot fit (e.g. `channels > conns×C`, `consumers > consumeCapable×C`,
  `publishers > publishCapable×C`, `queues > routingKeys×C` on the key axis,
  `routingKeys > queues×C` on the queue axis).
- A cap given while its axis is `even` → warning + ignored.

### Workload

| Flag | Default | Meaning |
|------|---------|---------|
| `-publishers` | `1` | publisher goroutines (fire `Publish` on their channel) |
| `-consumers` | `1` | consumer goroutines (registered, ack-able) |
| `-msgs` | `1000` | messages per publisher (total = `publishers × msgs`; check vs 65535) |
| `-size` | `128` | payload bytes; must be ≥ 16 (header) |
| `-ack` | `true` | consumers ack after recording each delivery |
| `-prefetch` | `10` | accepted for forward-compat only; broker hardcodes 10, warns if ≠ 10 |

### Run / measurement

| Flag | Default | Meaning |
|------|---------|---------|
| `-duration` | `0` | `0` = run to completion; `>0` = publishers stop at deadline, then drain |
| `-warmup` | `0` | traffic counted but excluded from stats; window starts at expiry |
| `-report` | `1s` | sampler cadence (CSV rows) |
| `-drain-timeout` | `30s` | max wait for the queue to empty at the end |
| `-output` | `bench-<runid>.csv` | CSV path; `-output -` = stdout |
| `-quiet` | `false` | suppress per-interval log lines |

## Placement rules (topology.go)

Given conns, roles, messaging counts, and the five distribution axes, the builder produces
a deterministic plan:

```
type channelSlot struct { conn int; ch int; role Role; pubGoroutines []int; consGoroutines []int }
type binding struct { key int; queue int }        // one directed key→queue edge
type Topology struct {
    conns []connPlan; publishCapable []*channelSlot; consumeCapable []*channelSlot
    bindings []binding                            // key→queue edges (Σd_k of them)
    keyQueues [][]int                             // per key, the bound queues (key-degree)
    queueKeys [][]int                             // per queue, the feeding keys (queue-degree)
}
```

1. **channel→conn** via conns-axis mode:
   - `even`: channel `i` → conn `i % conns` (≈ `channels/conns` per conn).
   - `uneven`/cap: pack per rules; conn's channel count capped by `-max-channels-per-conn`.
2. **Role stamping** per the resolution order above.
3. **consumer→channel** via channels-axis mode over `consumeCapable`: `even` = goroutine
   `j` → slot `j % len(consumeCapable)`; `uneven` = pack. A slot may hold several consumers
   (multi-consumer per channel).
4. **publisher→channel** via pub-channels-axis mode over `publishCapable`; same schema.
5. **publisher→key (deterministic, NOT an axis):** publisher goroutine `j` publishes to key
   `j % routing-keys`. The user asked no distribution axis here — only *how many* keys exist
   and the key/queue degrees matter. `published_k = (count of publishers with j % R == k) × msgs`.
6. **Message budget:** each publisher goroutine publishes exactly `-msgs` messages, so the
   deterministic per-channel publish load = `len(channel.pubGoroutines) × msgs`.
7. **key→queue binding** via `buildBindings()` (see "Binding" section): compute target
   degree sequences `d_k` / `e_q`, soften on mismatch with warnings, greedy-fill edges.
   `consumed_target = Σ_k published_k × d_k`.

The header (stderr) prints the full plan: per conn channel count/roles, per channel
consumer & publisher goroutine counts, the publish-capable list, plus the binding table
(`key → [queues]`, `queue ← [keys]`) and any softening warnings — so the run is
self-documenting and auditable (AGENTS.md audit-trail practice).

## Topology in practice — one fully worked example

Config:

```
-conns 3  -pub-conns 1  -cons-conns 0  -channels 6  -pub-channels 2  -cons-channels 1
-distribution even       # binds after -conns  → channels spread across conns
-distribution even       # binds after -channels  → consumers spread across consume-capable
-distribution even       # binds after -pub-channels  → publishers spread across publish-capable
-publishers 4  -consumers 4  -msgs 1000
```

**Step 1 — channel→conn (conns-axis `even`):** global index `i` → conn `i % 3`.

```
global idx | 0  1  2  3  4  5
conn       | 0  1  2  0  1  2     → conn0:{0,3}  conn1:{1,4}  conn2:{2,5}
```

**Step 2 — roles.** `pub-conns=1` → conn0 is publish-only: its channels {0,3} are `P`.
conn1, conn2 are mixed (mixed-conns = 3−1−0 = 2). Mixed slots in global order:
{1(c1), 2(c2), 4(c1), 5(c2)}. Carve `pub-channels=2` → {1,2} = `P`; next
`cons-channels=1` → {4} = `C`; rest → {5} = `B`.

```
conn0: ch0 P   ch3 P          (publish-only conn)
conn1: ch1 P   ch4 C          (mixed)
conn2: ch2 P   ch5 B          (mixed)
```

**Step 3 — capability lists (global index order).**
`publish-capable  = {0,3,1,2,5}` (P∪B) → 5 slots.
`consume-capable  = {4,5}`        (C∪B) → 2 slots.

**Step 4 — worker placement.**
Publishers (4, `even` over 5 slots): goroutine `j → slot j % 5` → j0→ch0, j1→ch3, j2→ch1,
j3→ch2; ch5 idle. Budget: ch0,ch3,ch1,ch2 each `1 × 1000`; total 4000.
Consumers (4, `even` over 2 slots): `j % 2` → ch4 gets j0+j2, ch5 gets j1+j3 →
**2 consumers per channel** (multi-consumer on a shared channel, F-004).

**Step 5 — preserved invariants to assert in the smoke test:** exactly 2 `P` / 1 `C` /
1 `B`; publish-capable = 5; consume-capable = 2; 2 consumers registered on ch4 and ch5 each;
consumers never placed on conn0 (publish-only conn).

The complete `-distribution` on the **topology axes** is visible in this example: **conns**
controls step 1, **channels** (consumers) and **pub-channels** (publishers) control step 4.
The two messaging axes (routing-keys, queues) are independent — see the next section. The
three topology-layer axes are a superset of the whole flag set's five; a run that only sets
topology flags uses the messaging defaults (1 exchange / 1 key / 1 queue, 1:1 binding).

## Messaging in practice — binding examples

Default (even/even, `key == queue` count): the 1:1 invariant, no warnings.

```
-routing-keys 4  -queues 4                        → k.i → q.i   (each d_k = e_q = 1)
```

**One queue fed by many keys — the queue-degree stress:**

```
-routing-keys 4  -queues 1  -distribution uneven -max-keys-per-queue 4
```

queue-degree `uneven`, no cap needed → `e_q0 = 4` (queue 0 fed by k0..k3); key-degree
`even` target `d = [1,0,0,0]` (`Σ = Q = 1`) disagrees with the queue-side total
(`Σ = R = 4`) → softening promotes the queue side and key degrees realize as
`d = [1,1,1,1]`; a `WARN` is emitted. `consumed_target = published × 1` (each message has
1 bound copy per key, regardless of which queue). Queue 0 drains 4 keys' traffic; compares
with `-queues 4` (each queue fed by exactly 1 key).

**One key fanning into many queues — the key-degree stress:**

```
-routing-keys 1  -queues 4  -distribution uneven -max-queues-per-key 4
```

key-degree `uneven` → `d_k0 = 4` (k0 binds q0..q3); queue-degree `even` target
`e = [1,0,0,0]` (`Σ = R = 1`) disagrees (key-side total `Σ = Q = 4`) → key side stays
primary, queue degrees realize as `[1,1,1,1]`; a `WARN` is emitted.
`consumed_target = published × 4`.

**Softening with warnings:**

```
-routing-keys 2  -queues 3  -distribution even   -distribution even
```

key-degree `even`: `d = [2,1]` (`Q mod R = 1` → key 0 gets the extra) → `Σ = 3`.
queue-degree `even`: `e = [1,1,0]` (`R mod Q = 2` → queues 0,1 get the extra; queue 2 binds
no key) → `Σ = 2`.
**Mismatch** → key side is primary (total 3); queue degrees scale up to total 3, e.g.
`[1,1,1]`; a `WARN` lists the adjustment. This run is legal but must print the softening
warning (smoke-tested); realized graph = k0→q0,q1; k1→q2.

```
-routing-keys 2  -queues 1
```

Even small count asymmetries warn: key-degree `even`: `d = [1,0]` (only 1 queue slot to
give, key 0 takes it) → `Σ = 1`. queue-degree `even`: `e = [2]` → `Σ = 2`. **Mismatch** →
queue side is primary (total 2); key degrees soften to `[1,1]`; `WARN` emitted; realized
graph = k0→q0, k1→q0 (both keys feed the single queue).

## Engine flow (engine.go)

1. **Topology setup.** `runid := base36(time.Now().UnixNano())`. Declare
   `exchanges` named `bench.ex.<runid>.<e>`, `routingKeys` named `bench.rk.<runid>.<k>`,
   `queues` named `bench.q.<runid>.<j>`, and bind every `(k,q)` edge from the binding graph
   on a short-lived setup client. Unique names → no cross-run interference.
2. **Instantiate topology**: `conns` SDK clients; per plan open channels; register consumer
   goroutines on their slots; assign publisher goroutines to their slots (publisher `j` →
   channel via the pub-channels axis, and → key `j % routing-keys`; see placement rules
   step 5).
3. **Warmup (optional):** run all workers for `-warmup`; discard counts; reset window.
4. **Measured phase:**
   - Publisher goroutines: mint seq globally, stamp `sentTimes[seq-1]`, `Publish` to their
     key. Consumers read per-consumer deliveries; decode header;
     `deliveryLatency = recv − sentTimes[seq-1]`; mark consumed; if `-ack`, time `Ack`,
     record ack latency. Immediately-read buffer keeps the SDK read-loop unblocked.
   - **Sampler** ticks at `-report`: snapshots counters + latency tails + per-role /
     per-conn / per-publish-channel / per-key / per-queue splits into an interval record,
     writes CSV, logs unless `-quiet`.
5. **Completion / drain.** Publishers finish → record publish-end; consumers run until
   `consumed == consumed_target`, or `-duration` elapses then drain till quiet for
   `-drain-timeout`; force-close (`client.Close()` is idempotent, unblocks readLoop).
6. **Summary → JSON on stdout.**

## Metrics (metrics.go)

Per-message: **delivery latency** (recv − send stamp), **ack latency** (Ack return −
recv, when acking), **publish latency** (local, enqueue+flush).

Aggregates (whole run and per interval): count, msgs/sec, min/avg/max, p50/p95/p99.

Counters: `published`, `delivered` (copies out of queues), `consumed` (deliveries
unique per queue), `duplicates` (seq redelivered on the same queue), `drops =
consumed_target − consumed` after drain (0 in clean runs).

**Decomposition (the point of the topology flags):**
- delivery-latency percentiles **per channel role** (`both`, `cons-dedicated`) in CSV/JSON —
  mixed vs separated compared directly;
- **per-connection rows** in JSON (delivered, consumed, pub_rate, avg delivery latency) —
  single-socket vs parallel sockets visible;
- **per-publish-channel rate** — 1-channel vs N-channel hammering ceiling visible;
- **per-routing-key and per-queue rows** in JSON (delivered, consumed, dup, avg delivery
  latency) — one-key-many-queues / one-queue-many-keys probes compared directly.

CSV columns (stable for Phase 4 diffing):

```
ts_start_rfc3339, window_ms, published, delivered, consumed,
pub_rate_per_s, delivered_rate_per_s, consumed_rate_per_s,
avg_delivery_lat_us, min_delivery_lat_us, max_delivery_lat_us,
p50_delivery_lat_us, p95_delivery_lat_us, p99_delivery_lat_us,
avg_ack_lat_us, duplicates, drops_cumulative,
both_lat_p95_us, cons_lat_p95_us
```

JSON summary shape (stdout, one line):

```json
{
  "runid": "...", "config": {...}, "topology": {"conns":[{"channels":4,"role":"pub"}],"publish_capable": 3, "consume_capable": 3, "exchanges": 3, "routing_keys": 6, "queues": 3, "bindings": [{"key":0,"queue":0},{"key":1,"queue":1},{"key":2,"queue":2},{"key":3,"queue":0}]},
  "published": 0, "delivered": 0, "consumed": 0, "duplicates": 0, "drops": 0, "consumed_target": 0,
  "softened_bindings": ["WARN ..."],
  "elapsed_ms": 0, "consumed_rate_per_s": 0.0,
  "delivery_latency_us": {"min":0,"avg":0,"max":0,"p50":0,"p95":0,"p99":0},
  "ack_latency_us": {"min":0,"avg":0,"max":0,"p50":0,"p95":0,"p99":0},
  "by_role": {"both":{"p95_us":0,"n":0},"cons":{"p95_us":0,"n":0}},
  "by_conn": [{"conn":0,"published":0,"consumed":0,"pub_rate_per_s":0.0,"avg_delivery_lat_us":0}],
  "by_publish_channel": [{"conn":0,"ch":0,"published":0,"rate_per_s":0.0}],
  "by_routing_key": [{"key":0,"published":0,"delivered":0,"consumed":0,"duplicates":0,"avg_delivery_lat_us":0}],
  "by_queue": [{"queue":0,"consumed":0,"duplicates":0,"avg_delivery_lat_us":0}]
}
```

Note on counting with bindings:
- `delivered` = copies routed out of the queues (a message published to key `k` yields `d_k`
  copies — one per bound queue). Across queues the same seq appears multiple times: that is
  **expected** fanout, not duplication.
- `consumed` = delivered copies that were **unique on the delivery's queue** (the seq had
  not been seen on that queue before).
- `duplicates` = delivered copies whose seq was already consumed on the **same** queue
  (redelivery) — the per-queue dup detector.
- `drops = consumed_target − consumed` after drain (0 in clean runs), where
  `consumed_target = Σ_k published_k × d_k`. Not `published − consumed` — that denominator
  is wrong under fanout.

## Verification & definition of done

Commands (must stay green):

```
go build ./...     go vet ./...     go test ./...
go test -race ./...    # includes bench smoke test
```

`bench_smoke_test.go` assertions:
- **topology math**: `even` 4 conns × 8 channels → 2/conn; role carving `2 conns, 6
  channels, pub 2, cons 2` → exactly 2 `P`, 2 `C`, 2 `B`; overflow
  (`pub-conns + cons-conns > conns`, carve > mixed slots, cap overflow) → `Validate` errors.
- **placement**: `even` 4 consumers over 2 consume-capable slots → 2+2; `uneven` no cap →
  all in slot 0; capped → verified packing counts.
- **binding**: `-routing-keys 4 -queues 4` even/even → 1:1 (`d_k = e_q = 1`), Σ matches Q,
  no warnings; `-routing-keys 4 -queues 1` queue-uneven → `e_q0 = 4`, all keys bound to
  queue 0; `-routing-keys 1 -queues 4` key-uneven → `d_k0 = 4`; cap overflow on either axis
  → `Validate` error.
- **softening**: `-routing-keys 2 -queues 3` even/even → mismatch produces
  `WARN`/`softened_bindings`, realized edge set still satisfies the (softened) degree
  targets, `consumed_target` recomputed from final graph.
- **accounting**: fanout run delivers `consumed_target = published × d_k` total; per-queue
  dup detection stays 0 on clean fanout; drops = `consumed_target − consumed`.
- **same-queue multi-consumer on ONE channel** (4 consumers, 1 slot) — broker tolerance.
- **1 channel vs N channels publish**: both runs deliver `consumed_target` messages,
  `drops == 0`, assignment tables differ (all on one publish-capable slot vs spread).
- **mixed vs separated**: `-conns 2 -channels 2` (every conn both) vs
  `-conns 2 -pub-conns 1 -cons-conns 1 -channels 2`; assert by_role/by_conn rows present
  and consumers never appear on a publish-only conn (assignment check).
- CSV header + ≥ 1 row on a tiny run; JSON totals match; `by_routing_key`/`by_queue` rows
  match the binding graph.

Manual acceptance runs (broker on `:5672` via `go run ./cmd/broker`):

```
# mixed: both consume & publish on the same connections
go run ./cmd/benchmark -addr 127.0.0.1:5672 -conns 4 -channels 4 \
  -publishers 2 -consumers 2 -msgs 5000 -ack -report 1s

# clearly separated connections
go run ./cmd/benchmark -addr 127.0.0.1:5672 -conns 4 -pub-conns 2 -cons-conns 2 \
  -channels 8 -publishers 4 -consumers 4 -msgs 10000

# channel-level split inside mixed conns
go run ./cmd/benchmark -addr 127.0.0.1:5672 -conns 2 -channels 6 \
  -pub-channels 2 -cons-channels 2 -publishers 2 -consumers 2

# hammer publish on ONE channel vs across N (compare by_publish_channel ceilings)
go run ./cmd/benchmark -addr 127.0.0.1:5672 -conns 1 -channels 1 -publishers 8 -msgs 5000
go run ./cmd/benchmark -addr 127.0.0.1:5672 -conns 4 -channels 8 -publishers 8 -msgs 5000

# no-ack observability of the prefetch stall (warning printed)
go run ./cmd/benchmark -addr 127.0.0.1:5672 -ack=false -msgs 50

# one queue fed by many keys (queue-degree) vs spread (by_queue/by_routing_key splits)
go run ./cmd/benchmark -addr 127.0.0.1:5672 -routing-keys 4 -queues 1 \
  -distribution uneven -max-keys-per-queue 4 -publishers 4 -consumers 2 -msgs 5000
go run ./cmd/benchmark -addr 127.0.0.1:5672 -routing-keys 4 -queues 4 \
  -publishers 4 -consumers 4 -msgs 5000

# one key fanning into many queues (key-degree) — consumed_target = published × 4
go run ./cmd/benchmark -addr 127.0.0.1:5672 -routing-keys 1 -queues 4 \
  -distribution uneven -max-queues-per-key 4 -publishers 2 -consumers 4 -msgs 5000

# softening example — degrees disagree, expect WARN + softened_bindings in JSON
go run ./cmd/benchmark -addr 127.0.0.1:5672 -routing-keys 2 -queues 3 \
  -distribution even -distribution even -publishers 2 -consumers 3 -msgs 5000
```

Exit codes: `0` success; `1` runtime/engine error; `2` flag-parse error.

## Risks & limitations (inherited and intentional)

- **Prefetch not client-settable** (no `basic.qos`; broker fixed at 10). `-prefetch` is
  validated, stored, and warned on. Phase 3's ack-pressure axis is approximated with ack
  pacing.
- **No-ack stall**: each consumer freezes at prefetch. Warned loudly; a stall probe only.
- **uint16 delivery-tag wraparound** (>65535 total in one run): warning; Phase 3
  deliberately exceeds it. Benchmark bodies carry their own 64-bit seq.
- **Non-deterministic consumer selection** (map iteration, `broker/queue.go:selectConsumer`):
  per-consumer counts uneven by design; assertions are aggregate-only. Do NOT preemptively fix.
- **No heartbeat**: fine locally; traffic keeps connections alive (Phase 3 note).
- **Fire-and-forget ack/confirm**: latency numbers are client-side, reproducible, relative.
- **Cap flags with `even`** are ignored with a warning — a cap only says something under
  packing, and silent semantics are worse than loud warnings.
- **Degree softening** can silently shape the graph if it only warned on stderr; every
  softened step is therefore also recorded in the run header and `softened_bindings` JSON so
  golden-file diffs (Phase 4) can't misinterpret the (adjusted) targets.
- **Drops definition is per-key-weighted**: with uneven fanout, `drops` compares against
  `consumed_target = Σ_k published_k × d_k`, NOT `published`. Misreading this is the #1
  reporting pitfall (assertion + docs).
- **Exchange count is derived (`key % exchanges`)**: there is no "publish to exchange N
  only" flag; to stress one exchange use `-exchanges 1`. This is a deliberate scope cut.

## Suggested implementation order

1. `internal/bench/config.go` — Config, order-sensitive `flag.Value` helpers, `Validate()`.
2. `internal/bench/topology.go` — role resolution + placement + `buildBindings` (two-degree
   targets, adaptive softening with warnings, index-ordered greedy fill); unit-assert its
   math first (this is the trickiest, most flag-dependent piece).
3. `internal/bench/worker.go` + `engine.go` — minimal single-conn path (include
   `consumed_target` from the binding graph).
4. `internal/bench/metrics.go` + `csv.go` — decomposition + stable columns.
5. `cmd/benchmark/main.go` — flag wiring, JSON summary (`by_routing_key`/`by_queue`/
   `softened_bindings`), exit codes.
6. `internal/bench/bench_smoke_test.go`; green `go build` / `go vet` / `go test` /
   `go test -race ./...`.
7. Manual acceptance runs above; sanity-check CSV columns, JSON splits, and warnings.
8. Optional: capture acceptance CSV/JSON samples under `docs/bench-examples/` so Phase 3
   baselines are easy to diff.

Phase 2 hands off cleanly to Phase 3: the scenario runner reuses `internal/bench.run`
unchanged, sweeping topology/role configs to produce the latency/ceiling curves and the
Phase 3 baseline snapshot.