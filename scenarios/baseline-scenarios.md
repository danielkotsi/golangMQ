# Baseline Scenarios

This document explains each scenario defined in `baseline.json` and every flag used by the benchmark CLI, including how they relate to the broker (`cmd/broker`, `internal/bench/*`).

## How to run

Using the runner (recommended):
```bash
# Run all scenarios in a set
./scripts/run_baseline.sh scenarios/baseline.json

# Run a single scenario
./scripts/run_baseline.sh scenarios/baseline.json -n single-conn-baseline
```

Results go to: `results/baselines/<git-sha-short>/<scenario-set>/<timestamp>/` containing `manifest.json`, `<name>.csv`, `<name>.json` (pretty-printed summary).

Or run the benchmark directly (ad-hoc):
```bash
./bin/benchbroker &
./bin/benchmark [flags] -output /path/to/out.csv  # JSON summary to stdout
```

## Scenarios (baseline.json)

| Scenario | Purpose | Topology summary | Workload |
| --- | --- | --- | --- |
| `single-conn-baseline` | Sanity check, minimal setup | 1 conn (mixed), 1 channel, both pub+cons capable | 1 pub, 1 cons, 10k msgs, 64 bytes, acks on, no warmup, run to completion |
| `fanout-even-4x8` | Even distribution stress | 4 conns (mixed by default), 8 channels total, evenly distributed across conns; `-pub-channels 4` carves 4 pub-capable slots from mixed conns | 16 pubs, 16 cons, 5k msgs each (80k total), 64 bytes, acks on, 0s warmup |
| `mixed-roles` | Pub/cons/mixed connection roles + channel carving | 6 conns: first 2 pub-only, next 2 cons-only, rest mixed (2). 12 channels total. Carve 2 pub + 2 cons from mixed-conn slots (rest become both) | 8 pubs, 8 cons, 5k msgs, 64 bytes, acks on, 0s warmup |
| `uneven-caps` | Packing with caps (uneven distribution) | 3 conns, 12 channels placed unevenly with `-max-channels-per-conn 6` | 12 pubs, 12 cons, 5k msgs, 64 bytes, acks on, 0s warmup |
| `no-ack` | No-ack path (stall probe behavior) | 2 conns (mixed), 4 channels | 8 pubs, 8 cons, 10k msgs (80k total), 64 bytes, `-ack=false`, 0s warmup |

Notes:
- With `-ack=false`, consumers do not ack. Per benchmark warnings: dispatch freezes each consumer at broker's fixed prefetch (10); also if total msgs > uint16 delivery-tag space (65535) tags may wrap. This is intentionally a stall/probe scenario in some configs.
- `-warmup` is excluded from measured stats; if all messages publish before warmup ends, the run fails with "warmup X exceeded workload".
- Total messages = `-publishers * -msgs`.

## Benchmark flags reference

### Connection/address
| Flag | Type | Default | Meaning (benchmark/broker) |
| --- | --- | --- | --- |
| `-addr` | string | `127.0.0.1:5672` | Broker address. Benchmark uses SDK (`github.com/danielkotsi/golangMQSDK`) to connect. |

### Topology
| Flag | Type | Default | Meaning |
| --- | --- | --- | --- |
| `-conns` | int (axis) | 1 | Total connections. Distribution axis (see `-distribution`). |
| `-pub-conns` | int | 0 | Connections dedicated to publishing (first N connections). Roles assigned in order: first `-pub-conns` = `pub-only`, next `-cons-conns` = `cons-only`, rest = `mixed`. |
| `-cons-conns` | int | 0 | Connections dedicated to consuming. |
| `-channels` | int (axis) | 1 | Total channels across all connections. Distribution axis. Channel-to-connection placement determined by `ConnsDist`. |
| `-pub-channels` | int (axis) | 0 | Publish-only channel slots carved from **mixed-conn** slots (only applies to mixed connections). Distribution axis (`PubChannelsDist`). |
| `-cons-channels` | int | 0 | Consume-only channel slots carved from **mixed-conn** slots. Carving order on mixed slots (by global index): first `-pub-channels` become publish-only, next `-cons-channels` become consume-only, remainder become both. |
| `-distribution` | enum (even\|uneven) | (unset) | **Order-sensitive**: binds to the immediately preceding axis flag (`-conns`, `-channels`, or `-pub-channels`). Sets `ConnsDist`/`ChannelsDist`/`PubChannelsDist` respectively. |

### Distribution caps (uneven only)
| Flag | Type | Default | Meaning |
| --- | --- | --- | --- |
| `-max-channels-per-conn` | int | 0 | Cap for uneven `-conns` distribution (channel->conn packing). If > 0 and uneven, channels are packed into conns with this cap; run fails if cannot fit. |
| `-max-consumers-per-channel` | int | 0 | Cap for uneven `-channels` distribution (consumer->channel placement). |
| `-max-publishers-per-channel` | int | 0 | Cap for uneven `-pub-channels` distribution (publisher->channel placement). |

### Workload
| Flag | Type | Default | Meaning |
| --- | --- | --- | --- |
| `-publishers` | int | 1 | Number of publisher goroutines. Placed over **publish-capable** channels (RolePublish ∪ RoleBoth) in global-index order per `PubChannelsDist`. |
| `-consumers` | int | 1 | Number of consumer goroutines. Placed over **consume-capable** channels (RoleConsume ∪ RoleBoth) in global-index order per `ChannelsDist`. |
| `-msgs` | int | 1000 | Messages per publisher. Total = `publishers * msgs`. |
| `-size` | int | 128 | Payload bytes. Must be >= 16 (header is 8-byte seq BE + 8-byte send-nanos BE = 16 bytes). Body is padded to `-size`. |
| `-ack` | bool | true | If true, consumers ack after each delivery and ack latency is recorded. If false, no acks (see warnings above). |
| `-prefetch` | int | 10 | **Accepted for forward-compat only; broker hardcodes 10**. Benchmark warns if used expecting effect. |

### Run control / measurement
| Flag | Type | Default | Meaning |
| --- | --- | --- | --- |
| `-duration` | duration | 0s | `0` = run to completion (publish all messages). `>0` = publishers stop at deadline, then drain phase begins (wait for queue to empty / drain-timeout). |
| `-warmup` | duration | 0s | Phase boundary: traffic is counted but **excluded from stats**. If workload finishes before/at warmup boundary in a way that exhausts messages, run fails ("warmup exceeded workload"). |
| `-report` | duration | 1s | Sampler cadence for CSV interval rows. Must be > 0. |
| `-drain-timeout` | duration | 30s | Max wait for queue to empty during drain. Must be > 0. |
| `-output` | string | `bench-<runid>.csv` | CSV path for intervals. If `-` then CSV goes to stdout and JSON summary goes to stderr. |
| `-quiet` | bool | false | Suppress per-interval log lines. |

## Broker notes (relevant)
- Broker listens on `:5672` by default in `cmd/broker/main.go`. Fixed config: `ChannelMax: 10`, `FramesMax: 10372`, `HeartbeatSec: 10`. JSON+newline framing over TCP with channel multiplexing.
- Fixed prefetch of 10 affects behavior when `-ack=false` (consumers can't drain beyond prefetch).
- Message identity header is 16 bytes total (seq + send time) used for latency/duplicate detection.

## Topology rules (quick)
1. **Channel->conn placement** via `ConnsDist` (even/uneven). Even interleaves globally; uneven packs with optional cap.
2. **Conn roles**: first `-pub-conns` pub-only, next `-cons-conns` cons-only, rest mixed.
3. **Channel roles on mixed conns**: carved in global-index order by `-pub-channels` then `-cons-channels` → remaining mixed become both. Dedicated conns force channel roles.
4. **Capabilities**: publish-capable = publish-only ∪ both (global index order), consume-capable = consume-only ∪ both (global index order).
5. **Worker placement**: consumers placed over consume-capable per `ChannelsDist`/caps; publishers over publish-capable per `PubChannelsDist`/caps.
