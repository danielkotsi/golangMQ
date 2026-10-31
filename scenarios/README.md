# Benchmarking

This directory contains baseline scenario definitions and documentation for running reproducible benchmarks against the broker.

## Quick start

### 1) Build binaries (if not present)
```bash
go build -trimpath -o bin/benchbroker ./cmd/broker
go build -trimpath -o bin/benchmark ./cmd/benchmark
```
Or let the runner build them automatically if missing.

### 2) Run scenarios with the runner (recommended)
```bash
# Run all scenarios from baseline.json
./scripts/run_baseline.sh scenarios/baseline.json

# Run a specific scenario
./scripts/run_baseline.sh scenarios/baseline.json -n single-conn-baseline
```

The runner will:
1. Build binaries if missing
2. Start a fresh `benchbroker` on `:5672` (killed on exit)
3. Create a results directory and write `manifest.json`
4. Run each scenario: save `<scenario>.csv` (intervals) and pretty-printed `<scenario>.json` (summary)
5. Print results location on completion

### 3) Run the benchmark directly (ad-hoc)
```bash
# Start broker
./bin/benchbroker &

# Run with flags (see baseline-scenarios.md for details)
./bin/benchmark -conns 1 -channels 1 -publishers 1 -consumers 1 -msgs 10000 -size 64 -ack -duration 0 -warmup 0s -report 1s -output /tmp/out.csv
# JSON summary goes to stdout by default
```

## Where results go
```
results/baselines/<git-sha-short>/<scenario-set>/<timestamp>/
├── manifest.json           # run metadata (commit, env, broker config, scenarios file, filter)
├── <scenario>.csv          # interval samples (per -report)
└── <scenario>.json         # pretty-printed summary (totals, rates, latencies, topology)
```

- `<scenario-set>` comes from the JSON filename (e.g. `baseline.json` → `baseline/`)
- Each run creates a new timestamped directory (reruns never overwrite)
- `manifest.json` records the exact `scenarios_file` used for provenance

## Files
- `baseline.json` - Default baseline scenarios
- `baseline-scenarios.md` - Detailed explanation of each scenario and every flag
- `scenario2.json` - Example of a custom scenario set (use as template)
- `README.md` - This file

## Creating custom scenarios
Create a new JSON file (e.g. `scenarios/mytest.json`) with:
```json
{
  "version": 1,
  "scenarios": [
    {
      "name": "my-scenario",
      "description": "Description here",
      "flags": ["-conns", "2", "-channels", "4", "-publishers", "4", "-consumers", "4", "-msgs", "5000", "-size", "64", "-ack", "-duration", "0", "-warmup", "0s", "-report", "1s"]
    }
  ]
}
```
Run with `./scripts/run_baseline.sh scenarios/mytest.json`. Results will go under `results/baselines/<sha>/mytest/<timestamp>/`.

For flag details and topology rules, see [baseline-scenarios.md](baseline-scenarios.md).

## Broker reuse / custom address

The runner can reuse an existing broker or use a custom address:

```bash
# Use a custom address
./scripts/run_baseline.sh scenarios/baseline.json --addr 127.0.0.1:5672

# Require an existing broker (don't start one)
./scripts/run_baseline.sh scenarios/baseline.json --no-start-broker

# Via environment variable
BENCH_BROKER_ADDR=127.0.0.1:5672 ./scripts/run_baseline.sh scenarios/baseline.json
```

- If a broker is already listening on the target address, the runner will **reuse it** and will **not** kill it on exit.
- If none is found and `--no-start-broker` is not set, the runner starts `bin/benchbroker` and kills it only if it started it.
- The manifest records `broker_addr`, `broker_started_by_us`, and `broker_pid` for provenance.
