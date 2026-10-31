#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: $(basename "$0") <scenarios.json> [-n <scenario-name>] [options]

Options:
  -n, --name <name>       Run only the scenario with this name (default: run all)
  --addr <host:port>      Broker address to use/check (default: 127.0.0.1:5672)
  --no-start-broker       Fail if no broker is listening, do not start one
  --reuse-broker          Prefer reusing existing broker if present (default behavior)
  -h, --help              Show this help message

Environment:
  BENCH_BROKER_ADDR       Override broker address (used if --addr not specified)

Examples:
  $(basename "$0") scenarios/baseline.json
  $(basename "$0") scenarios/baseline.json -n single-conn-baseline
  $(basename "$0") scenarios/baseline.json --addr 127.0.0.1:5672
  $(basename "$0") scenarios/baseline.json --no-start-broker
USAGE
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_BROKER="$ROOT/bin/benchbroker"
BIN_BENCH="$ROOT/bin/benchmark"

SCENARIOS_JSON=""
SCENARIO_FILTER=""
BROKER_ADDR="${BENCH_BROKER_ADDR:-127.0.0.1:5672}"
NO_START_BROKER=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    -n|--name)
      if [[ -z "${2:-}" ]]; then
        echo "Error: -n requires a scenario name" >&2
        usage
        exit 1
      fi
      SCENARIO_FILTER="$2"
      shift 2
      ;;
    --addr)
      if [[ -z "${2:-}" ]]; then
        echo "Error: --addr requires <host:port>" >&2
        usage
        exit 1
      fi
      BROKER_ADDR="$2"
      shift 2
      ;;
    --no-start-broker)
      NO_START_BROKER=true
      shift
      ;;
    --reuse-broker)
      shift
      ;;
    -*)
      echo "Error: unknown option $1" >&2
      usage
      exit 1
      ;;
    *)
      if [[ -z "$SCENARIOS_JSON" ]]; then
        SCENARIOS_JSON="$1"
      else
        echo "Error: unexpected argument $1" >&2
        usage
        exit 1
      fi
      shift
      ;;
  esac
done

if [[ -z "$SCENARIOS_JSON" ]]; then
  echo "Error: scenarios JSON file is required" >&2
  usage
  exit 1
fi

# Resolve to absolute path
if [[ "$SCENARIOS_JSON" != /* ]]; then
  SCENARIOS_JSON="$ROOT/$SCENARIOS_JSON"
fi

if [[ ! -f "$SCENARIOS_JSON" ]]; then
  echo "Error: scenarios file not found: $SCENARIOS_JSON" >&2
  exit 1
fi

# Parse host/port
BROKER_HOST="${BROKER_ADDR%:*}"
BROKER_PORT="${BROKER_ADDR##*:}"
if [[ -z "$BROKER_HOST" || -z "$BROKER_PORT" || "$BROKER_HOST" == "$BROKER_ADDR" ]]; then
  echo "Error: invalid broker address format (expected host:port): $BROKER_ADDR" >&2
  exit 1
fi

check_broker() {
  timeout 2 bash -c "echo > /dev/tcp/$BROKER_HOST/$BROKER_PORT" 2>/dev/null
}

COMMIT_SHORT="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")"
SET_NAME="$(basename "$SCENARIOS_JSON" .json)"
OUTDIR="$ROOT/results/baselines/$COMMIT_SHORT/$SET_NAME/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUTDIR"

if [[ ! -x "$BIN_BROKER" || ! -x "$BIN_BENCH" ]]; then
  echo "Building binaries..."
  (cd "$ROOT" && go build -trimpath -o "$BIN_BROKER" ./cmd/broker && go build -trimpath -o "$BIN_BENCH" ./cmd/benchmark)
fi

BROKER_STARTED_BY_US=false
BROKER_PID=""

if check_broker; then
  echo "Using existing broker at $BROKER_ADDR"
  BROKER_STARTED_BY_US=false
  BROKER_PID=""
else
  if [[ "$NO_START_BROKER" == "true" ]]; then
    echo "Error: no broker listening at $BROKER_ADDR and --no-start-broker specified" >&2
    exit 1
  fi
  echo "Starting broker at $BROKER_ADDR..."
  "$BIN_BROKER" &
  BROKER_PID=$!
  BROKER_STARTED_BY_US=true
  sleep 2
  if ! check_broker; then
    echo "Error: failed to start broker at $BROKER_ADDR" >&2
    kill "$BROKER_PID" 2>/dev/null || true
    exit 1
  fi
  echo "Broker started (pid $BROKER_PID)"
fi

trap 'if [[ "$BROKER_STARTED_BY_US" == "true" && -n "${BROKER_PID:-}" ]]; then kill "$BROKER_PID" 2>/dev/null || true; fi' EXIT

# Create manifest
cat > "$OUTDIR/manifest.json" << MANIFEST
{
  "git_sha": "$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")",
  "git_sha_short": "$COMMIT_SHORT",
  "go_version": "$(go version 2>/dev/null || echo "unknown")",
  "os": "$(uname -srm 2>/dev/null || echo "unknown")",
  "hostname": "$(hostname 2>/dev/null || echo "unknown")",
  "timestamp": "$(date -Iseconds)",
  "broker_config": {"ChannelMax": 10, "FramesMax": 10372, "HeartbeatSec": 10},
  "broker_addr": "$BROKER_ADDR",
  "broker_started_by_us": $BROKER_STARTED_BY_US,
  "broker_pid": $(if [[ "$BROKER_STARTED_BY_US" == "true" && -n "$BROKER_PID" ]]; then echo "$BROKER_PID"; else echo "null"; fi),
  "scenarios_file": "$SCENARIOS_JSON",
  "scenario_set": "$SET_NAME",
  "scenario_filter": "${SCENARIO_FILTER:-all}"
}
MANIFEST

export SCENARIO_FILTER
export BENCH_BROKER_ADDR="$BROKER_ADDR"

python3 - "$SCENARIOS_JSON" "$BIN_BENCH" "$OUTDIR" "$BROKER_ADDR" << 'PYTHON'
import json
import subprocess
import sys
import os

scenarios_file = sys.argv[1]
bench_bin = sys.argv[2]
outdir = sys.argv[3]
broker_addr = sys.argv[4]
target = os.environ.get('SCENARIO_FILTER') or None

with open(scenarios_file) as f:
    data = json.load(f)

scenarios = data.get('scenarios', [])
if not scenarios:
    print("No scenarios found in file", file=sys.stderr)
    sys.exit(1)

ran = 0
for scenario in scenarios:
    name = scenario['name']
    if target and name != target:
        continue
    print(f"Running scenario: {name}")
    csv_out = os.path.join(outdir, f"{name}.csv")
    json_out = os.path.join(outdir, f"{name}.json")
    flags = list(scenario.get('flags', []))
    cmd = [bench_bin] + flags + ['-addr', broker_addr] + ['-output', csv_out]
    try:
        result = subprocess.run(cmd, cwd=outdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        if result.returncode != 0:
            print(f"Scenario {name} failed with code {result.returncode}", file=sys.stderr)
            continue

        try:
            summary = json.loads(result.stdout)
            with open(json_out, 'w') as jf:
                json.dump(summary, jf, indent=2)
                jf.write('\n')
            print(f"Saved {json_out}")
        except json.JSONDecodeError:
            with open(json_out, 'w') as jf:
                jf.write(result.stdout)
            print(f"Saved {json_out} (raw, could not parse as JSON)")
        ran += 1
    except Exception as e:
        print(f"Error running {name}: {e}", file=sys.stderr)

if target and ran == 0:
    print(f"No scenario found with name '{target}'", file=sys.stderr)
    sys.exit(1)

print(f"Done. Ran {ran} scenario(s). Results in {outdir}")
PYTHON
