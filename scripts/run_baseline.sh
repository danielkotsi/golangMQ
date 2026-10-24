#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: $(basename "$0") <scenarios.json> [-n <scenario-name>]

Options:
  -n, --name <name>   Run only the scenario with this name (default: run all)
  -h, --help          Show this help message

Examples:
  $(basename "$0") scenarios/baseline.json
  $(basename "$0") scenarios/baseline.json -n single-conn-baseline
USAGE
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_BROKER="$ROOT/bin/benchbroker"
BIN_BENCH="$ROOT/bin/benchmark"

SCENARIOS_JSON=""
SCENARIO_FILTER=""

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

COMMIT_SHORT="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")"
SET_NAME="$(basename "$SCENARIOS_JSON" .json)"
OUTDIR="$ROOT/results/baselines/$COMMIT_SHORT/$SET_NAME/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUTDIR"

if [[ ! -x "$BIN_BROKER" || ! -x "$BIN_BENCH" ]]; then
  echo "Building binaries..."
  (cd "$ROOT" && go build -trimpath -o "$BIN_BROKER" ./cmd/broker && go build -trimpath -o "$BIN_BENCH" ./cmd/benchmark)
fi

# Start broker
"$BIN_BROKER" &
BROKER_PID=$!
trap 'kill $BROKER_PID 2>/dev/null || true' EXIT
sleep 2

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
  "scenarios_file": "$SCENARIOS_JSON",
  "scenario_set": "$SET_NAME",
  "scenario_filter": "${SCENARIO_FILTER:-all}"
}
MANIFEST

export SCENARIO_FILTER

python3 - "$SCENARIOS_JSON" "$BIN_BENCH" "$OUTDIR" << 'PYTHON'
import json
import subprocess
import sys
import os

scenarios_file = sys.argv[1]
bench_bin = sys.argv[2]
outdir = sys.argv[3]
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
    flags = scenario.get('flags', [])
    cmd = [bench_bin] + flags + ['-output', csv_out]
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
