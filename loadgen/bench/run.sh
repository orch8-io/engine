#!/usr/bin/env bash
# Reproducible comparative benchmark runner. See docs/BENCHMARKS.md.
#
#   loadgen/bench/run.sh --system orch8 --scenario throughput --n 1000 --concurrency 100
#   loadgen/bench/run.sh --system temporal --scenario crash_recovery --n 500 --concurrency 50 --kill-after 5
#
# Every run starts from a fresh stack (volumes removed) and is torn down on
# exit, including on Ctrl-C or failure.
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: run.sh --system orch8|temporal|inngest|hatchet|trigger
              --scenario throughput|crash_recovery
              [--n 1000] [--concurrency 100] [--warmup 50] [--runs 3]
              [--kill-after 5] [--down-for 5] [--deadline 600] [--keep]
USAGE
}

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYSTEM="" SCENARIO="" N=1000 CONCURRENCY=100 WARMUP=50 RUNS=3
KILL_AFTER=5 DOWN_FOR=5 DEADLINE=600 KEEP=0

while [ $# -gt 0 ]; do
  case "$1" in
    --system) SYSTEM="$2"; shift 2 ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
    --n) N="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --kill-after) KILL_AFTER="$2"; shift 2 ;;
    --down-for) DOWN_FOR="$2"; shift 2 ;;
    --deadline) DEADLINE="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$SYSTEM" in orch8|temporal|inngest|hatchet|trigger) ;; *) usage >&2; exit 2 ;; esac
case "$SCENARIO" in throughput|crash_recovery) ;; *) usage >&2; exit 2 ;; esac

SYSDIR="$HERE/systems/$SYSTEM"
COMPOSE_FILE="$SYSDIR/docker-compose.yml"
KILL_SERVICE="" NEEDS_NPM=0 SETUP_SCRIPT="" UNVERIFIED_STACK=0 SDK_PACKAGES=""
# shellcheck source=/dev/null
. "$SYSDIR/bench.env"

if [ "$UNVERIFIED_STACK" = "1" ] && [ "${TRIGGER_BENCH_UNVERIFIED_OK:-0}" != "1" ]; then
  echo "$SYSTEM: the compose stack is an unverified skeleton (see $COMPOSE_FILE)." >&2
  echo "Complete it, then re-run with TRIGGER_BENCH_UNVERIFIED_OK=1." >&2
  exit 3
fi

if [ "$NEEDS_NPM" = "1" ] && [ ! -d "$SYSDIR/node_modules" ]; then
  (cd "$SYSDIR" && npm install --no-audit --no-fund)
fi

RESULTS_DIR="$HERE/results"
mkdir -p "$RESULTS_DIR"
GIT_SHA="$(git -C "$HERE" rev-parse HEAD 2>/dev/null || echo unknown)"

now_iso() { node -e 'console.log(new Date().toISOString())'; }

sdk_versions() {
  local json="{" first=1 pkg version
  for pkg in $SDK_PACKAGES; do
    version="$(node -e 'try{console.log(require(process.argv[1]+"/package.json").version)}catch{console.log("")}' "$SYSDIR/node_modules/$pkg" 2>/dev/null || true)"
    [ -z "$version" ] && continue
    [ $first -eq 0 ] && json+=","
    json+="\"$pkg\":\"$version\""
    first=0
  done
  echo "$json}"
}

run_once() {
  local index="$1"
  local stamp out_dir project compose
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  out_dir="$RESULTS_DIR/$SYSTEM-$SCENARIO-$stamp-run$index"
  mkdir -p "$out_dir"
  : > "$out_dir/activity.jsonl"
  chmod 666 "$out_dir/activity.jsonl"
  project="bench-$SYSTEM-$index"
  export BENCH_OUT_DIR="$out_dir"
  compose="docker compose -p $project -f $COMPOSE_FILE"

  teardown() {
    if [ "$KEEP" = "1" ]; then
      echo "--keep: leaving project $project running"
    else
      $compose --profile worker down -v --remove-orphans >/dev/null 2>&1 || true
    fi
  }
  trap teardown EXIT INT TERM

  echo "== $SYSTEM / $SCENARIO / run $index -> $out_dir"
  $compose down -v --remove-orphans >/dev/null 2>&1 || true
  $compose up -d --build --wait
  if [ -n "$SETUP_SCRIPT" ]; then
    COMPOSE="$compose" "$SYSDIR/$SETUP_SCRIPT"
  fi
  if [ -f "$out_dir/driver.env" ]; then
    set -a
    # shellcheck source=/dev/null
    . "$out_dir/driver.env"
    set +a
  fi
  node "$HERE/lib/images.mjs" "$project" "$COMPOSE_FILE" "$out_dir/images.json" || true

  local started ended summary kill_at="" restart_at="" driver_pid
  started="$(now_iso)"
  (
    cd "$SYSDIR"
    BENCH_N="$N" BENCH_CONCURRENCY="$CONCURRENCY" BENCH_WARMUP="$WARMUP" \
      BENCH_SAMPLES="$out_dir/samples.jsonl" BENCH_DEADLINE_S="$DEADLINE" \
      node driver.mjs > "$out_dir/driver.out"
  ) &
  driver_pid=$!

  if [ "$SCENARIO" = "crash_recovery" ]; then
    sleep "$KILL_AFTER"
    kill_at="$(now_iso)"
    $compose kill -s SIGKILL "$KILL_SERVICE"
    sleep "$DOWN_FOR"
    restart_at="$(now_iso)"
    $compose start "$KILL_SERVICE"
  fi

  wait "$driver_pid" || echo "driver exited non-zero; summarising what it recorded" >&2
  ended="$(now_iso)"
  summary="$(tail -n 1 "$out_dir/driver.out" 2>/dev/null || true)"
  [ -z "$summary" ] && summary='{"completed":0,"failed":0,"errors":["driver produced no summary"]}'

  node "$HERE/lib/summarize.mjs" \
    --system="$SYSTEM" --scenario="$SCENARIO" --run-index="$index" \
    --n="$N" --concurrency="$CONCURRENCY" --warmup="$WARMUP" \
    --started="$started" --ended="$ended" --git-sha="$GIT_SHA" \
    --samples="$out_dir/samples.jsonl" --activity-log="$out_dir/activity.jsonl" \
    --images="$out_dir/images.json" --sdk="$(sdk_versions)" \
    --driver-summary="$summary" \
    ${kill_at:+--kill-at="$kill_at"} ${restart_at:+--restart-at="$restart_at"} \
    ${kill_at:+--killed-service="$KILL_SERVICE"} \
    --out="$out_dir/result.json"
  node "$HERE/validate-result.mjs" "$out_dir/result.json"

  teardown
  trap - EXIT INT TERM
}

for i in $(seq 1 "$RUNS"); do
  run_once "$i"
done
echo "done. Report the median of the $RUNS runs; see docs/BENCHMARKS.md."
