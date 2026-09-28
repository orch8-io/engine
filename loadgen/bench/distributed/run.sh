#!/usr/bin/env bash
# Distributed Orch8 benchmark: one `control` node + N `executor` nodes + W
# REST workers on a shared PostgreSQL, driven by the standard 3-step x 50 ms
# workload (../systems/orch8/driver.mjs). Methodology and how to read the
# result: docs/BENCHMARK_DISTRIBUTED.md.
#
#   # every role in its own container on this Docker host
#   loadgen/bench/distributed/run.sh --mode compose --executors 3 --workers 4 --n 2000 --concurrency 200
#
#   # real multi-host: roles placed by a hosts file (see hosts.example)
#   loadgen/bench/distributed/run.sh --mode ssh --hosts hosts.txt --n 5000 --concurrency 500
#
#   # single-host smoke without Docker: local processes, disposable Postgres DB
#   loadgen/bench/distributed/run.sh --mode local --server-bin target/debug/orch8-server \
#     --database-url postgres://localhost:5432/orch8_bench --executors 2 --workers 2 --n 300
#
# Produces results/distributed-<mode>-<stamp>-run<i>/result.json (validated
# against ../results.schema.json, including the `topology` block).
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: run.sh --mode compose|ssh|local
              [--executors 2] [--workers 2] [--worker-slots 100]
              [--n 1000] [--concurrency 100] [--warmup 50] [--runs 1] [--deadline 600]
              [--label TEXT] [--keep]
  compose: [--image REF] [--cpus-per-node N] [--mem-per-node 2g]
  local:   --server-bin PATH --database-url postgres://.../<disposable db>
  ssh:     --hosts FILE   (lines: "<role> <ssh-target> [count]"; roles: database-url, control, executor, worker)
USAGE
}

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="$(cd "$HERE/.." && pwd)"
MODE="" EXECUTORS=2 WORKERS=2 SLOTS=100 N=1000 CONCURRENCY=100 WARMUP=50 RUNS=1 DEADLINE=600
LABEL="" KEEP=0 IMAGE="" CPUS="" MEM="" SERVER_BIN="" DATABASE_URL="" HOSTS=""
API_KEY="bench-only-api-key-not-a-secret-0123456789"
ENC_KEY="00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --executors) EXECUTORS="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --worker-slots) SLOTS="$2"; shift 2 ;;
    --n) N="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --deadline) DEADLINE="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --image) IMAGE="$2"; shift 2 ;;
    --cpus-per-node) CPUS="$2"; shift 2 ;;
    --mem-per-node) MEM="$2"; shift 2 ;;
    --server-bin) SERVER_BIN="$2"; shift 2 ;;
    --database-url) DATABASE_URL="$2"; shift 2 ;;
    --hosts) HOSTS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done
case "$MODE" in compose|ssh|local) ;; *) usage >&2; exit 2 ;; esac

RESULTS_DIR="$BENCH/results"
mkdir -p "$RESULTS_DIR"
GIT_SHA="$(git -C "$HERE" rev-parse HEAD 2>/dev/null || echo unknown)"
now_iso() { node -e 'console.log(new Date().toISOString())'; }
hostinfo_local() { sh "$BENCH/lib/hostinfo.sh"; }
hostinfo_ssh() { ssh -o BatchMode=yes "$1" 'sh -s' < "$BENCH/lib/hostinfo.sh"; }

# with_count <hostinfo-json> <count> -> json with "processes"
with_count() { node -e 'const h=JSON.parse(process.argv[1]);h.processes=Number(process.argv[2]);console.log(JSON.stringify(h))' "$1" "$2"; }

# write_topology <out> <single_host> <engine_build> <db-json> <control-json> <executor-jsons> <worker-jsons>
# (executor/worker args are newline-separated JSON objects)
write_topology() {
  node - "$@" <<'NODE'
const [out, single, build, db, control, execs, workers, mode, label, nExec, nWork, slots] = process.argv.slice(2);
const lines = (s) => s.split("\n").filter(Boolean).map((l) => JSON.parse(l));
const topology = {
  mode, single_host: single === "1",
  ...(label ? { label } : {}),
  executors: Number(nExec), workers: Number(nWork), worker_slots: Number(slots),
  engine_build: build,
  ...(db ? { database: JSON.parse(db) } : {}),
  control: JSON.parse(control),
  executor_hosts: lines(execs),
  worker_hosts: lines(workers),
};
require("node:fs").writeFileSync(out, JSON.stringify(topology, null, 2));
NODE
}

PIDS=()
cleanup_local() {
  for pid in "${PIDS[@]:-}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; done
  for pid in "${PIDS[@]:-}"; do [ -n "$pid" ] && wait "$pid" 2>/dev/null || true; done
  PIDS=()
}

wait_http() { # url seconds
  local url="$1" deadline=$(( $(date +%s) + $2 ))
  until curl -fsS -o /dev/null "$url" 2>/dev/null; do
    [ "$(date +%s)" -ge "$deadline" ] && { echo "timed out waiting for $url" >&2; return 1; }
    sleep 1
  done
}

run_driver() { # out_dir base_url
  (
    cd "$BENCH/systems/orch8"
    ORCH8_URL="$2" BENCH_N="$N" BENCH_CONCURRENCY="$CONCURRENCY" BENCH_WARMUP="$WARMUP" \
      BENCH_SAMPLES="$1/samples.jsonl" BENCH_DEADLINE_S="$DEADLINE" \
      node driver.mjs > "$1/driver.out"
  ) || echo "driver exited non-zero; summarising what it recorded" >&2
}

summarize() { # out_dir started ended images_json notes
  local out_dir="$1" summary
  summary="$(tail -n 1 "$out_dir/driver.out" 2>/dev/null || true)"
  [ -z "$summary" ] && summary='{"completed":0,"failed":0,"errors":["driver produced no summary"]}'
  cat "$out_dir"/activity*.jsonl > "$out_dir/activity.jsonl.all" 2>/dev/null || true
  node "$BENCH/lib/summarize.mjs" \
    --system=orch8 --scenario=throughput --run-index="$RUN_INDEX" \
    --n="$N" --concurrency="$CONCURRENCY" --warmup="$WARMUP" \
    --started="$2" --ended="$3" --git-sha="$GIT_SHA" \
    --samples="$out_dir/samples.jsonl" --activity-log="$out_dir/activity.jsonl.all" \
    --images="$4" --sdk='{}' --driver-summary="$summary" \
    --topology="$out_dir/topology.json" ${5:+--notes="$5"} \
    --out="$out_dir/result.json"
  node "$BENCH/validate-result.mjs" "$out_dir/result.json"
}

# ── compose ──────────────────────────────────────────────────────────────────
run_compose() {
  local out_dir="$1" project="bench-dist-$RUN_INDEX" compose started ended info
  node "$HERE/gen-compose.mjs" --executors="$EXECUTORS" --workers="$WORKERS" --worker-slots="$SLOTS" \
    ${IMAGE:+--image="$IMAGE"} ${CPUS:+--cpus-per-node="$CPUS"} ${MEM:+--mem-per-node="$MEM"} \
    --out="$out_dir/compose.yml" >/dev/null
  compose="docker compose -p $project -f $out_dir/compose.yml"
  export BENCH_OUT_DIR="$out_dir"
  trap '[ "$KEEP" = 1 ] || $compose down -v --remove-orphans >/dev/null 2>&1 || true' EXIT INT TERM
  $compose down -v --remove-orphans >/dev/null 2>&1 || true
  $compose up -d --wait
  node "$BENCH/lib/images.mjs" "$project" "$out_dir/compose.yml" "$out_dir/images.json" || echo '{}' > "$out_dir/images.json"
  info="$(hostinfo_local)"
  write_topology "$out_dir/topology.json" 1 "${IMAGE:-ghcr.io/orch8-io/engine:latest}" \
    "$(with_count "$info" 1)" "$(with_count "$info" 1)" "$(with_count "$info" "$EXECUTORS")" \
    "$(with_count "$info" "$WORKERS")" compose "$LABEL" "$EXECUTORS" "$WORKERS" "$SLOTS"
  started="$(now_iso)"; run_driver "$out_dir" "http://127.0.0.1:18080/api/v1"; ended="$(now_iso)"
  summarize "$out_dir" "$started" "$ended" "$out_dir/images.json" \
    "compose mode: all containers on one Docker host (single-host, not a distributed result)${CPUS:+; cpus/node=$CPUS}${MEM:+; mem/node=$MEM}"
  [ "$KEEP" = 1 ] || $compose down -v --remove-orphans >/dev/null 2>&1 || true
  trap - EXIT INT TERM
}

# ── local processes ──────────────────────────────────────────────────────────
run_local() {
  local out_dir="$1" started ended info version cfg db_name admin_url i
  [ -x "$SERVER_BIN" ] || { echo "--server-bin must point at an orch8-server binary" >&2; exit 2; }
  [ -n "$DATABASE_URL" ] || { echo "--database-url is required (a DISPOSABLE database: it is dropped)" >&2; exit 2; }
  command -v psql >/dev/null || { echo "local mode needs psql to recreate the database" >&2; exit 2; }
  db_name="${DATABASE_URL##*/}"; db_name="${db_name%%\?*}"
  admin_url="${DATABASE_URL%/*}/postgres"
  case "$db_name" in *bench*) ;; *) echo "refusing to drop '$db_name': the database name must contain 'bench'" >&2; exit 2 ;; esac
  psql "$admin_url" -qc "DROP DATABASE IF EXISTS \"$db_name\"" -c "CREATE DATABASE \"$db_name\""
  cfg="$out_dir/empty.toml"; : > "$cfg"
  trap 'cleanup_local' EXIT INT TERM

  # `exec` so the backgrounded subshell *is* the server and `kill $!` reaches it.
  common_env() {
    exec env ORCH8_STORAGE_BACKEND=postgres ORCH8_DATABASE_URL="$DATABASE_URL" \
      ORCH8_API_KEY="$API_KEY" ORCH8_ENCRYPTION_KEY="$ENC_KEY" ORCH8_REQUIRE_TENANT_HEADER=true \
      ORCH8_LOG_LEVEL=warn ORCH8_LOG_JSON=true "$@"
  }
  common_env ORCH8_NODE_ROLE=control ORCH8_RUN_MIGRATIONS=true \
    ORCH8_HTTP_ADDR=127.0.0.1:18080 ORCH8_GRPC_ADDR=127.0.0.1:15051 \
    "$SERVER_BIN" --config "$cfg" > "$out_dir/control.log" 2>&1 &
  PIDS+=($!)
  wait_http "http://127.0.0.1:18080/health/ready" 120
  for i in $(seq 1 "$EXECUTORS"); do
    common_env ORCH8_NODE_ROLE=executor \
      ORCH8_HTTP_ADDR="127.0.0.1:$((18100 + i))" ORCH8_GRPC_ADDR="127.0.0.1:$((15100 + i))" \
      "$SERVER_BIN" --config "$cfg" > "$out_dir/executor-$i.log" 2>&1 &
    PIDS+=($!)
  done
  for i in $(seq 1 "$EXECUTORS"); do wait_http "http://127.0.0.1:$((18100 + i))/health/live" 120; done
  for i in $(seq 1 "$WORKERS"); do
    ORCH8_URL=http://127.0.0.1:18080/api/v1 ORCH8_API_KEY="$API_KEY" ORCH8_TENANT_ID=bench \
      WORKER_SLOTS="$SLOTS" ACTIVITY_LOG="$out_dir/activity-worker-$i.jsonl" \
      node "$BENCH/systems/orch8/worker.mjs" > "$out_dir/worker-$i.log" 2>&1 &
    PIDS+=($!)
  done

  version="$("$SERVER_BIN" --version 2>/dev/null | head -n 1 || echo unknown)"
  info="$(hostinfo_local)"
  write_topology "$out_dir/topology.json" 1 "$SERVER_BIN ($version)" \
    "$(with_count "$info" 1)" "$(with_count "$info" 1)" "$(with_count "$info" "$EXECUTORS")" \
    "$(with_count "$info" "$WORKERS")" local "$LABEL" "$EXECUTORS" "$WORKERS" "$SLOTS"
  echo '{}' > "$out_dir/images.json"
  started="$(now_iso)"; run_driver "$out_dir" "http://127.0.0.1:18080/api/v1"; ended="$(now_iso)"
  cleanup_local
  trap - EXIT INT TERM
  summarize "$out_dir" "$started" "$ended" "$out_dir/images.json" \
    "local mode: control, executors, workers, Postgres and driver as processes on ONE host — single-host smoke, not a distributed result. Server binary: $SERVER_BIN ($version)."
}

# ── ssh ──────────────────────────────────────────────────────────────────────
# hosts file, one role per line ('#' comments):
#   database-url  postgres://orch8:secret@10.0.0.5:5432/orch8_bench   (reachable from every host; dropped!)
#   control       ubuntu@10.0.0.10
#   executor      ubuntu@10.0.0.11  [count]
#   worker        ubuntu@10.0.0.21  [count]
# Every control/executor host needs `orch8-server` on PATH; every worker host
# needs Node 22 (worker.mjs is copied over). The driver runs here.
run_ssh() {
  local out_dir="$1" started ended control="" db_url="" line role target count
  local -a exec_targets=() exec_counts=() worker_targets=() worker_counts=()
  [ -f "$HOSTS" ] || { echo "--hosts FILE is required for --mode ssh" >&2; exit 2; }
  while read -r role target count; do
    case "$role" in ''|\#*) continue ;; esac
    case "$role" in
      database-url) db_url="$target" ;;
      control) control="$target" ;;
      executor) exec_targets+=("$target"); exec_counts+=("${count:-1}") ;;
      worker) worker_targets+=("$target"); worker_counts+=("${count:-1}") ;;
      *) echo "unknown role '$role' in $HOSTS" >&2; exit 2 ;;
    esac
  done < "$HOSTS"
  [ -n "$db_url" ] && [ -n "$control" ] && [ ${#exec_targets[@]} -gt 0 ] && [ ${#worker_targets[@]} -gt 0 ] ||
    { echo "$HOSTS needs database-url, control, >=1 executor and >=1 worker line" >&2; exit 2; }
  case "${db_url##*/}" in *bench*) ;; *) echo "refusing: database name must contain 'bench' (it is dropped)" >&2; exit 2 ;; esac
  local control_ip="${control#*@}"
  local base="http://$control_ip:18080/api/v1"
  local db_name="${db_url##*/}"; db_name="${db_name%%\?*}"
  local remote_env="ORCH8_STORAGE_BACKEND=postgres ORCH8_DATABASE_URL='$db_url' ORCH8_API_KEY=$API_KEY ORCH8_ENCRYPTION_KEY=$ENC_KEY ORCH8_REQUIRE_TENANT_HEADER=true ORCH8_LOG_LEVEL=warn"

  stop_remote() {
    for t in "$control" "${exec_targets[@]}" "${worker_targets[@]}"; do
      ssh -o BatchMode=yes "$t" 'pkill -f "orch8-bench-" || true' >/dev/null 2>&1 || true
    done
  }
  trap stop_remote EXIT INT TERM

  ssh -o BatchMode=yes "$control" "psql '${db_url%/*}/postgres' -qc 'DROP DATABASE IF EXISTS \"$db_name\"' -c 'CREATE DATABASE \"$db_name\"'"
  # exec -a names each process so stop_remote can find exactly ours.
  ssh -o BatchMode=yes "$control" "nohup bash -c \"exec -a orch8-bench-control env $remote_env ORCH8_NODE_ROLE=control ORCH8_RUN_MIGRATIONS=true ORCH8_HTTP_ADDR=0.0.0.0:18080 ORCH8_GRPC_ADDR=0.0.0.0:15051 orch8-server --config /dev/null\" > /tmp/orch8-bench-control.log 2>&1 &"
  wait_http "http://$control_ip:18080/health/ready" 180
  local i j
  for i in "${!exec_targets[@]}"; do
    for j in $(seq 1 "${exec_counts[$i]}"); do
      ssh -o BatchMode=yes "${exec_targets[$i]}" "nohup bash -c \"exec -a orch8-bench-exec-$j env $remote_env ORCH8_NODE_ROLE=executor ORCH8_HTTP_ADDR=127.0.0.1:$((18100 + j)) ORCH8_GRPC_ADDR=127.0.0.1:$((15100 + j)) orch8-server --config /dev/null\" > /tmp/orch8-bench-exec-$j.log 2>&1 &"
    done
  done
  for i in "${!worker_targets[@]}"; do
    scp -q -o BatchMode=yes "$BENCH/systems/orch8/worker.mjs" "${worker_targets[$i]}:/tmp/orch8-bench-worker.mjs"
    for j in $(seq 1 "${worker_counts[$i]}"); do
      ssh -o BatchMode=yes "${worker_targets[$i]}" "rm -f /tmp/orch8-bench-activity-$j.jsonl; nohup bash -c \"exec -a orch8-bench-worker-$j env ORCH8_URL=$base ORCH8_API_KEY=$API_KEY ORCH8_TENANT_ID=bench WORKER_SLOTS=$SLOTS ACTIVITY_LOG=/tmp/orch8-bench-activity-$j.jsonl node /tmp/orch8-bench-worker.mjs\" > /tmp/orch8-bench-worker-$j.log 2>&1 &"
    done
  done

  local exec_json="" worker_json="" total_exec=0 total_work=0 version
  for i in "${!exec_targets[@]}"; do
    exec_json+="$(with_count "$(hostinfo_ssh "${exec_targets[$i]}")" "${exec_counts[$i]}")"$'\n'
    total_exec=$((total_exec + exec_counts[i]))
  done
  for i in "${!worker_targets[@]}"; do
    worker_json+="$(with_count "$(hostinfo_ssh "${worker_targets[$i]}")" "${worker_counts[$i]}")"$'\n'
    total_work=$((total_work + worker_counts[i]))
  done
  version="$(ssh -o BatchMode=yes "$control" 'orch8-server --version' 2>/dev/null | head -n 1 || echo unknown)"
  local distinct
  distinct="$(printf '%s\n' "$control" "${exec_targets[@]}" "${worker_targets[@]}" | sort -u | wc -l | tr -d ' ')"
  write_topology "$out_dir/topology.json" "$([ "$distinct" = 1 ] && echo 1 || echo 0)" "orch8-server on PATH ($version)" \
    "" "$(with_count "$(hostinfo_ssh "$control")" 1)" "$exec_json" "$worker_json" ssh "$LABEL" \
    "$total_exec" "$total_work" "$SLOTS"
  echo '{}' > "$out_dir/images.json"

  started="$(now_iso)"; run_driver "$out_dir" "$base"; ended="$(now_iso)"
  for i in "${!worker_targets[@]}"; do
    for j in $(seq 1 "${worker_counts[$i]}"); do
      scp -q -o BatchMode=yes "${worker_targets[$i]}:/tmp/orch8-bench-activity-$j.jsonl" \
        "$out_dir/activity-w$i-$j.jsonl" 2>/dev/null || true
    done
  done
  stop_remote
  trap - EXIT INT TERM
  summarize "$out_dir" "$started" "$ended" "$out_dir/images.json" \
    "ssh mode: $distinct distinct hosts; database $db_name (host details not collected — record them in notes if it is a separate machine)"
}

for RUN_INDEX in $(seq 1 "$RUNS"); do
  OUT_DIR="$RESULTS_DIR/distributed-$MODE-$(date -u +%Y%m%dT%H%M%SZ)-run$RUN_INDEX"
  mkdir -p "$OUT_DIR"
  echo "== distributed / $MODE / executors=$EXECUTORS workers=$WORKERS / run $RUN_INDEX -> $OUT_DIR"
  "run_$MODE" "$OUT_DIR"
done
