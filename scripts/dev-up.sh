#!/usr/bin/env bash
# dev-up.sh — boot engine (if down), loadgen, and dashboard for local review.
#
# Usage:
#   ./scripts/dev-up.sh            # starts all three; dashboard in foreground
#   ./scripts/dev-down.sh          # stops background processes
#
# Engine:    http://localhost:18080
# Dashboard: http://localhost:5173

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENGINE_URL="http://localhost:18080"
ENGINE_BIN="$ROOT/target/aarch64-apple-darwin/debug/orch8-server"
ENGINE_PID_FILE="/tmp/orch8-engine.pid"
ENGINE_LOG="/tmp/orch8-engine.log"
LOADGEN_PID_FILE="/tmp/orch8-loadgen.pid"
LOADGEN_LOG="/tmp/orch8-loadgen.log"

c_green() { printf '\033[32m%s\033[0m\n' "$*"; }
c_blue()  { printf '\033[34m%s\033[0m\n' "$*"; }
c_dim()   { printf '\033[2m%s\033[0m\n' "$*"; }

wait_for_health() {
  local i=0
  while (( i < 40 )); do
    if curl -sf "$ENGINE_URL/health/live" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
    i=$((i + 1))
  done
  return 1
}

# ── Engine ───────────────────────────────────────────────────────────────
if curl -sf "$ENGINE_URL/health/live" >/dev/null 2>&1; then
  c_green "✓ engine already running at $ENGINE_URL"
else
  if [[ ! -x "$ENGINE_BIN" ]]; then
    echo "Engine binary missing: $ENGINE_BIN"
    echo "Build it first:  cargo build -p orch8-server"
    exit 1
  fi
  c_blue "→ starting engine..."
  ORCH8_HTTP_ADDR="0.0.0.0:18080" \
  ORCH8_STORAGE_BACKEND="sqlite" \
  ORCH8_DATABASE_URL="sqlite::memory:" \
  ORCH8_CORS_ORIGINS="*" \
  RUST_LOG="${RUST_LOG:-info}" \
    nohup "$ENGINE_BIN" --insecure >"$ENGINE_LOG" 2>&1 &
  echo $! > "$ENGINE_PID_FILE"
  if wait_for_health; then
    c_green "✓ engine up (pid $(cat "$ENGINE_PID_FILE"), log: $ENGINE_LOG)"
  else
    echo "✗ engine failed to become healthy — tail $ENGINE_LOG"
    tail -n 40 "$ENGINE_LOG" || true
    exit 1
  fi
fi

# ── Loadgen ──────────────────────────────────────────────────────────────
if [[ -f "$LOADGEN_PID_FILE" ]] && kill -0 "$(cat "$LOADGEN_PID_FILE")" 2>/dev/null; then
  c_green "✓ loadgen already running (pid $(cat "$LOADGEN_PID_FILE"))"
else
  c_blue "→ starting loadgen..."
  (
    cd "$ROOT/loadgen"
    ORCH8_URL="$ENGINE_URL" \
      nohup npx tsx src/index.ts --preset=steady >"$LOADGEN_LOG" 2>&1 &
    echo $! > "$LOADGEN_PID_FILE"
  )
  sleep 1
  if kill -0 "$(cat "$LOADGEN_PID_FILE")" 2>/dev/null; then
    c_green "✓ loadgen up (pid $(cat "$LOADGEN_PID_FILE"), log: $LOADGEN_LOG)"
  else
    echo "✗ loadgen died immediately — tail $LOADGEN_LOG"
    tail -n 40 "$LOADGEN_LOG" || true
  fi
fi

# ── Dashboard (foreground) ───────────────────────────────────────────────
c_dim  "────────────────────────────────────────────────────────────"
c_green "engine    → $ENGINE_URL"
c_green "dashboard → http://localhost:5173  (starting now, Ctrl-C to stop)"
c_green "loadgen   → tail -f $LOADGEN_LOG"
c_dim  "stop background procs:   ./scripts/dev-down.sh"
c_dim  "────────────────────────────────────────────────────────────"

cd "$ROOT/dashboard"
VITE_ORCH8_API_URL="$ENGINE_URL" exec npm run dev
