#!/usr/bin/env bash
# dev-down.sh — stop engine + loadgen started by dev-up.sh
set -u

stop_pid() {
  local label="$1" file="$2"
  if [[ -f "$file" ]]; then
    local pid
    pid="$(cat "$file")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      sleep 0.3
      kill -9 "$pid" 2>/dev/null || true
      echo "✓ stopped $label (pid $pid)"
    else
      echo "· $label pid $pid not running"
    fi
    rm -f "$file"
  else
    echo "· no $label pidfile"
  fi
}

stop_pid engine  /tmp/orch8-engine.pid
stop_pid loadgen /tmp/orch8-loadgen.pid
