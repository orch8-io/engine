#!/bin/sh
# Print one JSON object describing this host. POSIX sh, no dependencies, so it
# can run on a bare executor host over ssh:  ssh host 'sh -s' < hostinfo.sh
# Fields: host, os, cpu, cores, memory_bytes, hw_model (null when unknown).
set -u
esc() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' | tr -d '\n'; }
host="$(hostname 2>/dev/null || echo unknown)"
os="$(uname -srm 2>/dev/null || echo unknown)"
case "$(uname -s 2>/dev/null)" in
  Darwin)
    cpu="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
    cores="$(sysctl -n hw.ncpu 2>/dev/null || echo 0)"
    mem="$(sysctl -n hw.memsize 2>/dev/null || echo 0)"
    model="$(sysctl -n hw.model 2>/dev/null || true)"
    ;;
  Linux)
    cpu="$(sed -n 's/^model name[[:space:]]*:[[:space:]]*//p' /proc/cpuinfo 2>/dev/null | head -n 1)"
    [ -z "$cpu" ] && cpu="$(sed -n 's/^Model[[:space:]]*:[[:space:]]*//p' /proc/cpuinfo 2>/dev/null | head -n 1)"
    [ -z "$cpu" ] && cpu=unknown
    cores="$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 0)"
    mem="$(awk '/^MemTotal:/ {print $2 * 1024}' /proc/meminfo 2>/dev/null || echo 0)"
    model="$(cat /sys/devices/virtual/dmi/id/product_name 2>/dev/null || true)"
    ;;
  *) cpu=unknown cores=0 mem=0 model="" ;;
esac
if [ -n "$model" ]; then model_json="\"$(esc "$model")\""; else model_json=null; fi
printf '{"host":"%s","os":"%s","cpu":"%s","cores":%s,"memory_bytes":%s,"hw_model":%s}\n' \
  "$(esc "$host")" "$(esc "$os")" "$(esc "$cpu")" "${cores:-0}" "${mem:-0}" "$model_json"
