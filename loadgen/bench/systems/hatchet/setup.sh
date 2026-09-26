#!/usr/bin/env bash
# Hatchet post-start hook (called by run.sh with the compose command in
# $COMPOSE and the output dir in $BENCH_OUT_DIR): mint a client token, start
# the worker with it, and export it for the driver via $BENCH_OUT_DIR/driver.env.
# UNVERIFIED: admin binary path, flags, and the default tenant id follow the
# hatchet-lite docs as understood when this harness was written.
set -euo pipefail

DEFAULT_TENANT_ID="707d0855-80ab-4e1f-a156-f1c4546cbf52"
token=""
for _ in $(seq 1 60); do
  if token=$($COMPOSE exec -T hatchet-lite /hatchet-admin token create --config /config --tenant-id "$DEFAULT_TENANT_ID" 2>/dev/null | tail -n 1) && [ -n "$token" ]; then
    break
  fi
  sleep 2
done
if [ -z "$token" ]; then
  echo "hatchet setup: could not create a client token" >&2
  exit 1
fi

HATCHET_CLIENT_TOKEN="$token" $COMPOSE --profile worker up -d --build worker
{
  echo "HATCHET_CLIENT_TOKEN=$token"
  echo "HATCHET_CLIENT_TLS_STRATEGY=none"
  echo "HATCHET_CLIENT_HOST_PORT=127.0.0.1:7077"
} > "$BENCH_OUT_DIR/driver.env"
