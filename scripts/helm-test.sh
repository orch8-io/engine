#!/usr/bin/env bash
# Lint and render the Orch8 Helm chart for every ct-style values file in
# deploy/helm/orch8/ci, then assert that invalid combinations fail closed.
#
# Usage: scripts/helm-test.sh            (needs helm; fetches the optional
#                                         postgresql dependency on first run)
#        OUT_DIR=/tmp/render scripts/helm-test.sh   (keep rendered manifests)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHART="$ROOT/deploy/helm/orch8"
OUT_DIR="${OUT_DIR:-$(mktemp -d)}"
API_VERSIONS=(--api-versions monitoring.coreos.com/v1)
failures=0

# `helm template` refuses to render while a declared dependency is missing,
# even when its condition is false.
if [ ! -d "$CHART/charts" ] || ! ls "$CHART"/charts/postgresql-*.tgz >/dev/null 2>&1; then
  echo "--> helm dependency build (network required once)"
  helm dependency build "$CHART" >/dev/null
fi

for values in "$CHART"/ci/*-values.yaml; do
  name="$(basename "$values" .yaml)"
  echo "--> $name"
  if ! helm lint --strict "$CHART" -f "$values" >"$OUT_DIR/$name.lint" 2>&1; then
    echo "    FAIL helm lint"; cat "$OUT_DIR/$name.lint"; failures=$((failures + 1)); continue
  fi
  if ! helm template ci "$CHART" -f "$values" "${API_VERSIONS[@]}" >"$OUT_DIR/$name.yaml" 2>"$OUT_DIR/$name.err"; then
    echo "    FAIL helm template"; cat "$OUT_DIR/$name.err"; failures=$((failures + 1)); continue
  fi
  echo "    ok ($(grep -c '^kind:' "$OUT_DIR/$name.yaml") resources)"
done

# Content assertions on rendered output.
assert_contains() { # file pattern description
  if grep -q -- "$2" "$OUT_DIR/$1.yaml"; then echo "    ok: $3"; else echo "    FAIL: $3"; failures=$((failures + 1)); fi
}
assert_absent() {
  [ -f "$OUT_DIR/$1.yaml" ] || { echo "    FAIL: $3 (no render)"; failures=$((failures + 1)); return; }
  if grep -q -- "$2" "$OUT_DIR/$1.yaml"; then echo "    FAIL: $3"; failures=$((failures + 1)); else echo "    ok: $3"; fi
}
echo "--> content assertions"
assert_contains default-values 'value: "all_in_one"' "all-in-one role env"
assert_contains default-values '"/usr/local/bin/orch8", "migrate"' "migration hook job"
assert_contains default-values 'helm.sh/hook: pre-install,pre-upgrade' "migration runs before install/upgrade"
assert_contains default-values 'readOnlyRootFilesystem: true' "read-only root filesystem"
assert_contains default-values 'path: /health/ready' "readiness probe on /health/ready"
assert_contains split-values 'value: "control"' "control role"
assert_contains split-values 'value: "executor"' "executor role"
assert_contains split-values 'kind: HorizontalPodAutoscaler' "HPA rendered"
assert_contains split-values 'kind: PodDisruptionBudget' "PDB rendered"
assert_absent split-values 'kind: Job' "no migration job in server mode"
assert_absent existing-secret-values 'kind: Secret' "no chart Secret with existingSecret"
assert_contains sqlite-values 'type: Recreate' "sqlite uses Recreate strategy"
assert_contains sqlite-values 'kind: PersistentVolumeClaim' "sqlite PVC"
assert_absent sqlite-values 'kind: PodDisruptionBudget' "no PDB on single sqlite pod"
assert_contains gateway-values 'value: "gateway"' "gateway role"
assert_contains gateway-values 'value: "127.0.0.1:8080"' "gateway loopback HTTP"
assert_contains monitoring-values 'kind: ServiceMonitor' "ServiceMonitor"
assert_contains monitoring-values 'kind: PrometheusRule' "PrometheusRule"
assert_contains monitoring-values 'up{job="orch8"}' "rule job label templated"
assert_contains monitoring-values 'x-api-key:' "scrape config sends x-api-key"

# Invalid combinations must fail (template `fail` or schema validation).
expect_failure() { # description, helm args...
  local description="$1"; shift
  if helm template ci "$CHART" "$@" >/dev/null 2>&1; then
    echo "    FAIL (rendered but should not): $description"; failures=$((failures + 1))
  else
    echo "    ok (rejected): $description"
  fi
}
PG=(--set externalDatabase.url=postgres://u:p@db:5432/orch8)
echo "--> negative cases"
expect_failure "postgres without any database"
expect_failure "sqlite with 2 replicas" --set storage.backend=sqlite --set allInOne.replicas=2
expect_failure "sqlite in split mode" --set storage.backend=sqlite --set mode=split --set allInOne.replicas=1
expect_failure "unknown mode" "${PG[@]}" --set mode=edge
expect_failure "malformed encryption key" "${PG[@]}" --set secrets.encryptionKey=abc
expect_failure "gateway without TLS secret" "${PG[@]}" --set gateway.enabled=true
expect_failure "wildcard CORS with auth" "${PG[@]}" --set 'config.corsOrigins=*'
expect_failure "bad migrations mode" "${PG[@]}" --set migrations.mode=sometimes

echo
if [ "$failures" -ne 0 ]; then
  echo "helm chart tests: $failures failure(s); rendered output in $OUT_DIR"
  exit 1
fi
echo "helm chart tests: all passed (rendered output in $OUT_DIR)"
