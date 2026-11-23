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
assert_contains httproute-values '^kind: HTTPRoute$' "HTTPRoute rendered"
assert_contains httproute-values 'namespace: "edge-system"' "Gateway parent namespace"
assert_contains httproute-values 'sectionName: "https"' "Gateway listener"
assert_contains httproute-values 'engine.example.com' "HTTPRoute hostname"
assert_contains httproute-values '^        - name: ci-orch8-control$' "HTTPRoute targets control HTTP service"
assert_contains httproute-values 'topologyKey: "topology.kubernetes.io/zone"' "control zone spread"
assert_contains httproute-values 'minDomains: 2' "control requires two eligible domains for skew calculation"
assert_contains httproute-values 'topologyKey: "kubernetes.io/hostname"' "executor host spread"
assert_contains httproute-values '^              app.kubernetes.io/component: control$' "control spread selects control pods"
assert_contains httproute-values '^              app.kubernetes.io/component: executor$' "executor spread selects executor pods"
assert_absent httproute-values '^kind: Ingress$' "gateway route fixture disables ingress"
assert_contains ingress-values '^kind: Ingress$' "Ingress rendered"
assert_absent ingress-values '^kind: HTTPRoute$' "ingress fixture disables gateway route"
assert_absent split-values 'kind: Job' "no migration job in server mode"
assert_contains sqlite-snapshot-values '^kind: Job$' "one-shot SQLite snapshot job"
assert_contains sqlite-snapshot-values 'resourceNames: \["ci-orch8-all-in-one"\]' "snapshot job can patch only its deployment"
assert_contains sqlite-snapshot-values 'resourceNames: \["ci-orch8-data"\]' "snapshot job can read only its source claim"
assert_contains sqlite-snapshot-values 'app.kubernetes.io/component: sqlite-snapshot' "snapshot job uses its own role"
assert_contains sqlite-restore-values '^  dataSource:$' "restore claim populated from snapshot"
assert_contains sqlite-restore-values '^    apiGroup: snapshot.storage.k8s.io$' "restore claim points to the CSI snapshot API"
assert_absent sqlite-values 'app.kubernetes.io/component: sqlite-snapshot' "snapshot job is opt-in"
assert_absent existing-secret-values 'kind: Secret' "no chart Secret with existingSecret"
assert_contains sqlite-values 'type: Recreate' "sqlite uses Recreate strategy"
assert_contains sqlite-values 'kind: PersistentVolumeClaim' "sqlite PVC"
assert_absent sqlite-values 'kind: PodDisruptionBudget' "no PDB on single sqlite pod"
assert_contains gateway-values 'value: "gateway"' "gateway role"
assert_contains gateway-values 'value: "127.0.0.1:8080"' "gateway loopback HTTP"
assert_contains hybrid-executor-values 'app.kubernetes.io/component: executor' "hybrid executor deployment"
assert_absent hybrid-executor-values 'name: ORCH8_NODE_ROLE' "join token selects hybrid runtime"
assert_contains hybrid-executor-values 'name: ORCH8_JOIN_TOKEN' "join token from Secret"
assert_contains ingress-values 'name: ORCH8_CLOUD_OBSERVABILITY_API_KEY' "cloud observability key from Secret"
assert_absent hybrid-executor-values 'value: "control"' "no control deployment in executor mode"
assert_absent hybrid-executor-values 'kind: Ingress' "no ingress in executor mode"
assert_contains monitoring-values 'kind: ServiceMonitor' "ServiceMonitor"
assert_contains monitoring-values 'kind: PrometheusRule' "PrometheusRule"
assert_contains monitoring-values 'up{job="orch8"}' "rule job label templated"
assert_contains monitoring-values 'x-api-key:' "scrape config sends x-api-key"
assert_absent default-values '^kind: NetworkPolicy$' "network policies are opt-in"
assert_contains network-policy-values '^kind: NetworkPolicy$' "role network policies rendered"
assert_contains network-policy-values '^  name: ci-orch8-net-control$' "control network policy"
assert_contains network-policy-values '^  name: ci-orch8-net-executor$' "executor network policy"
assert_contains network-policy-values '^  name: ci-orch8-net-migrate$' "migration hook network policy"
assert_contains network-policy-values 'helm.sh/hook-weight: "-20"' "policy precedes the migration hook"
assert_contains network-policy-values 'orch8-test: trusted-client' "explicit control ingress allowlist"
assert_contains network-policy-values 'orch8-test: postgres' "external database pod selector"


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
expect_failure "network policy without external database peers" "${PG[@]}" --set networkPolicy.enabled=true
expect_failure "hybrid network policy without cloud egress" --set mode=executor --set hybrid.joinToken.existingSecret=j --set networkPolicy.enabled=true

expect_failure "sqlite with 2 replicas" --set storage.backend=sqlite --set allInOne.replicas=2
expect_failure "sqlite in split mode" --set storage.backend=sqlite --set mode=split --set allInOne.replicas=1
expect_failure "sqlite snapshot with postgres" "${PG[@]}" --set sqlite.snapshot.enabled=true
expect_failure "restore cannot replace an existing claim" -f "$CHART/ci/sqlite-restore-values.yaml" --set sqlite.persistence.existingClaim=other
expect_failure "snapshot requires a unique name" -f "$CHART/ci/sqlite-snapshot-values.yaml" --set sqlite.snapshot.name=
expect_failure "unknown mode" "${PG[@]}" --set mode=edge
expect_failure "malformed encryption key" "${PG[@]}" --set secrets.encryptionKey=abc
expect_failure "gateway without TLS secret" "${PG[@]}" --set gateway.enabled=true
expect_failure "wildcard CORS with auth" "${PG[@]}" --set 'config.corsOrigins=*'
expect_failure "bad migrations mode" "${PG[@]}" --set migrations.mode=sometimes
expect_failure "executor mode without join token secret" "${PG[@]}" --set mode=executor
expect_failure "executor mode with ingress" "${PG[@]}" --set mode=executor --set hybrid.joinToken.existingSecret=j --set ingress.enabled=true
expect_failure "route has no parent" "${PG[@]}" --set httpRoute.enabled=true
expect_failure "executor mode with HTTPRoute" --set mode=executor --set hybrid.joinToken.existingSecret=j --set httpRoute.enabled=true --set httpRoute.parent.name=edge
expect_failure "topology spread with zero skew" "${PG[@]}" --set mode=split --set 'control.topologySpreadConstraints[0].maxSkew=0' --set 'control.topologySpreadConstraints[0].topologyKey=kubernetes.io/hostname' --set 'control.topologySpreadConstraints[0].whenUnsatisfiable=DoNotSchedule'
expect_failure "topology spread with minDomains on a soft constraint" -f "$CHART/ci/httproute-values.yaml" --set 'executor.topologySpreadConstraints[0].maxSkew=1' --set 'executor.topologySpreadConstraints[0].topologyKey=kubernetes.io/hostname' --set 'executor.topologySpreadConstraints[0].whenUnsatisfiable=ScheduleAnyway' --set 'executor.topologySpreadConstraints[0].minDomains=2'
expect_failure "cloud observability without key secret" "${PG[@]}" --set cloudObservability.enabled=true
expect_failure "cloud observability over plain http" "${PG[@]}" --set cloudObservability.enabled=true --set cloudObservability.existingSecret=k --set cloudObservability.endpoint=http://x

echo
if [ "$failures" -ne 0 ]; then
  echo "helm chart tests: $failures failure(s); rendered output in $OUT_DIR"
  exit 1
fi
echo "helm chart tests: all passed (rendered output in $OUT_DIR)"
