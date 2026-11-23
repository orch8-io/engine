#!/usr/bin/env bash
# Exercise split scheduling and policy enforcement in a disposable Kind cluster.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
chart="$root/deploy/helm/orch8"
cluster="orch8-netpol-$$"
work="$(mktemp -d)"
export KUBECONFIG="$work/kubeconfig"
cluster_attempted=false
# Invoked by the EXIT trap.
# shellcheck disable=SC2329
cleanup() {
  local status=$?
  if ((status != 0)) && [[ -f "$KUBECONFIG" ]]; then
    kubectl --request-timeout=5s get pods -A -o wide >&2 || true
    kubectl --request-timeout=5s get events -A --sort-by=.lastTimestamp | tail -30 >&2 || true
  fi
  if [[ "$cluster_attempted" == true ]]; then
    timeout 45 kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

if ! timeout 20 docker info --format '{{.ServerVersion}}' >/dev/null; then
  echo "Docker did not respond before the networking fixture could create its cluster" >&2
  exit 1
fi
cluster_attempted=true
cat > "$work/kind.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  disableDefaultCNI: true
  podSubnet: 192.168.0.0/16
nodes:
  - role: control-plane
  - role: worker
YAML
timeout 360 kind create cluster --name "$cluster" --config "$work/kind.yaml"
calico_version="${CALICO_VERSION:-v3.31.0}"
kubectl apply -f "https://raw.githubusercontent.com/projectcalico/calico/$calico_version/manifests/calico.yaml"
kubectl -n kube-system rollout status daemonset/calico-node --timeout=240s
kubectl -n kube-system rollout status deployment/coredns --timeout=180s
kubectl taint node "$cluster-control-plane" node-role.kubernetes.io/control-plane-
kubectl label node "$cluster-control-plane" topology.kubernetes.io/zone=fixture-a
kubectl label node "$cluster-worker" topology.kubernetes.io/zone=fixture-b
kubectl create namespace netpol

kubectl -n netpol apply -f - <<'YAML'
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny
spec:
  podSelector: {}
  policyTypes: [Ingress, Egress]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: postgres-ingress
spec:
  podSelector:
    matchLabels: {orch8-test: postgres}
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector:
            matchLabels: {app.kubernetes.io/instance: smoke}
      ports: [{port: 5432, protocol: TCP}]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: fixture-client-egress
spec:
  podSelector:
    matchExpressions:
      - key: orch8-test
        operator: In
        values: [trusted-client, untrusted-client]
  policyTypes: [Egress]
  egress:
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/instance: smoke
              app.kubernetes.io/component: control
      ports: [{port: 8080, protocol: TCP}]
    - to:
        - namespaceSelector:
            matchLabels: {kubernetes.io/metadata.name: kube-system}
          podSelector:
            matchLabels: {k8s-app: kube-dns}
      ports: [{port: 53, protocol: UDP}, {port: 53, protocol: TCP}]
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  selector: {orch8-test: postgres}
  ports: [{port: 5432}]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
spec:
  selector:
    matchLabels: {orch8-test: postgres}
  template:
    metadata:
      labels: {orch8-test: postgres}
    spec:
      containers:
        - name: postgres
          image: postgres:17-alpine
          env:
            - {name: POSTGRES_USER, value: orch8}
            - {name: POSTGRES_PASSWORD, value: orch8}
            - {name: POSTGRES_DB, value: orch8}
          readinessProbe:
            exec:
              command: [pg_isready, -U, orch8, -d, orch8]
            periodSeconds: 2
YAML
kubectl -n netpol rollout status deployment/postgres --timeout=180s
helm dependency build "$chart"
api_key="orch8-network-policy-fixture-0000000000"
image_tag="${ORCH8_TEST_IMAGE_TAG:-$(awk -F'"' '/^appVersion:/ {print $2}' "$chart/Chart.yaml")}"
helm upgrade --install smoke "$chart" -n netpol --wait --timeout 6m \
  -f "$chart/ci/network-policy-values.yaml" \
  -f "$chart/ci/network-policy-spread.yaml" \
  --set-string "secrets.apiKey=$api_key" --set-string "image.tag=$image_tag"

for role in control executor; do
  zones="$(
    kubectl -n netpol get pods -l "app.kubernetes.io/instance=smoke,app.kubernetes.io/component=$role" \
      -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' |
      while read -r node; do
        [ -n "$node" ] && kubectl get node "$node" -o json | jq -r '.metadata.labels["topology.kubernetes.io/zone"]'
      done | sort -u
  )"
  test "$zones" = "$(printf 'fixture-a\nfixture-b')"
done

api_url="http://smoke-orch8-control:8080/api/v1"
kubectl -n netpol run trusted-client --image="ghcr.io/orch8-io/engine:$image_tag" \
  --restart=Never --labels orch8-test=trusted-client --env "ORCH8_URL=$api_url" \
  --env "ORCH8_API_KEY=$api_key" --env ORCH8_TENANT_ID=demo \
  --command -- /bin/sh -c 'sleep 1200'
kubectl -n netpol run untrusted-client --image=curlimages/curl:8.14.1 \
  --restart=Never --labels orch8-test=untrusted-client --command -- /bin/sh -c 'sleep 1200'
kubectl -n netpol wait --for=condition=Ready pod/trusted-client pod/untrusted-client --timeout=180s

kubectl -n netpol exec trusted-client -- /usr/local/bin/orch8 health

control_ip="$(kubectl -n netpol get service smoke-orch8-control -o jsonpath='{.spec.clusterIP}')"
probe_control() {
  kubectl -n netpol exec untrusted-client -- curl -fsS --max-time 3 "http://$control_ip:8080/health/ready"
}
if probe_control; then
  echo "untrusted pod reached the control API despite ingress policy" >&2
  exit 1
fi
kubectl -n netpol label pod untrusted-client orch8-test=trusted-client --overwrite
for ((attempt=0; attempt<20; attempt++)); do
  if probe_control >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
probe_control >/dev/null

kubectl -n netpol exec -i trusted-client -- sh -c 'cat > /tmp/workflow.json' <<'JSON'
{
  "id": "0191e4f2-a1b2-7c3d-8e4f-a5b6c7d8e9f0",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "network-policy-smoke",
  "version": 1,
  "created_at": "2026-01-01T00:00:00Z",
  "blocks": [{"type":"step","id":"local","handler":"noop"}]
}
JSON
client() { kubectl -n netpol exec trusted-client -- /usr/local/bin/orch8 --output json "$@"; }
client sequence apply /tmp/workflow.json
sequence="$(client sequence lookup demo default network-policy-smoke | jq -er '.id')"

executor=deployment/smoke-orch8-executor
executor_selector=app.kubernetes.io/instance=smoke,app.kubernetes.io/component=executor
executor_replicas="$(kubectl -n netpol get "$executor" -o jsonpath='{.spec.replicas}')"
kubectl -n netpol scale "$executor" --replicas=0
for ((attempt=0; attempt<60; attempt++)); do
  pods="$(kubectl -n netpol get pods -l "$executor_selector" -o name)"
  if [[ -z "$pods" ]]; then
    break
  fi
  sleep 2
done
if [[ -n "$pods" ]]; then
  echo "The executor pods did not terminate before the scheduling check" >&2
  exit 1
fi
instance="$(client instance create --sequence-id "$sequence" --tenant-id demo | jq -er '.id')"
for ((attempt=0; attempt<3; attempt++)); do
  client instance get "$instance" | jq -e '.state == "scheduled"' >/dev/null
  sleep 1
done
kubectl -n netpol scale "$executor" --replicas="$executor_replicas"
kubectl -n netpol rollout status "$executor" --timeout=180s
for ((attempt=0; attempt<60; attempt++)); do
  status="$(client instance get "$instance" | jq -er '.state')"
  if [[ "$status" == completed ]]; then
    client instance outputs "$instance" | jq -e '[.[] | select(.block_id == "local")] | length == 1' >/dev/null
    echo "An executor completed the queued workflow under default-deny networking. Both roles spanned the two zones."
    exit 0
  fi
  if [[ "$status" == failed || "$status" == cancelled ]]; then
    break
  fi
  sleep 2
done
echo "Workflow failed to complete under the rendered network policies" >&2
exit 1
