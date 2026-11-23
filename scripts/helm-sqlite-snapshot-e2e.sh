#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
chart="$root/deploy/helm/orch8"
cluster="orch8-sqlite-snap-$$"
work="$(mktemp -d)"
export KUBECONFIG="$work/kubeconfig"
job_image="orch8-kubectl-snapshot:test-$cluster"
cluster_attempted=false
job_image_attempted=false

# shellcheck disable=SC2329
cleanup() {
  local status=$?
  if ((status != 0)) && [[ -f "$KUBECONFIG" ]]; then
    kubectl -n snapshot-fixture --request-timeout=5s get pods,pvc,volumesnapshots >&2 || true
    kubectl -n snapshot-fixture --request-timeout=5s logs -l app.kubernetes.io/component=sqlite-snapshot --all-containers >&2 || true
  fi
  if [[ "$cluster_attempted" == true ]]; then
    timeout 45 kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  fi
  if [[ "$job_image_attempted" == true ]]; then
    timeout 30 docker image rm "$job_image" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

if ! timeout 20 docker info --format '{{.ServerVersion}}' >/dev/null; then
  echo "Docker did not respond before the snapshot fixture could create its cluster" >&2
  exit 1
fi
cluster_attempted=true
timeout 360 kind create cluster --name "$cluster" --image kindest/node:v1.33.1 --wait 180s
snapshotter="https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.2.0"
for resource in volumesnapshotclasses volumesnapshotcontents volumesnapshots; do
  kubectl apply -f "$snapshotter/client/config/crd/snapshot.storage.k8s.io_${resource}.yaml"
  kubectl wait --for=condition=Established "crd/$resource.snapshot.storage.k8s.io" --timeout=60s
done
for manifest in rbac-snapshot-controller setup-snapshot-controller; do
  kubectl apply -f "$snapshotter/deploy/kubernetes/snapshot-controller/$manifest.yaml"
done
git clone --quiet --depth 1 --branch v1.16.1 https://github.com/kubernetes-csi/csi-driver-host-path "$work/hostpath"
# The upstream socket proxy is used by CSI's own test suites.
rm "$work/hostpath/deploy/kubernetes-latest/hostpath/csi-hostpath-testing.yaml"
timeout 360 bash "$work/hostpath/deploy/kubernetes-latest/deploy.sh"
kubectl apply -f - <<'YAML'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: orch8-csi-test
provisioner: hostpath.csi.k8s.io
reclaimPolicy: Delete
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: orch8-csi-test
driver: hostpath.csi.k8s.io
deletionPolicy: Delete
YAML

# Use the verified upstream client with a shell and utilities in the job image.
mkdir "$work/job"
case "$(docker version --format '{{.Server.Arch}}')" in
  amd64|x86_64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) echo "Unsupported Docker server architecture" >&2; exit 1 ;;
esac
client_url="https://dl.k8s.io/release/v1.33.1/bin/linux/$arch/kubectl"
curl -fsSL "$client_url" -o "$work/job/kubectl"
digest="$(curl -fsSL "$client_url.sha256")"
test "$(sha256sum "$work/job/kubectl" | awk '{print $1}')" = "$digest"
chmod 0755 "$work/job/kubectl"
cat > "$work/job/Dockerfile" <<'DOCKERFILE'
FROM debian:bookworm-slim
COPY kubectl /usr/local/bin/kubectl
DOCKERFILE
job_image_attempted=true
docker build -t "$job_image" "$work/job"
kind load docker-image --name "$cluster" "$job_image"

helm dependency build "$chart"
kubectl create namespace snapshot-fixture
k() { kubectl --request-timeout=15s -n snapshot-fixture "$@"; }
api_key=orch8-sqlite-snapshot-test-api-key-0000000
encryption_key="$(openssl rand -hex 32)"
fixture_values="$chart/ci/sqlite-values.yaml"
common=(-n snapshot-fixture -f "$fixture_values"
  --set-string sqlite.persistence.storageClass=orch8-csi-test --set-string sqlite.persistence.size=64Mi
  --set-string "secrets.apiKey=$api_key" --set-string "secrets.encryptionKey=$encryption_key")
if [[ -n "${ORCH8_TEST_IMAGE_TAG:-}" ]]; then
  common+=(--set-string "image.tag=$ORCH8_TEST_IMAGE_TAG")
fi
helm install original "$chart" "${common[@]}" --wait --timeout=4m
k run client --image=curlimages/curl:8.14.1 --restart=Never --command -- sleep 1200
k wait pod/client --for=condition=Ready --timeout=120s

release=original
request() {
  local path=$1
  shift
  k exec client -- curl -fsS --max-time 10 -H "x-api-key: $api_key" -H 'x-tenant-id: demo' \
    -H 'content-type: application/json' "$@" "http://$release-orch8-all-in-one:8080/api/v1/$path"
}
sequence=0191e4f2-a1b2-7c3d-8e4f-a5b6c7d8e9f0
request sequences -X POST -d "$(cat <<JSON
{"id":"$sequence","tenant_id":"demo","namespace":"default","name":"snapshot-demo","version":1,"created_at":"2026-01-01T00:00:00Z","blocks":[{"type":"step","id":"review","handler":"noop","wait_for_input":{"prompt":"Approve recovery"}},{"type":"step","id":"finish","handler":"noop"}]}
JSON
)" >/dev/null
instance="$(request instances -X POST -d "{\"sequence_id\":\"$sequence\",\"tenant_id\":\"demo\",\"namespace\":\"default\",\"context\":{\"data\":{\"from_snapshot\":true}}}" | jq -er '.id')"

wait_run() {
  local expected=$1 status
  for ((try=0; try<55; try++)); do
    status="$(request "instances/$instance" | jq -er '.state')"
    [[ "$status" == "$expected" ]] && return 0
    [[ "$status" == failed || "$status" == cancelled ]] && break
    sleep 2
  done
  echo "Expected $expected for $release run $instance, got $status" >&2
  return 1
}
wait_run waiting
original_claim="$(k get pvc original-orch8-data -o json)"
original_uid="$(jq -er '.metadata.uid' <<<"$original_claim")"
original_volume="$(jq -er '.spec.volumeName | select(length > 0)' <<<"$original_claim")"
helm upgrade original "$chart" "${common[@]}" --set sqlite.snapshot.enabled=true \
  --set-string sqlite.snapshot.name=waiting-run \
  --set-string sqlite.snapshot.volumeSnapshotClassName=orch8-csi-test \
  --set-string "sqlite.snapshot.image=$job_image"
job="$(k get jobs -l app.kubernetes.io/component=sqlite-snapshot -o name)"
for ((attempt=0; attempt<150; attempt++)); do
  job_state="$(k get "$job" -o json | jq -r '
    [.status.conditions[]? | select(.status == "True" and (.type == "Complete" or .type == "Failed")) | .type]
    | first // ""
  ')"
  if [[ "$job_state" == Complete ]]; then break; fi
  if [[ "$job_state" == Failed ]]; then
    echo "The SQLite snapshot job failed" >&2
    exit 1
  fi
  sleep 2
done
test "$job_state" = Complete
k wait volumesnapshot/waiting-run --for='jsonpath={.status.readyToUse}=true' --timeout=60s
k get volumesnapshot/waiting-run -o json | jq -e --arg uid "$original_uid" '
  .metadata.annotations["orch8.io/sqlite-snapshot-claim-uid"] == $uid and
  .spec.source.persistentVolumeClaimName == "original-orch8-data"
' >/dev/null
k rollout status deployment/original-orch8-all-in-one --timeout=120s
wait_run waiting

helm install restored "$chart" "${common[@]}" --set-string sqlite.persistence.restoreFromSnapshot=waiting-run \
  --wait --timeout=4m
k get pvc restored-orch8-data -o json | jq -e --arg uid "$original_uid" --arg volume "$original_volume" '
  .status.phase == "Bound" and .metadata.uid != $uid and
  (.spec.volumeName | length > 0) and .spec.volumeName != $volume and
  .spec.dataSource.kind == "VolumeSnapshot" and .spec.dataSource.name == "waiting-run"
' >/dev/null
release=restored
wait_run waiting
request "instances/$instance" | jq -e '.context.data.from_snapshot == true' >/dev/null
request "instances/$instance/signals" -X POST -d '{"signal_type":{"custom":"human_input:review"},"payload":{"value":"yes"}}' >/dev/null
wait_run completed
release=original
wait_run waiting
echo "Restored the waiting run to a new CSI volume and completed it with its original ID."
