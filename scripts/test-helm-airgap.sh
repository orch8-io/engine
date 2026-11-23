#!/usr/bin/env bash
# Verify bundle assembly without Docker or run a full isolated Kind installation.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
cluster="orch8-airgap-$$"
cluster_created=false
platform=linux/amd64
# Invoked by the EXIT trap.
# shellcheck disable=SC2329
cleanup() {
  local status=$?
  if [[ "$cluster_created" == true ]]; then
    if ((status != 0)); then
      kubectl --request-timeout=5s get pods -A -o wide >&2 || true
      kubectl --request-timeout=5s get events -A --sort-by=.lastTimestamp | tail -20 >&2 || true
      local nodes node
      nodes="$(kind get nodes --name "$cluster" 2>/dev/null || true)"
      while IFS= read -r node; do
        if [[ -n "$node" ]]; then
          timeout 10 docker exec "$node" journalctl --no-pager -u containerd -n 30 >&2 || true
        fi
      done <<< "$nodes"
    fi
    timeout 45 kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

if [[ ${1:-} == --unit ]]; then
  mkdir "$work/bin"
  ORCH8_AIRGAP_TEST_REAL_HELM="$(command -v helm)"
  export ORCH8_AIRGAP_TEST_REAL_HELM
  cat > "$work/bin/helm" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${ORCH8_AIRGAP_TEST_BAD_CHART:-0}" == 1 && "$1" == show && "$2" == chart ]]; then
  printf 'name: postgresql\nversion: 16.7.26\n'
else
  exec "$ORCH8_AIRGAP_TEST_REAL_HELM" "$@"
fi
SCRIPT
  cat > "$work/bin/docker" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
arch="${ORCH8_AIRGAP_TEST_ARCH:-amd64}"
config="$(jq -nc --arg arch "$arch" '{os: "linux", architecture: $arch}')"
image_id="$(printf '%s\n' "$config" | sha256sum | awk '{print $1}')"
manifest="$(jq -nc --arg identifier "$image_id" '{schemaVersion: 2, config: {digest: ("sha256:" + $identifier)}}')"
manifest_id="$(printf '%s\n' "$manifest" | sha256sum | awk '{print $1}')"
case "$2" in
  pull) [[ "$3" == --platform && "$4" == "linux/$arch" && "$5" == *@sha256:* ]];;
  inspect)
    [[ "$3" == --format && "$4" == '{{.Id}} {{.Os}}/{{.Architecture}}' ]]
    if [[ "$arch" == arm64 ]]; then
      [[ "$5" == --platform && "$6" == linux/arm64 && "$7" == *@sha256:* ]]
      printf 'sha256:%s linux/arm64\n' "$manifest_id"
    else
      [[ "$5" == *@sha256:* ]]
      printf 'sha256:%s linux/amd64\n' "$image_id"
    fi
    ;;
  tag) [[ "$3" == *@sha256:* && "$4" == docker.io/orch8-airgap/engine:sha256-*-linux-"$arch" ]];;
  save)
    if [[ "$3" == --help ]]; then
      if [[ "$arch" == arm64 ]]; then printf '%s\n' '--platform'; fi
      exit 0
    fi
    if [[ "$arch" == arm64 ]]; then
      [[ "$3" == --platform && "$4" == linux/arm64 ]]
      shift 2
    fi
    [[ "$3" == --output ]]
    stage="$(mktemp -d)"
    trap 'rm -rf "$stage"' EXIT
    member="$image_id.json"
    archive_tag="${5#docker.io/}"
    if [[ "$arch" == arm64 ]]; then
      member="blobs/sha256/$image_id"
      archive_tag="$5"
      mkdir -p "$stage/blobs/sha256"
      printf '%s\n' "$manifest" > "$stage/blobs/sha256/$manifest_id"
    fi
    if [[ "${ORCH8_AIRGAP_TEST_WRONG_CONFIG:-0}" == 1 ]]; then
      config='{"os":"linux","architecture":"s390x"}'
    fi
    printf '%s\n' "$config" > "$stage/$member"
    jq -n --arg member "$member" --arg tag "$archive_tag" \
      '[{Config: $member, RepoTags: [$tag], Layers: []}]' > "$stage/manifest.json"
    entries=(manifest.json "$member")
    if [[ "$arch" == arm64 ]]; then entries+=("blobs/sha256/$manifest_id"); fi
    tar -cf "$4" -C "$stage" "${entries[@]}"
    ;;
  *) exit 1 ;;
esac
SCRIPT
  chmod +x "$work/bin/docker" "$work/bin/helm"
  export PATH="$work/bin:$PATH"
  source_image="ghcr.io/orch8-io/engine@sha256:$(printf '%064d' 9)"
  if bash "$root/scripts/build-helm-airgap.sh" ghcr.io/orch8-io/engine:latest linux/amd64 "$work/mutable.tar.gz"; then
    echo 'a mutable image tag was accepted' >&2
    exit 1
  fi
  if ORCH8_AIRGAP_TEST_BAD_CHART=1 bash "$root/scripts/build-helm-airgap.sh" "$source_image" linux/amd64 "$work/wrong-chart.tar.gz" >"$work/wrong-chart.log" 2>&1; then
    echo 'a cached chart with the wrong version was accepted' >&2
    exit 1
  fi
  grep -q 'PostgreSQL package metadata does not match Chart.lock' "$work/wrong-chart.log"
  test ! -e "$work/wrong-chart.tar.gz"
  if ORCH8_AIRGAP_TEST_WRONG_CONFIG=1 bash "$root/scripts/build-helm-airgap.sh" "$source_image" linux/amd64 "$work/wrong-image.tar.gz" >"$work/wrong-image.log" 2>&1; then
    echo 'an archive with the wrong image configuration was accepted' >&2
    exit 1
  fi
  grep -q 'saved image ID or platform differs from the pinned source' "$work/wrong-image.log"
  test ! -e "$work/wrong-image.tar.gz"
else
  if ! timeout 20 docker info --format '{{.ServerVersion}}' >/dev/null; then
    echo 'Docker did not respond before the offline installation fixture could prepare its bundle' >&2
    exit 1
  fi
  case "$(docker version --format '{{.Server.Arch}}')" in
    amd64|x86_64) platform=linux/amd64 ;;
    arm64|aarch64) platform=linux/arm64 ;;
    *) echo 'Unsupported Docker server architecture for the air-gap test' >&2; exit 1 ;;
  esac
  image_tag="${ORCH8_TEST_IMAGE_TAG:-$(awk -F'"' '/^appVersion:/ {print $2}' "$root/deploy/helm/orch8/Chart.yaml")}"
  tagged="ghcr.io/orch8-io/engine:$image_tag"
  docker image pull --platform "$platform" "$tagged"
  source_image="$(docker image inspect --format '{{index .RepoDigests 0}}' "$tagged")"
fi

bash "$root/scripts/build-helm-airgap.sh" "$source_image" "$platform" "$work/install.tar.gz"
mkdir "$work/install"
tar -xzf "$work/install.tar.gz" -C "$work/install"
cd "$work/install"
sha256sum -c SHA256SUMS
offline_image="$(jq -er --arg source "$source_image" '.images[] | select(.source == $source) | .local_name' inventory.json)"
jq -e '.image.pullPolicy == "Never" and .storage.backend == "sqlite"' values.json >/dev/null
dependency_version="$(tar -xOf chart.tgz orch8/charts/postgresql/Chart.yaml | awk '/^version: / {print $2}')"
locked_version="$(awk '/^  version: / {print $2}' "$root/deploy/helm/orch8/Chart.lock")"
test "$dependency_version" = "$locked_version"
jq -e --arg version "$locked_version" --arg platform "$platform" '
  .platform == $platform and .chart.dependencies == [{name:"postgresql",version:$version}]
' inventory.json >/dev/null
if [[ ${1:-} == --unit ]]; then
  ORCH8_AIRGAP_TEST_ARCH=arm64 bash "$root/scripts/build-helm-airgap.sh" "$source_image" linux/arm64 "$work/arm64.tar.gz"
  tar -xOzf "$work/arm64.tar.gz" inventory.json | jq -e --arg amd64 "$offline_image" \
    '.platform == "linux/arm64" and (.images[0].local_name | endswith("-linux-arm64")) and .images[0].local_name != $amd64' >/dev/null
  printf 'damaged\n' >> image.tar
  if sha256sum -c SHA256SUMS >/dev/null 2>&1; then
    echo 'a changed image archive still matched its checksum' >&2
    exit 1
  fi
  exit 0
fi

export KUBECONFIG="$work/kubeconfig"
cluster_created=true
timeout 360 kind create cluster --name "$cluster" --wait 120s
kubectl -n kube-system rollout status deployment/coredns --timeout=120s
kubectl -n local-path-storage rollout status deployment/local-path-provisioner --timeout=120s
kind load image-archive image.tar --name "$cluster"
nodes="$(kind get nodes --name "$cluster")"
test -n "$nodes"
while read -r node; do
  docker exec "$node" sh -ec '
    ip -4 route del default
    if [ -n "$(ip -6 route show default)" ]; then ip -6 route del default; fi
    test -z "$(ip -4 route show default)"
    test -z "$(ip -6 route show default)"
  '
  if docker exec "$node" ip -4 route get 1.1.1.1 || docker exec "$node" ip -6 route get 2606:4700:4700::1111; then
    echo 'Kind node can still reach an external network' >&2
    exit 1
  fi
done <<< "$nodes"

api_key=orch8-airgap-test-00000000000000000000000
helm upgrade --install isolated ./chart.tgz -f values.json --namespace airgap \
  --create-namespace --set-string "secrets.apiKey=$api_key" --wait --timeout 5m
deployment=deployment/isolated-orch8-all-in-one
kubectl -n airgap get "$deployment" -o json | jq -e --arg image "$offline_image" \
  '.spec.template.spec.containers[] | select(.image == $image and .imagePullPolicy == "Never")' >/dev/null
kubectl -n airgap exec -i "$deployment" -- sh -c 'cat > /tmp/airgap.json' <<'JSON'
{"id":"01900000-0000-7000-8000-000000000002","name":"offline-install","tenant_id":"demo",
 "namespace":"default","version":1,"created_at":"2026-01-01T00:00:00Z",
 "blocks":[{"type":"step","id":"one","handler":"noop"}]}
JSON
client() {
  kubectl -n airgap exec "$deployment" -- env ORCH8_URL=http://127.0.0.1:8080/api/v1 \
    ORCH8_TENANT_ID=demo "ORCH8_API_KEY=$api_key" /usr/local/bin/orch8 --output json "$@"
}
client sequence apply /tmp/airgap.json
sequence="$(client sequence lookup demo default offline-install | jq -er '.id')"
instance="$(client instance create --tenant-id demo --sequence-id "$sequence" | jq -er '.id')"
for ((attempt=0; attempt<60; attempt++)); do
  status="$(client instance get "$instance" | jq -er '.state')"
  if [[ "$status" == completed ]]; then
    client instance outputs "$instance" | jq -e '[.[] | select(.block_id == "one")] | length == 1' >/dev/null
    echo 'Workflow completed in the offline installation.'
    exit 0
  fi
  if [[ "$status" == failed || "$status" == cancelled ]]; then
    break
  fi
  sleep 2
done
echo 'Workflow did not complete in the offline installation' >&2
exit 1
