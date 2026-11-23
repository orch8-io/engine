#!/usr/bin/env bash
# Build an offline, single-node SQLite Helm bundle for one Linux platform.
# Usage: bash scripts/build-helm-airgap.sh IMAGE@sha256:DIGEST linux/amd64 bundle.tar.gz
set -euo pipefail

if [[ $# != 3 ]]; then
  echo 'usage: build-helm-airgap.sh IMAGE@sha256:DIGEST linux/{amd64,arm64} OUT.tar.gz' >&2
  exit 2
fi
source_image="$1"
platform="$2"
out_dir="$(cd "$(dirname "$3")" && pwd)"
destination="$out_dir/$(basename "$3")"
if [[ ! "$source_image" =~ ^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$ ]] ||
   [[ ! "$platform" =~ ^linux/(amd64|arm64)$ ]]; then
  echo 'supply a digest-pinned registry image and either linux/amd64 or linux/arm64' >&2
  exit 2
fi
if [[ -e "$destination" || -L "$destination" ]]; then
  echo 'output bundle already exists' >&2
  exit 2
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d "$out_dir/.orch8-airgap.XXXXXX")"
trap 'rm -rf "$work"' EXIT
stage="$work/bundle"
mkdir "$stage"
cp -R "$root/deploy/helm/orch8" "$work/chart"
locked_version="$(awk '/^  version: / {print $2}' "$work/chart/Chart.lock")"
if [[ ! "$locked_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo 'expected one locked PostgreSQL dependency in Chart.lock' >&2
  exit 1
fi
dependency="postgresql-$locked_version.tgz"
rm -rf "$work/chart/charts"
mkdir "$work/chart/charts"
if [[ -f "$root/deploy/helm/orch8/charts/$dependency" ]]; then
  cp "$root/deploy/helm/orch8/charts/$dependency" "$work/chart/charts/$dependency"
else
  helm dependency build "$work/chart" >&2
fi
metadata="$(helm show chart "$work/chart/charts/$dependency")"
if [[ "$(awk '/^name: / {print $2}' <<<"$metadata")" != postgresql ||
      "$(awk '/^version: / {print $2}' <<<"$metadata")" != "$locked_version" ]]; then
  echo 'PostgreSQL package metadata does not match Chart.lock' >&2
  exit 1
fi
helm package "$work/chart" --destination "$work" >&2
shopt -s nullglob
charts=("$work"/orch8-*.tgz)
[[ ${#charts[@]} == 1 ]]
mv "${charts[0]}" "$stage/chart.tgz"

docker image pull --platform "$platform" "$source_image" >&2
inspect=(docker image inspect --format '{{.Id}} {{.Os}}/{{.Architecture}}')
if ! source_info="$("${inspect[@]}" --platform "$platform" "$source_image" 2>/dev/null)"; then
  source_info="$("${inspect[@]}" "$source_image")"
fi
read -r source_id actual_platform <<<"$source_info"
[[ "$source_id" =~ ^sha256:[a-f0-9]{64}$ && "$actual_platform" == "$platform" ]]
digest="${source_image##*@sha256:}"
repository="docker.io/orch8-airgap/engine"
tag="sha256-$digest-${platform//\//-}"
offline_image="$repository:$tag"
docker image tag "$source_image" "$offline_image"
save_help="$(docker image save --help)"
if [[ "$save_help" == *--platform* ]]; then
  docker image save --platform "$platform" --output "$stage/image.tar" "$offline_image"
else
  docker image save --output "$stage/image.tar" "$offline_image"
fi

# Check the exact image that will be loaded on an offline node.
config_entry="$(tar -xOf "$stage/image.tar" manifest.json | jq -er --arg image "${offline_image#docker.io/}" '
  map(select((.RepoTags // []) | map(sub("^docker.io/"; "")) | index($image))) |
  select(length == 1) | .[0].Config
')"
if [[ ! "$config_entry" =~ ^([a-f0-9]{64}\.json|blobs/sha256/[a-f0-9]{64})$ ]]; then
  echo 'Docker archive has an invalid image config path' >&2
  exit 1
fi
tar -xOf "$stage/image.tar" "$config_entry" > "$work/image-config.json"
image_id="sha256:$(sha256sum "$work/image-config.json" | awk '{print $1}')"
saved_platform="$(jq -er '.os + "/" + .architecture' "$work/image-config.json")"
# The classic store identifies an image by its config digest. With the
# containerd store, platform-specific inspect identifies its OCI manifest.
verified_id=false
if [[ "$image_id" == "$source_id" ]]; then
  verified_id=true
elif tar -xOf "$stage/image.tar" "blobs/sha256/${source_id#sha256:}" > "$work/image-manifest.json" 2>/dev/null &&
     [[ "sha256:$(sha256sum "$work/image-manifest.json" | awk '{print $1}')" == "$source_id" ]] &&
     jq -e --arg config "$image_id" '.schemaVersion == 2 and .config.digest == $config' "$work/image-manifest.json" >/dev/null; then
  verified_id=true
fi
if [[ "$verified_id" == false || "$saved_platform" != "$platform" ]]; then
  echo 'saved image ID or platform differs from the pinned source' >&2
  exit 1
fi

jq -n --arg repo "$repository" --arg tag "$tag" '{
  mode: "allInOne", image: {repository: $repo, tag: $tag, pullPolicy: "Never"},
  storage: {backend: "sqlite"}, postgresql: {enabled: false},
  allInOne: {replicas: 1, autoscaling: {enabled: false}}
}' > "$stage/values.json"
helm template verify "$stage/chart.tgz" -f "$stage/values.json" > "$work/rendered.yaml"
rendered_image="$(awk '/^[[:space:]]*image:/ {gsub(/"/, "", $2); print $2}' "$work/rendered.yaml" | sort -u)"
if [[ "$rendered_image" != "$offline_image" ]]; then
  echo 'chart rendered an image absent from the bundle' >&2
  exit 1
fi
chart_digest="$(sha256sum "$stage/chart.tgz" | awk '{print $1}')"
jq -n --arg platform "$platform" --arg source "$source_image" --arg offline "$offline_image" \
  --arg image_id "$image_id" --arg digest "$chart_digest" --arg dependency "$locked_version" '{
    format: "orch8-helm-airgap", version: 1, platform: $platform,
    chart: {filename: "chart.tgz", sha256: $digest, dependencies: [{name: "postgresql", version: $dependency}]},
    values: "values.json",
    images: [{filename: "image.tar", source: $source, local_name: $offline, image_id: $image_id}]
  }' > "$stage/inventory.json"
(
  cd "$stage"
  sha256sum chart.tgz image.tar values.json inventory.json > SHA256SUMS
  tar -czf "$work/bundle.tar.gz" SHA256SUMS chart.tgz image.tar values.json inventory.json
)
ln "$work/bundle.tar.gz" "$destination"
echo "Wrote $destination"
