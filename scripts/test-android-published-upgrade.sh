#!/usr/bin/env bash
# Run on the SDK install workflow's disposable Android emulator.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
candidate="${ORCH8_VERSION#v}"
native_engine="${ORCH8_UPGRADE_ENGINE_BINARY:-}"
for tool in adb gradle; do
  if ! command -v "$tool" >/dev/null; then
    echo "$tool is required for the published Android upgrade test" >&2
    exit 1
  fi
done
if [[ -n "$native_engine" ]]; then
  if [[ ! -x "$native_engine" ]]; then
    echo "ORCH8_UPGRADE_ENGINE_BINARY must point to an executable orch8-server" >&2
    exit 1
  fi
  native_engine="$(cd "$(dirname "$native_engine")" && pwd)/$(basename "$native_engine")"
elif ! timeout 20 docker info --format '{{.ServerVersion}}' >/dev/null; then
  echo "Docker did not respond before the Android upgrade test could start its receiver" >&2
  exit 1
fi
work="$(mktemp -d)"
container="orch8-android-upgrade-$$"
proxy_pid=
engine_pid=
device_port=39841
application=io.orch8.inspection

# shellcheck disable=SC2329
cleanup() {
  local status=$?
  adb -e reverse --remove "tcp:$device_port" >/dev/null 2>&1 || true
  if [[ -n "$proxy_pid" ]]; then kill "$proxy_pid" 2>/dev/null || true; fi
  if [[ -z "$native_engine" ]]; then
    if ((status != 0)); then timeout 20 docker logs --tail=30 "$container" >&2 || true; fi
    timeout 20 docker rm -f "$container" >/dev/null 2>&1 || true
  elif [[ -n "$engine_pid" ]]; then
    if ((status != 0)); then tail -30 "$work/engine.log" >&2; fi
    kill "$engine_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

metadata=https://raw.githubusercontent.com/orch8-io/maven/main/io/orch8/orch8-mobile/maven-metadata.xml
curl -fsSL --retry 3 --max-time 25 -H 'Cache-Control: no-cache' "$metadata" -o "$work/maven.xml"
previous="$(python3 - "$candidate" "$work/maven.xml" <<'PY'
import re
import sys
import xml.etree.ElementTree as ET

candidate, metadata = sys.argv[1:]
pattern = r"[0-9]+\.[0-9]+\.[0-9]+"
if not re.fullmatch(pattern + r"(?:-[0-9A-Za-z.-]+)?", candidate):
    sys.exit("Expected an exact published SDK version")
published = {node.text for node in ET.parse(metadata).findall("./versioning/versions/version")}
if candidate not in published:
    sys.exit(f"Candidate SDK {candidate} is absent from the Maven version index")
core = tuple(map(int, candidate.split("-")[0].split(".")))
older = [(tuple(map(int, v.split("."))), v) for v in published
         if v and re.fullmatch(pattern, v) and tuple(map(int, v.split("."))) < core]
if not older:
    sys.exit(f"No earlier stable SDK is published for upgrade into {candidate}")
print(max(older)[1])
PY
)"
echo "Testing published Android SDK upgrade $previous -> $candidate"

cp -R "$root/examples/mobile-inspection" "$work/inspection"
cd "$work/inspection/android"
debug=app/src/debug
mkdir -p "$debug/res/xml" "$debug/res/raw"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=127.0.0.1 \
  -addext 'subjectAltName=IP:127.0.0.1' -keyout "$work/server.key" \
  -out "$debug/res/raw/fixture_ca.pem" 2>"$work/openssl.log"
cat > "$debug/AndroidManifest.xml" <<'XML'
<manifest xmlns:android="http://schemas.android.com/apk/res/android">
    <application android:networkSecurityConfig="@xml/upgrade_test_tls" />
</manifest>
XML
cat > "$debug/res/xml/upgrade_test_tls.xml" <<'XML'
<network-security-config>
    <domain-config>
        <domain>127.0.0.1</domain>
        <trust-anchors><certificates src="@raw/fixture_ca" /></trust-anchors>
    </domain-config>
</network-security-config>
XML

build_install() {
  local version=$1 code=$2
  gradle --no-daemon "-Porch8Version=$version" "-PinspectionVersionCode=$code" \
    :app:assembleDebug :app:assembleDebugAndroidTest
  adb -e install -r app/build/outputs/apk/debug/app-debug.apk
  adb -e install -r -t app/build/outputs/apk/androidTest/debug/app-debug-androidTest.apk
}

instrument() {
  local phase=$1 version=$2
  shift 2
  adb -e shell am instrument -w -r \
    -e class "$application.InspectionUpgradeTest" -e upgradePhase "$phase" \
    -e sdkVersion "$version" "$@" "$application.test/androidx.test.runner.AndroidJUnitRunner" \
    | tee "$work/$phase.log"
  grep -q 'OK (1 test)' "$work/$phase.log"
}

api_key=orch8-mobile-upgrade-fixture-000000000
encryption_key="$(openssl rand -hex 32)"
if [[ -z "$native_engine" ]]; then
  docker run -d --name "$container" -p 127.0.0.1::8080 \
    -e ORCH8_STORAGE_BACKEND=sqlite -e 'ORCH8_DATABASE_URL=sqlite:///tmp/orch8.db?mode=rwc' \
    -e ORCH8_NODE_ROLE=all_in_one -e ORCH8_HTTP_ADDR=0.0.0.0:8080 \
    -e "ORCH8_API_KEY=$api_key" -e "ORCH8_ENCRYPTION_KEY=$encryption_key" \
    "${ORCH8_UPGRADE_ENGINE_IMAGE:-ghcr.io/orch8-io/engine:$candidate}" >/dev/null
  base="http://$(docker port "$container" 8080/tcp)"
else
  read -r port grpc_port < <(python3 - <<'PY'
import socket
with socket.socket() as http, socket.socket() as grpc:
    for listener in (http, grpc):
        listener.bind(("127.0.0.1", 0))
    print(http.getsockname()[1], grpc.getsockname()[1])
PY
)
  (
    cd "$work"
    exec env ORCH8_STORAGE_BACKEND=sqlite "ORCH8_DATABASE_URL=sqlite://$work/engine.db?mode=rwc" \
      ORCH8_NODE_ROLE=all_in_one "ORCH8_HTTP_ADDR=127.0.0.1:$port" "ORCH8_GRPC_ADDR=127.0.0.1:$grpc_port" \
      "ORCH8_API_KEY=$api_key" "ORCH8_ENCRYPTION_KEY=$encryption_key" "$native_engine"
  ) >"$work/engine.log" 2>&1 &
  engine_pid=$!
  base="http://127.0.0.1:$port"
fi
for ((attempt=0; attempt<40; attempt++)); do
  if curl -fsS --max-time 2 "$base/health/ready" >/dev/null; then break; fi
  sleep 1
done
curl -fsS --max-time 5 "$base/health/ready" >/dev/null
api() {
  local route=$1
  shift
  curl -fsS --max-time 10 -H "x-api-key: $api_key" -H 'x-tenant-id: upgrade' \
    -H 'content-type: application/json' "$@" "$base/api/v1/$route"
}
sequence=0191e4f2-a1b2-7c3d-8e4f-a5b6c7d8e9f0
api sequences -X POST -d "{
  \"id\":\"$sequence\", \"tenant_id\":\"upgrade\", \"namespace\":\"default\",
  \"name\":\"inspection-upgrade-receiver\", \"version\":1, \"created_at\":\"2026-01-01T00:00:00Z\",
  \"blocks\":[{\"type\":\"step\",\"id\":\"accept\",\"handler\":\"noop\"}]
}" >/dev/null

# Replace the fresh-install fixture, then preserve app data across install -r.
adb -e uninstall "$application.test" >/dev/null 2>&1 || true
adb -e uninstall "$application" >/dev/null 2>&1 || true
adb -e shell svc wifi disable
adb -e shell svc data disable
build_install "$previous" 1
instrument queue "$previous"
read_app_file() {
  adb -e shell run-as "$application" cat "files/published-upgrade/$1" | tr -d '\r'
}
local_id="$(read_app_file submission-id)"
test -n "$local_id"
api "instances?sequence_id=$sequence" | jq -e '.items | length == 0' >/dev/null

adb -e shell am force-stop "$application"
build_install "$candidate" 2
test "$(read_app_file submission-id)" = "$local_id"

python3 "$root/scripts/fixtures/upgrade-tls-proxy.py" "$base" "$debug/res/raw/fixture_ca.pem" \
  "$work/server.key" "$work/proxy-port" &
proxy_pid=$!
for ((attempt=0; attempt<20; attempt++)); do
  [[ -s "$work/proxy-port" ]] && break
  kill -0 "$proxy_pid"
  sleep 1
done
adb -e reverse "tcp:$device_port" "tcp:$(cat "$work/proxy-port")"
adb -e shell svc wifi enable
adb -e shell svc data enable
receiver_args=(-e receiver "https://127.0.0.1:$device_port" -e sequence "$sequence" -e apiKey "$api_key")
instrument submit "$candidate" "${receiver_args[@]}"
first_receipt="$(api "instances?sequence_id=$sequence" | jq -er --arg local "$local_id" '
  .items | select(length == 1 and .[0].context.data.instance_id == $local) | .[0].id
')"

adb -e shell am force-stop "$application"
instrument deliver "$candidate" "${receiver_args[@]}"
receipt="$(read_app_file receipt-id)"
test "$receipt" = "$first_receipt"
api "instances?sequence_id=$sequence" | jq -e --arg local "$local_id" --arg receipt "$receipt" '
  .items | length == 1 and
  .[0].id == $receipt and
  .[0].context.data.instance_id == $local and
  .[0].context.data.inspection_result == "passed" and
  (.[0].idempotency_key | endswith("field-inspection:" + $local))
' >/dev/null
echo "The upgraded app retained submission $local_id and received one engine receipt."
