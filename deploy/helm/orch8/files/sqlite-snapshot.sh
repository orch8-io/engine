#!/bin/sh
set -eu

umask 077
export KUBECONFIG="$HOME/snapshot.kubeconfig"
cat > "$KUBECONFIG" <<'EOF'
apiVersion: v1
kind: Config
clusters:
  - name: local
    cluster:
      server: https://kubernetes.default.svc
      certificate-authority: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
users:
  - name: snapshot
    user:
      tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
contexts:
  - name: local
    context:
      cluster: local
      user: snapshot
current-context: local
EOF

owner_key='orch8.io/sqlite-snapshot'
generation_key='orch8.io/sqlite-snapshot-generation'
claim_key='orch8.io/sqlite-snapshot-claim-uid'
last_key='orch8.io/sqlite-last-snapshot'
deadline=$(($(date +%s) + TIMEOUT_SECONDS))

k() {
    kubectl --namespace "$POD_NAMESPACE" --request-timeout=8s "$@"
}

die() {
    echo "$*" >&2
    exit 1
}

wait_again() {
    [ "$(date +%s)" -lt "$deadline" ] || die "Timed out waiting for a fenced SQLite snapshot"
    sleep 2
}

get_deployment() {
    row=$(k get deployment "$DEPLOYMENT" -o 'go-template={{.metadata.resourceVersion}} {{.metadata.uid}} {{.metadata.generation}} {{or .status.observedGeneration 0}} {{.spec.replicas}} {{with .metadata.annotations}}{{or (index . "orch8.io/sqlite-snapshot") "-"}} {{or (index . "orch8.io/sqlite-snapshot-generation") "-"}} {{or (index . "orch8.io/sqlite-snapshot-claim-uid") "-"}} {{or (index . "orch8.io/sqlite-last-snapshot") "-"}}{{else}}- - - -{{end}}')
    read -r revision uid generation observed replicas owner fenced_generation fenced_claim_uid last <<EOF
$row
EOF
}

get_snapshot() {
    row=$(k get volumesnapshot "$SNAPSHOT" --ignore-not-found -o 'go-template={{or .spec.source.persistentVolumeClaimName "-"}} {{or .spec.volumeSnapshotClassName "-"}} {{with .metadata.annotations}}{{or (index . "orch8.io/sqlite-deployment-uid") "-"}} {{or (index . "orch8.io/sqlite-snapshot-generation") "-"}} {{or (index . "orch8.io/sqlite-snapshot-claim-uid") "-"}}{{else}}- - -{{end}} {{if .status}}{{if .status.error}}error{{else if .status.readyToUse}}ready{{else}}pending{{end}}{{else}}pending{{end}}')
    source_claim='' source_class='' source_uid='' source_generation='' source_claim_uid='' phase=''
    if [ -n "$row" ]; then
        read -r source_claim source_class source_uid source_generation source_claim_uid phase <<EOF
$row
EOF
    fi
}

verify_snapshot() {
    [ "$source_claim" = "$CLAIM" ] && [ "$source_class" = "$SNAPSHOT_CLASS" ] &&
        [ "$source_uid" = "$uid" ] || die "VolumeSnapshot already exists with a different source"
}

verify_fence() {
    [ "$owner" = "$SNAPSHOT" ] && [ "$generation" = "$fenced_generation" ] &&
        [ "$replicas" = 0 ] || die "Deployment changed while the SQLite snapshot was fenced"
}

get_claim_uid() {
    claim_uid=$(k get pvc "$CLAIM" -o 'go-template={{if and (eq .status.phase "Bound") (not .metadata.deletionTimestamp)}}{{.metadata.uid}}{{end}}')
    [ -n "$claim_uid" ] || die "SQLite claim is unbound or being deleted"
}

verify_claim() {
    get_claim_uid
    [ "$claim_uid" = "$fenced_claim_uid" ] || die "SQLite claim was replaced while the snapshot was fenced"
}

claim_is_unmounted() {
    rows=$(k get pods -o 'jsonpath={range .items[*]}{.metadata.name}{" "}{range .spec.volumes[*]}{.persistentVolumeClaim.claimName}{" "}{end}{"\n"}{end}') ||
        die "Could not verify the SQLite claim is unmounted"
    printf '%s\n' "$rows" | awk -v claim="$CLAIM" '{
        for (i = 2; i <= NF; ++i) {
            if ($i == claim) mounted = 1
        }
    } END { exit mounted }'
}

get_deployment
get_snapshot
if [ "$owner" = "-" ] && [ "$last" = "$SNAPSHOT" ]; then
    verify_snapshot
    [ "$phase" = ready ] || die "Previously completed VolumeSnapshot is unavailable"
    exit 0
fi

if [ "$owner" = "-" ]; then
    [ -z "$phase" ] || die "VolumeSnapshot already exists"
    [ "$replicas" = 1 ] || die "Expected one live SQLite replica before fencing"
    get_claim_uid
    expected_generation=$((generation + 1))
    k patch deployment "$DEPLOYMENT" --type=merge -p \
        "{\"metadata\":{\"resourceVersion\":\"$revision\",\"annotations\":{\"$owner_key\":\"$SNAPSHOT\",\"$generation_key\":\"$expected_generation\",\"$claim_key\":\"$claim_uid\"}},\"spec\":{\"replicas\":0}}"
fi

while :; do
    get_deployment
    verify_fence
    verify_claim
    if [ "$observed" = "$generation" ] && claim_is_unmounted; then
        break
    fi
    wait_again
done

get_snapshot
if [ -z "$phase" ]; then
    verify_claim
    k create -f - <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: $SNAPSHOT
  annotations:
    orch8.io/sqlite-deployment-uid: "$uid"
    $generation_key: "$fenced_generation"
    $claim_key: "$fenced_claim_uid"
spec:
  volumeSnapshotClassName: $SNAPSHOT_CLASS
  source:
    persistentVolumeClaimName: $CLAIM
EOF
fi

while :; do
    get_deployment
    verify_fence
    verify_claim
    claim_is_unmounted || die "A pod mounted the SQLite claim during the snapshot"
    get_snapshot
    verify_snapshot
    [ "$source_generation" = "$fenced_generation" ] || die "VolumeSnapshot is from a different fence generation"
    [ "$source_claim_uid" = "$fenced_claim_uid" ] || die "VolumeSnapshot has a different claim identity"
    case "$phase" in
        ready) break ;;
        error) die "CSI snapshot failed. Deployment remains fenced for recovery" ;;
    esac
    wait_again
done

# resourceVersion prevents an intervening deployment edit between the final
# fence verification and the replica restore. A retry can finish this step.
verify_claim
k patch deployment "$DEPLOYMENT" --type=merge -p \
    "{\"metadata\":{\"resourceVersion\":\"$revision\",\"annotations\":{\"$owner_key\":null,\"$generation_key\":null,\"$claim_key\":null,\"$last_key\":\"$SNAPSHOT\"}},\"spec\":{\"replicas\":1}}"
echo "VolumeSnapshot $SNAPSHOT ready. SQLite deployment resumed"
