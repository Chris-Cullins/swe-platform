#!/usr/bin/env bash
# Keyless live acceptance against the existing disposable kind fixture.
# Do not print credentials, response bodies, or raw Kubernetes objects.
set -euo pipefail
BASE_URL=$1 NAMESPACE=$2 SOURCE=$3 SOURCE_UID=$4 ENVIRONMENT=$5 ENVIRONMENT_UID=$6
: "${SWE_BROWSER_TOKEN:?existing console principal required}"
FIXTURE=e2e-workspace-proof
TMP=$(mktemp -d)
cleanup() {
	kubectl -n "$NAMESPACE" delete rolebinding,role "$FIXTURE" --ignore-not-found >/dev/null 2>&1 || true
	kubectl -n "$NAMESPACE" delete environment "$FIXTURE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
	rm -rf "$TMP"
}
trap cleanup EXIT
inspect() {
	SWE_CONTROL_PLANE_URL="$BASE_URL" SWE_CONTROL_PLANE_TOKEN="$SWE_BROWSER_TOKEN" \
		bin/swe --namespace "$NAMESPACE" follow-up inspect "$SOURCE" --run-uid "$SOURCE_UID" --json > "$TMP/inspection.json"
}
STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer $SWE_BROWSER_TOKEN" -H "SWE-Run-UID: $SOURCE_UID" \
	"$BASE_URL/api/v1/namespaces/$NAMESPACE/runs/$SOURCE/follow-up")
[[ "$STATUS" == 403 ]] || { echo "FAIL: inspection without execution-use returned $STATUS"; exit 1; }
kubectl -n "$NAMESPACE" create role "$FIXTURE" --verb=use --resource=environments.swe.dev --resource-name="$ENVIRONMENT" >/dev/null
kubectl -n "$NAMESPACE" create rolebinding "$FIXTURE" --role="$FIXTURE" --serviceaccount="$NAMESPACE:e2e-console" >/dev/null
kubectl -n "$NAMESPACE" get run "$SOURCE" -o json > "$TMP/source.json"
kubectl -n "$NAMESPACE" get environment "$ENVIRONMENT" -o json > "$TMP/environment.json"
jq -e --arg uid "$ENVIRONMENT_UID" '.status.workspaceRef.environmentUID == $uid and (.status.workspaceRef.incarnation | length > 0)' "$TMP/source.json" >/dev/null
INCARNATION=$(jq -r '.status.workspaceRef.incarnation' "$TMP/source.json")
jq -e --arg incarnation "$INCARNATION" '.status.workspace.incarnation == $incarnation and (.status.workspace.pvcRef.uid | length > 0) and .status.claimedBy == null' "$TMP/environment.json" >/dev/null
inspect
jq -e --arg uid "$SOURCE_UID" --arg incarnation "$INCARNATION" \
	'.sourceUID == $uid and .workspaceIncarnation == $incarnation and (.reason == "EligibleActive" or .reason == "WakeConsentRequired") and (has("prompt") | not) and (has("pvcRef") | not)' "$TMP/inspection.json" >/dev/null
STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer $SWE_BROWSER_TOKEN" -H 'SWE-Run-UID: stale-source' \
	"$BASE_URL/api/v1/namespaces/$NAMESPACE/runs/$SOURCE/follow-up")
[[ "$STATUS" == 409 ]] || { echo "FAIL: stale inspection returned $STATUS"; exit 1; }
kubectl -n "$NAMESPACE" delete rolebinding "$FIXTURE" >/dev/null
STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer $SWE_BROWSER_TOKEN" -H "SWE-Run-UID: $SOURCE_UID" \
	"$BASE_URL/api/v1/namespaces/$NAMESPACE/runs/$SOURCE/follow-up")
[[ "$STATUS" == 403 ]] || { echo "FAIL: revoked inspection returned $STATUS"; exit 1; }

# Isolated storage destruction: never mutate the shared fake-agent workspace.
jq --arg name "$FIXTURE" --arg ns "$NAMESPACE" \
	'{apiVersion:"swe.dev/v1alpha1",kind:"Environment",metadata:{name:$name,namespace:$ns},spec:{templateRef:.spec.templateRef,projectRef:.spec.projectRef}}' \
	"$TMP/environment.json" | kubectl create -f - >/dev/null
kubectl -n "$NAMESPACE" wait --for=condition=Ready environment/"$FIXTURE" --timeout=3m >/dev/null
kubectl -n "$NAMESPACE" get environment "$FIXTURE" -o json > "$TMP/new.json"
PVC=$(jq -r '.status.workspace.pvcRef.name' "$TMP/new.json")
PVC_UID=$(jq -r '.status.workspace.pvcRef.uid' "$TMP/new.json")
[[ "$PVC" != null && "$PVC_UID" != null ]] || { echo 'FAIL: new workspace has no storage proof'; exit 1; }
bin/swe --namespace "$NAMESPACE" environment hold "$FIXTURE" >/dev/null
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Paused environment/"$FIXTURE" --timeout=3m >/dev/null
kubectl -n "$NAMESPACE" wait --for=delete pod/"env-$FIXTURE" --timeout=60s >/dev/null
kubectl -n "$NAMESPACE" get pvc "$PVC" -o json | jq \
	'{apiVersion:"v1",kind:"PersistentVolumeClaim",metadata:{name:.metadata.name,namespace:.metadata.namespace,ownerReferences:.metadata.ownerReferences},spec:{accessModes:.spec.accessModes,resources:.spec.resources,storageClassName:.spec.storageClassName}}' > "$TMP/replacement.json"
kubectl -n "$NAMESPACE" delete pvc "$PVC" --wait=true --timeout=60s >/dev/null
bin/swe --namespace "$NAMESPACE" environment release "$FIXTURE" >/dev/null
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Failed environment/"$FIXTURE" --timeout=60s >/dev/null
if kubectl -n "$NAMESPACE" get pvc "$PVC" >/dev/null 2>&1 || kubectl -n "$NAMESPACE" get pod "env-$FIXTURE" >/dev/null 2>&1; then
	echo 'FAIL: missing bound workspace was recreated or executed'; exit 1
fi
kubectl create -f "$TMP/replacement.json" >/dev/null
REPLACEMENT_UID=$(kubectl -n "$NAMESPACE" get pvc "$PVC" -o jsonpath='{.metadata.uid}')
[[ "$REPLACEMENT_UID" != "$PVC_UID" ]] || { echo 'FAIL: replacement fixture retained old UID'; exit 1; }
# A new hold revision forces a fresh reconciliation rather than observing stale failure.
bin/swe --namespace "$NAMESPACE" environment hold "$FIXTURE" >/dev/null
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Paused environment/"$FIXTURE" --timeout=60s >/dev/null
bin/swe --namespace "$NAMESPACE" environment release "$FIXTURE" >/dev/null
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Failed environment/"$FIXTURE" --timeout=60s >/dev/null
kubectl -n "$NAMESPACE" get environment "$FIXTURE" -o json | jq -e --arg uid "$PVC_UID" '.status.workspace.pvcRef.uid == $uid' >/dev/null
if kubectl -n "$NAMESPACE" get pod "env-$FIXTURE" >/dev/null 2>&1; then
	echo 'FAIL: replacement workspace executed'; exit 1
fi
echo 'PASS: exact workspace association, authorized advisory inspection, revocation and disposable storage replacement fence'
