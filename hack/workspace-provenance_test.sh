#!/usr/bin/env bash
# Keyless live acceptance against the existing disposable kind fixture.
# Do not print credentials, response bodies, or raw Kubernetes objects.
set -euo pipefail
BASE_URL=$1 NAMESPACE=$2 SOURCE=$3 SOURCE_UID=$4 ENVIRONMENT=$5 ENVIRONMENT_UID=$6
: "${SWE_BROWSER_TOKEN:?existing console principal required}"
FIXTURE=e2e-workspace-proof
for resource in environment role rolebinding; do
	EXISTING=$(kubectl -n "$NAMESPACE" get "$resource" "$FIXTURE" --ignore-not-found -o name)
	[[ -z "$EXISTING" ]] || { echo 'FAIL: workspace fixture name already exists'; exit 1; }
done
TMP=$(mktemp -d)
CREATED_UID=""
CREATE_ATTEMPTED=false
cleanup() {
	local result=$? cleanup_ok=true remaining
	trap - EXIT
	set +e
	if [[ "$CREATE_ATTEMPTED" == true ]]; then
		if [[ "$CREATED_UID" =~ ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ ]]; then
			jq -n --arg uid "$CREATED_UID" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' |
				kubectl delete --raw "/apis/swe.dev/v1alpha1/namespaces/$NAMESPACE/environments/$FIXTURE" -f - >/dev/null 2>&1 || cleanup_ok=false
		else
			# Unknown create outcome: observe absence only, never adopt a replacement UID.
			remaining=$(kubectl -n "$NAMESPACE" get environment "$FIXTURE" --ignore-not-found -o name 2>/dev/null) || cleanup_ok=false
			[[ -z "$remaining" ]] || cleanup_ok=false
		fi
	fi
	kubectl -n "$NAMESPACE" delete rolebinding,role "$FIXTURE" --ignore-not-found >/dev/null 2>&1 || cleanup_ok=false
	if [[ "$cleanup_ok" == true ]]; then rm -rf "$TMP"; else
		echo 'FAIL: workspace fixture cleanup uncertain; no adoption or deletion retry attempted' >&2
		[[ "$result" != 0 ]] || result=1
	fi
	exit "$result"
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
# kubectl create role rejects custom verbs client-side; the API admits this named SAR grant.
cat <<EOF | kubectl -n "$NAMESPACE" create -f - >/dev/null
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: $FIXTURE
rules:
  - apiGroups: ["swe.dev"]
    resources: ["environments"]
    resourceNames: ["$ENVIRONMENT"]
    verbs: ["use"]
EOF
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
CREATE_ATTEMPTED=true
CREATED_UID=$(jq --arg name "$FIXTURE" --arg ns "$NAMESPACE" \
	'{apiVersion:"swe.dev/v1alpha1",kind:"Environment",metadata:{name:$name,namespace:$ns},spec:{templateRef:.spec.templateRef,projectRef:.spec.projectRef}}' \
	"$TMP/environment.json" | kubectl create -f - -o jsonpath='{.metadata.uid}')
[[ "$CREATED_UID" =~ ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ ]] || { echo 'FAIL: workspace create returned no exact UID'; exit 1; }
kubectl -n "$NAMESPACE" wait --for=condition=Ready environment/"$FIXTURE" --timeout=3m >/dev/null
kubectl -n "$NAMESPACE" get environment "$FIXTURE" -o json > "$TMP/new.json"
jq -e --arg uid "$CREATED_UID" '.metadata.uid == $uid' "$TMP/new.json" >/dev/null
PVC=$(jq -r '.status.workspace.pvcRef.name' "$TMP/new.json")
PVC_UID=$(jq -r '.status.workspace.pvcRef.uid' "$TMP/new.json")
[[ "$PVC" != null && "$PVC_UID" != null ]] || { echo 'FAIL: new workspace has no storage proof'; exit 1; }
bin/swe --namespace "$NAMESPACE" environment hold "$FIXTURE" >/dev/null
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Paused environment/"$FIXTURE" --timeout=3m >/dev/null
kubectl -n "$NAMESPACE" wait --for=delete pod/"env-$FIXTURE" --timeout=60s >/dev/null
kubectl -n "$NAMESPACE" get pvc "$PVC" -o json | jq \
	'{apiVersion:"v1",kind:"PersistentVolumeClaim",metadata:{name:.metadata.name,namespace:.metadata.namespace,ownerReferences:.metadata.ownerReferences},spec:{accessModes:.spec.accessModes,resources:.spec.resources,storageClassName:.spec.storageClassName}}' > "$TMP/replacement.json"
jq -n --arg uid "$PVC_UID" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' |
	kubectl delete --raw "/api/v1/namespaces/$NAMESPACE/persistentvolumeclaims/$PVC" -f - >/dev/null
kubectl -n "$NAMESPACE" wait --for=delete pvc/"$PVC" --timeout=60s >/dev/null
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
