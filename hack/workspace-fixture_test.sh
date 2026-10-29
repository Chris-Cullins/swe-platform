#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT
export ROOT TEST_TMP
# Execute the real helper; replace only its external boundaries. Stop at Ready
# (before destructive storage checks) to exercise the actual EXIT trap on both
# successful and failed commands, including uncertain create/delete responses.
cat > "$TEST_TMP/run.sh" <<'SH'
set -euo pipefail
export SWE_BROWSER_TOKEN=SENSITIVE_TOKEN
MODE=$1
curl() {
	if [[ "$*" == *stale-source* ]]; then printf 409; else printf 403; fi
}
bin/swe() { printf '%s' '{"sourceUID":"source-uid","workspaceIncarnation":"incarnation","reason":"EligibleActive"}'; }
kubectl() {
	case "$*" in
		*' create -f -'*) cat > "$TEST_TMP/grant" ;;
		*' create rolebinding '*) : ;;
		*' get run '*) printf '%s' '{"status":{"workspaceRef":{"environmentUID":"env-uid","incarnation":"incarnation"}}}' ;;
		*' get environment shared -o json'*) printf '%s' '{"spec":{"templateRef":"small","projectRef":"p"},"status":{"workspace":{"incarnation":"incarnation","pvcRef":{"uid":"disk-uid"}}}}' ;;
		'create -f - -o jsonpath={.metadata.uid}')
			cat > "$TEST_TMP/created"
			if [[ "$MODE" == unknown ]]; then return 37; fi
			printf 15bd9bf1-eef4-4e49-a6ae-02cd99ca9695 ;;
		*' wait --for=condition=Ready '*)
			case "$MODE" in success|cleanup-fails-success) exit 0 ;; *) exit 37 ;; esac ;;
		'delete --raw '*)
			cat >> "$TEST_TMP/deletes"
			[[ "$*" == 'delete --raw /apis/swe.dev/v1alpha1/namespaces/team/environments/e2e-workspace-proof -f -' ]] || return 93
			if [[ "$MODE" == replacement || "$MODE" == cleanup-fails-success ]]; then
				echo SENSITIVE_CLEANUP_TOKEN >&2; return 44
			fi ;;
		*' delete rolebinding,role '*) echo grants-removed >> "$TEST_TMP/cleanup" ;;
		*' delete rolebinding '*) : ;;
		*' get '* )
			if [[ "$MODE" == collision ]]; then echo existing-object; fi
			if [[ "$MODE" == unknown && -f "$TEST_TMP/created" ]]; then echo environment/replacement; fi ;;
		*) echo 'unexpected boundary' >&2; return 94 ;;
	esac
}
source "$ROOT/hack/workspace-provenance_test.sh" http://fixture team source source-uid shared env-uid
SH
for mode in success failed replacement unknown cleanup-fails-success collision; do
	rm -f "$TEST_TMP/"{deletes,created,grant,cleanup}
	set +e
	TMPDIR="$TEST_TMP" bash "$TEST_TMP/run.sh" "$mode" > "$TEST_TMP/output" 2>&1
	status=$?
	set -e
	want=37
	case "$mode" in success) want=0 ;; cleanup-fails-success|collision) want=1 ;; esac
	[[ "$status" == "$want" ]] || { echo "FAIL: $mode exit=$status expected=$want"; cat "$TEST_TMP/output"; exit 1; }
	! grep -q SENSITIVE "$TEST_TMP/output"
	if [[ "$mode" == collision ]]; then
		[[ ! -e "$TEST_TMP/cleanup" && ! -e "$TEST_TMP/deletes" && ! -e "$TEST_TMP/grant" ]]
		continue
	fi
	grep -Fx '    verbs: ["use"]' "$TEST_TMP/grant" >/dev/null
	grep -Fx '    resources: ["environments"]' "$TEST_TMP/grant" >/dev/null
	grep -Fx '    resourceNames: ["shared"]' "$TEST_TMP/grant" >/dev/null
	[[ $(grep -c 'apiGroups:' "$TEST_TMP/grant") == 1 ]]
	[[ $(wc -l < "$TEST_TMP/cleanup") == 1 ]]
	if [[ "$mode" == unknown ]]; then
		[[ ! -e "$TEST_TMP/deletes" ]]
	else
		jq -e '.kind == "DeleteOptions" and .preconditions.uid == "15bd9bf1-eef4-4e49-a6ae-02cd99ca9695"' "$TEST_TMP/deletes" >/dev/null
		[[ $(grep -c 'DeleteOptions' "$TEST_TMP/deletes") == 1 ]]
	fi
	if [[ "$mode" == replacement || "$mode" == unknown || "$mode" == cleanup-fails-success ]]; then
		grep -Fx 'FAIL: workspace fixture cleanup uncertain; no adoption or deletion retry attempted' "$TEST_TMP/output" >/dev/null
	fi
done
echo 'PASS: actual workspace helper grants named use, preserves exit/privacy and UID-fences cleanup without adoption/retry'
