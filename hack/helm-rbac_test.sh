#!/usr/bin/env bash
set -euo pipefail

for mode in scoped trusted-admin; do
	render=$(helm template rbac-test charts/swe-platform \
		--namespace rbac-system \
		--set "tenancy.mode=$mode" \
		--show-only templates/rbac.yaml)

	control_plane_role=$(awk -v RS='---' '
		/kind: ClusterRole\n/ && /app.kubernetes.io\/component: control-plane/ &&
		/resources: \["environments"\]/ { print; exit }
	' <<<"$render")

	base_environment_rule=$(awk -v RS='  - apiGroups:' '
		/\["swe.dev"\]/ && /resources: \["environments"\]/ &&
		/"get"/ && /"patch"/ { print; exit }
	' <<<"$control_plane_role")
	if [[ -z "$control_plane_role" || -z "$base_environment_rule" ]]; then
		echo "$mode render has no control-plane ClusterRole with required Environment get/patch authority" >&2
		exit 1
	fi
	operator_role=$(awk -v RS='---' '
		/kind: ClusterRole\n/ && /resources: \["persistentvolumeclaims", "pods"\]/ &&
		/resources: \["secrets"\]/ { print; exit }
	' <<<"$render")
	operator_secret_rule=$(awk -v RS='  - apiGroups:' '
		/\[""\]/ && /resources: \["secrets"\]/ && /"list"/ && /"watch"/ { print; exit }
	' <<<"$operator_role")
	if [[ -z "$operator_role" || -z "$operator_secret_rule" ]]; then
		echo "$mode render must let the operator watch owned credential Secret teardown" >&2
		exit 1
	fi
	if grep -Eq 'resources: \[[^]]*"environments/status"|^[[:space:]]*-[[:space:]]*environments/status([[:space:]]|$)' <<<"$control_plane_role"; then
		echo "$mode portal-disabled control-plane ClusterRole must not grant any environments/status authority" >&2
		exit 1
	fi
	status_policy=$(helm template rbac-test charts/swe-platform \
		--namespace rbac-system \
		--set "tenancy.mode=$mode" \
		--show-only templates/control-plane-portal-status-policy.yaml)
	if ! grep -Fq 'object.?status.?provisioning == oldObject.?status.?provisioning' <<<"$status_policy"; then
		echo "$mode portal-disabled render must pre-stage the control-plane provisioning status fence" >&2
		exit 1
	fi

	portal_render=$(helm template rbac-test charts/swe-platform \
		--namespace rbac-system \
		--set "tenancy.mode=$mode" \
		--set controlPlane.portal.enabled=true \
		--set-string controlPlane.portal.suffix=portal.test)
	portal_control_plane_role=$(awk -v RS='---' '
		/kind: ClusterRole\n/ && /app.kubernetes.io\/component: control-plane/ &&
		/resources: \["environments\/status"\]/ { print; exit }
	' <<<"$portal_render")
	portal_status_rule=$(awk -v RS='  - apiGroups:' '
		/\["swe.dev"\]/ && /resources: \["environments\/status"\]/ { print; exit }
	' <<<"$portal_control_plane_role")
	if [[ -z "$portal_control_plane_role" || -z "$portal_status_rule" ]] ||
		[[ $(grep -Ec 'resources: \["environments/status"\]' <<<"$portal_control_plane_role") -ne 1 ]] ||
		! grep -Eq 'verbs: \["update"\]' <<<"$portal_status_rule" ||
		grep -Eq '"(create|delete|deletecollection|get|list|patch|watch)"' <<<"$portal_status_rule"; then
		echo "$mode portal-enabled control-plane ClusterRole must grant exactly update on environments/status" >&2
		exit 1
	fi
	portal_status_policy=$(awk -v RS='---' '
		/kind: ValidatingAdmissionPolicy\n/ && /portal-control-plane-service-account/ { print; exit }
	' <<<"$portal_render")
	if [[ -z "$portal_status_policy" ]] ||
		! grep -Fq 'system:serviceaccount:rbac-system:rbac-test-swe-platform-control-plane' <<<"$portal_status_policy" ||
		! grep -Fq 'object.?status.?provisioning == oldObject.?status.?provisioning' <<<"$portal_status_policy"; then
		echo "$mode portal-enabled render must field-fence the exact control-plane ServiceAccount from provisioning status" >&2
		exit 1
	fi
done

if helm template rbac-test charts/swe-platform --namespace rbac-system \
	--set tenancy.mode=scoped --set operator.githubApp.enabled=true >/dev/null 2>&1; then
	echo "GitHub App enablement without administrator-owned identity/Secret references must fail" >&2
	exit 1
fi
github_app_render=$(helm template rbac-test charts/swe-platform \
	--namespace rbac-system \
	--set tenancy.mode=scoped \
	--set operator.githubApp.enabled=true \
	--set-string operator.githubApp.clientID=Iv1.test \
	--set-string operator.githubApp.secretName=github-app-private-key)
github_app_operator=$(awk -v RS='---' '
	/kind: Deployment/ && /app.kubernetes.io\/component: operator/ { print; exit }
' <<<"$github_app_render")
if [[ -z "$github_app_operator" ]] ||
	! grep -Fq -- '--github-app-client-id=Iv1.test' <<<"$github_app_operator" ||
	! grep -Fq -- '--github-app-private-key-file=/var/run/secrets/swe-platform-github-app/private-key.pem' <<<"$github_app_operator" ||
	! grep -Fq 'secretName: github-app-private-key' <<<"$github_app_operator" ||
	! grep -Fq 'key: private-key.pem' <<<"$github_app_operator"; then
	echo "GitHub App render did not mount the exact administrator-owned key only for the operator" >&2
	exit 1
fi

echo "scoped and trusted-admin Helm RBAC plus disabled-by-default GitHub App mounts are fenced"
