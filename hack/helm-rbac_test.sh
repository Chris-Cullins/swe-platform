#!/usr/bin/env bash
set -euo pipefail

assert_portal_status_policy_covers_environment_status() {
	local policy=$1
	local description=$2
	local field
	local -A status_fields=()
	local -A frozen_fields=()

	while IFS= read -r field; do
		status_fields["$field"]=1
	done < <(awk '
		/^          status:$/ { in_status = 1; next }
		in_status && /^            properties:$/ { in_properties = 1; next }
		in_properties && /^              [[:alnum:]][[:alnum:]]*:$/ {
			field = $1
			sub(/:$/, "", field)
			print field
			next
		}
		in_properties && /^            [^ ]/ { exit }
	' charts/swe-platform/crds/swe.dev_environments.yaml)
	if (( ${#status_fields[@]} == 0 )); then
		echo "$description could not read Environment status fields from the chart CRD" >&2
		exit 1
	fi

	while IFS= read -r field; do
		frozen_fields["$field"]=1
	done < <(sed -nE 's/^[[:space:]]*- expression: object\.\?status\.\?([[:alnum:]]+) == oldObject\.\?status\.\?\1$/\1/p' <<<"$policy")

	for field in "${!frozen_fields[@]}"; do
		if [[ -z ${status_fields[$field]+x} ]]; then
			echo "$description freezes unknown Environment status field $field" >&2
			exit 1
		fi
	done
	for field in "${!status_fields[@]}"; do
		case "$field" in
		portalRoutes|nextPortalRouteGeneration)
			if [[ -n ${frozen_fields[$field]+x} ]]; then
				echo "$description must leave portal-owned Environment status field $field writable" >&2
				exit 1
			fi
			;;
		*)
			if [[ -z ${frozen_fields[$field]+x} ]]; then
				echo "$description does not freeze controller-owned Environment status field $field" >&2
				exit 1
			fi
			;;
		esac
	done
}
environment_crd=config/crd/bases/swe.dev_environments.yaml
mapfile -t environment_spec_fields < <(awk '
	/^          spec:$/ { in_spec = 1; next }
	in_spec && /^            properties:$/ { in_properties = 1; next }
	in_properties && /^              [a-zA-Z][a-zA-Z0-9]*:$/ {
		field = $1; sub(/:$/, "", field); print field
	}
	in_spec && /^          status:$/ { exit }
' "$environment_crd")
mapfile -t environment_lifecycle_fields < <(awk '
	/^              lifecycle:$/ { in_lifecycle = 1; next }
	in_lifecycle && /^                properties:$/ { in_properties = 1; next }
	in_properties && /^                  [a-zA-Z][a-zA-Z0-9]*:$/ {
		field = $1; sub(/:$/, "", field); print field
	}
	in_lifecycle && /^              [a-zA-Z][a-zA-Z0-9]*:$/ { exit }
' "$environment_crd")
mapfile -t environment_status_fields < <(awk '
	/^          status:$/ { in_status = 1; next }
	in_status && /^            properties:$/ { in_properties = 1; next }
	in_properties && /^              [a-zA-Z][a-zA-Z0-9]*:$/ {
		field = $1; sub(/:$/, "", field); print field
	}
	in_status && /^          subresources:$/ { exit }
' "$environment_crd")

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
	operator_scope_role=$(awk -v RS='---' '
		/kind: ClusterRole\n/ && /resources: \["runtimeclasses"\]/ &&
		/resources: \["storageclasses", "csidrivers"\]/ { print; exit }
	' <<<"$render")
	operator_system_role=$(awk -v RS='---' '
		/kind: Role\n/ && /resources: \["installations"\]/ &&
		/resources: \["installations\/status"\]/ && /resources: \["configmaps"\]/ { print; exit }
	' <<<"$render")
	if [[ -z "$operator_scope_role" || -z "$operator_system_role" ]]; then
		echo "$mode render lacks Installation isolation dependency watches or system status authority" >&2
		exit 1
	fi
	installation_status_rule=$(awk -v RS='  - apiGroups:' '
		/\["swe.dev"\]/ && /resources: \["installations\/status"\]/ { print; exit }
	' <<<"$operator_system_role")
	configmap_rule=$(awk -v RS='  - apiGroups:' '
		/\[""\]/ && /resources: \["configmaps"\]/ { print; exit }
	' <<<"$operator_system_role")
	if grep -Eq 'resources: \["(installations|installations/status|configmaps)"\]' <<<"$operator_role$operator_scope_role" ||
		grep -Eq '"(create|delete|patch|update)"' <<<"$operator_scope_role" ||
		grep -Eq '"(create|delete|list|watch)"' <<<"$installation_status_rule" ||
		grep -Eq '"(create|delete|patch|update)"' <<<"$configmap_rule"; then
		echo "$mode render grants Installation isolation authority beyond exact system status and dependency observation" >&2
		exit 1
	fi
	disabled_status_rule=$(awk -v RS='  - apiGroups:' '
		/\["swe.dev"\]/ && /resources: \["environments\/status"\]/ { print; exit }
	' <<<"$control_plane_role")
	if [[ -z "$disabled_status_rule" ]] ||
		[[ $(grep -Ec 'resources: \["environments/status"\]' <<<"$control_plane_role") -ne 1 ]] ||
		! grep -Eq 'verbs: \["update"\]' <<<"$disabled_status_rule" ||
		grep -Eq '"(create|delete|deletecollection|get|list|patch|watch)"' <<<"$disabled_status_rule"; then
		echo "$mode portal-disabled control-plane ClusterRole must retain exactly update on environments/status for durable route denial" >&2
		exit 1
	fi
	status_policy=$(helm template rbac-test charts/swe-platform \
		--namespace rbac-system \
		--set "tenancy.mode=$mode" \
		--show-only templates/control-plane-portal-status-policy.yaml)
	environment_intent_policy=$(awk -v RS='---' '
		/kind: ValidatingAdmissionPolicy\n/ && /name: control-plane-service-account/ { print; exit }
	' <<<"$status_policy")
	if [[ -z "$environment_intent_policy" ]] ||
		! grep -Fq 'system:serviceaccount:rbac-system:rbac-test-swe-platform-control-plane' <<<"$environment_intent_policy" ||
		! grep -Fq "resources: [\"environments\"]" <<<"$environment_intent_policy" ||
		! grep -Fq "object.spec.lifecycle.wake.expectedSuspensionReason == 'Idle'" <<<"$environment_intent_policy" ||
		! grep -Fq "lifecycle.swe.dev/activity-terminal" <<<"$environment_intent_policy" ||
		! grep -Fq "lifecycle.swe.dev/activity-portal" <<<"$environment_intent_policy" ||
		! grep -Fq "object.metadata.?generateName == oldObject.metadata.?generateName" <<<"$environment_intent_policy"; then
		echo "$mode render must pre-stage the exact control-plane Environment intent fence" >&2
		exit 1
	fi
	for field in "${environment_spec_fields[@]}"; do
		[[ "$field" == lifecycle ]] && continue
		if ! grep -Fq "object.?spec.?$field == oldObject.?spec.?$field" <<<"$environment_intent_policy"; then
			echo "$mode Environment intent policy does not freeze spec.$field" >&2
			exit 1
		fi
	done
	for field in "${environment_lifecycle_fields[@]}"; do
		[[ "$field" == wake ]] && continue
		if ! grep -Fq "object.?spec.?lifecycle.?$field == oldObject.?spec.?lifecycle.?$field" <<<"$environment_intent_policy"; then
			echo "$mode Environment intent policy does not freeze spec.lifecycle.$field" >&2
			exit 1
		fi
	done
	portal_status_policy=$(awk -v RS='---' '
		/kind: ValidatingAdmissionPolicy\n/ && /portal-control-plane-service-account/ { print; exit }
	' <<<"$status_policy")
	assert_portal_status_policy_covers_environment_status "$portal_status_policy" "$mode portal-disabled render"
	for field in "${environment_status_fields[@]}"; do
		[[ "$field" == portalRoutes || "$field" == nextPortalRouteGeneration ]] && continue
		if ! grep -Fq "object.?status.?$field == oldObject.?status.?$field" <<<"$portal_status_policy"; then
			echo "$mode portal status policy does not freeze status.$field" >&2
			exit 1
		fi
	done
	operator_status_policy=$(awk -v RS='---' '
		/kind: ValidatingAdmissionPolicy\n/ && /operator-service-account/ { print; exit }
	' <<<"$status_policy")
	if [[ -z "$operator_status_policy" ]] ||
		! grep -Fq 'system:serviceaccount:rbac-system:rbac-test-swe-platform' <<<"$operator_status_policy" ||
		! grep -Fq 'object.?status.?portalRoutes == oldObject.?status.?portalRoutes' <<<"$operator_status_policy" ||
		! grep -Fq 'object.?status.?nextPortalRouteGeneration == oldObject.?status.?nextPortalRouteGeneration' <<<"$operator_status_policy"; then
		echo "$mode portal-disabled render must pre-stage the exact operator ServiceAccount portal status fence" >&2
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
		! grep -Fq 'system:serviceaccount:rbac-system:rbac-test-swe-platform-control-plane' <<<"$portal_status_policy"; then
		echo "$mode portal-enabled render must field-fence the exact control-plane ServiceAccount" >&2
		exit 1
	fi
	assert_portal_status_policy_covers_environment_status "$portal_status_policy" "$mode portal-enabled render"
done

assert_admission_policy_names() {
	local description=$1 render=$2 suffix resource_names policy_name name
	for suffix in env-intent portal-status; do
		mapfile -t resource_names < <(awk -v suffix="$suffix" '
			/^metadata:$/ { in_metadata = 1; next }
			in_metadata && $1 == "name:" && $2 ~ ("-" suffix "$") { print $2; in_metadata = 0 }
		' <<<"$render")
		if [[ ${#resource_names[@]} -ne 2 || "${resource_names[0]}" != "${resource_names[1]}" ]]; then
			echo "$description must render matching $suffix policy and binding names" >&2
			exit 1
		fi
		for name in "${resource_names[@]}"; do
			if (( ${#name} > 63 )); then
				echo "$description $suffix admission resource name exceeds 63 characters: $name" >&2
				exit 1
			fi
		done
		policy_name=$(awk -v suffix="$suffix" '$1 == "policyName:" && $2 ~ ("-" suffix "$") { print $2; exit }' <<<"$render")
		if [[ -z "$policy_name" || "$policy_name" != "${resource_names[0]}" ]]; then
			echo "$description $suffix binding must reference its bounded policy name" >&2
			exit 1
		fi
	done
}

render_admission_policies() {
	local namespace=$1
	shift
	helm template swe-platform charts/swe-platform \
		--namespace "$namespace" \
		--set tenancy.mode=scoped \
		--show-only templates/control-plane-portal-status-policy.yaml \
		"$@"
}

standard_policy_render=$(render_admission_policies swe-platform-system)
assert_admission_policy_names "standard release" "$standard_policy_render"
long_name_override=$(printf 'n%.0s' {1..63})
long_name_policy_render=$(render_admission_policies swe-platform-system --set-string "nameOverride=$long_name_override")
assert_admission_policy_names "long nameOverride release" "$long_name_policy_render"
long_fullname_override=$(printf 'f%.0s' {1..63})
long_fullname_policy_render=$(render_admission_policies swe-platform-system --set-string "fullnameOverride=$long_fullname_override")
assert_admission_policy_names "long fullnameOverride release" "$long_fullname_policy_render"
other_namespace_policy_render=$(render_admission_policies another-system --set-string "fullnameOverride=$long_fullname_override")
assert_admission_policy_names "long fullnameOverride release in another namespace" "$other_namespace_policy_render"
if [[ $(awk '/^  name: .*-(env-intent|portal-status)$/ { print $2 }' <<<"$long_fullname_policy_render") == \
	$(awk '/^  name: .*-(env-intent|portal-status)$/ { print $2 }' <<<"$other_namespace_policy_render") ]]; then
	echo "long fullnameOverride admission names must retain distinct namespace hashes" >&2
	exit 1
fi

for control_plane_enabled in true false; do
	for service_account_create in true false; do
		if collision_output=$(helm template rbac-test charts/swe-platform \
			--namespace rbac-system \
			--set tenancy.mode=scoped \
			--set "controlPlane.enabled=$control_plane_enabled" \
			--set "serviceAccount.create=$service_account_create" \
			--set-string serviceAccount.name=rbac-test-swe-platform-control-plane 2>&1); then
			echo "controlPlane.enabled=$control_plane_enabled serviceAccount.create=$service_account_create accepted colliding ServiceAccount identities" >&2
			exit 1
		fi
		if ! grep -Fq 'serviceAccount.name must not equal the derived control-plane ServiceAccount name' <<<"$collision_output"; then
			echo "controlPlane.enabled=$control_plane_enabled serviceAccount.create=$service_account_create failed without collision validation" >&2
			exit 1
		fi
	done
done
helm template rbac-test charts/swe-platform \
	--namespace rbac-system \
	--set tenancy.mode=scoped \
	--set controlPlane.enabled=false \
	--set serviceAccount.create=false \
	--set-string serviceAccount.name=external-operator >/dev/null

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

disabled_control_plane_render=$(helm template rbac-test charts/swe-platform \
	--namespace rbac-system \
	--set tenancy.mode=scoped \
	--set controlPlane.enabled=false \
	--set-string controlPlane.sessions.backend=invalid \
	--set-string controlPlane.image.digest=invalid \
	--set controlPlane.metrics.port=0 \
	--set controlPlane.portal.enabled=true \
	--set-string controlPlane.portal.suffix=invalid)
if awk -v RS='---' '
	/kind: (Deployment|Service|ServiceAccount|ClusterRole)\n/ &&
	/app.kubernetes.io\/component: control-plane/ { found=1 }
	END { exit !found }
' <<<"$disabled_control_plane_render"; then
	echo "controlPlane.enabled=false rendered a control-plane workload, Service, identity, or RBAC" >&2
	exit 1
fi

if output=$(helm template rbac-test charts/swe-platform --namespace rbac-system \
	--set tenancy.mode=scoped --set controlPlane.enabled=false \
	--set-string image.digest=invalid 2>&1); then
	echo "invalid operator image digest was accepted while the control plane was disabled" >&2
	exit 1
elif ! grep -Fq 'image.digest must be a sha256 digest' <<<"$output"; then
	echo "invalid operator image digest failed for an unexpected reason: $output" >&2
	exit 1
fi
if output=$(helm template rbac-test charts/swe-platform --namespace rbac-system \
	--set tenancy.mode=scoped --set controlPlane.enabled=true \
	--set-string controlPlane.sessions.backend=invalid 2>&1); then
	echo "invalid enabled control-plane backend was accepted" >&2
	exit 1
elif ! grep -Fq 'controlPlane.sessions.backend must be memory or postgres' <<<"$output"; then
	echo "invalid enabled control-plane backend failed for an unexpected reason: $output" >&2
	exit 1
fi

echo "scoped and trusted-admin Helm RBAC, disabled control-plane values, and GitHub App mounts are fenced"
