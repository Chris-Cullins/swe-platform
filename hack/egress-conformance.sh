#!/usr/bin/env bash
# Default-off, one-shot diagnostic runner for a disposable kind cluster created
# from hack/kind-calico-conformance.yaml. Output is human-readable diagnostics,
# not reusable evidence, attestation, or a runtime input.
set -euo pipefail

NS=swe-egress-conformance
SELECTOR='swe.dev/egress-conformance=eligible'
PYTHON='docker.io/library/python:3.12.11-alpine3.22'
CALICO_MANIFEST_URL='https://raw.githubusercontent.com/projectcalico/calico/v3.32.1/manifests/calico.yaml'
CALICO_MANIFEST_SHA256='a1df919d9721cf667accdc3e72848911b0cb25cfab7d2478ad0c996302c95744'
CALICO_NODE_IMAGE='quay.io/calico/node:v3.32.1'
CALICO_CNI_IMAGE='quay.io/calico/cni:v3.32.1'
CALICO_CONTROLLERS_IMAGE='quay.io/calico/kube-controllers:v3.32.1'
KUBERNETES_VERSION=v1.35.0
MANIFEST=
KIND_CONFIG=
CONTEXT=
FIXTURE_CREATED=false

die() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }
# CONTEXT is assigned before the first kubectl call. Keeping the pin in one
# wrapper also protects cleanup if another process changes current-context.
kubectl() { command kubectl --context "$CONTEXT" "$@"; }
cleanup() {
	[[ -z "$MANIFEST" ]] || rm -f "$MANIFEST"
	[[ -z "$KIND_CONFIG" ]] || rm -f "$KIND_CONFIG"
	[[ "$FIXTURE_CREATED" = false ]] || kubectl delete namespace "$NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
gate() {
	[[ "${EGRESS_CONFORMANCE_EXPERIMENTAL:-}" == 1 ]] || die "experimental runner disabled; set EGRESS_CONFORMANCE_EXPERIMENTAL=1"
	[[ -n "${EGRESS_CONFORMANCE_EXPECTED_KIND_CLUSTER:-}" && -n "${EGRESS_CONFORMANCE_EXPECTED_CONTEXT:-}" && -n "${EGRESS_CONFORMANCE_EXPECTED_CLUSTER_UID:-}" ]] || die "exact kind cluster, context, and kube-system UID are required"
	KIND_CLUSTER=$EGRESS_CONFORMANCE_EXPECTED_KIND_CLUSTER
	CONTEXT=$EGRESS_CONFORMANCE_EXPECTED_CONTEXT
	[[ "$CONTEXT" == "kind-$KIND_CLUSTER" ]] || die "expected context is not the exact kind cluster context"
	kind get clusters | grep -Fxq "$KIND_CLUSTER" || die "expected kind cluster does not exist"
	[[ "$(kubectl config current-context)" == "$EGRESS_CONFORMANCE_EXPECTED_CONTEXT" ]] || die "kube context mismatch"
	local active_config expected_config connection_filter
	KIND_CONFIG=$(mktemp)
	kind get kubeconfig --name "$KIND_CLUSTER" >"$KIND_CONFIG"
	active_config=$(kubectl config view --raw --flatten -o json)
	expected_config=$(KUBECONFIG="$KIND_CONFIG" kubectl config view --raw --flatten -o json)
	rm -f "$KIND_CONFIG"
	KIND_CONFIG=
	connection_filter='. as $root|.contexts[]|select(.name==$ctx)|.context.cluster as $cluster|$root.clusters[]|select(.name==$cluster)|[.cluster.server,.cluster["certificate-authority-data"]]|@tsv'
	[[ "$(jq -er --arg ctx "$CONTEXT" "$connection_filter" <<<"$active_config")" == "$(jq -er --arg ctx "$CONTEXT" "$connection_filter" <<<"$expected_config")" ]] || die "active context server or CA differs from exact kind kubeconfig"
	[[ "$(kubectl get namespace kube-system -o jsonpath='{.metadata.uid}')" == "$EGRESS_CONFORMANCE_EXPECTED_CLUSTER_UID" ]] || die "kube-system UID mismatch"
	[[ "$(kubectl version -o json | jq -er '.serverVersion.gitVersion')" == "$KUBERNETES_VERSION" ]] || die "server version is not $KUBERNETES_VERSION"
	local nodes; nodes=$(kubectl get nodes -o json)
	jq -e --arg cluster "$KIND_CLUSTER" --arg selector "swe.dev/egress-conformance" --arg v "$KUBERNETES_VERSION" '
	 .items|length==3 and ([.[].metadata.name]|sort)==([$cluster+"-control-plane",$cluster+"-worker",$cluster+"-worker2"]|sort) and
	 ([.[]|select(.metadata.labels["node-role.kubernetes.io/control-plane"]!=null)]|length)==1 and
	 ([.[]|select(.metadata.labels[$selector]=="eligible" and .metadata.labels["node-role.kubernetes.io/control-plane"]==null)]|length)==2 and
	 all(.[];.status.nodeInfo.kubeletVersion==$v)' <<<"$nodes" >/dev/null || die "kind fixture must have one control-plane and two exact eligible v1.35.0 workers"
	mapfile -t NODES < <(jq -r --arg selector "swe.dev/egress-conformance" '.items[]|select(.metadata.labels[$selector]=="eligible")|.metadata.name' <<<"$nodes" | sort)
}
require_clean_cluster() {
	! kubectl get namespace "$NS" >/dev/null 2>&1 || die "fixture namespace already exists"
	[[ "$(kubectl get customresourcedefinitions.apiextensions.k8s.io -o json | jq '.items|length')" == 0 ]] || die "clean fixture must start without CRDs"
	[[ "$(kubectl get validatingwebhookconfigurations.admissionregistration.k8s.io -o json | jq '.items|length')" == 0 ]] || die "validating admission webhooks are unsupported"
	[[ "$(kubectl get mutatingwebhookconfigurations.admissionregistration.k8s.io -o json | jq '.items|length')" == 0 ]] || die "mutating admission webhooks are unsupported"
	[[ "$(kubectl get apiservices.apiregistration.k8s.io -o json | jq '[.items[]|select(.spec.service!=null)]|length')" == 0 ]] || die "aggregated APIServices are unsupported"
	jq -e '[.items[].metadata.name]|sort==["kube-proxy"]' < <(kubectl -n kube-system get daemonsets.apps -o json) >/dev/null || die "clean fixture has unexpected daemonsets"
	pass "clean kind topology and extension surface"
}
install_calico() {
	MANIFEST=$(mktemp)
	curl -fsSL "$CALICO_MANIFEST_URL" -o "$MANIFEST"
	[[ "$(sha256sum "$MANIFEST" | cut -d' ' -f1)" == "$CALICO_MANIFEST_SHA256" ]] || die "Calico manifest content hash mismatch"
	kubectl create -f "$MANIFEST" >/dev/null
	kubectl wait --for=condition=Established crd/felixconfigurations.crd.projectcalico.org --timeout=120s >/dev/null
	cat <<'YAML' | kubectl apply -f - >/dev/null
apiVersion: crd.projectcalico.org/v1
kind: FelixConfiguration
metadata: {name: default}
spec: {defaultEndpointToHostAction: Drop, interfacePrefix: cali}
YAML
	kubectl -n kube-system set env daemonset/calico-node FELIX_DEFAULTENDPOINTTOHOSTACTION- FELIX_IPV6SUPPORT=true IP6=autodetect CALICO_IPV6POOL_CIDR=fd00:10:244::/64 >/dev/null
	kubectl -n kube-system rollout status daemonset/calico-node --timeout=240s >/dev/null
	kubectl -n kube-system rollout status deployment/calico-kube-controllers --timeout=240s >/dev/null
	kubectl wait --for=condition=Ready nodes --all --timeout=180s >/dev/null
	pass "pinned Calico manifest installed"
}
check_cluster_shape() {
	local crds ds pods controllers
	crds=$(kubectl get customresourcedefinitions.apiextensions.k8s.io -o json)
	[[ "$(jq -r '.items[].metadata.name' <<<"$crds" | sort | sha256sum | cut -d' ' -f1)" == '30065969bde7934ba7d78c31a2da2fc2a9ee522fb1c864b6baaa39de55d5f237' ]] || die "CRD inventory is not the pinned Calico fixture"
	[[ "$(kubectl get validatingwebhookconfigurations.admissionregistration.k8s.io -o json | jq '.items|length')" == 0 && "$(kubectl get mutatingwebhookconfigurations.admissionregistration.k8s.io -o json | jq '.items|length')" == 0 ]] || die "admission webhooks appeared"
	[[ "$(kubectl get apiservices.apiregistration.k8s.io -o json | jq '[.items[]|select(.spec.service!=null)]|length')" == 0 ]] || die "aggregated APIService appeared"
	ds=$(kubectl -n kube-system get daemonsets.apps -o json)
	jq -e --arg node "$CALICO_NODE_IMAGE" --arg cni "$CALICO_CNI_IMAGE" '([.items[].metadata.name]|sort)==["calico-node","kube-proxy"] and ([.items[]|select(.metadata.name=="calico-node")]|length)==1 and (.items[]|select(.metadata.name=="calico-node")) as $d|([$d.spec.template.spec.containers[]|{name,image}]==[{name:"calico-node",image:$node}]) and ([$d.spec.template.spec.initContainers[]|{name,image}]==[{name:"upgrade-ipam",image:$cni},{name:"install-cni",image:$cni},{name:"ebpf-bootstrap",image:$node}]) and ($d.spec.template.spec.ephemeralContainers//[])==[] and ([$d.spec.template.spec.containers[],$d.spec.template.spec.initContainers[]]|all((.envFrom//[])==[])) and ([$d.spec.template.spec.containers[]|.env[]?|select(.name|ascii_downcase=="felix_defaultendpointtohostaction")]|length)==0' <<<"$ds" >/dev/null || die "unexpected Calico daemonset/container shape"
	pods=$(kubectl -n kube-system get pods -l k8s-app=calico-node -o json)
	jq -e --arg node "$CALICO_NODE_IMAGE" --arg cni "$CALICO_CNI_IMAGE" --argjson count "$(kubectl get nodes -o json | jq '.items|length')" '.items|length==$count and all(.items[]; ([.spec.containers[]|{name,image}]==[{name:"calico-node",image:$node}]) and ([.spec.initContainers[]|{name,image}]==[{name:"upgrade-ipam",image:$cni},{name:"install-cni",image:$cni},{name:"ebpf-bootstrap",image:$node}]) and (.spec.ephemeralContainers//[])==[] and (.status.conditions|any(.type=="Ready" and .status=="True")) and ([.status.containerStatuses[]|select(.name=="calico-node" and .ready==true and (.imageID|test("@sha256:[0-9a-f]{64}$")))]|length)==1)' <<<"$pods" >/dev/null || die "unexpected Calico pod/image shape"
	controllers=$(kubectl -n kube-system get deployments.apps -o json)
	jq -e --arg image "$CALICO_CONTROLLERS_IMAGE" '([.items[].metadata.name]|sort)==["calico-kube-controllers","coredns"] and ([.items[]|select(.metadata.name=="calico-kube-controllers")]|length)==1 and (.items[]|select(.metadata.name=="calico-kube-controllers")) as $d|([$d.spec.template.spec.containers[]|{name,image}]==[{name:"calico-kube-controllers",image:$image}]) and ($d.spec.template.spec.initContainers//[])==[] and ($d.spec.template.spec.ephemeralContainers//[])==[] and ([$d.spec.template.spec.containers[]|.envFrom[]?]|length)==0' <<<"$controllers" >/dev/null || die "unexpected Calico kube-controller shape"
	jq -e '.items|length==1 and .[0].metadata.name=="default" and .[0].spec.defaultEndpointToHostAction=="Drop" and .[0].spec.interfacePrefix=="cali" and (.[0].metadata.annotations//{}|keys|all(startswith("config.projectcalico.org/")|not))' < <(kubectl get felixconfigurations.crd.projectcalico.org -o json) >/dev/null || die "Felix configuration override"
	jq -e '.items|length==1 and .[0].metadata.name=="default" and ((.[0].spec.controllers.node.hostEndpoint.autoCreate//"Disabled")=="Disabled") and (.[0].metadata.annotations//{}|keys|all(startswith("config.projectcalico.org/")|not))' < <(kubectl get kubecontrollersconfigurations.crd.projectcalico.org -o json) >/dev/null || die "kube-controller override"
	[[ "$(kubectl get hostendpoints.crd.projectcalico.org -o json | jq '.items|length')" == 0 ]] || die "HostEndpoints are unsupported"
	jq -e '.items|length==1 and .[0].metadata.name=="default" and ((.[0].spec.order//1000000)==1000000) and ((.[0].spec.defaultAction//"Deny")=="Deny")' < <(kubectl get tiers.crd.projectcalico.org -o json) >/dev/null || die "Tier inventory is not canonical"
	for node in "${NODES[@]}"; do
		local pod; pod=$(jq -r --arg n "$node" '.items[]|select(.spec.nodeName==$n)|.metadata.name' <<<"$pods")
		[[ -n "$pod" ]] || die "missing calico-node on $node"
		kubectl -n kube-system exec "$pod" -c calico-node -- env EXPECTED_FELIX_HOSTNAME="$node" sh -s <hack/egress-conformance-felix-check.sh >/dev/null || die "effective Felix override on $node"
	done
	pass "Calico/Felix/controller shape and policy prerequisites"
}
server_pod() { # name node role hostNetwork protocol:ports...
	local name=$1 node=$2 role=$3 host=$4 code command add='[]'; shift 4
	code=$(cat hack/egress-conformance-server.py)
	command=$(jq -cn --arg code "$code" --args '$ARGS.positional|["python3","-c",$code]+.' -- "$@")
	[[ "$role" != udp ]] || add='[NET_BIND_SERVICE]'
	cat <<YAML | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: {name: "$name", namespace: "$NS", labels: {swe.dev/egress-role: "$role"}}
spec:
  nodeName: "$node"
  hostNetwork: $host
  dnsPolicy: ClusterFirstWithHostNet
  automountServiceAccountToken: false
  enableServiceLinks: false
  tolerations: [{operator: Exists}]
  securityContext: {seccompProfile: {type: RuntimeDefault}}
  containers:
  - name: fixture
    image: $PYTHON
    command: $command
    securityContext: {allowPrivilegeEscalation: false, privileged: false, capabilities: {drop: [ALL], add: $add}, seccompProfile: {type: RuntimeDefault}}
    readinessProbe: {exec: {command: [sh, -ec, "test -f /tmp/ready"]}, initialDelaySeconds: 1, periodSeconds: 1}
YAML
}
create_fixture() {
	FIXTURE_CREATED=true
	kubectl create namespace "$NS" >/dev/null
	for node in "${NODES[@]}"; do
		local n=${node//./-}
		server_pod "control-$n" "$node" control false
		server_pod "allowed-$n" "$node" allowed false tcp:8443,8444 udp:8443
		server_pod "nonproxy-$n" "$node" nonproxy false tcp:8443
		server_pod "resident-$n" "$node" resident true tcp:18080
		server_pod "udp-$n" "$node" udp false udp:53,443
	done
	cat <<'YAML' | kubectl apply -f - >/dev/null
apiVersion: crd.projectcalico.org/v1
kind: NetworkPolicy
metadata: {name: restricted-probe, namespace: swe-egress-conformance}
spec:
  selector: "swe.dev/egress-role == 'probe'"
  types: [Egress]
  egress:
  - {action: Allow, protocol: TCP, destination: {selector: "swe.dev/egress-role == 'allowed'", ports: [8443]}}
  - {action: Allow, protocol: UDP, destination: {selector: "swe.dev/egress-role == 'allowed'", ports: [8443]}}
YAML
	for node in "${NODES[@]}"; do server_pod "probe-${node//./-}" "$node" probe false; done
	kubectl wait -n "$NS" --for=condition=Ready pod --all --timeout=180s >/dev/null
	jq -e --arg ns "$NS" '.items|length==1 and .[0].metadata.name=="restricted-probe" and .[0].metadata.namespace==$ns' < <(kubectl get networkpolicies.crd.projectcalico.org -A -o json) >/dev/null || die "fixture Calico policy inventory changed"
	[[ "$(kubectl get networkpolicies.networking.k8s.io -A -o json | jq '.items|length')" == 0 && "$(kubectl get globalnetworkpolicies.crd.projectcalico.org -o json | jq '.items|length')" == 0 && "$(kubectl get clusternetworkpolicies.policy.networking.k8s.io -o json | jq '.items|length')" == 0 ]] || die "unexpected active policy"
	[[ "$(kubectl get hostendpoints.crd.projectcalico.org -o json | jq '.items|length')" == 0 ]] || die "HostEndpoint appeared"
	jq -e --arg ns "$NS" '.metadata.name==$ns and .metadata.labels=={"kubernetes.io/metadata.name":$ns} and (.metadata.annotations//{})=={}' < <(kubectl get namespace "$NS" -o json) >/dev/null || die "Namespace-derived Profile inputs changed"
	jq -e '.metadata.name=="default" and (.metadata.labels//{})=={} and (.metadata.annotations//{})=={} and (.imagePullSecrets//[])==[]' < <(kubectl -n "$NS" get serviceaccount default -o json) >/dev/null || die "ServiceAccount-derived Profile inputs changed"
	pass "canonical isolated fixture created"
}
ip_for() { kubectl -n "$NS" get pod "$1" -o json | jq -r --arg f "$2" '.status.podIPs[].ip|select(if $f=="ipv4" then contains(":")|not else contains(":") end)' | head -1; }
attempt() { timeout 8 kubectl --context "$CONTEXT" -n "$NS" exec "$1" -- python3 - once "$2" "$3" "$4" <hack/egress-conformance-client.py; }
expect_reached() { [[ "$(attempt "$1" "$2" "$3" "$4" 2>/dev/null)" == REACHED ]] || die "$5 positive control could not reach $3:$4"; }
expect_denied() {
	local check=$1 proto=$2 probe=$3 control=$4 ip=$5 port=$6
	expect_reached "$probe" "$proto" "$ALLOWED" 8443 "$check selected-policy"
	expect_reached "$control" "$proto" "$ip" "$port" "$check target"
	[[ "$(attempt "$probe" "$proto" "$ip" "$port" 2>/dev/null)" == UNREACHED ]] || die "$check unexpectedly reached $ip:$port"
	expect_reached "$probe" "$proto" "$ALLOWED" 8443 "$check selected-policy post-check"
	expect_reached "$control" "$proto" "$ip" "$port" "$check target post-check"
	pass "$check denied ($ip:$port)"
}
validate_external() {
	python3 - "$1" "$2" <<'PY'
import ipaddress, sys
a=ipaddress.ip_address(sys.argv[1]); family=sys.argv[2]
assert a.version == (4 if family == "ipv4" else 6) and not getattr(a, "ipv4_mapped", None)
if sys.argv[1] not in ("169.254.169.254", "fd00:ec2::254"):
    assert a.is_global
PY
}
run_checks() {
	for node in "${NODES[@]}"; do
		local n=${node//./-} control="control-${node//./-}" probe="probe-${node//./-}" other other_control
		for other in "${NODES[@]}"; do [[ "$other" == "$node" ]] || break; done
		[[ "$other" != "$node" ]] || die "no independent worker available for resident-node control"
		other_control="control-${other//./-}"
		for family in ipv4 ipv6; do
			local suffix=${family^^} nonproxy udpip hostip metadata public443 public8443
			ALLOWED=$(ip_for "allowed-$n" "$family"); nonproxy=$(ip_for "nonproxy-$n" "$family"); udpip=$(ip_for "udp-$n" "$family")
			hostip=$(kubectl get node "$node" -o json | jq -r --arg f "$family" '.status.addresses[]|select(.type=="InternalIP")|.address|select(if $f=="ipv4" then contains(":")|not else contains(":") end)' | head -1)
			[[ -n "$ALLOWED" && -n "$nonproxy" && -n "$udpip" && -n "$hostip" ]] || die "$node lacks $family fixture addresses"
			expect_reached "$probe" tcp "$ALLOWED" 8443 "allowed target"; pass "$node/$family allowed target reached"
			expect_denied "$node/$family proxy-wrong-port" tcp "$probe" "$control" "$ALLOWED" 8444
			expect_denied "$node/$family non-proxy-8443" tcp "$probe" "$control" "$nonproxy" 8443
			expect_denied "$node/$family resident-node-live-port" tcp "$probe" "$other_control" "$hostip" 18080
			expect_denied "$node/$family UDP-53" udp "$probe" "$control" "$udpip" 53
			expect_denied "$node/$family UDP-443" udp "$probe" "$control" "$udpip" 443
			if [[ "$family" == ipv6 ]]; then expect_denied "$node/$family IPv6-direct" tcp "$probe" "$control" "$nonproxy" 8443; fi
			local metadata_var="EGRESS_CONFORMANCE_METADATA_${suffix}" public443_var="EGRESS_CONFORMANCE_PUBLIC_443_${suffix}" public8443_var="EGRESS_CONFORMANCE_PUBLIC_8443_${suffix}"
			metadata=${!metadata_var:-}; public443=${!public443_var:-}; public8443=${!public8443_var:-}
			[[ -n "$metadata" && -n "$public443" && -n "$public8443" ]] || die "$node/$family external positive-control literals are required"
			[[ "$metadata" == "$( [[ "$family" == ipv4 ]] && echo 169.254.169.254 || echo fd00:ec2::254 )" ]] || die "$metadata_var must be the designated metadata literal"
			validate_external "$metadata" "$family"; validate_external "$public443" "$family"; validate_external "$public8443" "$family"
			expect_denied "$node/$family metadata" tcp "$probe" "$control" "$metadata" 80
			expect_denied "$node/$family direct-public-443" tcp "$probe" "$control" "$public443" 443
			expect_denied "$node/$family direct-public-8443" tcp "$probe" "$control" "$public8443" 8443
			if [[ "$family" == ipv4 ]]; then
				local api service_ip; service_ip=$(kubectl get service kubernetes -o jsonpath='{.spec.clusterIP}')
				api=$(kubectl get endpoints kubernetes -o jsonpath='{.subsets[0].addresses[0].ip}')
				expect_denied "$node/ipv4 Kubernetes-Service" tcp "$probe" "$control" "$service_ip" 443
				expect_denied "$node/ipv4 direct-API" tcp "$probe" "$control" "$api" 6443
			fi
		done
	done
}
run() {
	trap cleanup EXIT
	gate; require_clean_cluster
	install_calico; check_cluster_shape; create_fixture; run_checks
	check_cluster_shape
	echo "PASS: all experimental diagnostic checks completed; no reusable proof was produced"
}
[[ "${1:-}" == run && $# == 1 ]] || die "usage: $0 run"
run
