#!/usr/bin/env bash
# Sourced by acceptance. Only fixed fields leave this helper; log contents never do.
e2e_forward_diagnostics() {
	local status="$1" line="$2" pid="${PORT_FORWARD_PID:-}" log="$3"
	local configured=false alive=false readable=false sample=""
	local lost=false refused=false bind=false upgrade=false
	[[ "$status" =~ ^[0-9]{1,3}$ ]] || status=1
	[[ "$line" =~ ^[0-9]{1,9}$ ]] || line=0
	[[ -z "$pid" ]] || configured=true
	if [[ "$pid" =~ ^[1-9][0-9]*$ ]] && kill -0 "$pid" 2>/dev/null; then
		alive=true
	fi
	# Regular files only: a missing file or FIFO must never block EXIT cleanup.
	if [[ -f "$log" && -r "$log" ]]; then
		if sample=$(head -c 65536 -- "$log" 2>/dev/null | tr -d '\000'); then
			readable=true
		fi
	fi
	[[ "$sample" != *"lost connection to pod"* ]] || lost=true
	[[ "$sample" != *"connection refused"* ]] || refused=true
	[[ "$sample" != *"unable to listen on any of the requested ports"* ]] || bind=true
	[[ "$sample" != *"error upgrading connection"* ]] || upgrade=true
	printf 'e2e_forward exit=%s line=%s configured=%s alive=%s log_readable=%s scan_limit=65536 lost_connection=%s connection_refused=%s bind_failure=%s upgrade_failure=%s\n' \
		"$status" "$line" "$configured" "$alive" "$readable" "$lost" "$refused" "$bind" "$upgrade"
}

e2e_exit() {
	local status="$1" line="$2"
	trap - EXIT
	set +e
	if [[ "$status" != 0 ]]; then
		# Isolate diagnostic failure (including exit) from cleanup and original status.
		( e2e_forward_diagnostics "$status" "$line" "${CONTROL_PLANE_FORWARD_LOG:-/tmp/swe-platform-port-forward.log}" 2>/dev/null ) >&2
	fi
	cleanup || true
	exit "$status"
}
