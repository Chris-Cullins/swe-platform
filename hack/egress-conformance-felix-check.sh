#!/bin/sh
# Runs inside calico-node. Tests may point PROC_ROOT at a synthetic proc tree.
set -eu
proc_root=${PROC_ROOT:-/proc}
pids=
for process in "$proc_root"/[0-9]*; do
	[ -f "$process/cmdline" ] || continue
	args=$(tr '\0' '\n' <"$process/cmdline" 2>/dev/null || :)
	first=$(printf '%s\n' "$args" | sed -n '1p')
	case "${first##*/}" in felix) is_felix=true ;; *) is_felix=false ;; esac
	if printf '%s\n' "$args" | grep -qx -- '-felix'; then is_felix=true; fi
	if [ "${first##*/}" = calico ] && printf '%s\n' "$args" | awk 'previous=="component" && $0=="felix"{found=1} {previous=$0} END{exit !found}'; then is_felix=true; fi
	[ "$is_felix" = false ] || pids="$pids ${process##*/}"
done
set -- $pids
[ "$#" -eq 1 ] || { echo "expected exactly one felix process" >&2; exit 1; }
pid=$1
environment="$proc_root/$pid/environ"
cmdline="$proc_root/$pid/cmdline"
tr '\0' '\n' <"$environment" | grep -Eiq '^felix_defaultendpointtohostaction=' && { echo "Felix environment override" >&2; exit 1; }
tr '\0' '\n' <"$environment" | grep -Eiq '^felix_interfaceprefix=' && { echo "Felix interface-prefix environment override" >&2; exit 1; }
args=$(tr '\0' '\n' <"$cmdline")
printf '%s\n' "$args" | grep -Eiq 'default[-_]?endpoint[-_]?to[-_]?host[-_]?action' && { echo "Felix argument override" >&2; exit 1; }
printf '%s\n' "$args" | grep -Eiq 'interface[-_]?prefix' && { echo "Felix interface-prefix argument override" >&2; exit 1; }
config_file=
expect_config=false
while IFS= read -r argument; do
	if [ "$expect_config" = true ]; then
		[ -z "$config_file" ] || { echo "multiple Felix config-file options" >&2; exit 1; }
		config_file=$argument; expect_config=false; continue
	fi
	case "$argument" in
		-c|--config-file) expect_config=true ;;
		--config-file=*) [ -z "$config_file" ] || { echo "multiple Felix config-file options" >&2; exit 1; }; config_file=${argument#*=}; [ -n "$config_file" ] || { echo "empty Felix config-file argument" >&2; exit 1; } ;;
		-c?*) [ -z "$config_file" ] || { echo "multiple Felix config-file options" >&2; exit 1; }; config_file=${argument#-c} ;;
	esac
done <<EOF
$args
EOF
[ "$expect_config" = false ] || { echo "missing Felix config-file argument" >&2; exit 1; }
config_file=${config_file:-/etc/calico/felix.cfg}
case "$config_file" in /*) ;; *) echo "relative Felix config-file path" >&2; exit 1 ;; esac
[ -r "$config_file" ] || { echo "unreadable selected Felix config-file" >&2; exit 1; }
grep -Eiq '^[[:space:]]*default[-_]?endpoint[-_]?to[-_]?host[-_]?action[[:space:]]*[:=]' "$config_file" && { echo "Felix config-file override" >&2; exit 1; }
grep -Eiq '^[[:space:]]*interface[-_]?prefix[[:space:]]*[:=]' "$config_file" && { echo "Felix interface-prefix config-file override" >&2; exit 1; }
env_hostname=$(tr '\0' '\n' <"$environment" | awk -F= 'tolower($1)=="felix_felixhostname"{sub(/^[^=]*=/, ""); print}')
env_hostname_count=$(tr '\0' '\n' <"$environment" | awk -F= 'tolower($1)=="felix_felixhostname"{count++} END{print count+0}')
[ "$env_hostname_count" -le 1 ] || { echo "multiple Felix hostname environment values" >&2; exit 1; }
[ "$env_hostname_count" -eq 0 ] || [ -n "$env_hostname" ] || { echo "empty Felix hostname environment value" >&2; exit 1; }
file_hostnames=$(awk '/[:=]/{line=$0; key=line; sub(/^[[:space:]]*/, "", key); sub(/[[:space:]]*[:=].*$/, "", key); if (tolower(key)=="felixhostname") {sub(/^[^:=]*[:=][[:space:]]*/, "", line); print line}}' "$config_file")
file_hostname_count=$(awk '/[:=]/{key=$0; sub(/^[[:space:]]*/, "", key); sub(/[[:space:]]*[:=].*$/, "", key); if (tolower(key)=="felixhostname") count++} END{print count+0}' "$config_file")
[ "$file_hostname_count" -le 1 ] || { echo "ambiguous Felix hostname config-file values" >&2; exit 1; }
[ "$file_hostname_count" -eq 0 ] || [ -n "$file_hostnames" ] || { echo "empty Felix hostname config-file value" >&2; exit 1; }
file_hostname=$file_hostnames
effective_hostname=${env_hostname:-${file_hostname:-$(hostname | tr '[:upper:]' '[:lower:]')}}
[ -n "${EXPECTED_FELIX_HOSTNAME:-}" ] && [ "$effective_hostname" = "$EXPECTED_FELIX_HOSTNAME" ] || { echo "effective Felix hostname mismatch" >&2; exit 1; }
printf 'FILE:%s\n' "$config_file"; sha256sum "$config_file"
printf 'HOSTNAME:%s\n' "$effective_hostname"
printf 'PID:%s\n' "$pid"
sha256sum "$cmdline" "$environment"
