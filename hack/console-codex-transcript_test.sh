#!/usr/bin/env bash
# Observe the existing keyless Codex fixture through the real console and SSE feed.
set -euo pipefail
: "${SWE_BROWSER_TOKEN:?browser session exchange token is required}"
BASE_URL=${1:?control-plane URL required}
NAMESPACE=${2:?namespace required}
RUN_NAME=${3:?Run name required}
RUN_UID=${4:?Run UID required}
SESSION="swe-codex-$$"
browser() { agent-browser --session "$SESSION" "$@"; }
stage() { STAGE=$1; echo "console-codex: $STAGE"; }
cleanup() {
  local result=$?
  if [[ "$result" != 0 ]]; then echo "FAIL: console-codex stage=$STAGE exit=$result" >&2; fi
  browser close >/dev/null 2>&1 || true
}
trap cleanup EXIT
stage session-exchange
browser open "$BASE_URL/login" >/dev/null
python3 - <<'PY' | browser eval --stdin >/dev/null
import json, os
token = json.dumps(os.environ['SWE_BROWSER_TOKEN'])
print(f"(async () => {{ if (!(await fetch('/api/v1/session', {{method:'POST',headers:{{Authorization:'Bearer '+{token}}}}})).ok) throw new Error('Session exchange failed'); }})()")
PY
unset SWE_BROWSER_TOKEN
stage exact-run-navigation
browser open "$BASE_URL/namespaces/$NAMESPACE/runs" >/dev/null
browser wait --fn "!!document.querySelector('a.card[href$=\"/$RUN_NAME/overview\"]')" >/dev/null
browser click "a.card[href$='/$RUN_NAME/overview']" >/dev/null
browser wait --fn "history.state?.usr?.runUID === '$RUN_UID'" >/dev/null
browser wait --text 'Task ·' >/dev/null
browser find role link click --name Transcript --exact >/dev/null
stage readable-output
browser wait --text 'Codex agent-reported message' >/dev/null
browser wait --text 'command: fake-check --fixture' >/dev/null
browser wait --text 'usage is not accounting' >/dev/null
browser eval '(() => { const text = document.querySelector(".transcript")?.textContent || ""; for (const marker of ["codex-credential-present", "status: failed; exit_code: 7", "fake-codex-check-output", "not platform verification", "fake-codex-stderr-marker"]) if (!text.includes(marker)) throw new Error("Missing expected Codex presentation"); if (document.querySelector(".raw-event pre")) throw new Error("Raw event was rendered eagerly"); })()' >/dev/null
stage raw-disclosure
browser eval '(() => { const raw = document.querySelector(".codex-output")?.parentElement?.querySelector("details.raw-event"); if (!raw) throw new Error("Missing raw disclosure"); raw.open = true; })()' >/dev/null
browser wait --fn '[...document.querySelectorAll(".raw-event pre")].some(e => e.textContent.includes("codex.process-output") && e.textContent.includes("executionId"))' >/dev/null
echo 'PASS: Codex console exact UID, message, command, provenance, stderr and lazy raw disclosure'
