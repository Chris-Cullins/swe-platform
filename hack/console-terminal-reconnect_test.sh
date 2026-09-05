#!/usr/bin/env bash
# Drive the embedded console and real xterm against the existing e2e control plane.
# A shell exit closes the server-side tmux stream; no WebSocket or xterm mocks.
set -euo pipefail
: "${SWE_BROWSER_TOKEN:?browser session exchange token is required}"
BASE_URL=${1:?control-plane URL required}
NAMESPACE=${2:?namespace required}
RUN_NAME=${3:?Run name required}
RUN_UID=${4:?Run UID required}
ENV_UID=${5:?Environment UID required}
SESSION="swe-terminal-$$"
browser() { agent-browser --session "$SESSION" "$@"; }
stage() { STAGE=$1; echo "console-terminal: $STAGE"; }
cleanup() {
  local result=$?
  if [[ "$result" != 0 ]]; then
    echo "FAIL: console-terminal stage=$STAGE exit=$result" >&2
    # Never dump page text, cookies, headers, API bodies, or URL queries.
    browser eval '({path:location.pathname,login:!!document.querySelector("input[type=password]"),alert:!!document.querySelector("[role=alert]"),terminalRoots:document.querySelectorAll(".xterm").length,terminalStatus:[...document.querySelectorAll("[role=status]")].map(e=>e.textContent).filter(s=>/^Terminal: (Connecting|Connected|Disconnected|Connection error|Terminal data error)$/.test(s)),requests:performance.getEntriesByType("resource").slice(-12).map(e=>({path:new URL(e.name).pathname,status:e.responseStatus}))})' >&2 || true
  fi
  browser close >/dev/null 2>&1 || true
}
trap cleanup EXIT

stage login-page
browser open "$BASE_URL/login" >/dev/null
# Keep the credential out of command arguments and output. Only the real session
# exchange is automated here; the page then uses its ordinary HttpOnly cookie.
stage session-exchange
python3 - <<'PY' | browser eval --stdin >/dev/null
import json, os
token = json.dumps(os.environ['SWE_BROWSER_TOKEN'])
print(f"(async () => {{ if (!(await fetch('/api/v1/session', {{method:'POST',headers:{{Authorization:'Bearer '+{token}}}}})).ok) throw new Error('Session exchange failed'); }})()")
PY
unset SWE_BROWSER_TOKEN
stage cookie-session-read
browser eval '(async () => { const response = await fetch("/api/v1/session"); if (response.status !== 200) throw new Error(`Cookie session read HTTP ${response.status}`); return {sessionReadStatus:response.status}; })()'
stage run-api-read
browser eval "(async () => { const response = await fetch('/api/v1/namespaces/$NAMESPACE/runs/$RUN_NAME', {headers:{'SWE-Run-UID':'$RUN_UID'}}); if (response.status !== 200) throw new Error('Exact Run read HTTP '+response.status); const run=await response.json(); if (run.uid !== '$RUN_UID' || run.environment?.uid !== '$ENV_UID' || !run.terminalAvailable) throw new Error('Exact terminal association unavailable'); return {runReadStatus:response.status,exactAssociation:true}; })()"
stage run-page
browser open "$BASE_URL/namespaces/$NAMESPACE/runs/$RUN_NAME/overview" >/dev/null
stage run-detail-render
browser wait --text 'Task ·' >/dev/null
# Observe native sockets without replacing transport, events, or xterm. Keep the
# host reference so unmount checks detect leaked children even in detached DOM.
browser eval --stdin >/dev/null <<'JS'
window.terminalSockets = [];
window.WebSocket = class extends WebSocket {
  constructor(...args) { super(...args); window.terminalSockets.push(this); }
};
JS
stage terminal-open
browser find role link click --name Terminal --exact >/dev/null
browser wait --text 'Terminal: Connected' >/dev/null
browser eval 'window.terminalHost = document.querySelector(".terminal"); if (terminalHost.querySelectorAll(".xterm").length !== 1) throw new Error("Expected one initial xterm root");' >/dev/null

for attempt in 1 2 3; do
  stage "shell-exit-$attempt"
  # Input goes through xterm, not a direct socket send. Split the marker so shell
  # echo cannot satisfy the output assertion before the command actually runs.
  browser find role textbox click --name 'Terminal input' --exact >/dev/null
  browser keyboard type "printf 'browser-reconnect-%s\\n' $attempt; exit" >/dev/null
  browser press Enter >/dev/null
  stage "server-disconnect-$attempt"
  browser wait --text 'Terminal: Disconnected' >/dev/null
  stage "shell-output-$attempt"
  browser wait --text "browser-reconnect-$attempt" >/dev/null
  stage "reconnect-$attempt"
  browser find role button click --name 'Reconnect terminal' --exact >/dev/null
  browser wait --text 'Terminal: Connected' >/dev/null
  browser eval "if (terminalHost !== document.querySelector('.terminal') || terminalHost.querySelectorAll('.xterm').length !== 1 || document.querySelectorAll('.xterm').length !== 1) throw new Error('Reconnect $attempt leaked or replaced terminal host'); if (terminalSockets.length !== $((attempt + 1))) throw new Error('Unexpected socket count'); if (terminalSockets.slice(0,-1).some(s => s.readyState !== WebSocket.CLOSED)) throw new Error('Old socket still open'); if (terminalSockets.some(s => new URL(s.url).pathname !== '/api/v1/namespaces/$NAMESPACE/runs/$RUN_NAME/terminal/$RUN_UID/$ENV_UID')) throw new Error('Terminal identity changed');" >/dev/null
  echo "PASS: real browser reconnect $attempt: one xterm root, old socket closed, exact Run/Environment URL retained"
done
stage terminal-unmount
browser find role link click --name Overview --exact >/dev/null
browser wait --fn '!document.querySelector(".terminal")' >/dev/null
browser eval 'if (terminalHost.querySelectorAll(".xterm").length !== 0 || document.querySelectorAll(".xterm").length !== 0) throw new Error("Unmount leaked xterm DOM");' >/dev/null
echo 'PASS: real xterm unmount removes root from retained host and document'
