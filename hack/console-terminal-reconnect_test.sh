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
    browser eval 'window.terminalSocketSnapshot ? window.terminalSocketSnapshot() : {socketObserverAvailable:false}' >&2 || true
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
browser eval --stdin >/dev/null <<'SOCKET_OBSERVER'
window.terminalSockets = [];
window.terminalInput = '';
// Diagnostics only: first 8 sockets, first 16 events each, saturating counters.
// Event kinds: 0 created, 1 open, 2 error, 3 close. Times are relative milliseconds.
const socketRecords = [];
const socketStarted = performance.now();
let socketCount = 0, omittedSockets = 0, saturated = false;
const increment = value => {
  if (value === 65535) { saturated = true; return value; }
  return value + 1;
};
const integer = (value, max) => Number.isInteger(value) && value >= 0 && value <= max ? value : null;
const observeSocket = socket => {
  socketCount = increment(socketCount);
  if (socketRecords.length === 8) { omittedSockets = increment(omittedSockets); return; }
  const record = {socket, events: [], eventCount: 0, omittedEvents: 0};
  socketRecords.push(record);
  const capture = (kind, event) => {
    record.eventCount = increment(record.eventCount);
    if (record.events.length === 16) { record.omittedEvents = increment(record.omittedEvents); return; }
    record.events.push({kind, ms: Math.min(3600000, Math.max(0, Math.round(performance.now() - socketStarted))),
      readyState: integer(socket.readyState, 3),
      code: kind === 3 ? integer(event.code, 65535) : null,
      clean: kind === 3 && typeof event.wasClean === 'boolean' ? event.wasClean : null});
  };
  capture(0);
  socket.addEventListener('open', event => capture(1, event));
  socket.addEventListener('error', event => capture(2, event));
  socket.addEventListener('close', event => capture(3, event));
};
window.terminalSocketSnapshot = () => ({socketObserverAvailable: true, socketCount, omittedSockets, saturated,
  sockets: socketRecords.map(record => ({readyState: integer(record.socket.readyState, 3),
    eventCount: record.eventCount, omittedEvents: record.omittedEvents,
    events: record.events.map(event => ({kind: event.kind, ms: event.ms, readyState: event.readyState,
      code: event.code, clean: event.clean}))}))});
window.WebSocket = class extends WebSocket {
  constructor(...args) { super(...args); window.terminalSockets.push(this); observeSocket(this); }
  send(data) {
    if (ArrayBuffer.isView(data)) window.terminalInput += new TextDecoder().decode(data);
    super.send(data);
  }
};
SOCKET_OBSERVER
stage terminal-open
browser find role link click --name Terminal --exact >/dev/null
browser wait --text 'Terminal: Connected' >/dev/null
browser eval 'window.terminalHost = document.querySelector(".terminal"); if (terminalHost.querySelectorAll(".xterm").length !== 1) throw new Error("Expected one initial xterm root");' >/dev/null

for attempt in 1 2 3; do
  stage "shell-exit-$attempt"
  # Input goes through xterm, not a direct socket send. Split the marker so shell
  # echo cannot satisfy the output assertion before the command actually runs.
  command="printf 'browser-reconnect-%s\\n' $attempt; exit"
  expected=$(printf '%s\r' "$command" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')
  browser eval "window.terminalInput=''; window.expectedTerminalInput=$expected;" >/dev/null
  # xterm's helper textarea is an offscreen input sink, not a mouse hit target.
  # Click the visible terminal as a user would and prove keyboard focus first.
  browser scrollintoview '.terminal .xterm-screen' >/dev/null
  browser click '.terminal .xterm-screen' >/dev/null
  browser wait --fn 'document.activeElement === document.querySelector(".xterm-helper-textarea")' >/dev/null
  browser keyboard type "$command" >/dev/null
  browser press Enter >/dev/null
  stage "outbound-input-$attempt"
  browser eval 'if (terminalInput !== expectedTerminalInput) throw new Error(`Fixture input mismatch: actual ${terminalInput.length} expected ${expectedTerminalInput.length} characters`); ({fixtureInputMatches:true,bytes:terminalInput.length})'
  stage "shell-output-$attempt"
  browser wait --text "browser-reconnect-$attempt" >/dev/null
  stage "server-disconnect-$attempt"
  browser wait --text 'Terminal: Disconnected' >/dev/null
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
stage changes-review
# Observe the same native response the component renders, rather than racing a
# second request against a newer capture. Do not print review bytes or headers.
browser eval 'window.changesRevision = undefined; const changesFetch = window.fetch; window.fetch = async (...args) => { const response = await changesFetch(...args); if (new URL(response.url).pathname.endsWith("/changes") && response.ok) window.changesRevision = (await response.clone().json()).revision; return response; };' >/dev/null
browser find role link click --name Changes --exact >/dev/null
browser wait --text 'Review limits:' >/dev/null
browser eval 'const review=document.querySelector("[aria-label=\"Run changes\"]"); if (!review || document.querySelector("[role=alert]") || review.textContent.includes("Comparison unavailable:")) throw new Error("Run Changes review unavailable"); if (!review.textContent.includes("Pre-existing edits are part of the baseline, not attributed to this Run.")) throw new Error("Missing baseline attribution explanation"); if (document.querySelector("nav[aria-label=\"Run sections\"] a[aria-current=page]")?.textContent !== "Changes") throw new Error("Changes tab not active");' >/dev/null
browser eval 'if (!(window.changesRevision > 0) || document.querySelector("[aria-label=\"Observation revision\"]")?.textContent !== `Revision ${window.changesRevision}`) throw new Error("Missing returned observation revision");' >/dev/null
echo 'PASS: real console Changes tab renders authenticated retained review, returned revision, and baseline attribution'
stage generated-run-name
# Inspect a fresh form without submitting or starting another live task.
browser open "$BASE_URL/namespaces/$NAMESPACE/runs/new" >/dev/null
browser wait --text 'stable create key' >/dev/null
browser eval '(() => { const input=[...document.querySelectorAll(".runform label")].find(label => label.firstChild.textContent === "Name")?.querySelector("input"); if (!input || !/^run-[a-f0-9]{32}$/.test(input.value)) throw new Error("Missing valid generated Run name"); window.generatedRunName=input.value; })()' >/dev/null
browser find role button click --name 'Create run' --exact >/dev/null
browser wait --fn '!!document.querySelector(".runform [role=alert]")' >/dev/null
browser eval '(() => { const input=document.querySelector(".runform input"); if (input?.value !== window.generatedRunName) throw new Error("Validation changed generated name"); })()' >/dev/null
echo 'PASS: New run displays a valid generated editable name and retains it after validation without creating a task'
