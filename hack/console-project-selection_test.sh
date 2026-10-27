#!/usr/bin/env bash
# Real Project discovery/create with an isolated, minimally authorized browser identity.
set -euo pipefail
: "${SWE_BROWSER_TOKEN:?browser session exchange token is required}"
BASE_URL=${1:?control-plane URL required}
NAMESPACE=${2:?namespace required}
PROJECT=${3:?Project name required}
PROJECT_UID=${4:?Project UID required}
RUN_NAME=${5:?fixed fixture Run name required}
UID_FILE=${6:?returned Run UID output file required}
SESSION="swe-project-$$"
STAGE=initialization
browser() { agent-browser --session "$SESSION" "$@"; }
stage() { STAGE=$1; echo "console-project: $STAGE"; }
retain_uid() {
  browser eval 'window.__projectFixtureUID || ""' 2>/dev/null |
    jq -er 'select(type == "string" and test("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$"))' > "$UID_FILE"
}
cleanup() {
  local result=$? session_closed=false
  trap - EXIT
  set +e
  # Capture only the UID returned by this browser's create response, never adopt a GET.
  if [[ ! -s "$UID_FILE" ]]; then retain_uid; fi
  if browser eval '(async () => { const r = await fetch("/api/v1/session", {method:"DELETE"}); if (r.status !== 204) throw new Error("Session cleanup failed"); })()' >/dev/null 2>&1; then session_closed=true; fi
  browser close >/dev/null 2>&1 || true
  if [[ "$result" != 0 || "$session_closed" != true ]]; then
    echo "FAIL: console-project stage=$STAGE exit=$result session_closed=$session_closed" >&2
    [[ "$result" != 0 ]] || result=1
  fi
  exit "$result"
}
trap cleanup EXIT
stage session-exchange
browser open "$BASE_URL/login" >/dev/null 2>&1
python3 - <<'PY' | browser eval --stdin >/dev/null 2>&1
import json, os
token = json.dumps(os.environ['SWE_BROWSER_TOKEN'])
print(f"(async () => {{ if (!(await fetch('/api/v1/session', {{method:'POST',headers:{{Authorization:'Bearer '+{token}}}}})).ok) throw new Error('Session exchange failed'); }})()")
PY
unset SWE_BROWSER_TOKEN
stage discovery
browser open "$BASE_URL/namespaces/$NAMESPACE/runs/new" >/dev/null 2>&1
browser wait --fn "!!document.querySelector('button[aria-label=\"Select $PROJECT UID $PROJECT_UID\"]')" >/dev/null 2>&1
# Unrelated collection access must remain denied; the form must still work.
browser eval "(async () => { const r = await fetch('/api/v1/namespaces/$NAMESPACE/runs?limit=1'); if (r.body) await r.body.cancel(); if (r.status !== 403) throw new Error('Unexpected Run list authorization'); })()" >/dev/null 2>&1
browser click "button[aria-label='Select $PROJECT UID $PROJECT_UID']" >/dev/null 2>&1
browser find role textbox fill "$RUN_NAME" --name Name --exact >/dev/null 2>&1
browser find label 'Prompt / task' fill 'Keyless exact Project console acceptance' >/dev/null 2>&1
browser find label 'Credential profile' fill "$RUN_NAME" >/dev/null 2>&1
stage create
# Observe the actual response without substituting requests/responses or printing bodies.
browser eval "(() => { const original = window.fetch.bind(window); window.fetch = async (...args) => { const r = await original(...args); if (args[0] === '/api/v1/namespaces/$NAMESPACE/runs' && args[1]?.method === 'POST' && r.ok) { const created = await r.clone().json(); if (created.name === '$RUN_NAME' && typeof created.uid === 'string') window.__projectFixtureUID = created.uid; } return r; }; })()" >/dev/null 2>&1
browser find role button click --name 'Create run' --exact >/dev/null 2>&1
browser wait --fn '!!window.__projectFixtureUID' >/dev/null 2>&1
retain_uid
RETURNED_UID=$(cat "$UID_FILE")
browser wait --fn "location.pathname.endsWith('/runs/$RUN_NAME/overview') && history.state?.usr?.runUID === '$RETURNED_UID'" >/dev/null 2>&1
stage exact-intent
browser eval "(async () => { const r = await fetch('/api/v1/namespaces/$NAMESPACE/runs/$RUN_NAME', {headers:{'SWE-Run-UID':'$RETURNED_UID'}}); if (r.status !== 200) throw new Error('Exact Run read failed'); const run = await r.json(); if (run.uid !== '$RETURNED_UID' || run.name !== '$RUN_NAME' || run.intent.selector.project !== '$PROJECT' || run.intent.selector.projectUID !== '$PROJECT_UID' || run.intent.credentialProfile !== '$RUN_NAME' || run.intent.selector.template || run.intent.selector.environment || run.environment) throw new Error('Exact create intent mismatch'); })()" >/dev/null 2>&1
echo 'PASS: console Project discovery, exact UID create/response/navigation with denied Run list'
