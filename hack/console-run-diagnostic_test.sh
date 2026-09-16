#!/usr/bin/env bash
# Read the existing fake-adapter failure through the real authenticated console.
set -euo pipefail
: "${SWE_BROWSER_TOKEN:?browser session exchange token is required}"
BASE_URL=${1:?control-plane URL required}
NAMESPACE=${2:?namespace required}
RUN_NAME=${3:?failed Run name required}
RUN_UID=${4:?Run UID required}
SCREENSHOT=${5:-}
SESSION="swe-diagnostic-$$"
browser() { agent-browser --session "$SESSION" "$@"; }
stage() { STAGE=$1; echo "console-diagnostic: $STAGE"; }
cleanup() {
  local result=$?
  if [[ "$result" != 0 ]]; then
    echo "FAIL: console-diagnostic stage=$STAGE exit=$result" >&2
    browser eval '({path:location.pathname,login:!!document.querySelector("input[type=password]"),alert:!!document.querySelector("[role=alert]"),diagnostic:!!document.querySelector("[aria-label=\"Run status\"]")})' >&2 || true
  fi
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
stage exact-diagnostic-read
browser eval "(async () => { const r = await fetch('/api/v1/namespaces/$NAMESPACE/runs/$RUN_NAME', {headers:{'SWE-Run-UID':'$RUN_UID'}}); if (r.status !== 200) throw new Error('Exact Run HTTP '+r.status); const run = await r.json(); if (run.uid !== '$RUN_UID' || run.state !== 'Failed' || run.diagnostic?.code !== 'AdapterFailed' || run.diagnostic.message !== 'The agent reported a failure.' || run.diagnostic.nextAction !== 'Review the transcript for agent-reported details before starting another run.') throw new Error('Unexpected failure diagnostic'); return {exactDiagnostic:true}; })()"
stage failure-detail
browser open "$BASE_URL/namespaces/$NAMESPACE/runs/$RUN_NAME/overview" >/dev/null
browser set viewport 1280 900 2 >/dev/null
browser wait --text 'The agent reported a failure.' >/dev/null
browser wait --text 'Review the transcript for agent-reported details before starting another run.' >/dev/null
if [[ -n "$SCREENSHOT" ]]; then browser screenshot "$SCREENSHOT" >/dev/null; fi
stage review-transcript
browser find role link click --name 'Review transcript' --exact >/dev/null
browser wait --fn "location.pathname.endsWith('/runs/$RUN_NAME/transcript') && !!document.querySelector('[aria-label=\"Run status\"]')" >/dev/null
browser wait --text 'Task ·' >/dev/null
echo 'PASS: exact safe failure diagnostic, next action, transcript navigation, and task context'
