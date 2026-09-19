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
stage run-filters
browser open "$BASE_URL/namespaces/$NAMESPACE/runs" >/dev/null
browser wait --fn "!!document.querySelector('a.card[href$=\"/$RUN_NAME/overview\"]')" >/dev/null
browser eval "(() => { const attention = document.querySelector('#run-attention-filter'); if (!attention || attention.checked) throw new Error('Attention must default off'); })()" >/dev/null
browser check '#run-attention-filter' >/dev/null
browser select '#run-state-filter' Failed >/dev/null
browser select '#run-agent-filter' codex >/dev/null
browser wait --fn "document.querySelector('#run-attention-filter')?.checked && !!document.querySelector('a.card[href$=\"/$RUN_NAME/overview\"]') && [...document.querySelectorAll('a.card')].every(card => card.querySelector('.pill')?.textContent === 'Failed' && [...card.querySelectorAll('dt')].find(dt => dt.textContent === 'Agent')?.nextElementSibling?.textContent === 'codex')" >/dev/null
stage run-filters-no-match
browser select '#run-state-filter' NeedsInput >/dev/null
browser wait --text 'No runs match the filters.' >/dev/null
browser eval "(() => { if (document.querySelector('a.card')) throw new Error('Unexpected filtered card'); })()" >/dev/null
stage run-filters-clear
browser find role button click --name 'Clear filters' --exact >/dev/null
browser wait --fn "!document.querySelector('#run-attention-filter')?.checked && !!document.querySelector('a.card[href$=\"/$RUN_NAME/overview\"]') && [...document.querySelectorAll('.run-filters select')].every(select => select.value === '')" >/dev/null
browser check '#run-attention-filter' >/dev/null
browser select '#run-state-filter' Failed >/dev/null
browser select '#run-agent-filter' codex >/dev/null
stage failure-detail
browser click "a.card[href$='/$RUN_NAME/overview']" >/dev/null
browser wait --fn "location.pathname.endsWith('/runs/$RUN_NAME/overview') && history.state?.usr?.runUID === '$RUN_UID'" >/dev/null
browser set viewport 1280 900 2 >/dev/null
browser wait --text 'The agent reported a failure.' >/dev/null
browser wait --text 'Review the transcript for agent-reported details before starting another run.' >/dev/null
if [[ -n "$SCREENSHOT" ]]; then browser screenshot "$SCREENSHOT" >/dev/null; fi
stage review-transcript
browser find role link click --name 'Review transcript' --exact >/dev/null
browser wait --fn "location.pathname.endsWith('/runs/$RUN_NAME/transcript') && !!document.querySelector('[aria-label=\"Run status\"]')" >/dev/null
browser wait --text 'Task ·' >/dev/null
echo 'PASS: Run filters, clear/no-match, exact UID navigation, safe failure diagnostic, next action, transcript navigation, and task context'
