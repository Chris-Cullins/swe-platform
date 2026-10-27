import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spawnSync } from 'node:child_process'
import { expect, it } from 'vitest'

const source = readFileSync('../hack/e2e.sh', 'utf8')
const cleanup = source.slice(source.indexOf('\tcleanup_project_console() {'), source.indexOf('\n\ttrap cleanup_project_console EXIT'))

it('executes fixture cleanup with returned UID precondition only, no adoption/retry, safe errors and original failure preserved', () => {
  const uid = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'
  for (const scenario of ['known', 'delete-failure', 'unknown-present', 'unknown-absent', 'invalid'] as const) {
    for (const status of [0, 37]) {
      const dir = mkdtempSync(join(tmpdir(), 'project-cleanup-'))
      const fixtureDir = join(dir, 'fixture')
      const log = join(dir, 'calls')
      try {
        const prepare = spawnSync('mkdir', [fixtureDir])
        expect(prepare.status).toBe(0)
        if (scenario === 'known' || scenario === 'delete-failure') writeFileSync(join(fixtureDir, 'run-uid'), uid)
        if (scenario === 'invalid') writeFileSync(join(fixtureDir, 'run-uid'), 'invalid-sensitive-sentinel')
        const result = spawnSync('bash', ['-c', `set -euo pipefail
${cleanup}
fixture=e2e-console-project-selection
fixture_dir="$FIXTURE_DIR"
PROJECT_NAMESPACE=isolated
kubectl() {
  if [[ "$1" == delete && "$2" == --raw ]]; then
    printf 'raw %s\\n' "$3" >> "$CALLS"
    cat >> "$CALLS"
    echo sensitive-response-sentinel
    [[ "$SCENARIO" != delete-failure ]]
  elif [[ "$3" == get ]]; then
    echo get >> "$CALLS"
    [[ "$SCENARIO" != unknown-present ]] || echo run/sensitive-sentinel
  else
    echo grants >> "$CALLS"
  fi
}
trap cleanup_project_console EXIT
exit ${status}`], { encoding: 'utf8', env: { ...process.env, FIXTURE_DIR: fixtureDir, CALLS: log, SCENARIO: scenario } })
        const uncertain = !['known', 'unknown-absent'].includes(scenario)
        expect(result.status).toBe(status || (uncertain ? 1 : 0))
        expect(result.stdout).toBe('')
        expect(result.stderr).toBe(uncertain ? 'FAIL: Project console cleanup uncertain; no adoption or deletion retry attempted\n' : '')
        const calls = readFileSync(log, 'utf8')
        expect(calls.match(/grants/g)).toHaveLength(1)
        if (scenario === 'known' || scenario === 'delete-failure') {
          expect(calls.match(/raw /g)).toHaveLength(1)
          expect(calls).toContain('/apis/swe.dev/v1alpha1/namespaces/isolated/runs/e2e-console-project-selection')
          expect(calls).not.toContain('get')
          expect(JSON.parse(calls.slice(calls.indexOf('{'), calls.lastIndexOf('}') + 1))).toEqual({ apiVersion: 'v1', kind: 'DeleteOptions', preconditions: { uid } })
        } else expect(calls).not.toContain('raw ')
      } finally { rmSync(dir, { recursive: true, force: true }) }
    }
  }
})
