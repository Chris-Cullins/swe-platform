import { readFileSync } from 'node:fs'
import { spawnSync } from 'node:child_process'
import { runInNewContext } from 'node:vm'
import { TextDecoder } from 'node:util'
import { describe, expect, it, vi } from 'vitest'

const helper = readFileSync('../hack/console-terminal-reconnect_test.sh', 'utf8')
const observer = helper.split("<<'SOCKET_OBSERVER'\n")[1].split('\nSOCKET_OBSERVER')[0]
const failureExpression = helper.match(/browser eval '(window\.terminalSocketSnapshot[^']+)'/)![1]
type Snapshot = {
  socketObserverAvailable: boolean; socketCount: number; omittedSockets: number; saturated: boolean
  sockets: { readyState: number; eventCount: number; omittedEvents: number
    events: { kind: number; ms: number; readyState: number; code: number | null; clean: boolean | null }[] }[]
}

function fixture() {
  let now = 100
  class NativeSocket extends EventTarget {
    readyState = 0
    sent = vi.fn()
    send(data: unknown) { this.sent(data) }
    constructor(readonly url: string) { super() }
  }
  const window = { WebSocket: NativeSocket, terminalSockets: [] as NativeSocket[], terminalInput: '' }
  const context = { window, WebSocket: NativeSocket, performance: { now: () => now }, TextDecoder, ArrayBuffer }
  runInNewContext(observer, context)
  return { window, context, time: (value: number) => { now = value },
    snapshot: () => runInNewContext(failureExpression, context) as Snapshot }
}

describe('acceptance native socket diagnostics', () => {
  it('prints only on failure and preserves the original exit even when diagnostics fail', () => {
    const cleanup = helper.slice(helper.indexOf('cleanup() {'), helper.indexOf('\ntrap cleanup EXIT'))
    for (const status of [0, 37]) {
      const result = spawnSync('bash', ['-c', `set -euo pipefail
${cleanup}
browser() { if [[ "$1" == eval ]]; then printf '%s\\n' "$DIAGNOSTICS"; return 9; fi; }
STAGE=test-stage
trap cleanup EXIT
exit ${status}`], { encoding: 'utf8', env: { ...process.env, DIAGNOSTICS: JSON.stringify(fixture().snapshot()) } })
      expect(result.status).toBe(status)
      expect(result.stdout).toBe('')
      if (status === 0) expect(result.stderr).toBe('')
      else {
        expect(result.stderr).toContain('FAIL: console-terminal stage=test-stage exit=37')
        expect(result.stderr).toContain('"socketObserverAvailable":true')
      }
    }
  })

  it('captures lifecycle timing/state without altering native sends or exposing sensitive data', () => {
    const f = fixture()
    const secret = 'SENSITIVE_SOCKET_SENTINEL'
    const socket = new f.window.WebSocket(`ws://host/${secret}?token=${secret}`)
    const applicationOpen = vi.fn()
    socket.addEventListener('open', applicationOpen)
    f.time(117); socket.readyState = 1; socket.dispatchEvent(new Event('open'))
    const bytes = new TextEncoder().encode(secret)
    socket.send(bytes)
    expect(socket.sent).toHaveBeenCalledExactlyOnceWith(bytes)
    expect(f.window.terminalInput).toBe(secret)
    expect(applicationOpen).toHaveBeenCalledOnce()
    f.time(123); socket.dispatchEvent(Object.assign(new Event('error'), { message: secret, headers: secret }))
    f.time(141); socket.readyState = 3
    const reason = vi.fn(() => secret)
    const close = Object.assign(new Event('close'), { code: 4001, wasClean: false, payload: secret })
    Object.defineProperty(close, 'reason', { get: reason })
    socket.dispatchEvent(close)
    const snapshot = f.snapshot()
    expect(snapshot.sockets[0]).toEqual({ readyState: 3, eventCount: 4, omittedEvents: 0, events: [
      { kind: 0, ms: 0, readyState: 0, code: null, clean: null },
      { kind: 1, ms: 17, readyState: 1, code: null, clean: null },
      { kind: 2, ms: 23, readyState: 1, code: null, clean: null },
      { kind: 3, ms: 41, readyState: 3, code: 4001, clean: false },
    ] })
    expect(JSON.stringify(snapshot)).not.toContain(secret)
    expect(reason).not.toHaveBeenCalled()
    expect(f.window.terminalSockets).toEqual([socket])
  })

  it('distinguishes connecting, no-open failure and observer unavailable', () => {
    expect(runInNewContext(failureExpression, { window: {} })).toEqual({ socketObserverAvailable: false })
    const f = fixture()
    const socket = new f.window.WebSocket('ws://unused')
    expect(f.snapshot().sockets[0].events.map(e => e.kind)).toEqual([0])
    socket.readyState = 3
    socket.dispatchEvent(new Event('error'))
    socket.dispatchEvent(Object.assign(new Event('close'), { code: 1006, wasClean: false }))
    expect(f.snapshot().sockets[0].events.map(e => e.kind)).toEqual([0, 2, 3])
    expect(f.snapshot().sockets[0].events[2]).toMatchObject({ code: 1006, clean: false, readyState: 3 })
  })

  it('bounds retained sockets/events, saturates counters and caps time', () => {
    const f = fixture()
    for (let i = 0; i < 10; i++) new f.window.WebSocket('ws://unused')
    const socket = f.window.terminalSockets[0]
    f.time(9_000_000)
    for (let i = 0; i < 65537; i++) socket.dispatchEvent(new Event('error'))
    const snapshot = f.snapshot()
    expect(snapshot).toMatchObject({ socketCount: 10, omittedSockets: 2, saturated: true })
    expect(snapshot.sockets).toHaveLength(8)
    expect(snapshot.sockets[0]).toMatchObject({ eventCount: 65535, omittedEvents: 65522 })
    expect(snapshot.sockets[0].events).toHaveLength(16)
    expect(snapshot.sockets[0].events[15].ms).toBe(3600000)
    expect(JSON.stringify(snapshot).length).toBeLessThan(4000)
  })

  it('does not serialize nonnumeric close data or coerce arbitrary objects', () => {
    const f = fixture()
    const socket = new f.window.WebSocket('ws://unused')
    const poison = { toJSON: vi.fn(() => 'SENSITIVE_SENTINEL'), toString: vi.fn(() => 'SENSITIVE_SENTINEL') }
    socket.dispatchEvent(Object.assign(new Event('close'), { code: poison, wasClean: poison }))
    expect(f.snapshot().sockets[0].events[1]).toMatchObject({ code: null, clean: null })
    expect(JSON.stringify(f.snapshot())).not.toContain('SENSITIVE_SENTINEL')
    expect(poison.toJSON).not.toHaveBeenCalled()
    expect(poison.toString).not.toHaveBeenCalled()
  })
})
