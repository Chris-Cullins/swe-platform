import { act, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { Transcript } from './Transcript'

class Events {
  static current: Events
  static instances: Events[] = []
  static CONNECTING = 0
  static OPEN = 1
  static CLOSED = 2
  listeners = new Map<string, EventListener>()
  readyState = Events.CONNECTING
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  close = vi.fn()
  constructor(public url: string) { Events.current = this; Events.instances.push(this) }
  addEventListener(type: string, listener: EventListener) { this.listeners.set(type, listener) }
  emit(type: string, data: string, id = '') { this.listeners.get(type)?.(new MessageEvent(type, { data, lastEventId: id })) }
  open() { this.readyState = Events.OPEN; this.onopen?.() }
  async fail(state = Events.CLOSED) { this.readyState = state; await this.onerror?.() }
}

function processData(records: unknown[]) {
  const bytes = new TextEncoder().encode(`${records.map(record => JSON.stringify(record)).join('\n')}\n`)
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return {
    executionId: 'execution-1', stream: 'stdout', offset: 0, nextOffset: bytes.length,
    retainedFrom: 0, producedEnd: bytes.length, eof: true, data: btoa(binary),
  }
}

afterEach(() => { Events.instances = []; vi.restoreAllMocks(); vi.unstubAllGlobals() })
describe('Transcript', () => {
  it('uses an unchanged encoded EventSource URL, orders events, and deduplicates IDs and sequences', () => {
    vi.stubGlobal('EventSource', Events)
    const view = render(<Transcript namespace="a/b" run="run one" />)
    act(() => {
      Events.current.emit('transcript', '{"sequence":3,"data":{"opaque":true}}', 'three')
      Events.current.emit('transcript', '{"sequence":1,"data":"first"}', 'one')
      Events.current.emit('transcript', '{"sequence":2,"data":"duplicate id"}', 'one')
      Events.current.emit('transcript', '{"sequence":1,"data":"duplicate sequence"}', 'other')
    })
    expect(Events.current.url).toBe('/api/v1/namespaces/a%2Fb/runs/run%20one/transcript')
    const items = screen.getAllByRole('listitem')
    expect(items).toHaveLength(2)
    expect(items[0]).toHaveTextContent('Eventfirst')
    expect(items[1]).toHaveTextContent('"opaque": true')
    view.unmount()
    expect(Events.current.close).toHaveBeenCalledOnce()
  })

  it('retains the actual gap payload without requiring an SSE id or sequence', () => {
    vi.stubGlobal('EventSource', Events)
    render(<Transcript namespace="n" run="r" />)
    act(() => {
      Events.current.emit('transcript', '{"sequence":2,"data":"before"}', '2')
      Events.current.emit('transcript-gap', '{"resumeAfter":"cursor-9","earliestSequence":4,"latestSequence":8}')
      Events.current.emit('transcript', '{"sequence":4,"data":"after"}', '4')
    })
    expect(screen.getByText(/History before sequence 4 is unavailable/)).toBeInTheDocument()
    expect(screen.getByText(/available through sequence 8/)).toBeInTheDocument()
    expect(screen.getByText('cursor-9')).toBeInTheDocument()
    const items = screen.getAllByRole('listitem')
    expect(items).toHaveLength(3)
    expect(items[0]).toHaveTextContent('Eventbefore')
    expect(items[1]).toHaveTextContent('Transcript gap')
    expect(items[1]).toHaveTextContent('partial records from before it are not joined')
    expect(items[2]).toHaveTextContent('Eventafter')
  })

  it('resets stream data when the run or namespace changes', () => {
    vi.stubGlobal('EventSource', Events)
    const view = render(<Transcript namespace="n" run="first" />)
    const first = Events.current
    act(() => first.emit('transcript', '{"sequence":1,"data":"old run"}', '1'))
    view.rerender(<Transcript namespace="n" run="second" />)
    expect(first.close).toHaveBeenCalledOnce()
    expect(Events.current.url).toContain('/runs/second/transcript')
    expect(screen.queryByText('old run')).not.toBeInTheDocument()
    const second = Events.current
    act(() => second.emit('transcript', '{"sequence":1,"data":"old namespace"}', '2'))
    view.rerender(<Transcript namespace="other" run="second" />)
    expect(second.close).toHaveBeenCalledOnce()
    expect(Events.current.url).toContain('/namespaces/other/')
    expect(screen.queryByText('old namespace')).not.toBeInTheDocument()
  })

  it('safely renders unknown objects, JSON strings, and plain opaque strings', () => {
    vi.stubGlobal('EventSource', Events)
    render(<Transcript namespace="n" run="r" />)
    act(() => {
      Events.current.emit('transcript', '{"sequence":1,"source":"new","type":"thing","data":{"x":1}}', '1')
      Events.current.emit('transcript', '{"sequence":2,"data":"hello"}', '2')
      Events.current.emit('transcript', 'plain output', '3')
    })
    expect(screen.getAllByText(/"x": 1/)).not.toHaveLength(0)
    expect(screen.getAllByText('hello')).not.toHaveLength(0)
    expect(screen.getAllByText('plain output')).not.toHaveLength(0)
    expect(screen.getAllByText('Raw transport event')).toHaveLength(3)
  })

  it('renders known Claude records safely and retains raw transport inspection', () => {
    vi.stubGlobal('EventSource', Events)
    render(<Transcript namespace="n" run="claude" />)
    const data = processData([
      { type: 'system', subtype: 'init', session_id: 'session-1', model: 'claude-sonnet' },
      { type: 'assistant', message: { model: 'claude-sonnet', content: [{ type: 'text', text: '<b>safe text</b>' }, { type: 'tool_use', name: 'Read', input: { file_path: 'README.md' } }] } },
      { type: 'result', subtype: 'success', is_error: false, result: 'Finished', num_turns: 1 },
    ])
    act(() => Events.current.emit('transcript', JSON.stringify({
      id: 'event-1', sequence: 1, source: 'claude-code', type: 'claude-code.process-output', data,
    }), 'event-1'))

    expect(screen.getByRole('heading', { name: 'Claude system · init' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Claude assistant' })).toBeInTheDocument()
    expect(screen.getByText('<b>safe text</b>')).toBeInTheDocument()
    expect(document.querySelector('.claude-text b')).toBeNull()
    expect(screen.getByText('Tool use: Read')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Claude result · success' })).toBeInTheDocument()
    expect(screen.getByText('Finished')).toBeInTheDocument()
    expect(screen.getByText('Raw transport event')).toBeInTheDocument()
    expect(screen.getByText(/"executionId": "execution-1"/)).toBeInTheDocument()
  })

  it('checks auth then replaces a fatally closed opened source for gap-aware replay', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({ authenticated: true, username: 'alex' })))
    vi.stubGlobal('EventSource', Events)
    render(<Transcript namespace="n" run="r" />)
    const stale = Events.current
    act(() => {
      stale.open()
      stale.emit('transcript', '{"sequence":1,"data":"before disconnect"}', 'cursor-1')
    })
    await act(async () => stale.fail())
    expect(fetch).toHaveBeenCalledWith('/api/v1/session', expect.objectContaining({ credentials: 'same-origin' }))
    expect(stale.close).toHaveBeenCalledOnce()
    expect(Events.instances).toHaveLength(2)
    const fresh = Events.current
    expect(fresh).not.toBe(stale)
    expect(fresh.url).toBe(stale.url)
    act(() => {
      fresh.open()
      fresh.emit('transcript-gap', '{"resumeAfter":"fresh-cursor","earliestSequence":3,"latestSequence":4}')
      fresh.emit('transcript', '{"sequence":3,"data":"retained replay"}', 'cursor-3')
    })
    const items = screen.getAllByRole('listitem')
    expect(items).toHaveLength(3)
    expect(items[0]).toHaveTextContent('Eventbefore disconnect')
    expect(items[1]).toHaveTextContent('Transcript gap')
    expect(items[2]).toHaveTextContent('Eventretained replay')
  })

  it('leaves CONNECTING errors to native cursor-aware reconnect', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch')
    vi.stubGlobal('EventSource', Events)
    render(<Transcript namespace="n" run="r" />)
    const stream = Events.current
    act(() => stream.open())
    await act(async () => stream.fail(Events.CONNECTING))
    expect(screen.getByRole('status')).toHaveTextContent('Reconnecting')
    expect(Events.instances).toHaveLength(1)
    expect(fetch).not.toHaveBeenCalled()
  })
})
