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
    expect(screen.getAllByRole('listitem').map(item => item.textContent)).toEqual(['Eventfirst', 'Event{\n  "opaque": true\n}'])
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
    expect(screen.getAllByRole('listitem').map(item => item.textContent)).toEqual([
      'Eventbefore',
      expect.stringContaining('Transcript gap'),
      'Eventafter',
    ])
  })

  it('resets stream data when the run changes', () => {
    vi.stubGlobal('EventSource', Events)
    const view = render(<Transcript namespace="n" run="first" />)
    const first = Events.current
    act(() => first.emit('transcript', '{"sequence":1,"data":"old run"}', '1'))
    view.rerender(<Transcript namespace="n" run="second" />)
    expect(first.close).toHaveBeenCalledOnce()
    expect(Events.current.url).toContain('/runs/second/transcript')
    expect(screen.queryByText('old run')).not.toBeInTheDocument()
  })

  it('safely renders unknown objects, JSON strings, and plain opaque strings', () => {
    vi.stubGlobal('EventSource', Events)
    render(<Transcript namespace="n" run="r" />)
    act(() => {
      Events.current.emit('transcript', '{"sequence":1,"source":"new","type":"thing","data":{"x":1}}', '1')
      Events.current.emit('transcript', '{"sequence":2,"data":"hello"}', '2')
      Events.current.emit('transcript', 'plain output', '3')
    })
    expect(screen.getByText(/"x": 1/)).toBeInTheDocument()
    expect(screen.getByText('hello')).toBeInTheDocument()
    expect(screen.getByText('plain output')).toBeInTheDocument()
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
    expect(screen.getAllByRole('listitem').map(item => item.textContent)).toEqual([
      'Eventbefore disconnect',
      expect.stringContaining('Transcript gap'),
      'Eventretained replay',
    ])
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
