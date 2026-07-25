import { act, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import Terminal from './Terminal'

const mocks = vi.hoisted(() => ({
  write: vi.fn(), disposeTerminal: vi.fn(), disposeInput: vi.fn(), fit: vi.fn(), disposeFit: vi.fn(), input: undefined as ((value: string) => void) | undefined,
}))
vi.mock('@xterm/xterm', () => ({ Terminal: class { cols = 80; rows = 24; loadAddon() {} open() {} write = mocks.write; dispose = mocks.disposeTerminal; onData(fn: (value: string) => void) { mocks.input = fn; return { dispose: mocks.disposeInput } } } }))
vi.mock('@xterm/addon-fit', () => ({ FitAddon: class { fit = mocks.fit; dispose = mocks.disposeFit } }))

class Socket {
  static current: Socket
  static OPEN = 1
  readyState = 1
  binaryType = ''
  send = vi.fn()
  close = vi.fn()
  onopen: (() => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((event: MessageEvent) => void) | null = null
  constructor(public url: string) { Socket.current = this }
}
class Observer {
  static current: Observer
  observe = vi.fn(); disconnect = vi.fn()
  constructor(public callback: () => void) { Observer.current = this }
}

afterEach(() => { vi.clearAllMocks(); vi.unstubAllGlobals() })
describe('Terminal', () => {
  it('opens, resizes, handles binary traffic, and cleans up', async () => {
    vi.stubGlobal('WebSocket', Socket)
    vi.stubGlobal('ResizeObserver', Observer)
    const view = render(<Terminal namespace="team/a" run="run one" runUID="run/uid" environment="env one" environmentUID="env/uid" />)
    expect(screen.getByRole('application', { name: 'Terminal for env one' })).toBeInTheDocument()
    expect(Socket.current.url).toContain('/namespaces/team%2Fa/runs/run%20one/terminal/run%2Fuid/env%2Fuid')
    act(() => Socket.current.onopen?.())
    expect(JSON.parse(Socket.current.send.mock.calls[0][0])).toEqual({ type: 'open', cols: 80, rows: 24 })
    act(() => { mocks.input?.('abc'); Observer.current.callback() })
    expect(ArrayBuffer.isView(Socket.current.send.mock.calls[1][0])).toBe(true)
    expect(JSON.parse(Socket.current.send.mock.calls[2][0]).type).toBe('resize')
    const bytes = new Uint8Array([65, 66])
    await act(async () => Socket.current.onmessage?.(new MessageEvent('message', { data: bytes.buffer })))
    const blob = new Blob([bytes])
    Object.defineProperty(blob, 'arrayBuffer', { value: async () => bytes.buffer })
    await act(async () => Socket.current.onmessage?.(new MessageEvent('message', { data: blob })))
    expect(mocks.write).toHaveBeenCalledTimes(2)
    view.unmount()
    expect(Observer.current.disconnect).toHaveBeenCalledOnce()
    expect(Socket.current.close).toHaveBeenCalledOnce()
    expect(mocks.disposeInput).toHaveBeenCalledOnce()
    expect(mocks.disposeFit).toHaveBeenCalledOnce()
    expect(mocks.disposeTerminal).toHaveBeenCalledOnce()
  })

  it('remounts on exact identity replacement without retaining the old socket', () => {
    vi.stubGlobal('WebSocket', Socket)
    vi.stubGlobal('ResizeObserver', Observer)
    const view = render(<Terminal namespace="team" run="run" runUID="run-old" environment="env" environmentUID="env-old" />)
    const oldSocket = Socket.current
    view.rerender(<Terminal namespace="team" run="run" runUID="run-new" environment="env" environmentUID="env-new" />)
    expect(oldSocket.close).toHaveBeenCalledOnce()
    expect(Socket.current).not.toBe(oldSocket)
    expect(Socket.current.url).toContain('/runs/run/terminal/run-new/env-new')
  })
})
