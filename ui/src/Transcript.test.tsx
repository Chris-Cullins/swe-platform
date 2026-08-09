import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { SSEFramer, Transcript } from './Transcript'
import { MAX_TRANSCRIPT_ITEMS } from './TranscriptTimeline'

const reducerCalls = vi.hoisted(() => vi.fn())
vi.mock('./ClaudeTranscript', async importOriginal => {
  const actual = await importOriginal<typeof import('./ClaudeTranscript')>()
  return {
    ...actual,
    updateClaudeTranscript: (...args: Parameters<typeof actual.updateClaudeTranscript>) => {
      reducerCalls()
      return actual.updateClaudeTranscript(...args)
    },
  }
})

class Events {
  static current: Events
  static instances: Events[] = []
  controller!: ReadableStreamDefaultController<Uint8Array>
  close = vi.fn(() => this.controller.close())
  cancel = vi.fn()
  constructor(public url: string, public init: RequestInit) {
    Events.current = this
    Events.instances.push(this)
  }
  response(status = 200, statusText = '', contentType = 'text/event-stream') { return new Response(new ReadableStream({ start: controller => { this.controller = controller }, cancel: this.cancel }), { status, statusText, headers: { 'Content-Type': contentType } }) }
  emit(type: string, data: string, id = '') {
    this.controller.enqueue(new TextEncoder().encode(`${id ? `id: ${id}\n` : ''}event: ${type}\ndata: ${data}\n\n`))
  }
  fail(error = new Error('stream interrupted')) { this.controller.error(error) }
  write(data: Uint8Array) { this.controller.enqueue(data) }
}

function mockStreams() {
  return vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init = {}) => new Events(String(path), init).response())
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
  it('sends the exact Run UID and SSE headers, orders events, and deduplicates IDs and sequences', async () => {
    mockStreams()
    const view = render(<Transcript namespace="a/b" run="run one" identity="uid/Exact Value" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    act(() => {
      Events.current.emit('transcript', '{"sequence":3,"data":{"opaque":true}}', 'three')
      Events.current.emit('transcript', '{"sequence":1,"data":"first"}', 'one')
      Events.current.emit('transcript', '{"sequence":2,"data":"duplicate id"}', 'one')
      Events.current.emit('transcript', '{"sequence":1,"data":"duplicate sequence"}', 'other')
    })
    expect(Events.current.url).toBe('/api/v1/namespaces/a%2Fb/runs/run%20one/transcript')
    expect(Events.current.init).toEqual(expect.objectContaining({ credentials: 'same-origin' }))
    expect(Events.current.init.headers).toEqual(expect.objectContaining({ Accept: 'text/event-stream', 'SWE-Run-UID': 'uid/Exact Value' }))
    const items = await screen.findAllByRole('listitem')
    expect(items).toHaveLength(2)
    expect(items[0]).toHaveTextContent('Eventfirst')
    expect(items[1]).toHaveTextContent('"opaque": true')
    view.unmount()
    expect((Events.current.init.signal as AbortSignal).aborted).toBe(true)
  })

  it('retains the actual gap payload without requiring an SSE id or sequence', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="r" identity="uid-r" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    act(() => {
      Events.current.emit('transcript', '{"sequence":2,"data":"before"}', '2')
      Events.current.emit('transcript-gap', '{"resumeAfter":"cursor-9","earliestSequence":4,"latestSequence":8}')
      Events.current.emit('transcript', '{"sequence":4,"data":"after"}', '4')
    })
    expect(await screen.findByText(/History before sequence 4 is unavailable/)).toBeInTheDocument()
    expect(screen.getByText(/available through sequence 8/)).toBeInTheDocument()
    expect(screen.getByText('cursor-9')).toBeInTheDocument()
    const items = screen.getAllByRole('listitem')
    expect(items).toHaveLength(3)
    expect(items[0]).toHaveTextContent('Eventbefore')
    expect(items[1]).toHaveTextContent('Transcript gap')
    expect(items[1]).toHaveTextContent('partial records from before it are not joined')
    expect(items[2]).toHaveTextContent('Eventafter')
  })

  it('resets stream data when the run or namespace changes', async () => {
    mockStreams()
    const view = render(<Transcript namespace="n" run="first" identity="uid-first" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    const first = Events.current
    act(() => first.emit('transcript', '{"sequence":1,"data":"old run"}', '1'))
    expect(await screen.findByText('old run')).toBeInTheDocument()
    view.rerender(<Transcript namespace="n" run="second" identity="uid-second" />)
    expect((first.init.signal as AbortSignal).aborted).toBe(true)
    await waitFor(() => expect(Events.instances).toHaveLength(2))
    expect(Events.current.url).toContain('/runs/second/transcript')
    expect(screen.queryByText('old run')).not.toBeInTheDocument()
    const second = Events.current
    act(() => second.emit('transcript', '{"sequence":1,"data":"old namespace"}', '2'))
    expect(await screen.findByText('old namespace')).toBeInTheDocument()
    view.rerender(<Transcript namespace="other" run="second" identity="uid-second" />)
    expect((second.init.signal as AbortSignal).aborted).toBe(true)
    await waitFor(() => expect(Events.instances).toHaveLength(3))
    expect(Events.current.url).toContain('/namespaces/other/')
    expect(screen.queryByText('old namespace')).not.toBeInTheDocument()
  })

  it('safely renders unknown objects, JSON strings, and plain opaque strings', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="r" identity="uid-r" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    act(() => {
      Events.current.emit('transcript', '{"sequence":1,"source":"new","type":"thing","data":{"x":1}}', '1')
      Events.current.emit('transcript', '{"sequence":2,"data":"hello"}', '2')
      Events.current.emit('transcript', 'plain output', '3')
    })
    expect(await screen.findAllByText(/"x": 1/)).not.toHaveLength(0)
    expect(screen.getAllByText('hello')).not.toHaveLength(0)
    expect(screen.getAllByText('plain output')).not.toHaveLength(0)
    expect(screen.getAllByText('Raw transport event')).toHaveLength(3)
  })

  it('renders known Claude records safely and mounts raw transport JSON only when opened', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="claude" identity="uid-claude" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    const data = processData([
      { type: 'system', subtype: 'init', session_id: 'session-1', model: 'claude-sonnet' },
      { type: 'assistant', message: { model: 'claude-sonnet', content: [{ type: 'text', text: '<b>safe text</b>' }, { type: 'tool_use', name: 'Read', input: { file_path: 'README.md' } }] } },
      { type: 'result', subtype: 'success', is_error: false, result: 'Finished', num_turns: 1 },
    ])
    act(() => Events.current.emit('transcript', JSON.stringify({
      id: 'event-1', sequence: 1, source: 'claude-code', type: 'claude-code.process-output', data,
    }), 'event-1'))

    expect(await screen.findByRole('heading', { name: 'Claude system · init' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Claude assistant' })).toBeInTheDocument()
    expect(screen.getByText('<b>safe text</b>')).toBeInTheDocument()
    expect(document.querySelector('.claude-text b')).toBeNull()
    expect(screen.getByText('Tool use: Read')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Claude result · success' })).toBeInTheDocument()
    expect(screen.getByText('Finished')).toBeInTheDocument()
    expect(screen.getByText('Raw transport event')).toBeInTheDocument()
    expect(screen.queryByText(/"executionId": "execution-1"/)).not.toBeInTheDocument()
    await userEvent.click(screen.getByText('Raw transport event'))
    expect(screen.getByText(/"executionId": "execution-1"/)).toBeInTheDocument()
  })

  it('coalesces a live replay while visibly bounding history with a client-only assembly reset boundary', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="bounded" identity="uid-bounded" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    reducerCalls.mockClear()
    act(() => {
      for (let sequence = 1; sequence <= MAX_TRANSCRIPT_ITEMS + 2; sequence += 1) {
        Events.current.emit('transcript', JSON.stringify({ sequence, data: `event-${sequence}` }), String(sequence))
      }
    })
    expect(await screen.findByText('Client display history limit')).toBeInTheDocument()
    expect(reducerCalls).toHaveBeenCalledOnce()
    expect(screen.getByText(/browser dropped 2 older timeline items/)).toBeInTheDocument()
    expect(screen.getByText(/separate from any server transcript gap/)).toBeInTheDocument()
    expect(screen.queryByText('event-1')).not.toBeInTheDocument()
    expect(screen.getAllByRole('listitem')).toHaveLength(MAX_TRANSCRIPT_ITEMS + 1)
  })

  it('reconnects with the exact UID and last committed event ID for gap-aware replay', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="r" identity="uid-r-EXACT" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    const stale = Events.current
    act(() => {
      stale.emit('transcript', '{"sequence":1,"data":"before disconnect"}', 'cursor-1')
    })
    expect(await screen.findByText('before disconnect')).toBeInTheDocument()
    act(() => stale.close())
    await waitFor(() => expect(Events.instances).toHaveLength(2))
    const fresh = Events.current
    expect(fresh).not.toBe(stale)
    expect(fresh.url).toBe(stale.url)
    expect(fresh.init.headers).toEqual(expect.objectContaining({
      Accept: 'text/event-stream', 'SWE-Run-UID': 'uid-r-EXACT', 'Last-Event-ID': 'cursor-1',
    }))
    act(() => {
      fresh.emit('transcript-gap', '{"resumeAfter":"fresh-cursor","earliestSequence":3,"latestSequence":4}')
      fresh.emit('transcript', '{"sequence":3,"data":"retained replay"}', 'cursor-3')
    })
    await waitFor(() => expect(screen.getAllByRole('listitem')).toHaveLength(3))
    const items = screen.getAllByRole('listitem')
    expect(items).toHaveLength(3)
    expect(items[0]).toHaveTextContent('Eventbefore disconnect')
    expect(items[1]).toHaveTextContent('Transcript gap')
    expect(items[2]).toHaveTextContent('Eventretained replay')
  })

  it('reconnects a body-read failure with the same UID and committed cursor', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="r" identity="uid-exact" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    const interrupted = Events.current
    act(() => interrupted.emit('transcript', '{"sequence":1,"data":"committed"}', 'cursor-1'))
    expect(await screen.findByText('committed')).toBeInTheDocument()
    act(() => interrupted.fail())
    await waitFor(() => expect(Events.instances).toHaveLength(2))
    expect(Events.current.init.headers).toEqual(expect.objectContaining({
      'SWE-Run-UID': 'uid-exact', 'Last-Event-ID': 'cursor-1',
    }))
  })

  it('retries an initial transport failure with the same exact UID', async () => {
    let requests = 0
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init = {}) => {
      requests += 1
      if (requests === 1) throw new TypeError('network unavailable')
      return new Events(String(path), init).response()
    })
    render(<Transcript namespace="n" run="r" identity="uid-exact" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    expect(requests).toBe(2)
    expect(Events.current.init.headers).toEqual(expect.objectContaining({ 'SWE-Run-UID': 'uid-exact' }))
    expect(Events.current.init.headers).not.toHaveProperty('Last-Event-ID')
  })

  it('cancels the response reader after a terminal oversized event', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="r" identity="uid-exact" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    const stream = Events.current
    act(() => stream.write(new Uint8Array((8 << 20) + 1).fill(120)))
    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('Transcript event exceeds'))
    expect(stream.cancel).toHaveBeenCalledOnce()
    expect(Events.instances).toHaveLength(1)
  })

  it('accepts an exact byte-limit frame body and rejects one byte over', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="limit" identity="uid-exact" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    const exact = new Uint8Array((8 << 20) + 2).fill(32)
    exact.set(new TextEncoder().encode('data:'), 0)
    exact.set([10, 10], exact.length - 2)
    act(() => Events.current.write(exact))
    expect(screen.getByRole('status')).toHaveTextContent('Connected')
    act(() => Events.current.write(new Uint8Array((8 << 20) + 1).fill(120)))
    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('Transcript event exceeds'))
    expect(Events.instances).toHaveLength(1)
  })

  it('recognizes all nine line-ending pairs at every delimiter byte boundary', () => {
    const encoder = new TextEncoder()
    const decoder = new TextDecoder()
    const body = encoder.encode('event:transcript\ndata:{"sequence":1,"data":"combinations"}')
    const endings = [[10], [13], [13, 10]]
    for (const first of endings) for (const second of endings) {
      const delimiter = Uint8Array.from([...first, ...second])
      for (let split = 0; split <= delimiter.length; split += 1) {
        const frames: Uint8Array[] = []
        const framer = new SSEFramer()
        const consume = (frame: Uint8Array) => frames.push(frame.slice())
        framer.push(body, consume)
        framer.push(delimiter.slice(0, split), consume)
        framer.push(delimiter.slice(split), consume)
        framer.finish(consume)
        expect(frames.map(frame => decoder.decode(frame)), `${first}+${second} split ${split}`).toEqual([decoder.decode(body)])
      }
    }
  })

  it('preserves CRLF fields under deterministic randomized chunking', () => {
    const encoder = new TextEncoder()
    const decoder = new TextDecoder()
    const bodies = Array.from({ length: 24 }, (_, index) => `event:transcript\r\ndata:first-${index}\r\ndata:second-${index}\r\nid:${index}`)
    const delimiters = ['\n\n', '\n\r', '\n\r\n', '\r\r', '\r\r\n', '\r\n\n', '\r\n\r', '\r\n\r\n']
    const source = encoder.encode(bodies.map((body, index) => body + delimiters[index % delimiters.length]).join(''))
    for (let seed = 1; seed <= 32; seed += 1) {
      const frames: string[] = []
      const framer = new SSEFramer()
      let state = seed
      let offset = 0
      while (offset < source.length) {
        state = (state * 1664525 + 1013904223) >>> 0
        const end = Math.min(source.length, offset + 1 + state % 19)
        framer.push(source.slice(offset, end), frame => frames.push(decoder.decode(frame)))
        offset = end
      }
      framer.finish(frame => frames.push(decoder.decode(frame)))
      expect(frames, `seed ${seed}`).toEqual(bodies)
    }
  })

  it('parses ordinary CRLF field lines before a CRLF-delimited blank line', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="crlf-fields" identity="uid-exact" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    act(() => Events.current.write(new TextEncoder().encode('event:transcript\r\nid:1\r\ndata:{"sequence":1,"data":"crlf-fields"}\r\n\r\n')))
    expect(await screen.findByText('crlf-fields')).toBeInTheDocument()
  })

  it('handles a split UTF-8 scalar, colonless data, and many frames in one source chunk', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="bytes" identity="uid-exact" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    const encoder = new TextEncoder()
    const snowman = encoder.encode('event:transcript\ndata:{"sequence":1,"data":"snowman ☃"}\n\n')
    const scalar = snowman.indexOf(0xe2)
    act(() => {
      Events.current.write(snowman.slice(0, scalar + 1))
      Events.current.write(snowman.slice(scalar + 1))
      const frames = Array.from({ length: 100 }, (_, index) => `event:transcript\ndata\ndata:{"sequence":${index + 2},"data":"many-${index}"}\n\n`).join('')
      Events.current.write(encoder.encode(frames))
    })
    expect(await screen.findByText('snowman ☃')).toBeInTheDocument()
    await waitFor(() => expect(screen.getAllByRole('listitem')).toHaveLength(101))
    expect(screen.getByText('many-99')).toBeInTheDocument()
  })

  it('counts colonless data and its inserted newline in the retained byte budget', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="colonless-budget" identity="uid-exact" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    const payload = 'x'.repeat(2 * 1024 * 1024)
    act(() => Events.current.write(new TextEncoder().encode(`id:1\nevent:transcript\ndata\ndata:${payload}\n\n`)))
    expect(await screen.findByText(/approximately 2,097,153 raw bytes/)).toBeInTheDocument()
  })

  it('copies only a tiny unresolved suffix after processing a large source chunk', async () => {
    mockStreams()
    render(<Transcript namespace="n" run="suffix" identity="uid-exact" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    const frames = Array.from({ length: 1000 }, (_, index) => `event:transcript\ndata:{"sequence":${index + 1},"data":"bulk-${index}"}\n\n`).join('')
    act(() => {
      Events.current.write(new TextEncoder().encode(`${frames}event:trans`))
      Events.current.write(new TextEncoder().encode('cript\ndata:{"sequence":1001,"data":"suffix-complete"}\n\n'))
    })
    expect(await screen.findByText('suffix-complete')).toBeInTheDocument()
  })

  it('terminates malformed UTF-8 and unresolved EOF without reconnecting', async () => {
    mockStreams()
    const firstView = render(<Transcript namespace="n" run="utf8" identity="uid-utf8" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    act(() => Events.current.write(Uint8Array.from([100, 97, 116, 97, 58, 0xc3, 10, 10])))
    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('Malformed UTF-8'))
    expect(Events.instances).toHaveLength(1)
    firstView.unmount()

    render(<Transcript namespace="n" run="eof" identity="uid-eof" />)
    await waitFor(() => expect(Events.instances).toHaveLength(2))
    act(() => {
      Events.current.write(new TextEncoder().encode('data:unfinished'))
      Events.current.close()
    })
    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('Malformed transcript event stream at EOF'))
    await new Promise(resolve => setTimeout(resolve, 300))
    expect(Events.instances).toHaveLength(2)
  })

  it('cancels a retryable error response before reconnecting with the same UID', async () => {
    let requests = 0
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init = {}) => {
      requests += 1
      const stream = new Events(String(path), init)
      return requests === 1 ? stream.response(503, 'Unavailable') : stream.response()
    })
    render(<Transcript namespace="n" run="r" identity="uid-exact" />)
    await waitFor(() => expect(Events.instances).toHaveLength(2))
    expect(Events.instances[0].cancel).toHaveBeenCalledOnce()
    expect(Events.instances[1].init.headers).toEqual(expect.objectContaining({ 'SWE-Run-UID': 'uid-exact' }))
  })

  it('cancels a terminal wrong-content-type response', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init = {}) => new Events(String(path), init).response(200, '', 'application/json'))
    render(<Transcript namespace="n" run="r" identity="uid-exact" />)
    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('Expected transcript event stream'))
    expect(Events.instances).toHaveLength(1)
    expect(Events.instances[0].cancel).toHaveBeenCalledOnce()
  })

  it('recovers an expired reconnect cursor once without dropping the UID', async () => {
    let requests = 0
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init = {}) => {
      requests += 1
      if (requests === 2) return new Response('', { status: 410, statusText: 'Gone' })
      return new Events(String(path), init).response()
    })
    render(<Transcript namespace="n" run="r" identity="uid-exact" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    const first = Events.current
    act(() => {
      first.emit('transcript', '{"sequence":1,"data":"before retention"}', 'cursor-1')
      first.close()
    })
    await waitFor(() => expect(requests).toBe(3))
    expect(Events.instances).toHaveLength(2)
    expect(Events.current.init.headers).toEqual(expect.objectContaining({ 'SWE-Run-UID': 'uid-exact' }))
    expect(Events.current.init.headers).not.toHaveProperty('Last-Event-ID')
    act(() => Events.current.emit('transcript-gap', '{"resumeAfter":"fresh","earliestSequence":4}'))
    expect(await screen.findByText(/History before sequence 4/)).toBeInTheDocument()
  })

  it('treats a stale Run UID conflict as terminal', async () => {
    let requests = 0
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init = {}) => {
      requests += 1
      if (requests === 2) return new Response('', { status: 409, statusText: 'Conflict' })
      return new Events(String(path), init).response()
    })
    render(<Transcript namespace="n" run="r" identity="stale-uid" />)
    await waitFor(() => expect(Events.instances).toHaveLength(1))
    act(() => {
      Events.current.emit('transcript', '{"sequence":1,"data":"old"}', 'cursor-1')
      Events.current.close()
    })
    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('Conflict'))
    await new Promise(resolve => setTimeout(resolve, 300))
    expect(requests).toBe(2)
  })

})
