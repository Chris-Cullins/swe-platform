import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { CodexProcessOutput, reduceCodexTranscript } from './CodexTranscript'
import { appendTimelineItem, type TranscriptRenderItem } from './TranscriptTimeline'

const encode = (text: string) => new TextEncoder().encode(text)
const message = (text = 'hello 界🙂') => JSON.stringify({ type: 'item.completed', item: { id: 'item_0', type: 'agent_message', text } })
function chunk(id: number, bytes: Uint8Array | string, offset = 0, extra: Record<string, unknown> = {}): TranscriptRenderItem {
  const data = typeof bytes === 'string' ? encode(bytes) : bytes
  const envelope = { executionId: 'a', stream: 'stdout', offset, nextOffset: offset + data.length, retainedFrom: 0,
    producedEnd: offset + data.length, eof: false, data: btoa(Array.from(data, byte => String.fromCharCode(byte)).join('')), ...extra }
  const entry = { id: String(id), sequence: id, source: 'codex', type: 'codex.process-output', data: envelope, raw: envelope }
  return { kind: 'event', entry, position: id, rawBytes: encode(JSON.stringify({ sequence: id, source: entry.source, type: entry.type, data: envelope })).length }
}
const parts = (timeline: TranscriptRenderItem[]) => [...reduceCodexTranscript(timeline).values()].flatMap(p => p.parts)
const messages = (timeline: TranscriptRenderItem[]) => parts(timeline).filter(p => p.label === 'Codex agent-reported message').map(p => p.text)

describe('bounded pinned Codex presentation', () => {
  it('reconstructs every UTF8 split, asymmetric overlap, complete replay and replay EOF exactly once', () => {
    const bytes = encode(message())
    for (let split = 1; split < bytes.length; split++) {
      const start = Math.max(0, split - 7)
      const timeline = [chunk(1, bytes.slice(0, split)), chunk(2, bytes.slice(start), start, { eof: true }), chunk(3, bytes, 0, { eof: true })]
      expect(messages(timeline), `split ${split}`).toEqual(['hello 界🙂'])
    }
    expect(messages([chunk(1, bytes), chunk(2, bytes, 0, { eof: true })])).toEqual(['hello 界🙂'])
  })

  it('matches pinned commands, nullable exit and all five usage fields with explicit provenance', () => {
    const command = { type: 'item.completed', item: { id: 'cmd', type: 'command_execution', command: 'ls', aggregated_output: 'a.txt\n', status: 'declined', exit_code: null } }
    const records = [{ type: 'thread.started', thread_id: '67e55044-10b1-426f-9247-bb680e5fe0c8' }, { type: 'turn.started' }, command,
      { type: 'turn.completed', usage: { input_tokens: 10, cached_input_tokens: 3, cache_write_input_tokens: 4, output_tokens: 29, reasoning_output_tokens: 7 } },
      { type: 'turn.failed', error: { message: 'backend failed' } }]
    const result = parts([chunk(1, records.map(r => JSON.stringify(r)).join('\n'), 0, { eof: true })])
    expect(result).toHaveLength(5)
    expect(result.every(p => !p.raw)).toBe(true)
    expect(result[2]).toMatchObject({ label: 'Codex agent-reported command (not platform verification)', text: 'command: ls\nstatus: declined; exit_code: null\na.txt\n' })
    expect(result[3].label).toContain('usage is not accounting')
    expect(result[3].text).toContain('"cache_write_input_tokens":4')
    expect(result[4].text).toContain('backend failed')
  })

  it('keeps stdout and split UTF8 stderr independent and exposes unfinished chunks', () => {
    const stdout = encode(message('stdout only') + '\n')
    const stderr = encode('stderr 界🙂')
    const timeline = [chunk(1, stdout.slice(0, 19)), chunk(2, stderr.slice(0, -2), 0, { stream: 'stderr' }),
      chunk(3, stdout.slice(19), 19), chunk(4, stderr.slice(-2), stderr.length - 2, { stream: 'stderr', eof: true })]
    expect(messages(timeline)).toEqual(['stdout only'])
    expect(parts(timeline).filter(p => p.label === 'Codex stderr (agent output)').map(p => p.text)).toEqual(['stderr 界🙂'])
    expect(reduceCodexTranscript([timeline[0]]).get('1')?.pending).toContain('partial record')
  })

  it('falls back for missing usage fields, unsafe integers and unsupported Codex envelopes', () => {
    const usage = { input_tokens: 1, cached_input_tokens: 2, cache_write_input_tokens: 3, output_tokens: 4, reasoning_output_tokens: 5 }
    for (const key of Object.keys(usage)) {
      const incomplete: Record<string, number> = { ...usage }; delete incomplete[key]
      expect(parts([chunk(1, JSON.stringify({ type: 'turn.completed', usage: incomplete }), 0, { eof: true })])[0].raw).toBe(true)
    }
    expect(parts([chunk(1, JSON.stringify({ type: 'turn.completed', usage: { ...usage, output_tokens: Number.MAX_SAFE_INTEGER + 1 } }), 0, { eof: true })])[0].raw).toBe(true)
    const unsupported = chunk(1, 'unknown')
    if (unsupported.kind === 'event') unsupported.entry.type = 'codex.future'
    expect(parts([unsupported])[0].label).toContain('unsupported Codex envelope')
    for (const extra of [{ offset: -1 }, { nextOffset: 100 }, { executionId: '界'.repeat(86) }, { data: 'eB==' }, { producedEnd: 0 }, { retainedFrom: 1 }]) {
      expect(parts([chunk(1, 'x', 0, extra)])[0].raw).toBe(true)
    }
  })

  it('keeps a deeply nested invalid envelope from crashing raw fallback rendering', () => {
    const nested = chunk(1, '')
    if (nested.kind === 'event') nested.entry.data = JSON.parse('['.repeat(20000) + '0' + ']'.repeat(20000))
    expect(parts([nested])[0]).toMatchObject({ raw: true, text: '[Envelope preview unavailable: cannot serialize retained data]' })
    render(<CodexProcessOutput presentation={reduceCodexTranscript([nested]).get('1')!} />)
    expect(screen.getByText('[Envelope preview unavailable: cannot serialize retained data]')).toBeInTheDocument()
  })

  it.each(['server', 'client', 'process', 'execution', 'invalid', 'unsupported'])('never joins across %s boundaries', boundary => {
    const text = message('wrong join')
    const split = text.length - 5
    const timeline = [chunk(1, text.slice(0, split))]
    let offset = split, executionId = 'a', gapBytes = 0
    if (boundary === 'server') timeline.push({ kind: 'gap', gap: { resumeAfter: 'opaque' }, position: 2, rawBytes: 1 })
    if (boundary === 'client') timeline.push({ kind: 'client-gap', droppedItems: 1, droppedRawBytes: 1, position: 2, rawBytes: 0 })
    if (boundary === 'process') { offset += 3; gapBytes = 3 }
    if (boundary === 'execution') { offset = 0; executionId = 'b' }
    if (boundary === 'invalid') timeline.push(chunk(2, '', 0, { eof: 'not boolean' }))
    if (boundary === 'unsupported') timeline.push({ kind: 'event', position: 2, rawBytes: 1, entry: { id: '2', sequence: 2, source: 'pi', data: 'opaque', raw: 'opaque' } })
    timeline.push(chunk(3, text.slice(split) + '\n' + message('recovered') + '\n', offset, { executionId, gapBytes }))
    expect(messages(timeline)).toEqual(['recovered'])
    expect(parts(timeline).some(p => p.raw && p.label.includes('incomplete line'))).toBe(true)
  })

  it('rejects conflicting and unverifiable overlap without interpreting a fabricated join', () => {
    const first = encode(message('original'))
    const split = first.length - 6
    const conflict = first.slice(split - 3); conflict[0] ^= 1
    const next = encode('\n' + message('recovery') + '\n')
    const timeline = [chunk(1, first.slice(0, split)), chunk(2, conflict, split - 3), chunk(3, next, split)]
    expect(messages(timeline)).toEqual(['recovery'])
    expect(parts(timeline).some(p => p.label.includes('conflicting or unverifiable overlap'))).toBe(true)
    const many = Array.from({ length: 5 }, (_, i) => chunk(i, 'x'.repeat(64 << 10), i * (64 << 10)))
    many.push(chunk(9, 'x', 0))
    expect(parts(many).some(p => p.label.includes('conflicting or unverifiable overlap'))).toBe(true)
  })

  it('bounds oversized records, raw/unsupported envelopes, many records, text and execution identities', () => {
    const huge = Array.from({ length: 5 }, (_, i) => chunk(i, 'x'.repeat(64 << 10), i * (64 << 10)))
    huge.push(chunk(6, '\n' + message('after oversized') + '\n', 5 * (64 << 10)))
    expect(messages(huge)).toEqual(['after oversized'])
    expect(parts(huge).some(p => p.label.includes('oversized line prefix'))).toBe(true)
    const oversizedEnvelope = chunk(1, 'x'); oversizedEnvelope.rawBytes = (128 << 10) + 1
    expect(parts([oversizedEnvelope])[0].label).toContain('oversized')
    expect(parts([chunk(1, 'x'.repeat((64 << 10) + 1))])[0].raw).toBe(true)
    const outputs = [chunk(1, message('x'.repeat(20000)) + '\n'), chunk(2, 'y'.repeat(9000) + '\n', 0, { executionId: 'b' }),
      chunk(3, Array.from({ length: 30 }, (_, i) => message(String(i))).join('\n') + '\n', 0, { executionId: 'c' })]
    const presentations = [...reduceCodexTranscript(outputs).values()]
    expect(presentations[0].parts[0].text.length).toBeLessThan(16500)
    expect(presentations[0].parts[0].text).toContain('output truncated')
    expect(presentations[1].parts[0].text.length).toBeLessThan(4200)
    expect(presentations[2].parts).toHaveLength(17)
    expect(presentations[2].parts.at(-1)?.label).toBe('Presentation limit')
    const budget = [...reduceCodexTranscript([chunk(1, [message('a'.repeat(15000)), message('b'.repeat(15000)), message('c'.repeat(15000))].join('\n') + '\n')]).values()][0]
    expect(budget.bytes).toBe(30000)
    expect(budget.parts).toHaveLength(3)
    expect(budget.parts[2].label).toBe('Presentation limit')
    const executions = Array.from({ length: 70 }, (_, i) => chunk(i, message(String(i)) + '\n', 0, { executionId: String(i) }))
    expect(messages(executions)).toHaveLength(64)
    expect(parts(executions).at(-1)?.label).toContain('execution tracking limit')
    expect(messages([chunk(1, message('a') + '\n'), chunk(2, message('b') + '\n', 0, { executionId: 'b' }), chunk(3, message('retired') + '\n')])).toEqual(['a', 'b'])
  })

  it('replays only retained timeline after eviction, never retaining a dropped partial or old presentation', () => {
    let timeline: TranscriptRenderItem[] = []
    const partial = message('old').slice(0, -5)
    timeline = appendTimelineItem(timeline, chunk(0, partial))
    for (let i = 1; i <= 129; i++) timeline = appendTimelineItem(timeline, chunk(i, '', partial.length))
    timeline = appendTimelineItem(timeline, chunk(130, 'old"}}\n' + message('new') + '\n', partial.length))
    expect(timeline[0].kind).toBe('client-gap')
    expect(messages(timeline)).toEqual(['new'])
    expect(reduceCodexTranscript(timeline).size).toBeLessThanOrEqual(128)
    expect(reduceCodexTranscript([]).size).toBe(0)
  })

  it('renders stderr and malicious content as bounded text, never markup or terminal control', () => {
    const text = '<img src=x onerror=alert(1)>\x1b[31m\u202e'
    const timeline = [chunk(1, message(text) + '\n'), chunk(2, text, 0, { stream: 'stderr', eof: true })]
    const result = [...reduceCodexTranscript(timeline).values()]
    render(<>{result.map((presentation, i) => <CodexProcessOutput key={i} presentation={presentation} />)}</>)
    expect(document.querySelector('img')).toBeNull()
    expect(screen.getByText('Codex stderr (agent output)')).toBeInTheDocument()
    expect(document.body.textContent).toContain('\\u001b[31m\\u202e')
    expect(document.body.textContent).not.toContain('\x1b')
    expect(parts([chunk(1, new Uint8Array([255, 10]))])[0].label).toContain('malformed')
    expect(parts([chunk(1, '{"type":"future.event","payload":"visible"}\n')])[0].text).toContain('visible')
  })
})
