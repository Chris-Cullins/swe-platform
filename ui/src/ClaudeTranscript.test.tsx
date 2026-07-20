import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { ClaudeProcessOutput, MAX_PARTIAL_RECORD_BYTES, reduceClaudeTranscript, updateClaudeTranscript, type ClaudeEventPresentation, type ClaudePresentationPart } from './ClaudeTranscript'
import type { TranscriptEntry, TranscriptRenderItem } from './Transcript'
import { appendTimelineItem, MAX_TRANSCRIPT_ITEMS, MAX_TRANSCRIPT_RAW_BYTES, transcriptRawBytes } from './TranscriptTimeline'

const encoder = new TextEncoder()

function base64(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary)
}

function event(id: string, sequence: number, data: unknown, source = 'claude-code', type = 'claude-code.process-output'): TranscriptRenderItem {
  const raw = { id, sequence, source, type, data }
  const entry: TranscriptEntry = { id, sequence, source, type, data, raw }
  return { kind: 'event', entry, position: sequence, rawBytes: JSON.stringify(raw).length }
}

function output(
  id: string,
  sequence: number,
  executionId: string,
  stream: 'stdout' | 'stderr',
  offset: number,
  bytes: Uint8Array,
  overrides: Partial<Record<'nextOffset' | 'gapBytes' | 'retainedFrom' | 'producedEnd' | 'eof' | 'data', unknown>> = {},
): TranscriptRenderItem {
  const nextOffset = offset + bytes.length
  return event(id, sequence, {
    executionId,
    stream,
    offset,
    nextOffset,
    retainedFrom: 0,
    producedEnd: nextOffset,
    eof: false,
    data: base64(bytes),
    ...overrides,
  })
}

function records(presentations: ReadonlyMap<string, ClaudeEventPresentation>): unknown[] {
  return parts(presentations).filter((part): part is Extract<ClaudePresentationPart, { kind: 'record' }> => part.kind === 'record').map(part => part.record)
}

function parts(presentations: ReadonlyMap<string, ClaudeEventPresentation>): ClaudePresentationPart[] {
  return [...presentations.values()].flatMap(presentation => presentation.parts)
}

function diagnostics(presentations: ReadonlyMap<string, ClaudeEventPresentation>): string[] {
  return parts(presentations).filter((part): part is Extract<ClaudePresentationPart, { kind: 'diagnostic' }> => part.kind === 'diagnostic').map(part => part.message)
}

describe('Claude transcript reducer', () => {
  it('assembles realistic system, assistant, and result records across arbitrary byte and UTF-8 boundaries', () => {
    const expected = [
      { type: 'system', subtype: 'init', session_id: 'session-1', model: 'claude-sonnet', claude_code_version: '2.1.215', cwd: '/workspace', tools: ['Read', 'Bash'], future: { retained: true } },
      { type: 'assistant', session_id: 'session-1', message: { id: 'msg-1', role: 'assistant', model: 'claude-sonnet', content: [
        { type: 'thinking', thinking: 'Check the boundary.' },
        { type: 'text', text: 'Safe <b>text</b> 🌍' },
        { type: 'tool_use', id: 'tool-1', name: 'Read', input: { file_path: 'src/main.ts' } },
      ], stop_reason: 'tool_use', usage: { output_tokens: 42 } }, parent_tool_use_id: null },
      { type: 'result', subtype: 'success', is_error: false, result: 'Completed safely', duration_ms: 1234, num_turns: 2, total_cost_usd: 0.012, structured_output: { changed: true } },
    ]
    const bytes = encoder.encode(`${expected.map(record => JSON.stringify(record)).join('\n')}\n`)
    const emoji = bytes.findIndex(byte => byte === 0xf0)
    const boundaries = [...new Set([1, 7, 19, emoji + 1, emoji + 3, 101, bytes.length])].filter(boundary => boundary > 0 && boundary <= bytes.length).sort((a, b) => a - b)
    const timeline: TranscriptRenderItem[] = []
    let start = 0
    boundaries.forEach((end, index) => {
      timeline.push(output(`chunk-${index}`, index + 1, 'execution-1', 'stdout', start, bytes.subarray(start, end), {
        producedEnd: bytes.length,
        eof: end === bytes.length,
      }))
      start = end
    })

    expect(records(reduceClaudeTranscript(timeline))).toEqual(expected)
  })

  it('does not parse a partial line and emits one record after exact and overlapping retries', () => {
    const line = encoder.encode(`${JSON.stringify({ type: 'assistant', message: { content: [{ type: 'text', text: 'only once' }] } })}\n`)
    const prefix = line.subarray(0, 17)
    const timeline = [
      output('partial', 1, 'execution', 'stdout', 0, prefix, { producedEnd: line.length }),
      output('exact-prefix-retry', 2, 'execution', 'stdout', 0, prefix, { producedEnd: line.length }),
      output('overlap-with-tail', 3, 'execution', 'stdout', 0, line, { eof: true }),
      output('exact-complete-retry', 4, 'execution', 'stdout', 0, line, { eof: true }),
    ]

    const partial = reduceClaudeTranscript(timeline.slice(0, 2))
    expect(records(partial)).toHaveLength(0)
    expect(records(reduceClaudeTranscript(timeline))).toEqual([{ type: 'assistant', message: { content: [{ type: 'text', text: 'only once' }] } }])
  })

  it('reports process retention/offset gaps and conflicting overlaps without joining corrupt records', () => {
    const line = encoder.encode(`${JSON.stringify({ type: 'assistant', message: { content: [{ type: 'text', text: 'valid' }] } })}\n`)
    const prefix = line.subarray(0, 20)
    const conflicting = prefix.slice()
    conflicting[5] ^= 1
    const gappedTail = line.subarray(prefix.length + 3)
    const presentations = reduceClaudeTranscript([
      output('prefix', 1, 'execution', 'stdout', 0, prefix, { producedEnd: line.length }),
      output('conflict', 2, 'execution', 'stdout', 0, conflicting, { producedEnd: line.length }),
      output('gap', 3, 'execution', 'stdout', prefix.length + 3, gappedTail, {
        gapBytes: 3,
        retainedFrom: prefix.length + 3,
        producedEnd: line.length,
        eof: true,
      }),
    ])

    expect(diagnostics(presentations)).toEqual(expect.arrayContaining([
      expect.stringContaining('Conflicting stdout overlap'),
      expect.stringContaining('Process retention gap: 3 stdout bytes'),
      expect.stringContaining('Discarded 20 buffered stdout bytes'),
      expect.stringContaining('Malformed Claude JSON record'),
    ]))
    expect(records(presentations)).toHaveLength(0)
  })

  it('shows malformed base64/JSON, stderr, and unknown Claude records as diagnostics', () => {
    const malformedBase64 = event('base64', 1, {
      executionId: 'bad', stream: 'stdout', offset: 0, nextOffset: 1, retainedFrom: 0, producedEnd: 1, eof: true, data: 'Y===',
    })
    const malformedJSON = encoder.encode('not JSON\n')
    const unknown = encoder.encode(`${JSON.stringify({ type: 'future-event', payload: { retained: true } })}\n`)
    const stderr = encoder.encode('warning 🌍\n')
    const split = stderr.findIndex(byte => byte === 0xf0) + 2
    const presentations = reduceClaudeTranscript([
      malformedBase64,
      output('json', 2, 'json', 'stdout', 0, malformedJSON, { eof: true }),
      output('unknown', 3, 'unknown', 'stdout', 0, unknown, { eof: true }),
      output('stderr-a', 4, 'stderr', 'stderr', 0, stderr.subarray(0, split), { producedEnd: stderr.length }),
      output('stderr-b', 5, 'stderr', 'stderr', split, stderr.subarray(split), { producedEnd: stderr.length, eof: true }),
    ])

    expect(diagnostics(presentations)).toEqual(expect.arrayContaining([
      expect.stringContaining('strict standard base64'),
      expect.stringContaining('Malformed Claude JSON record'),
      expect.stringContaining('execution changed'),
    ]))
    expect(records(presentations)).toEqual([{ type: 'future-event', payload: { retained: true } }])
    expect(parts(presentations)).toContainEqual(expect.objectContaining({ kind: 'stderr', text: 'warning 🌍' }))

    for (const presentation of presentations.values()) render(<ClaudeProcessOutput presentation={presentation} />)
    expect(screen.getByText(/Unrecognized Claude record type: future-event/)).toBeInTheDocument()
    expect(screen.getByText('warning 🌍')).toBeInTheDocument()
  })

  it('keeps execution streams independent and makes execution replacement visible', () => {
    const first = encoder.encode(`${JSON.stringify({ type: 'system', subtype: 'init', session_id: 'one' })}\n`)
    const second = encoder.encode(`${JSON.stringify({ type: 'result', subtype: 'success', is_error: false, result: 'two' })}\n`)
    const presentations = reduceClaudeTranscript([
      output('one', 1, 'execution-one', 'stdout', 0, first, { eof: true }),
      output('two', 2, 'execution-two', 'stdout', 0, second, { eof: true }),
    ])

    expect(records(presentations)).toHaveLength(2)
    expect(diagnostics(presentations)).toContain('Claude Code execution changed from execution-one to execution-two; streams are assembled independently')
  })

  it('resets partial stream assembly at an outer retained-history gap', () => {
    const line = encoder.encode(`${JSON.stringify({ type: 'assistant', message: { content: [{ type: 'text', text: 'must not join' }] } })}\n`)
    const split = 20
    const presentations = reduceClaudeTranscript([
      output('before-gap', 1, 'execution', 'stdout', 0, line.subarray(0, split), { producedEnd: line.length }),
      { kind: 'gap', gap: { resumeAfter: 'cursor', earliestSequence: 2, latestSequence: 3 }, position: 2, rawBytes: 50 },
      output('after-gap', 3, 'execution', 'stdout', split, line.subarray(split), { producedEnd: line.length, eof: true }),
    ])

    expect(records(presentations)).toHaveLength(0)
    expect(diagnostics(presentations)).toEqual(expect.arrayContaining([
      expect.stringContaining(`stdout begins at offset ${split}`),
      expect.stringContaining('Malformed Claude JSON record'),
    ]))
  })

  it('does not join partial stream bytes across a client-retention boundary', () => {
    const line = encoder.encode(`${JSON.stringify({ type: 'assistant', message: { content: [{ type: 'text', text: 'must not join' }] } })}\n`)
    const split = 20
    const presentations = reduceClaudeTranscript([
      output('before-client-gap', 1, 'execution', 'stdout', 0, line.subarray(0, split), { producedEnd: line.length }),
      { kind: 'client-gap', position: 2, droppedItems: 1, droppedRawBytes: 100, rawBytes: 0 },
      output('after-client-gap', 3, 'execution', 'stdout', split, line.subarray(split), { producedEnd: line.length, eof: true }),
    ])
    expect(records(presentations)).toHaveLength(0)
    expect(diagnostics(presentations)).toEqual(expect.arrayContaining([
      expect.stringContaining(`stdout begins at offset ${split}`),
      expect.stringContaining('Malformed Claude JSON record'),
    ]))
  })

  it('does not claim unknown source/type events', () => {
    const unknown = event('unknown', 1, { future: true }, 'future-adapter', 'future.output')
    expect(reduceClaudeTranscript([unknown])).toEqual(new Map())
  })

  it('caps oversized records, reports truncation once, and resumes after newline', () => {
    const oversized = new Uint8Array(MAX_PARTIAL_RECORD_BYTES + 1).fill(0x78)
    const resumed = encoder.encode(`\n${JSON.stringify({ type: 'result', subtype: 'success', is_error: false, result: 'resumed' })}\n`)
    const total = oversized.length + resumed.length
    const timeline: TranscriptRenderItem[] = []
    let offset = 0
    let sequence = 1
    while (offset < oversized.length) {
      const end = Math.min(offset + 64 * 1024, oversized.length)
      timeline.push(output(`oversized-${sequence}`, sequence, 'execution', 'stdout', offset, oversized.subarray(offset, end), { producedEnd: total }))
      offset = end
      sequence += 1
    }
    timeline.push(output('resumed', sequence, 'execution', 'stdout', offset, resumed, { producedEnd: total, eof: true }))

    const presentations = reduceClaudeTranscript(timeline)
    expect(diagnostics(presentations).filter(message => message.includes('exceeded'))).toHaveLength(1)
    expect(records(presentations)).toEqual([{ type: 'result', subtype: 'success', is_error: false, result: 'resumed' }])
  })

  it('rejects non-canonical base64 and unsafe integer offsets', () => {
    const invalidBase64 = event('base64-pad-bits', 1, {
      executionId: 'base64', stream: 'stdout', offset: 0, nextOffset: 1, retainedFrom: 0, producedEnd: 1, eof: true, data: 'AB==',
    })
    const unsafeOffset = event('unsafe-offset', 2, {
      executionId: 'unsafe', stream: 'stdout', offset: Number.MAX_SAFE_INTEGER + 1, nextOffset: Number.MAX_SAFE_INTEGER + 1,
      retainedFrom: 0, producedEnd: Number.MAX_SAFE_INTEGER + 1, eof: true,
    })
    expect(diagnostics(reduceClaudeTranscript([invalidBase64, unsafeOffset]))).toEqual(expect.arrayContaining([
      expect.stringContaining('canonical standard base64'),
      expect.stringContaining('offset must be a non-negative safe integer'),
    ]))
  })

  it('reports malformed UTF-8 and parses valid/malformed EOF remainders without LF', () => {
    const valid = encoder.encode(JSON.stringify({ type: 'system', subtype: 'init', session_id: 'eof' }))
    const malformed = encoder.encode('{not JSON')
    const invalidUTF8 = new Uint8Array([0xc3, 0x28, 0x0a])
    const presentations = reduceClaudeTranscript([
      output('utf8', 1, 'utf8', 'stdout', 0, invalidUTF8, { eof: true }),
      output('valid-eof', 2, 'valid-eof', 'stdout', 0, valid, { eof: true }),
      output('malformed-eof', 3, 'malformed-eof', 'stdout', 0, malformed, { eof: true }),
    ])
    expect(records(presentations)).toEqual([{ type: 'system', subtype: 'init', session_id: 'eof' }])
    expect(diagnostics(presentations)).toEqual(expect.arrayContaining([
      expect.stringContaining('invalid UTF-8'),
      expect.stringContaining('Malformed Claude JSON record'),
    ]))
  })

  it('assembles interleaved stdout/stderr independently for the same execution', () => {
    const stdout = encoder.encode(`${JSON.stringify({ type: 'assistant', message: { content: [{ type: 'text', text: 'interleaved' }] } })}\n`)
    const stderr = encoder.encode('diagnostic\n')
    const split = 13
    const presentations = reduceClaudeTranscript([
      output('stdout-a', 1, 'execution', 'stdout', 0, stdout.subarray(0, split), { producedEnd: stdout.length }),
      output('stderr', 2, 'execution', 'stderr', 0, stderr, { eof: true }),
      output('stdout-b', 3, 'execution', 'stdout', split, stdout.subarray(split), { producedEnd: stdout.length, eof: true }),
    ])
    expect(records(presentations)).toEqual([{ type: 'assistant', message: { content: [{ type: 'text', text: 'interleaved' }] } }])
    expect(parts(presentations)).toContainEqual(expect.objectContaining({ kind: 'stderr', text: 'diagnostic' }))
  })

  it('increments monotonic suffixes and replays safely on out-of-order events, gaps, and trims', () => {
    const first = output('first', 2, 'execution', 'stdout', 0, encoder.encode('{}\n'))
    let timeline = appendTimelineItem([], first)
    let reduction = updateClaudeTranscript(undefined, timeline)
    expect(reduction).toEqual(expect.objectContaining({ mode: 'replay', processedItems: 1 }))

    timeline = appendTimelineItem(timeline, output('second', 3, 'execution', 'stdout', 3, encoder.encode('{}\n')))
    reduction = updateClaudeTranscript(reduction, timeline)
    expect(reduction).toEqual(expect.objectContaining({ mode: 'append', processedItems: 1 }))

    const outOfOrder = appendTimelineItem(timeline, event('older', 1, { opaque: true }, 'other', 'event'))
    expect(updateClaudeTranscript(reduction, outOfOrder)).toEqual(expect.objectContaining({ mode: 'replay', processedItems: 3 }))

    const withGap = appendTimelineItem(timeline, { kind: 'gap', gap: { resumeAfter: 'cursor' }, position: 4, rawBytes: 10 })
    expect(updateClaudeTranscript(reduction, withGap)).toEqual(expect.objectContaining({ mode: 'replay', processedItems: 3 }))

    const full = Array.from({ length: MAX_TRANSCRIPT_ITEMS }, (_, index) => event(`generic-${index}`, index, index, 'other', 'event'))
    const fullReduction = updateClaudeTranscript(undefined, full)
    const trimmed = appendTimelineItem(full, event('trim', MAX_TRANSCRIPT_ITEMS, 'trim', 'other', 'event'))
    const trimmedReduction = updateClaudeTranscript(fullReduction, trimmed)
    expect(trimmed[0]).toEqual(expect.objectContaining({ kind: 'client-gap', droppedItems: 1 }))
    expect(trimmedReduction).toEqual(expect.objectContaining({ mode: 'replay', processedItems: MAX_TRANSCRIPT_ITEMS + 1 }))
  })

  it('keeps maximum-window trim replay bounded and responsive', () => {
    const chunk = new Uint8Array(64 * 1024).fill(0x20)
    let timeline: TranscriptRenderItem[] = []
    let reduction = updateClaudeTranscript(undefined, timeline)
    let offset = 0
    let elapsed = 0
    for (let sequence = 1; sequence <= 40; sequence += 1) {
      const next = appendTimelineItem(timeline, output(`chunk-${sequence}`, sequence, 'execution', 'stdout', offset, chunk))
      const start = performance.now()
      reduction = updateClaudeTranscript(reduction, next)
      elapsed = Math.max(elapsed, performance.now() - start)
      timeline = next
      offset += chunk.length
    }
    expect(timeline[0]).toEqual(expect.objectContaining({ kind: 'client-gap' }))
    expect(timeline.length).toBeLessThanOrEqual(MAX_TRANSCRIPT_ITEMS + 1)
    expect(transcriptRawBytes(timeline)).toBeLessThanOrEqual(MAX_TRANSCRIPT_RAW_BYTES)
    expect(elapsed).toBeLessThan(1000)
    expect(reduction.mode).toBe('replay')
  })

  it('formats decoded record JSON only after its details is opened', async () => {
    const record = { type: 'system', subtype: 'init', session_id: 'lazy', nested: { large: true } }
    const stringify = vi.spyOn(JSON, 'stringify')
    render(<ClaudeProcessOutput presentation={{ parts: [{ kind: 'record', offset: 0, record }] }} />)
    expect(stringify).not.toHaveBeenCalledWith(record, null, 2)
    await userEvent.click(screen.getByText('Claude record JSON'))
    expect(stringify).toHaveBeenCalledWith(record, null, 2)
    expect(screen.getByText(/"session_id": "lazy"/)).toBeInTheDocument()
  })
})
