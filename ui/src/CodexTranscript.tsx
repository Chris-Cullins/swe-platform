import { memo } from 'react'
import type { TranscriptRenderItem } from './TranscriptTimeline'
import './codex.css'

// Pinned rust-v0.144.6 exec JSONL presentation, not a shared transcript schema.
// Keep recognition/provenance aligned with internal/adapters/codex/transcript.go.
const LINE_LIMIT = 256 << 10
const ENVELOPE_LIMIT = 128 << 10
const CHUNK_LIMIT = 64 << 10
const OUTPUT_LIMIT = 16 << 10
const RAW_LIMIT = 4 << 10
const EVENT_LIMIT = 32 << 10
const PART_LIMIT = 16
const encoder = new TextEncoder()
type ObjectValue = Record<string, unknown>
type Part = { label: string; text: string; raw: boolean }
export type CodexPresentation = { parts: Part[]; bytes: number; limited: boolean; location?: string; pending?: string }
type Stream = { next: number; started: boolean; tail: Uint8Array; line: Uint8Array; discard: boolean }
type Chunk = { executionId: string; stream: 'stdout' | 'stderr'; offset: number; nextOffset: number; gapBytes: number; eof: boolean; bytes: Uint8Array }
const object = (value: unknown): value is ObjectValue => value !== null && typeof value === 'object' && !Array.isArray(value)
const uint = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
const stream = (): Stream => ({ next: 0, started: false, tail: new Uint8Array(), line: new Uint8Array(), discard: false })
const join = (a: Uint8Array, b: Uint8Array) => { const result = new Uint8Array(a.length + b.length); result.set(a); result.set(b, a.length); return result }
const decode = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

function safeText(text: string, limit: number): string {
  let result = '', size = 0
  for (const char of text) {
    const code = char.codePointAt(0)!
    const value = /[\p{Cc}\p{Cf}]/u.test(char) && char !== '\n' && char !== '\t' ? `\\u${code.toString(16).padStart(4, '0')}` : char
    const bytes = encoder.encode(value).length
    if (size + bytes > limit) return result + '\n[output truncated; raw transport event retained]'
    result += value; size += bytes
  }
  return result
}

function emit(p: CodexPresentation, label: string, text: string, raw = false) {
  if (p.limited) return
  const rendered = safeText(text, raw ? RAW_LIMIT : OUTPUT_LIMIT)
  const bytes = encoder.encode(rendered).length
  if (p.parts.length === PART_LIMIT || p.bytes + bytes > EVENT_LIMIT) {
    p.limited = true
    p.parts.push({ label: 'Presentation limit', text: 'More output omitted from this presentation; raw transport event retained.', raw: true })
    return
  }
  p.bytes += bytes
  p.parts.push({ label, text: rendered, raw })
}
function raw(p: CodexPresentation, reason: string, bytes: Uint8Array | string) {
  emit(p, `Raw fallback: ${reason}`, typeof bytes === 'string' ? bytes : decode(bytes), true)
}

function parseChunk(data: unknown): Chunk | undefined {
  if (!object(data) || typeof data.executionId !== 'string' || !data.executionId || encoder.encode(data.executionId).length > 256 ||
    (data.stream !== 'stdout' && data.stream !== 'stderr') || typeof data.eof !== 'boolean' ||
    !uint(data.offset) || !uint(data.nextOffset) || !uint(data.retainedFrom) || !uint(data.producedEnd) ||
    (data.gapBytes !== undefined && !uint(data.gapBytes))) return
  const encoded = data.data ?? ''
  if (typeof encoded !== 'string' || encoded.length > Math.ceil(CHUNK_LIMIT / 3) * 4 ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)) return
  let bytes: Uint8Array
  try {
    const binary = atob(encoded)
    if (btoa(binary) !== encoded) return
    bytes = Uint8Array.from(binary, char => char.charCodeAt(0))
  } catch { return }
  const gapBytes = data.gapBytes as number | undefined ?? 0
  if (bytes.length > CHUNK_LIMIT || data.nextOffset - data.offset !== bytes.length || data.retainedFrom > data.offset ||
    gapBytes > data.offset || data.producedEnd < data.nextOffset || (data.eof && data.producedEnd !== data.nextOffset)) return
  return { executionId: data.executionId, stream: data.stream, offset: data.offset, nextOffset: data.nextOffset, gapBytes, eof: data.eof, bytes }
}

function line(p: CodexPresentation, name: string, bytes: Uint8Array) {
  if (name === 'stderr') { emit(p, 'Codex stderr (agent output)', decode(bytes)); return }
  let record: unknown
  let text: string
  try { text = new TextDecoder('utf-8', { fatal: true }).decode(bytes); record = JSON.parse(text) } catch { raw(p, 'malformed Codex JSONL', bytes); return }
  if (object(record)) {
    const item = record.item
    if (record.type === 'item.completed' && object(item) && typeof item.id === 'string') {
      if (item.type === 'agent_message' && typeof item.text === 'string') { emit(p, 'Codex agent-reported message', item.text); return }
      if (item.type === 'command_execution' && typeof item.command === 'string' && typeof item.aggregated_output === 'string' &&
        (item.exit_code === null || Number.isSafeInteger(item.exit_code)) &&
        typeof item.status === 'string' && ['completed', 'failed', 'declined', 'in_progress'].includes(item.status)) {
        emit(p, 'Codex agent-reported command (not platform verification)',
          `command: ${item.command}\nstatus: ${item.status}; exit_code: ${item.exit_code}\n${item.aggregated_output}`)
        return
      }
    }
    if ((record.type === 'thread.started' && typeof record.thread_id === 'string') || record.type === 'turn.started' ||
      (record.type === 'turn.failed' && object(record.error) && typeof record.error.message === 'string')) {
      emit(p, 'Codex agent-reported metadata', text); return
    }
    if (record.type === 'turn.completed' && object(record.usage)) {
      const usage = record.usage
      if (['input_tokens', 'cached_input_tokens', 'cache_write_input_tokens', 'output_tokens', 'reasoning_output_tokens'].every(key => Number.isSafeInteger(usage[key]))) {
        emit(p, 'Codex agent-reported metadata (usage is not accounting)', text); return
      }
      raw(p, 'malformed or unsafe-integer Codex usage metadata', text); return
    }
  }
  raw(p, 'unknown or malformed Codex record', text)
}

function flush(s: Stream, p: CodexPresentation, reason: string) {
  if (s.line.length) raw(p, `${reason}; incomplete line`, s.line)
  s.line = new Uint8Array()
  s.discard = true
}

function process(s: Stream, c: Chunk, p: CodexPresentation) {
  let bytes = c.bytes
  if (s.started && c.offset < s.next) {
    const overlap = Math.min(s.next - c.offset, bytes.length)
    const start = s.next - s.tail.length
    if (c.offset < start || bytes.subarray(0, overlap).some((byte, index) => byte !== s.tail[c.offset - start + index])) {
      flush(s, p, 'conflicting or unverifiable overlap')
      raw(p, 'conflicting or unverifiable overlap; chunk not interpreted', bytes)
      return
    }
    bytes = bytes.subarray(overlap)
    if (!bytes.length) {
      if (c.eof && c.nextOffset === s.next && s.line.length) { line(p, c.stream, s.line); s.line = new Uint8Array() }
      return
    }
  }
  if ((!s.started && c.offset !== 0) || (s.started && c.offset > s.next) || (c.gapBytes > 0 && c.offset >= s.next)) {
    flush(s, p, `${c.stream} before process gap`)
    emit(p, 'Codex process gap', `${c.stream} expected=${s.next} offset=${c.offset} gapBytes=${c.gapBytes}; resuming at next newline`)
    s.tail = new Uint8Array()
  }
  s.started = true; s.next = c.nextOffset
  s.tail = join(s.tail, bytes).slice(-LINE_LIMIT)
  let start = 0
  while (start < bytes.length) {
    const newline = bytes.indexOf(10, start)
    const part = bytes.subarray(start, newline < 0 ? bytes.length : newline)
    if (s.discard) raw(p, `${c.stream} fragment after loss or oversized line`, part)
    else if (s.line.length + part.length > LINE_LIMIT) {
      raw(p, `${c.stream} oversized line prefix`, join(s.line, part.subarray(0, RAW_LIMIT)))
      s.line = new Uint8Array(); s.discard = true
    } else s.line = join(s.line, part)
    if (newline < 0) break
    if (!s.discard) line(p, c.stream, s.line)
    s.line = new Uint8Array(); s.discard = false
    start = newline + 1
  }
  if (c.eof && s.line.length) { line(p, c.stream, s.line); s.line = new Uint8Array() }
  if (s.line.length) p.pending = `${s.line.length} buffered ${c.stream} bytes at this event; partial record, not yet interpreted. Raw transport event retained.`
  else if (!p.parts.length) p.pending = 'Process chunk accepted or verified replay; no new complete record.'
}

// Replay only the bounded retained timeline. No parser state survives eviction or
// exact Run replacement. A current execution owns two bounded lines/replay windows;
// retired identities consume a capped set, never another pair of stream buffers.
// eslint-disable-next-line react-refresh/only-export-components
export function reduceCodexTranscript(timeline: readonly TranscriptRenderItem[]): ReadonlyMap<string, CodexPresentation> {
  const result = new Map<string, CodexPresentation>()
  const seen = new Set<string>()
  let current = '', disabled = false, streams = { stdout: stream(), stderr: stream() }
  let previous: CodexPresentation | undefined
  const boundary = (reason: string, target = previous) => {
    if (target) for (const s of Object.values(streams)) flush(s, target, reason)
  }
  for (const item of timeline) {
    if (item.kind !== 'event') { boundary(item.kind === 'gap' ? 'server transcript gap' : 'client display history limit'); continue }
    const { entry } = item
    if (entry.source !== 'codex') { boundary('unsupported transcript event'); continue }
    const p: CodexPresentation = { parts: [], bytes: 0, limited: false }
    result.set(entry.id, p)
    const c = entry.type === 'codex.process-output' && item.rawBytes <= ENVELOPE_LIMIT ? parseChunk(entry.data) : undefined
    if (!c) {
      boundary('invalid or unsupported Codex envelope', p)
      raw(p, 'invalid, oversized or unsupported Codex envelope', JSON.stringify(entry.data))
      previous = p; continue
    }
    p.location = `Execution ${safeText(c.executionId, 256)} · ${c.stream} · bytes ${c.offset}-${c.nextOffset}`
    if (disabled) { raw(p, 'execution tracking limit reached', c.bytes); continue }
    if (c.executionId !== current) {
      if (seen.has(c.executionId)) { raw(p, 'retired execution replay; not reinterpreted', c.bytes); continue }
      boundary('execution changed', p)
      streams = { stdout: stream(), stderr: stream() }
      if (seen.size === 64) { disabled = true; raw(p, 'execution tracking limit reached', c.bytes); continue }
      current = c.executionId; seen.add(current)
    }
    process(streams[c.stream], c, p)
    previous = p
  }
  return result
}

export const CodexProcessOutput = memo(function CodexProcessOutput({ presentation }: { presentation: CodexPresentation }) {
  return <div className="codex-output">
    {presentation.location && <p className="hint">{presentation.location}</p>}
    {presentation.parts.map((part, index) => <section className={part.raw ? 'codex-part codex-raw' : 'codex-part'} key={index}>
      <h3>{part.label}</h3><pre>{part.text}</pre>
    </section>)}
    {presentation.pending && <p className="hint">{presentation.pending}</p>}
  </div>
})
