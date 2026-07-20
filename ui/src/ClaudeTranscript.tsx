import { memo } from 'react'
import { LazyJSONDetails, LazyTextDetails } from './LazyDetails'
import type { TranscriptRenderItem } from './TranscriptTimeline'

export const CLAUDE_PROCESS_OUTPUT_KEY = 'claude-code:claude-code.process-output'
export const MAX_PARTIAL_RECORD_BYTES = 256 * 1024

type JSONRecord = Record<string, unknown>

export type ClaudePresentationPart =
  | { kind: 'record'; offset: number; record: unknown }
  | { kind: 'stderr'; offset: number; text: string }
  | { kind: 'diagnostic'; severity: 'info' | 'warning' | 'error'; message: string; detail?: string }

export interface ClaudeEventPresentation {
  executionId?: string
  stream?: 'stdout' | 'stderr'
  offset?: number
  nextOffset?: number
  parts: ClaudePresentationPart[]
}

interface ProcessOutput {
  executionId: string
  stream: 'stdout' | 'stderr'
  offset: number
  nextOffset: number
  gapBytes: number
  retainedFrom: number
  producedEnd: number
  eof: boolean
  bytes: Uint8Array
}

interface AcceptedChunk {
  start: number
  bytes: Uint8Array
}

interface AcceptedSegment {
  start: number
  end: number
  chunks: AcceptedChunk[]
}

interface StreamState {
  expected?: number
  segments: AcceptedSegment[]
  lineParts: Uint8Array[]
  lineLength: number
  lineOffset?: number
  discardingLine: boolean
  reportedGaps: Set<string>
}

interface RendererState {
  streams: Map<string, StreamState>
  executions: Set<string>
  currentExecution?: string
}

function isRecord(value: unknown): value is JSONRecord {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function uintField(object: JSONRecord, key: string, optional = false): number | string {
  const value = object[key]
  if (optional && value === undefined) return 0
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) return `${key} must be a non-negative safe integer`
  return value
}

function decodeBase64(value: unknown): Uint8Array | string {
  if (value === undefined) return new Uint8Array()
  if (typeof value !== 'string') return 'data must be a base64 string when present'
  if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) return 'data is not strict standard base64'
  try {
    const binary = atob(value)
    if (btoa(binary) !== value) return 'data is not canonical standard base64'
    return Uint8Array.from(binary, character => character.charCodeAt(0))
  } catch {
    return 'data is not valid standard base64'
  }
}

function parseProcessOutput(value: unknown): ProcessOutput | string {
  if (!isRecord(value)) return 'process output payload must be an object'
  if (typeof value.executionId !== 'string' || value.executionId.length === 0) return 'executionId must be a non-empty string'
  if (value.stream !== 'stdout' && value.stream !== 'stderr') return 'stream must be stdout or stderr'
  if (typeof value.eof !== 'boolean') return 'eof must be a boolean'

  const offset = uintField(value, 'offset')
  const nextOffset = uintField(value, 'nextOffset')
  const gapBytes = uintField(value, 'gapBytes', true)
  const retainedFrom = uintField(value, 'retainedFrom')
  const producedEnd = uintField(value, 'producedEnd')
  if (typeof offset === 'string') return offset
  if (typeof nextOffset === 'string') return nextOffset
  if (typeof gapBytes === 'string') return gapBytes
  if (typeof retainedFrom === 'string') return retainedFrom
  if (typeof producedEnd === 'string') return producedEnd

  const bytes = decodeBase64(value.data)
  if (typeof bytes === 'string') return bytes
  if (offset + bytes.length !== nextOffset) return `nextOffset ${nextOffset} does not equal offset ${offset} plus ${bytes.length} decoded bytes`
  if (retainedFrom > offset) return `retainedFrom ${retainedFrom} is after offset ${offset}`
  if (gapBytes > offset) return `gapBytes ${gapBytes} exceeds offset ${offset}`
  if (producedEnd < nextOffset) return `producedEnd ${producedEnd} is before nextOffset ${nextOffset}`
  if (value.eof && producedEnd !== nextOffset) return `eof is true before producedEnd ${producedEnd}`
  return {
    executionId: value.executionId,
    stream: value.stream,
    offset,
    nextOffset,
    gapBytes,
    retainedFrom,
    producedEnd,
    eof: value.eof,
    bytes,
  }
}

function newStreamState(): StreamState {
  return { segments: [], lineParts: [], lineLength: 0, discardingLine: false, reportedGaps: new Set() }
}

function cloneRendererState(state: RendererState): RendererState {
  return {
    currentExecution: state.currentExecution,
    executions: new Set(state.executions),
    streams: new Map([...state.streams].map(([key, stream]) => [key, {
      ...stream,
      segments: stream.segments.map(segment => ({ ...segment, chunks: [...segment.chunks] })),
      lineParts: [...stream.lineParts],
      reportedGaps: new Set(stream.reportedGaps),
    }])),
  }
}

function compareAccepted(state: StreamState, start: number, bytes: Uint8Array): { status: 'equal' | 'unknown' } | { status: 'conflict'; offset: number } {
  let position = start
  let sourceIndex = 0
  while (sourceIndex < bytes.length) {
    const segment = state.segments.find(candidate => candidate.start <= position && position < candidate.end)
    if (!segment) return { status: 'unknown' }
    const chunk = segment.chunks.find(candidate => candidate.start <= position && position < candidate.start + candidate.bytes.length)
    if (!chunk) return { status: 'unknown' }
    const chunkIndex = position - chunk.start
    const length = Math.min(bytes.length - sourceIndex, chunk.bytes.length - chunkIndex)
    for (let index = 0; index < length; index += 1) {
      if (bytes[sourceIndex + index] !== chunk.bytes[chunkIndex + index]) return { status: 'conflict', offset: position + index }
    }
    position += length
    sourceIndex += length
  }
  return { status: 'equal' }
}

function acceptBytes(state: StreamState, start: number, bytes: Uint8Array) {
  if (bytes.length === 0) return
  let segment = state.segments.at(-1)
  if (!segment || segment.end !== start) {
    segment = { start, end: start, chunks: [] }
    state.segments.push(segment)
  }
  segment.chunks.push({ start, bytes })
  segment.end += bytes.length
}

function combinedLine(state: StreamState): Uint8Array {
  if (state.lineParts.length === 1) return state.lineParts[0]
  const line = new Uint8Array(state.lineLength)
  let offset = 0
  for (const part of state.lineParts) {
    line.set(part, offset)
    offset += part.length
  }
  return line
}

function clearLine(state: StreamState) {
  state.lineParts = []
  state.lineLength = 0
  state.lineOffset = undefined
}

function appendLinePart(state: StreamState, stream: 'stdout' | 'stderr', offset: number, part: Uint8Array, presentation: ClaudeEventPresentation) {
  if (part.length === 0 || state.discardingLine) return
  if (state.lineLength + part.length > MAX_PARTIAL_RECORD_BYTES) {
    presentation.parts.push({
      kind: 'diagnostic',
      severity: 'warning',
      message: `${stream} record beginning at byte ${state.lineOffset ?? offset} exceeded ${MAX_PARTIAL_RECORD_BYTES} buffered bytes; discarded until newline or EOF`,
    })
    clearLine(state)
    state.discardingLine = true
    return
  }
  if (state.lineLength === 0) state.lineOffset = offset
  state.lineParts.push(part)
  state.lineLength += part.length
}

function finishLine(state: StreamState, stream: 'stdout' | 'stderr', presentation: ClaudeEventPresentation) {
  if (state.lineLength === 0) {
    clearLine(state)
    return
  }
  const offset = state.lineOffset ?? 0
  let text: string
  try {
    text = new TextDecoder('utf-8', { fatal: true }).decode(combinedLine(state))
  } catch {
    presentation.parts.push({ kind: 'diagnostic', severity: 'error', message: `${stream} contains invalid UTF-8 in the record beginning at byte ${offset}` })
    clearLine(state)
    return
  }
  clearLine(state)
  if (stream === 'stderr') {
    presentation.parts.push({ kind: 'stderr', offset, text: text.replace(/\r$/, '') })
    return
  }
  try {
    presentation.parts.push({ kind: 'record', offset, record: JSON.parse(text) as unknown })
  } catch {
    presentation.parts.push({ kind: 'diagnostic', severity: 'error', message: `Malformed Claude JSON record at stdout byte ${offset}`, detail: text })
  }
}

function processBytes(state: StreamState, stream: 'stdout' | 'stderr', start: number, bytes: Uint8Array, presentation: ClaudeEventPresentation) {
  let partStart = 0
  for (let index = 0; index < bytes.length; index += 1) {
    if (bytes[index] !== 0x0a) continue
    const part = bytes.subarray(partStart, index)
    appendLinePart(state, stream, start + partStart, part, presentation)
    if (state.discardingLine) {
      clearLine(state)
      state.discardingLine = false
    } else {
      finishLine(state, stream, presentation)
    }
    partStart = index + 1
  }
  const remainder = bytes.subarray(partStart)
  appendLinePart(state, stream, start + partStart, remainder, presentation)
}

function discardPartialLine(state: StreamState, stream: 'stdout' | 'stderr', presentation: ClaudeEventPresentation, reason: string) {
  if (state.discardingLine) {
    state.discardingLine = false
    clearLine(state)
    return
  }
  if (state.lineLength === 0) return
  const offset = state.lineOffset ?? 0
  presentation.parts.push({
    kind: 'diagnostic',
    severity: 'warning',
    message: `Discarded ${state.lineLength} buffered ${stream} bytes from byte ${offset}: ${reason}`,
    detail: new TextDecoder().decode(combinedLine(state)),
  })
  clearLine(state)
}

function processOutput(output: ProcessOutput, state: StreamState, presentation: ClaudeEventPresentation) {
  const gapKey = `${output.offset - output.gapBytes}:${output.offset}`
  if (output.gapBytes > 0 && !state.reportedGaps.has(gapKey)) {
    state.reportedGaps.add(gapKey)
    presentation.parts.push({
      kind: 'diagnostic',
      severity: 'warning',
      message: `Process retention gap: ${output.gapBytes} ${output.stream} bytes are unavailable before offset ${output.offset} (retained from ${output.retainedFrom})`,
    })
  }

  if (state.expected === undefined) {
    if (output.offset > 0 && output.gapBytes !== output.offset) {
      presentation.parts.push({
        kind: 'diagnostic',
        severity: 'warning',
        message: `${output.stream} begins at offset ${output.offset}; preceding bytes are not present in this timeline (retained from ${output.retainedFrom})`,
      })
    }
    state.expected = output.offset
  }

  const expected = state.expected
  let novel = output.bytes
  let novelOffset = output.offset
  if (output.offset > expected) {
    const missing = output.offset - expected
    if (missing !== output.gapBytes) {
      presentation.parts.push({
        kind: 'diagnostic',
        severity: 'warning',
        message: `Process offset gap: expected ${output.stream} byte ${expected}, received ${output.offset}; ${missing} bytes are unavailable`,
      })
    }
    discardPartialLine(state, output.stream, presentation, 'an offset gap interrupted the record')
    state.expected = output.offset
  } else if (output.offset < expected) {
    const overlapLength = Math.min(output.bytes.length, expected - output.offset)
    const comparison = compareAccepted(state, output.offset, output.bytes.subarray(0, overlapLength))
    if (comparison.status === 'conflict') {
      presentation.parts.push({
        kind: 'diagnostic',
        severity: 'error',
        message: `Conflicting ${output.stream} overlap at byte ${comparison.offset}; the retry chunk was ignored`,
      })
      return
    }
    if (comparison.status === 'unknown') {
      presentation.parts.push({
        kind: 'diagnostic',
        severity: 'warning',
        message: `Unverifiable ${output.stream} overlap at bytes ${output.offset}-${output.offset + overlapLength}; the chunk was ignored`,
      })
      return
    }
    novel = output.bytes.subarray(overlapLength)
    novelOffset = output.offset + overlapLength
  }

  if (novel.length > 0) {
    acceptBytes(state, novelOffset, novel)
    processBytes(state, output.stream, novelOffset, novel, presentation)
    state.expected = novelOffset + novel.length
  }
  if (output.eof && state.discardingLine) {
    state.discardingLine = false
    clearLine(state)
  } else if (output.eof && state.lineLength > 0) {
    finishLine(state, output.stream, presentation)
  }
}

function processItem(state: RendererState, result: Map<string, ClaudeEventPresentation>, item: TranscriptRenderItem) {
  if (item.kind === 'gap' || item.kind === 'client-gap') {
    state.streams.clear()
    return
  }
  const { entry } = item
  if (`${entry.source ?? ''}:${entry.type ?? ''}` !== CLAUDE_PROCESS_OUTPUT_KEY) return
  const presentation: ClaudeEventPresentation = { parts: [] }
  result.set(entry.id, presentation)
  const output = parseProcessOutput(entry.data)
  if (typeof output === 'string') {
    presentation.parts.push({ kind: 'diagnostic', severity: 'error', message: `Invalid Claude Code process envelope: ${output}` })
    return
  }
  Object.assign(presentation, {
    executionId: output.executionId,
    stream: output.stream,
    offset: output.offset,
    nextOffset: output.nextOffset,
  })
  if (!state.executions.has(output.executionId)) {
    state.executions.add(output.executionId)
    if (state.currentExecution !== undefined) {
      presentation.parts.push({
        kind: 'diagnostic',
        severity: 'info',
        message: `Claude Code execution changed from ${state.currentExecution} to ${output.executionId}; streams are assembled independently`,
      })
    }
    state.currentExecution = output.executionId
  }
  const key = `${output.executionId}\u0000${output.stream}`
  let stream = state.streams.get(key)
  if (!stream) {
    stream = newStreamState()
    state.streams.set(key, stream)
  }
  processOutput(output, stream, presentation)
}

export interface ClaudeTranscriptReduction {
  presentations: ReadonlyMap<string, ClaudeEventPresentation>
  timeline: readonly TranscriptRenderItem[]
  mode: 'append' | 'replay'
  processedItems: number
  state: RendererState
}

function replayClaudeTranscript(timeline: readonly TranscriptRenderItem[]): ClaudeTranscriptReduction {
  const result = new Map<string, ClaudeEventPresentation>()
  const state: RendererState = { streams: new Map(), executions: new Set() }
  for (const item of timeline) processItem(state, result, item)
  return { presentations: result, timeline, mode: 'replay', processedItems: timeline.length, state }
}

function isMonotonicSuffix(previous: readonly TranscriptRenderItem[], next: readonly TranscriptRenderItem[]): boolean {
  if (next.length !== previous.length + 1) return false
  for (let index = 0; index < previous.length; index += 1) {
    if (next[index] !== previous[index]) return false
  }
  return next.at(-1)?.kind === 'event'
}

// Ordinary monotonic SSE appends process one item. Ordering changes, either gap
// type, and client-window trims replay only the bounded retained timeline.
// eslint-disable-next-line react-refresh/only-export-components
export function updateClaudeTranscript(previous: ClaudeTranscriptReduction | undefined, timeline: readonly TranscriptRenderItem[]): ClaudeTranscriptReduction {
  if (!previous || !isMonotonicSuffix(previous.timeline, timeline)) return replayClaudeTranscript(timeline)
  const state = cloneRendererState(previous.state)
  const result = new Map(previous.presentations)
  processItem(state, result, timeline[timeline.length - 1])
  return { presentations: result, timeline, mode: 'append', processedItems: 1, state }
}

// eslint-disable-next-line react-refresh/only-export-components
export function reduceClaudeTranscript(timeline: readonly TranscriptRenderItem[]): ReadonlyMap<string, ClaudeEventPresentation> {
  return replayClaudeTranscript(timeline).presentations
}

function stringField(record: JSONRecord, key: string): string | undefined {
  return typeof record[key] === 'string' ? record[key] : undefined
}

function AssistantRecord({ record }: { record: JSONRecord }) {
  const message = isRecord(record.message) ? record.message : undefined
  const content = Array.isArray(message?.content) ? message.content : typeof message?.content === 'string' ? [message.content] : []
  const metadata = [stringField(message ?? {}, 'model'), stringField(message ?? {}, 'stop_reason'), stringField(record, 'error')].filter(Boolean)
  return <article className="claude-record claude-assistant">
    <h3>Claude assistant</h3>
    {metadata.length > 0 && <p className="claude-metadata">{metadata.join(' · ')}</p>}
    {content.length === 0 && <p className="claude-diagnostic warning">Assistant record has no directly displayable content.</p>}
    {content.map((block, index) => {
      if (typeof block === 'string') return <p className="claude-text" key={index}>{block}</p>
      if (!isRecord(block)) return <LazyJSONDetails key={index} summary="Unrecognized assistant content" value={block} />
      const type = stringField(block, 'type') ?? 'unknown'
      if (type === 'text' && typeof block.text === 'string') return <p className="claude-text" key={index}>{block.text}</p>
      if (type === 'thinking' && typeof block.thinking === 'string') return <LazyTextDetails key={index} summary="Thinking" text={block.thinking} />
      if ((type === 'tool_use' || type === 'server_tool_use') && typeof block.name === 'string') return <section className="claude-tool" key={index}>
        <strong>{type === 'server_tool_use' ? 'Server tool' : 'Tool use'}: {block.name}</strong>
        {'input' in block && <LazyJSONDetails summary="Tool input" value={block.input} />}
      </section>
      return <section className="claude-diagnostic warning" key={index}>
        <strong>Unrecognized assistant content block: {type}</strong>
        <LazyJSONDetails summary="Content block JSON" value={block} />
      </section>
    })}
    <LazyJSONDetails summary="Claude record JSON" value={record} />
  </article>
}

function SystemRecord({ record }: { record: JSONRecord }) {
  const subtype = stringField(record, 'subtype') ?? 'unknown subtype'
  const facts = [
    ['Session', stringField(record, 'session_id')],
    ['Model', stringField(record, 'model')],
    ['Version', stringField(record, 'claude_code_version')],
    ['Working directory', stringField(record, 'cwd')],
  ].filter((fact): fact is [string, string] => fact[1] !== undefined)
  return <article className="claude-record claude-system">
    <h3>Claude system · {subtype}</h3>
    {facts.length > 0 && <dl>{facts.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>}
    {Array.isArray(record.tools) && <p className="claude-metadata">Tools: {record.tools.map(String).join(', ')}</p>}
    <LazyJSONDetails summary="Claude record JSON" value={record} />
  </article>
}

function ResultRecord({ record }: { record: JSONRecord }) {
  const subtype = stringField(record, 'subtype') ?? 'unknown subtype'
  const failed = record.is_error === true || subtype !== 'success'
  const facts = [
    ['Turns', typeof record.num_turns === 'number' ? String(record.num_turns) : undefined],
    ['Duration', typeof record.duration_ms === 'number' ? `${record.duration_ms} ms` : undefined],
    ['Cost', typeof record.total_cost_usd === 'number' ? `$${record.total_cost_usd}` : undefined],
    ['Stop reason', stringField(record, 'stop_reason')],
  ].filter((fact): fact is [string, string] => fact[1] !== undefined)
  return <article className={`claude-record claude-result ${failed ? 'failed' : 'succeeded'}`}>
    <h3>Claude result · {subtype}</h3>
    {typeof record.result === 'string' && <p className="claude-text">{record.result}</p>}
    {facts.length > 0 && <dl>{facts.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>}
    {'structured_output' in record && <LazyJSONDetails summary="Structured output" value={record.structured_output} />}
    <LazyJSONDetails summary="Claude record JSON" value={record} />
  </article>
}

function ClaudeRecord({ record, offset }: { record: unknown; offset: number }) {
  if (!isRecord(record)) return <section className="claude-diagnostic warning">
    <strong>Unrecognized Claude record at stdout byte {offset}</strong><LazyJSONDetails summary="Claude record JSON" value={record} />
  </section>
  if (record.type === 'system') return <SystemRecord record={record} />
  if (record.type === 'assistant') return <AssistantRecord record={record} />
  if (record.type === 'result') return <ResultRecord record={record} />
  return <section className="claude-diagnostic warning">
    <strong>Unrecognized Claude record type: {typeof record.type === 'string' ? record.type : '(missing)'}</strong>
    <LazyJSONDetails summary="Claude record JSON" value={record} />
  </section>
}

export const ClaudeProcessOutput = memo(function ClaudeProcessOutput({ presentation }: { presentation: ClaudeEventPresentation }) {
  const range = presentation.offset === undefined ? undefined : presentation.offset === presentation.nextOffset
    ? `byte ${presentation.offset}`
    : `bytes ${presentation.offset}-${presentation.nextOffset}`
  return <div className="claude-output">
    {presentation.executionId && <p className="claude-metadata">Execution {presentation.executionId} · {presentation.stream} · {range}</p>}
    {presentation.parts.map((part, index) => {
      if (part.kind === 'record') return <ClaudeRecord key={`${part.kind}:${part.offset}:${index}`} record={part.record} offset={part.offset} />
      if (part.kind === 'stderr') return <section className="claude-diagnostic error" key={`${part.kind}:${part.offset}:${index}`}>
        <strong>Claude Code stderr · byte {part.offset}</strong><pre>{part.text}</pre>
      </section>
      return <section className={`claude-diagnostic ${part.severity}`} key={`${part.kind}:${index}`}>
        <strong>Claude transcript diagnostic</strong><p>{part.message}</p>{part.detail !== undefined && <LazyTextDetails summary="Diagnostic detail" text={part.detail} />}
      </section>
    })}
    {presentation.parts.length === 0 && <p className="hint">Process chunk accepted; waiting for a complete record.</p>}
  </div>
})
