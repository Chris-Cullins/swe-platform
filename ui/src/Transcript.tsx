import { memo, useEffect, useState, type ReactNode } from 'react'
import { api } from './api'
import { CLAUDE_PROCESS_OUTPUT_KEY, ClaudeProcessOutput, updateClaudeTranscript, type ClaudeTranscriptReduction } from './ClaudeTranscript'
import { LazyJSONDetails } from './LazyDetails'
import { appendTimelineItem, type TranscriptEntry, type TranscriptGap, type TranscriptRenderItem } from './TranscriptTimeline'

export type { TranscriptEntry, TranscriptGap, TranscriptRenderItem } from './TranscriptTimeline'

type Renderer = (data: unknown) => ReactNode
const opaque: Renderer = data => {
  if (typeof data === 'string') return data
  if (data === null || typeof data === 'number' || typeof data === 'boolean') return String(data)
  try { return JSON.stringify(data, null, 2) } catch { return '[Unrenderable event data]' }
}

const OpaqueEvent = memo(function OpaqueEvent({ data }: { data: unknown }) {
  return <pre>{opaque(data)}</pre>
})

function parseEntry(event: MessageEvent): TranscriptEntry | undefined {
  let value: unknown
  try { value = JSON.parse(event.data) } catch { value = event.data }
  const object = value && typeof value === 'object' ? value as Record<string, unknown> : undefined
  const sequence = Number(object?.sequence ?? event.lastEventId)
  if (!Number.isFinite(sequence)) return
  const id = event.lastEventId || (typeof object?.id === 'string' ? object.id : `sequence:${sequence}`)
  return {
    id,
    sequence,
    source: typeof object?.source === 'string' ? object.source : undefined,
    type: typeof object?.type === 'string' ? object.type : undefined,
    data: object && 'data' in object ? object.data : value,
    raw: value,
  }
}

function parseGap(event: MessageEvent): TranscriptGap | undefined {
  try {
    const value: unknown = JSON.parse(event.data)
    if (!value || typeof value !== 'object') return
    const object = value as Record<string, unknown>
    if (typeof object.resumeAfter !== 'string' && typeof object.resumeAfter !== 'number') return
    const optionalSequence = (key: 'earliestSequence' | 'latestSequence') =>
      typeof object[key] === 'number' && Number.isFinite(object[key]) ? object[key] : undefined
    return {
      resumeAfter: String(object.resumeAfter),
      earliestSequence: optionalSequence('earliestSequence'),
      latestSequence: optionalSequence('latestSequence'),
    }
  } catch { return }
}

function GapNotice({ gap }: { gap: TranscriptGap }) {
  return <li className="gap"><strong>Transcript gap</strong><p>
    {gap.earliestSequence === undefined
      ? 'Earlier transcript history is unavailable.'
      : `History before sequence ${gap.earliestSequence} is unavailable.`}
    {gap.latestSequence !== undefined && ` Retained history is available through sequence ${gap.latestSequence}.`}
    {' '}Recovery cursor: <code>{gap.resumeAfter}</code>.
  </p><p>Adapter stream assembly restarts after this boundary; partial records from before it are not joined to later bytes.</p></li>
}

function ClientGapNotice({ item }: { item: Extract<TranscriptRenderItem, { kind: 'client-gap' }> }) {
  return <li className="client-gap"><strong>Client display history limit</strong><p>
    This browser dropped {item.droppedItems} older timeline {item.droppedItems === 1 ? 'item' : 'items'}
    {' '}(approximately {item.droppedRawBytes.toLocaleString()} raw bytes) to keep the console bounded.
    This is separate from any server transcript gap. Adapter stream assembly restarted at this boundary.
  </p></li>
}

function RawTransportEvent({ entry }: { entry: TranscriptEntry }) {
  return <LazyJSONDetails className="raw-event" summary="Raw transport event" value={entry.raw} />
}

interface TranscriptState {
  timeline: TranscriptRenderItem[]
  claude: ClaudeTranscriptReduction
}

const MAX_SSE_BUFFER = 8 << 20
const INITIAL_SSE_BUFFER = 4096

class TerminalStreamError extends Error {}

// CRLF has greedy precedence over the byte-identical CR + LF combination
// while streaming. The ambiguous CR + LF delimiter is accepted only at EOF.
// Completed frames are consumed synchronously, and large accumulators shrink
// after dispatch so an unresolved suffix never retains a large source buffer.
// Exported for deterministic chunk-boundary/property tests.
export class SSEFramer {
  private bytes = new Uint8Array(INITIAL_SSE_BUFFER)
  private length = 0
  private scan = 0
  private previousEndingStart = -1
  private previousEndingEnd = -1

  push(chunk: Uint8Array, consume: (frame: Uint8Array) => void) {
    for (const byte of chunk) {
      this.append(byte)
      this.scanAvailable(consume, false)
      this.checkBodyLimit()
    }
  }

  finish(consume: (frame: Uint8Array) => void) {
    this.scanAvailable(consume, true)
    if (this.length && this.previousEndingEnd === this.length && this.previousEndingEnd-this.previousEndingStart === 2 &&
      this.bytes[this.previousEndingStart] === 13 && this.bytes[this.previousEndingStart + 1] === 10) {
      // At EOF only, resolve the otherwise ambiguous CRLF as CR + LF.
      this.consumeFrame(this.previousEndingStart, this.length, consume)
    }
    if (this.length !== 0) throw new TerminalStreamError('Malformed transcript event stream at EOF')
  }

  private append(byte: number) {
    if (this.length === MAX_SSE_BUFFER + 4) throw new TerminalStreamError(`Transcript event exceeds ${MAX_SSE_BUFFER} bytes`)
    if (this.length === this.bytes.length) {
      const grown = new Uint8Array(Math.min(MAX_SSE_BUFFER + 4, this.bytes.length * 2))
      grown.set(this.bytes)
      this.bytes = grown
    }
    this.bytes[this.length++] = byte
  }

  private checkBodyLimit() {
    let bodyBytes = this.length
    if (this.scan < this.length) {
      bodyBytes = this.previousEndingEnd === this.scan ? this.previousEndingStart : this.scan
    } else if (this.previousEndingEnd === this.length) {
      bodyBytes = this.previousEndingStart
    }
    if (bodyBytes > MAX_SSE_BUFFER) throw new TerminalStreamError(`Transcript event exceeds ${MAX_SSE_BUFFER} bytes`)
  }

  private consumeFrame(bodyLength: number, consumedEnd: number, consume: (frame: Uint8Array) => void) {
    if (bodyLength > MAX_SSE_BUFFER) throw new TerminalStreamError(`Transcript event exceeds ${MAX_SSE_BUFFER} bytes`)
    consume(this.bytes.subarray(0, bodyLength))
    const suffixLength = this.length - consumedEnd
    if (this.bytes.length > INITIAL_SSE_BUFFER) {
      const compact = new Uint8Array(Math.max(INITIAL_SSE_BUFFER, suffixLength))
      compact.set(this.bytes.subarray(consumedEnd, this.length))
      this.bytes = compact
    } else if (suffixLength) {
      this.bytes.copyWithin(0, consumedEnd, this.length)
    }
    this.length = suffixLength
    this.scan = 0
    this.previousEndingStart = -1
    this.previousEndingEnd = -1
  }

  private scanAvailable(consume: (frame: Uint8Array) => void, eof: boolean) {
    while (this.scan < this.length) {
      const start = this.scan
      const byte = this.bytes[start]
      let endingLength = 0
      if (byte === 10) endingLength = 1
      else if (byte === 13) {
        if (start + 1 === this.length && !eof) return
        endingLength = start + 1 < this.length && this.bytes[start + 1] === 10 ? 2 : 1
      } else {
        this.scan += 1
        continue
      }
      const end = start + endingLength
      if (this.previousEndingEnd === start) {
        this.consumeFrame(this.previousEndingStart, end, consume)
        continue
      }
      this.previousEndingStart = start
      this.previousEndingEnd = end
      this.scan = end
    }
  }
}

function dataPayloadBytes(bytes: Uint8Array): number {
  let fields = 0
  let total = 0
  for (let start = 0; start <= bytes.length;) {
    let end = start
    while (end < bytes.length && bytes[end] !== 10 && bytes[end] !== 13) end += 1
    const length = end - start
    if (length >= 4 && bytes[start] === 100 && bytes[start + 1] === 97 && bytes[start + 2] === 116 && bytes[start + 3] === 97 && (length === 4 || bytes[start + 4] === 58)) {
      let valueStart = start + 4
      if (length > 4) {
        valueStart += 1
        if (valueStart < end && bytes[valueStart] === 32) valueStart += 1
      }
      total += end - valueStart
      fields += 1
    }
    if (end === bytes.length) break
    if (bytes[end] === 13 && bytes[end + 1] === 10) end += 1
    start = end + 1
  }
  return total + Math.max(0, fields - 1)
}

function emptyTranscriptState(): TranscriptState {
  const timeline: TranscriptRenderItem[] = []
  return { timeline, claude: updateClaudeTranscript(undefined, timeline) }
}

export function Transcript({ namespace, run, identity }: { namespace: string; run: string; identity: string }) {
  const [status, setStatus] = useState('Connecting')
  const [transcript, setTranscript] = useState<TranscriptState>(emptyTranscriptState)
  useEffect(() => {
    setTranscript(emptyTranscriptState())
    setStatus('Connecting')
    const controller = new AbortController()
    let disposed = false
    let lastEventID = ''
    let established = false
    let freshRecoveryUsed = false
    let reconnectWait = 250
    let queuedTimeline: TranscriptRenderItem[] = []
    let frame: number | undefined
    let timer: number | undefined
    const reconnectDelay = () => new Promise<void>(resolve => {
      const onAbort = () => {
        window.clearTimeout(reconnect)
        resolve()
      }
      const reconnect = window.setTimeout(() => {
        controller.signal.removeEventListener('abort', onAbort)
        resolve()
      }, reconnectWait)
      controller.signal.addEventListener('abort', onAbort, { once: true })
    })
    const flushTimeline = () => {
      frame = undefined
      timer = undefined
      const timeline = queuedTimeline
      setTranscript(current => {
        if (timeline === current.timeline) return current
        return { timeline, claude: updateClaudeTranscript(current.claude, timeline) }
      })
    }
    const scheduleTimelineFlush = () => {
      if (frame !== undefined || timer !== undefined) return
      if (typeof window.requestAnimationFrame === 'function') {
        frame = window.requestAnimationFrame(flushTimeline)
      } else {
        timer = window.setTimeout(flushTimeline, 16)
      }
    }
    const onTranscript = (raw: Event, rawBytes: number) => {
      if (disposed) return
      const entry = parseEntry(raw as MessageEvent)
      if (!entry) return
      if (queuedTimeline.some(item => item.kind === 'event' && (item.entry.id === entry.id || item.entry.sequence === entry.sequence))) return
      const next: TranscriptRenderItem = { kind: 'event', entry, position: entry.sequence, rawBytes }
      queuedTimeline = appendTimelineItem(queuedTimeline, next)
      scheduleTimelineFlush()
    }
    const onGap = (raw: Event, rawBytes: number) => {
      if (disposed) return
      const gap = parseGap(raw as MessageEvent)
      if (!gap) return
      if (queuedTimeline.some(item => item.kind === 'gap' && item.gap.resumeAfter === gap.resumeAfter && item.gap.earliestSequence === gap.earliestSequence && item.gap.latestSequence === gap.latestSequence)) return
      const lastPosition = queuedTimeline.reduce((latest, item) => Math.max(latest, item.position), 0)
      const position = gap.earliestSequence ?? lastPosition + 0.5
      const next: TranscriptRenderItem = { kind: 'gap', gap, position, rawBytes }
      queuedTimeline = appendTimelineItem(queuedTimeline, next)
      scheduleTimelineFlush()
    }
    const connect = async () => {
      while (!disposed) {
        try {
          const response = await api.transcript(namespace, run, identity, controller.signal, lastEventID || undefined)
          const cancelResponse = async () => {
            if (response.body) await response.body.cancel().catch(() => undefined)
          }
          if (response.status === 401) {
            await cancelResponse()
            return
          }
          if (!response.ok || !response.body) {
            if (established && lastEventID && !freshRecoveryUsed && (response.status === 400 || response.status === 410)) {
              await cancelResponse()
              lastEventID = ''
              freshRecoveryUsed = true
              setStatus('Recovering transcript')
              await reconnectDelay()
              continue
            }
            if (response.status === 408 || response.status === 429 || response.status >= 500) {
              await cancelResponse()
              setStatus('Reconnecting')
              await reconnectDelay()
              continue
            }
            await cancelResponse()
            setStatus(response.statusText || `Request failed (${response.status})`)
            return
          }
          const mediaType = response.headers.get('Content-Type')?.split(';', 1)[0].trim().toLowerCase()
          if (mediaType !== 'text/event-stream') {
            await cancelResponse()
            setStatus(`Expected transcript event stream, got ${mediaType || 'unknown content type'}`)
            return
          }
          established = true
          freshRecoveryUsed = false
          setStatus('Connected')
          const reader = response.body.getReader()
          const decoder = new TextDecoder('utf-8', { fatal: true })
          const framer = new SSEFramer()
          const consumeFrame = (frameBytes: Uint8Array) => {
            let block: string
            try { block = decoder.decode(frameBytes) } catch { throw new TerminalStreamError('Malformed UTF-8 in transcript event stream') }
            let type = 'message'
            let id = ''
            let hasID = false
            const data: string[] = []
            for (const line of block.split(/\r\n|\n|\r/)) {
              if (!line || line.startsWith(':')) continue
              const colon = line.indexOf(':')
              const field = colon < 0 ? line : line.slice(0, colon)
              const fieldValue = colon < 0 ? '' : line.slice(colon + 1).replace(/^ /, '')
              if (field === 'event') type = fieldValue
              else if (field === 'id' && !fieldValue.includes('\0')) { id = fieldValue; hasID = true }
              else if (field === 'data') data.push(fieldValue)
              else if (field === 'retry' && /^\d+$/.test(fieldValue)) reconnectWait = Math.min(Number(fieldValue), 30_000)
            }
            if (hasID) lastEventID = id
            if (!data.length) return
            const event = new MessageEvent(type, { data: data.join('\n'), lastEventId: lastEventID })
            const rawBytes = dataPayloadBytes(frameBytes)
            if (type === 'transcript') onTranscript(event, rawBytes)
            else if (type === 'transcript-gap') onGap(event, rawBytes)
          }
          try {
            while (!disposed) {
              const { value, done } = await reader.read()
              if (value) framer.push(value, consumeFrame)
              if (done) {
                framer.finish(consumeFrame)
                break
              }
            }
          } finally {
            await reader.cancel().catch(() => undefined)
          }
          if (!disposed) {
            setStatus('Reconnecting')
            await reconnectDelay()
          }
        } catch (error) {
          if (disposed || controller.signal.aborted) return
          if (!(error instanceof TerminalStreamError)) {
            setStatus('Reconnecting')
            await reconnectDelay()
            continue
          }
          setStatus(error instanceof Error ? error.message : 'Disconnected')
          return
        }
      }
    }
    void connect()
    return () => {
      disposed = true
      if (frame !== undefined) window.cancelAnimationFrame(frame)
      if (timer !== undefined) window.clearTimeout(timer)
      controller.abort()
    }
  }, [namespace, run, identity])
  const { timeline, claude } = transcript
  return <section><p role="status" aria-live="polite">Transcript: {status}</p>
    {!timeline.length ? <p>No transcript events yet.</p> : <ol className="transcript">
      {timeline.map(item => {
        if (item.kind === 'gap') return <GapNotice key={`gap:${item.gap.resumeAfter}:${item.gap.earliestSequence}:${item.gap.latestSequence}`} gap={item.gap} />
        if (item.kind === 'client-gap') return <ClientGapNotice key={`client-gap:${item.droppedItems}:${item.droppedRawBytes}`} item={item} />
        const { entry } = item
        const key = `${entry.source || ''}:${entry.type || ''}`
        const presentation = claude.presentations.get(entry.id)
        return <li key={`event:${entry.id}`}><span>{[entry.source, entry.type].filter(Boolean).join(' / ') || 'Event'}</span>
          {key === CLAUDE_PROCESS_OUTPUT_KEY && presentation
            ? <ClaudeProcessOutput presentation={presentation} />
            : <OpaqueEvent data={entry.data} />}
          <RawTransportEvent entry={entry} />
        </li>
      })}
    </ol>}
  </section>
}
