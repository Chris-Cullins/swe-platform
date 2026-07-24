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

function emptyTranscriptState(): TranscriptState {
  const timeline: TranscriptRenderItem[] = []
  return { timeline, claude: updateClaudeTranscript(undefined, timeline) }
}

export function Transcript({ namespace, run, identity }: { namespace: string; run: string; identity?: string }) {
  const [status, setStatus] = useState('Connecting')
  const [transcript, setTranscript] = useState<TranscriptState>(emptyTranscriptState)
  useEffect(() => {
    setTranscript(emptyTranscriptState())
    setStatus('Connecting')
    const url = api.transcriptUrl(namespace, run)
    let stream: EventSource | undefined
    let disposed = false
    let freshRecoveryUsed = false
    let queuedTimeline: TranscriptRenderItem[] = []
    let frame: number | undefined
    let timer: number | undefined
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
    const onTranscript = (raw: Event) => {
      if (disposed) return
      const entry = parseEntry(raw as MessageEvent)
      if (!entry) return
      if (queuedTimeline.some(item => item.kind === 'event' && (item.entry.id === entry.id || item.entry.sequence === entry.sequence))) return
      const next: TranscriptRenderItem = { kind: 'event', entry, position: entry.sequence, rawBytes: (raw as MessageEvent).data.length }
      queuedTimeline = appendTimelineItem(queuedTimeline, next)
      scheduleTimelineFlush()
    }
    const onGap = (raw: Event) => {
      if (disposed) return
      const gap = parseGap(raw as MessageEvent)
      if (!gap) return
      if (queuedTimeline.some(item => item.kind === 'gap' && item.gap.resumeAfter === gap.resumeAfter && item.gap.earliestSequence === gap.earliestSequence && item.gap.latestSequence === gap.latestSequence)) return
      const lastPosition = queuedTimeline.reduce((latest, item) => Math.max(latest, item.position), 0)
      const position = gap.earliestSequence ?? lastPosition + 0.5
      const next: TranscriptRenderItem = { kind: 'gap', gap, position, rawBytes: (raw as MessageEvent).data.length }
      queuedTimeline = appendTimelineItem(queuedTimeline, next)
      scheduleTimelineFlush()
    }
    const connect = () => {
      const next = new EventSource(url)
      stream = next
      let opened = false
      next.onopen = () => {
        if (disposed || stream !== next) return
        opened = true
        freshRecoveryUsed = false
        setStatus('Connected')
      }
      next.onerror = async () => {
        if (disposed || stream !== next) return
        if (next.readyState === EventSource.CONNECTING) {
          setStatus('Reconnecting')
          return
        }
        if (next.readyState !== EventSource.CLOSED) return
        setStatus('Checking session')
        try {
          await api.session()
        } catch (error) {
          if (!disposed && stream === next) setStatus(error instanceof Error ? error.message : 'Disconnected')
          return
        }
        if (disposed || stream !== next || next.readyState !== EventSource.CLOSED) return
        if (!opened || freshRecoveryUsed) {
          setStatus('Disconnected')
          return
        }
        freshRecoveryUsed = true
        next.close()
        setStatus('Recovering transcript')
        connect()
      }
      next.addEventListener('transcript', onTranscript)
      next.addEventListener('transcript-gap', onGap)
    }
    connect()
    return () => {
      disposed = true
      if (frame !== undefined) window.cancelAnimationFrame(frame)
      if (timer !== undefined) window.clearTimeout(timer)
      stream?.close()
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
