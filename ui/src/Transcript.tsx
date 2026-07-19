import { useEffect, useState, type ReactNode } from 'react'
import { api } from './api'

export interface TranscriptEntry {
  id: string
  sequence: number
  source?: string
  type?: string
  data: unknown
}

export interface TranscriptGap {
  resumeAfter: string
  earliestSequence?: number
  latestSequence?: number
}

type TimelineItem =
  | { kind: 'event'; entry: TranscriptEntry; position: number }
  | { kind: 'gap'; gap: TranscriptGap; position: number }

type Renderer = (data: unknown) => ReactNode
const opaque: Renderer = data => {
  if (typeof data === 'string') return data
  if (data === null || typeof data === 'number' || typeof data === 'boolean') return String(data)
  try { return JSON.stringify(data, null, 2) } catch { return '[Unrenderable event data]' }
}

// Agent adapters can register precise renderers without changing stream handling.
// eslint-disable-next-line react-refresh/only-export-components
export const transcriptRenderers: Record<string, Renderer> = {}

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
  </p></li>
}

export function Transcript({ namespace, run }: { namespace: string; run: string }) {
  const [status, setStatus] = useState('Connecting')
  const [timeline, setTimeline] = useState<TimelineItem[]>([])
  useEffect(() => {
    setTimeline([])
    setStatus('Connecting')
    const url = api.transcriptUrl(namespace, run)
    let stream: EventSource | undefined
    let disposed = false
    let freshRecoveryUsed = false
    const onTranscript = (raw: Event) => {
      const entry = parseEntry(raw as MessageEvent)
      if (!entry) return
      setTimeline(current => {
        if (current.some(item => item.kind === 'event' && (item.entry.id === entry.id || item.entry.sequence === entry.sequence))) return current
        const next: TimelineItem = { kind: 'event', entry, position: entry.sequence }
        return [...current, next].sort((a, b) => a.position - b.position || (a.kind === 'gap' ? -1 : 1))
      })
    }
    const onGap = (raw: Event) => {
      const gap = parseGap(raw as MessageEvent)
      if (!gap) return
      setTimeline(current => {
        if (current.some(item => item.kind === 'gap' && item.gap.resumeAfter === gap.resumeAfter && item.gap.earliestSequence === gap.earliestSequence && item.gap.latestSequence === gap.latestSequence)) return current
        const lastPosition = current.reduce((latest, item) => Math.max(latest, item.position), 0)
        const position = gap.earliestSequence ?? lastPosition + 0.5
        const next: TimelineItem = { kind: 'gap', gap, position }
        return [...current, next].sort((a, b) => a.position - b.position || (a.kind === 'gap' ? -1 : 1))
      })
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
      stream?.close()
    }
  }, [namespace, run])
  return <section><p role="status" aria-live="polite">Transcript: {status}</p>
    {!timeline.length ? <p>No transcript events yet.</p> : <ol className="transcript">
      {timeline.map(item => {
        if (item.kind === 'gap') return <GapNotice key={`gap:${item.gap.resumeAfter}:${item.gap.earliestSequence}:${item.gap.latestSequence}`} gap={item.gap} />
        const { entry } = item
        const key = `${entry.source || ''}:${entry.type || ''}`
        const renderer = transcriptRenderers[key] || opaque
        return <li key={`event:${entry.id}`}><span>{[entry.source, entry.type].filter(Boolean).join(' / ') || 'Event'}</span><pre>{renderer(entry.data)}</pre></li>
      })}
    </ol>}
  </section>
}
