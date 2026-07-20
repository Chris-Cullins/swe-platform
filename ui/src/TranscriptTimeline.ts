export const MAX_TRANSCRIPT_ITEMS = 128
export const MAX_TRANSCRIPT_RAW_BYTES = 2 * 1024 * 1024

export interface TranscriptEntry {
  id: string
  sequence: number
  source?: string
  type?: string
  data: unknown
  raw: unknown
}

export interface TranscriptGap {
  resumeAfter: string
  earliestSequence?: number
  latestSequence?: number
}

export type TranscriptRenderItem =
  | { kind: 'event'; entry: TranscriptEntry; position: number; rawBytes: number }
  | { kind: 'gap'; gap: TranscriptGap; position: number; rawBytes: number }
  | { kind: 'client-gap'; position: number; droppedItems: number; droppedRawBytes: number; rawBytes: 0 }

function order(item: TranscriptRenderItem): number {
  if (item.kind === 'client-gap') return -1
  return item.kind === 'gap' ? 0 : 1
}

export function transcriptRawBytes(timeline: readonly TranscriptRenderItem[]): number {
  return timeline.reduce((total, item) => total + item.rawBytes, 0)
}

export function appendTimelineItem(timeline: readonly TranscriptRenderItem[], item: TranscriptRenderItem): TranscriptRenderItem[] {
  const priorBoundary = timeline[0]?.kind === 'client-gap' ? timeline[0] : undefined
  const retained = priorBoundary ? timeline.slice(1) : [...timeline]
  const frontier = retained[0]?.position
  if (priorBoundary && frontier !== undefined && item.position < frontier) {
    return [{
      ...priorBoundary,
      droppedItems: priorBoundary.droppedItems + 1,
      droppedRawBytes: priorBoundary.droppedRawBytes + item.rawBytes,
    }, ...retained]
  }

  const items = [...retained, item].sort((left, right) => left.position - right.position || order(left) - order(right))
  let rawBytes = transcriptRawBytes(items)
  let droppedItems = 0
  let droppedRawBytes = 0
  while (items.length > MAX_TRANSCRIPT_ITEMS || rawBytes > MAX_TRANSCRIPT_RAW_BYTES) {
    const dropped = items.shift()
    if (!dropped) break
    droppedItems += 1
    droppedRawBytes += dropped.rawBytes
    rawBytes -= dropped.rawBytes
  }
  if (droppedItems === 0) return priorBoundary ? [priorBoundary, ...items] : items
  return [{
    kind: 'client-gap',
    position: items[0]?.position ?? item.position,
    droppedItems: (priorBoundary?.droppedItems ?? 0) + droppedItems,
    droppedRawBytes: (priorBoundary?.droppedRawBytes ?? 0) + droppedRawBytes,
    rawBytes: 0,
  }, ...items]
}
