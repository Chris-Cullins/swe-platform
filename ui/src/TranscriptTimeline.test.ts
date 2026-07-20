import { describe, expect, it } from 'vitest'
import { appendTimelineItem, MAX_TRANSCRIPT_ITEMS, MAX_TRANSCRIPT_RAW_BYTES, transcriptRawBytes, type TranscriptRenderItem } from './TranscriptTimeline'

function item(sequence: number, rawBytes = 1): TranscriptRenderItem {
  const raw = { sequence, data: sequence }
  return { kind: 'event', entry: { id: String(sequence), sequence, data: sequence, raw }, position: sequence, rawBytes }
}

describe('bounded transcript timeline', () => {
  it('bounds retained display items and inserts a cumulative client-only retention boundary', () => {
    let timeline: TranscriptRenderItem[] = []
    for (let sequence = 1; sequence <= MAX_TRANSCRIPT_ITEMS + 3; sequence += 1) timeline = appendTimelineItem(timeline, item(sequence))

    expect(timeline[0]).toEqual(expect.objectContaining({ kind: 'client-gap', droppedItems: 3, droppedRawBytes: 3 }))
    expect(timeline.slice(1)).toHaveLength(MAX_TRANSCRIPT_ITEMS)
    expect(timeline[1]).toEqual(expect.objectContaining({ position: 4 }))
  })

  it('bounds approximate raw bytes and drops late arrivals behind the retained frontier', () => {
    const large = Math.floor(MAX_TRANSCRIPT_RAW_BYTES * 0.6)
    let timeline = appendTimelineItem([], item(1, large))
    timeline = appendTimelineItem(timeline, item(2, large))
    expect(timeline[0]).toEqual(expect.objectContaining({ kind: 'client-gap', droppedItems: 1, droppedRawBytes: large }))
    expect(transcriptRawBytes(timeline)).toBeLessThanOrEqual(MAX_TRANSCRIPT_RAW_BYTES)

    const retained = timeline[1]
    timeline = appendTimelineItem(timeline, item(0, 10))
    expect(timeline[0]).toEqual(expect.objectContaining({ kind: 'client-gap', droppedItems: 2, droppedRawBytes: large + 10 }))
    expect(timeline[1]).toBe(retained)
  })
})
