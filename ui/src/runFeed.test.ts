import { QueryClient, QueryObserver } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { api, ApiProblem } from './api'
import type { RunSummary, RunSummaryList, RunWatchEvent } from './contracts'
import { applyRunEvent, consumeSSE, listRunSnapshot, refreshMatchingDetail } from './runFeed'

afterEach(() => vi.restoreAllMocks())

const summary = (name: string, uid = `${name}-uid`): RunSummary => ({
  name, uid, generation: 1, createdAt: '2026-07-24T00:00:00Z', agent: 'amp', promptPreview: `Task ${name}`,
  cancelRequested: false, state: 'Running',
})

describe('Run summary watch', () => {
  it('applies additions, modifications, and UID-fenced deletes', () => {
    const added: RunWatchEvent = { type: 'ADDED', resourceVersion: '2', run: summary('two') }
    const modified: RunWatchEvent = { type: 'MODIFIED', resourceVersion: '3', run: { ...summary('one'), state: 'Succeeded' } }
    const staleDelete: RunWatchEvent = { type: 'DELETED', resourceVersion: '4', run: summary('one', 'replacement-uid') }
    const deleted: RunWatchEvent = { type: 'DELETED', resourceVersion: '5', run: summary('one') }
    let snapshot: RunSummaryList = { items: [summary('one')], resourceVersion: '1' }
    snapshot = applyRunEvent(snapshot, added)
    snapshot = applyRunEvent(snapshot, modified)
    snapshot = applyRunEvent(snapshot, staleDelete)
    expect(snapshot.items.map(run => [run.name, run.state])).toEqual([['one', 'Succeeded'], ['two', 'Running']])
    snapshot = applyRunEvent(snapshot, deleted)
    expect(snapshot).toEqual({ items: [summary('two')], resourceVersion: '5' })
  })

  it('parses run, checkpoint, and ID-less relist events in order', async () => {
    const body = [
      'event: run\nid: 2\ndata: {"type":"ADDED"}\n\n',
      'event: run-checkpoint\nid: 3\ndata: {"resourceVersion":"3"}\n\n',
      'event: run-relist\ndata:\n\n',
    ].join('')
    const seen: Array<{ event: string; id?: string }> = []
    await consumeSSE(new Response(body), event => { seen.push({ event: event.event, id: event.id }) }, new AbortController().signal)
    expect(seen).toEqual([{ event: 'run', id: '2' }, { event: 'run-checkpoint', id: '3' }, { event: 'run-relist', id: undefined }])
  })

  it('restarts an inconsistent or expired continuation chain and rejects repeated cursors', async () => {
    const pages = vi.spyOn(api, 'runSummaries')
      .mockResolvedValueOnce({ items: [summary('stale')], resourceVersion: '1', continue: 'next' })
      .mockResolvedValueOnce({ items: [], resourceVersion: '2' })
      .mockRejectedValueOnce(new ApiProblem({ type: 'expired', title: 'expired', status: 410 }, 410))
      .mockResolvedValueOnce({ items: [summary('current')], resourceVersion: '3' })
    await expect(listRunSnapshot('ns')).resolves.toEqual({ items: [summary('current')], resourceVersion: '3' })
    expect(pages).toHaveBeenCalledTimes(4)

    pages.mockReset()
      .mockResolvedValueOnce({ items: [], resourceVersion: '4', continue: 'same' })
      .mockResolvedValueOnce({ items: [], resourceVersion: '4', continue: 'same' })
    await expect(listRunSnapshot('ns')).rejects.toThrow('repeated a continuation cursor')
  })

  it('aborts an in-flight relist before publishing its result', async () => {
    let resolve!: (value: RunSummaryList) => void
    vi.spyOn(api, 'runSummaries').mockImplementation(() => new Promise(value => { resolve = value }))
    const controller = new AbortController()
    const snapshot = listRunSnapshot('ns', controller.signal)
    controller.abort()
    resolve({ items: [summary('private')], resourceVersion: '9' })
    await expect(snapshot).rejects.toHaveProperty('name', 'AbortError')
  })

  it('cancels and replaces a matching detail query while its first request is still loading', async () => {
    const client = new QueryClient()
    let resolveFirst!: (value: { uid: string; generation: number }) => void
    let firstSignal: AbortSignal | undefined
    const queryFn = vi.fn(({ signal }: { signal: AbortSignal }) => {
      if (!firstSignal) {
        firstSignal = signal
        return new Promise<{ uid: string; generation: number }>(resolve => { resolveFirst = resolve })
      }
      return Promise.resolve({ uid: 'one-uid', generation: 1 })
    })
    const observer = new QueryObserver(client, { queryKey: ['run', 'ns', 'one'], queryFn, retry: false })
    const unsubscribe = observer.subscribe(() => undefined)
    await vi.waitFor(() => expect(queryFn).toHaveBeenCalledOnce())
    refreshMatchingDetail(client, 'ns', summary('one'))
    await vi.waitFor(() => expect(queryFn).toHaveBeenCalledTimes(2))
    expect(firstSignal?.aborted).toBe(true)
    resolveFirst({ uid: 'stale-uid', generation: 0 })
    await vi.waitFor(() => expect(client.getQueryData(['run', 'ns', 'one'])).toEqual({ uid: 'one-uid', generation: 1 }))
    unsubscribe()
  })
})
