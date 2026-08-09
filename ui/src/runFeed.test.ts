import { QueryClient, QueryObserver } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { api, ApiProblem } from './api'
import type { RunSummary, RunSummaryList, RunWatchEvent } from './contracts'
import { applyRunEvent, consumeSSE, listRunSnapshot, reconcileRunSnapshot, reconcileRunWatchPublication, refreshMatchingDetail, waitForRunFeedRetry } from './runFeed'

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
      'event: run-relist\ndata: {"reason":"resource-version-expired"}\n\n',
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
    const key = ['run', 'ns', 'one', 'one-uid']
    const observer = new QueryObserver(client, { queryKey: key, queryFn, retry: false })
    const unsubscribe = observer.subscribe(() => undefined)
    await vi.waitFor(() => expect(queryFn).toHaveBeenCalledOnce())
    refreshMatchingDetail(client, 'ns', summary('one'))
    await vi.waitFor(() => expect(queryFn).toHaveBeenCalledTimes(2))
    expect(firstSignal?.aborted).toBe(true)
    resolveFirst({ uid: 'stale-uid', generation: 0 })
    await vi.waitFor(() => expect(client.getQueryData(key)).toEqual({ uid: 'one-uid', generation: 1 }))
    unsubscribe()
  })

  it('uses distinct live and fallback snapshot publication semantics on concrete UID keys', async () => {
    const client = new QueryClient()
    const match = vi.fn().mockResolvedValue({ uid: 'one-uid', generation: 1 })
    const replaced = vi.fn().mockResolvedValue({ uid: 'old-uid', generation: 1 })
    const matchObserver = new QueryObserver(client, { queryKey: ['run', 'ns', 'one', 'one-uid'], queryFn: match, retry: false, staleTime: Infinity })
    const replacedObserver = new QueryObserver(client, { queryKey: ['run', 'ns', 'old', 'old-uid'], queryFn: replaced, retry: false, staleTime: Infinity })
    const unsubscribeMatch = matchObserver.subscribe(() => undefined)
    const unsubscribeReplaced = replacedObserver.subscribe(() => undefined)
    await vi.waitFor(() => expect(match).toHaveBeenCalledOnce())
    await vi.waitFor(() => expect(replaced).toHaveBeenCalledOnce())
    match.mockClear(); replaced.mockClear()
    replaced.mockRejectedValue(new ApiProblem({ type: 'conflict', title: 'replacement', status: 409 }, 409))

    reconcileRunSnapshot(client, 'ns', { items: [summary('one')], resourceVersion: undefined }, false)
    await vi.waitFor(() => expect(replaced).toHaveBeenCalledOnce())
    expect(match).not.toHaveBeenCalled()

    reconcileRunSnapshot(client, 'ns', { items: [summary('one')], resourceVersion: '2' }, true)
    await vi.waitFor(() => expect(match).toHaveBeenCalledOnce())
    expect(client.getQueryData(['run', 'ns', 'one'])).toBeUndefined()
    unsubscribeMatch(); unsubscribeReplaced()
  })

  it('settles current deletes once while a stale delete never touches the replacement UID', async () => {
    const client = new QueryClient()
    const old = vi.fn().mockRejectedValue(new ApiProblem({ type: 'gone', title: 'gone', status: 404 }, 404))
    const replacement = vi.fn().mockResolvedValue({ uid: 'replacement-uid', generation: 1 })
    const oldObserver = new QueryObserver(client, { queryKey: ['run', 'ns', 'one', 'old-uid'], queryFn: old, retry: false })
    const replacementObserver = new QueryObserver(client, { queryKey: ['run', 'ns', 'one', 'replacement-uid'], queryFn: replacement, retry: false })
    const unsubscribeOld = oldObserver.subscribe(() => undefined)
    const unsubscribeReplacement = replacementObserver.subscribe(() => undefined)
    await vi.waitFor(() => expect(old).toHaveBeenCalledOnce())
    await vi.waitFor(() => expect(replacement).toHaveBeenCalledOnce())
    old.mockClear(); replacement.mockClear()
    replacement.mockRejectedValue(new ApiProblem({ type: 'gone', title: 'gone', status: 404 }, 404))

    const staleDelete: RunWatchEvent = { type: 'DELETED', resourceVersion: '3', run: summary('one', 'old-uid') }
    reconcileRunWatchPublication(client, 'ns', staleDelete, { items: [summary('one', 'replacement-uid')], resourceVersion: '3' })
    await Promise.resolve()
    expect(old).not.toHaveBeenCalled()
    expect(replacement).not.toHaveBeenCalled()

    const currentDelete: RunWatchEvent = { type: 'DELETED', resourceVersion: '4', run: summary('one', 'replacement-uid') }
    reconcileRunWatchPublication(client, 'ns', currentDelete, { items: [], resourceVersion: '4' })
    await vi.waitFor(() => expect(replacement).toHaveBeenCalledOnce())
    reconcileRunWatchPublication(client, 'ns', currentDelete, { items: [], resourceVersion: '4' })
    await Promise.resolve()
    expect(replacement).toHaveBeenCalledOnce()
    unsubscribeOld(); unsubscribeReplacement()
  })

  it('serializes a delete behind an in-flight modification and settles the deleted identity', async () => {
    const client = new QueryClient()
    let resolveModified!: (value: { uid: string; generation: number }) => void
    const queryFn = vi.fn()
      .mockResolvedValueOnce({ uid: 'one-uid', generation: 1 })
      .mockImplementationOnce(() => new Promise(resolve => { resolveModified = resolve }))
      .mockRejectedValueOnce(new ApiProblem({ type: 'gone', title: 'gone', status: 404 }, 404))
    const key = ['run', 'ns', 'one', 'one-uid'] as const
    const observer = new QueryObserver(client, { queryKey: key, queryFn, retry: false })
    const unsubscribe = observer.subscribe(() => undefined)
    await vi.waitFor(() => expect(queryFn).toHaveBeenCalledOnce())

    const modified: RunWatchEvent = { type: 'MODIFIED', resourceVersion: '2', run: { ...summary('one'), generation: 2 } }
    reconcileRunWatchPublication(client, 'ns', modified, { items: [modified.run], resourceVersion: '2' })
    await vi.waitFor(() => expect(queryFn).toHaveBeenCalledTimes(2))
    const deleted: RunWatchEvent = { type: 'DELETED', resourceVersion: '3', run: modified.run }
    reconcileRunWatchPublication(client, 'ns', deleted, { items: [], resourceVersion: '3' })
    await Promise.resolve()
    expect(queryFn).toHaveBeenCalledTimes(2)

    resolveModified({ uid: 'one-uid', generation: 2 })
    await vi.waitFor(() => expect(queryFn).toHaveBeenCalledTimes(3))
    await vi.waitFor(() => expect(client.getQueryState(key)?.error).toBeInstanceOf(ApiProblem))
    reconcileRunWatchPublication(client, 'ns', deleted, { items: [], resourceVersion: '3' })
    await Promise.resolve()
    expect(queryFn).toHaveBeenCalledTimes(3)
    unsubscribe()
  })

  it('does not refresh a newer detail from an older generation', () => {
    const client = new QueryClient()
    client.setQueryData(['run', 'ns', 'one', 'one-uid'], { uid: 'one-uid', generation: 2 })
    const cancel = vi.spyOn(client, 'cancelQueries')
    refreshMatchingDetail(client, 'ns', { ...summary('one'), generation: 1 })
    expect(cancel).not.toHaveBeenCalled()
  })

  it('settles a pending reconnect delay immediately when its feed is aborted', async () => {
    vi.useFakeTimers()
    const controller = new AbortController()
    let settled = false
    const waiting = waitForRunFeedRetry(controller.signal).then(() => { settled = true })
    expect(vi.getTimerCount()).toBe(1)
    controller.abort()
    await waiting
    expect(settled).toBe(true)
    expect(vi.getTimerCount()).toBe(0)
    vi.useRealTimers()
  })
})
