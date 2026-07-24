import React from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api, ApiProblem, fallbackPollInterval, notifyUnauthorized, queryKeys } from './api'
import type { Run, RunSummary, RunSummaryList, RunWatchEvent } from './contracts'

const MAX_PAGES = 100
const MAX_SNAPSHOT_ATTEMPTS = 3
const MAX_EVENT_BYTES = 64 * 1024
const FALLBACK_STATUSES = new Set([404, 405, 501])
class Relisted extends Error {}

export async function listRunSnapshot(namespace: string, signal?: AbortSignal): Promise<RunSummaryList> {
  for (let attempt = 0; attempt < MAX_SNAPSHOT_ATTEMPTS; attempt += 1) {
    signal?.throwIfAborted()
    try {
      const snapshot = await listRunSnapshotAttempt(namespace, signal)
      signal?.throwIfAborted()
      if (snapshot) return snapshot
    } catch (error) {
      if (!(error instanceof ApiProblem) || error.status !== 410) throw error
    }
  }
  throw new Error(`Run snapshot did not remain consistent after ${MAX_SNAPSHOT_ATTEMPTS} attempts.`)
}

async function listRunSnapshotAttempt(namespace: string, signal?: AbortSignal): Promise<RunSummaryList | undefined> {
  const items: RunSummary[] = []
  const cursors = new Set<string>()
  let cursor: string | undefined
  let resourceVersion: string | undefined
  for (let page = 0; page < MAX_PAGES; page += 1) {
    const result = await api.runSummaries(namespace, { limit: 200, ...(cursor ? { continue: cursor } : {}), signal })
    signal?.throwIfAborted()
    if (page > 0 && result.resourceVersion !== resourceVersion) return undefined
    resourceVersion ||= result.resourceVersion
    items.push(...result.items.map(item => {
      const legacy = item as RunSummary & Partial<Run>
      return legacy.agent !== undefined ? item : {
        name: legacy.name, uid: legacy.uid, generation: legacy.generation || 0, createdAt: legacy.createdAt,
        agent: legacy.intent?.agent || '', promptPreview: legacy.intent?.prompt || '',
        cancelRequested: legacy.cancelRequested, state: legacy.state, environment: legacy.environment,
      }
    }))
    if (!result.continue) return { items, resourceVersion }
    if (cursors.has(result.continue)) throw new Error('Run snapshot repeated a continuation cursor.')
    cursors.add(result.continue)
    cursor = result.continue
  }
  throw new Error('Run snapshot exceeded the pagination limit.')
}

type SSE = { event: string; data: string; id?: string }
export async function consumeSSE(response: Response, handler: (event: SSE) => Promise<void> | void, signal: AbortSignal) {
  if (!response.body) throw new Error('Run watch response has no body.')
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  try {
    while (!signal.aborted) {
      const { value, done } = await reader.read()
      buffer += decoder.decode(value, { stream: !done }).replace(/\r\n/g, '\n')
      if (new TextEncoder().encode(buffer).length > MAX_EVENT_BYTES && !buffer.includes('\n\n')) throw new Error('Run watch event exceeds 64 KiB.')
      let boundary: number
      while ((boundary = buffer.indexOf('\n\n')) >= 0) {
        const block = buffer.slice(0, boundary); buffer = buffer.slice(boundary + 2)
        if (new TextEncoder().encode(block).length > MAX_EVENT_BYTES) throw new Error('Run watch event exceeds 64 KiB.')
        let event = 'message'; let data = ''; let id: string | undefined
        for (const line of block.split('\n')) {
          if (line.startsWith('event:')) event = line.slice(6).trimStart()
          else if (line.startsWith('data:')) data += `${data ? '\n' : ''}${line.slice(5).trimStart()}`
          else if (line.startsWith('id:')) id = line.slice(3).trimStart()
        }
        await handler({ event, data, id })
      }
      if (done) return
    }
  } finally {
    try { await reader.cancel() } catch { /* the stream may already be closed or aborted */ }
    reader.releaseLock()
  }
}

export function applyRunEvent(snapshot: RunSummaryList, event: RunWatchEvent): RunSummaryList {
  const index = snapshot.items.findIndex(run => run.uid === event.run.uid)
  if (event.type === 'DELETED') return { ...snapshot, resourceVersion: event.resourceVersion, items: index < 0 ? snapshot.items : snapshot.items.filter(run => run.uid !== event.run.uid) }
  const items = snapshot.items.filter(run => run.name !== event.run.name || run.uid === event.run.uid)
  const retainedIndex = items.findIndex(run => run.uid === event.run.uid)
  if (retainedIndex < 0) items.push(event.run); else items[retainedIndex] = event.run
  return { ...snapshot, resourceVersion: event.resourceVersion, items }
}

function refreshMatchingDetail(queryClient: ReturnType<typeof useQueryClient>, namespace: string, summary: RunSummary) {
  const detail = queryClient.getQueryData<Run>(queryKeys.run(namespace, summary.name))
  if (!detail) return
  if (detail.uid !== summary.uid) {
    void queryClient.invalidateQueries({ queryKey: queryKeys.run(namespace, summary.name), exact: true })
    return
  }
  if (detail.generation !== undefined && summary.generation !== undefined && detail.generation > summary.generation) return
  void queryClient.invalidateQueries({ queryKey: queryKeys.run(namespace, summary.name), exact: true })
}

export function useRunFeed(namespace: string) {
  const queryClient = useQueryClient()
  const [fallback, setFallback] = React.useState(false)
  const [watchError, setWatchError] = React.useState<Error>()
  const query = useQuery({
    queryKey: queryKeys.runs(namespace), queryFn: ({ signal }) => listRunSnapshot(namespace, signal),
    refetchInterval: fallback ? fallbackPollInterval : false,
    staleTime: fallback ? 0 : Infinity,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  })
  React.useEffect(() => { setFallback(false); setWatchError(undefined) }, [namespace])
  React.useEffect(() => {
    const snapshot = queryClient.getQueryData<RunSummaryList>(queryKeys.runs(namespace))
    const resourceVersion = snapshot?.resourceVersion
    if (!snapshot || fallback) return
    if (!resourceVersion) { setFallback(true); return }
    const controller = new AbortController()
    const unsubscribe = queryClient.getQueryCache().subscribe(event => {
      const key = event.query.queryKey
      if (event.type === 'removed' && key[0] === 'runs' && key[1] === namespace) controller.abort()
    })
    let cursor = resourceVersion
    let watchStart = resourceVersion
    let resume = false
    let everConnected = false
    let retry: ReturnType<typeof setTimeout> | undefined
    const relist = async () => {
      const snapshot = await listRunSnapshot(namespace, controller.signal)
      if (controller.signal.aborted) return false
      if (!snapshot.resourceVersion) { setFallback(true); controller.abort(); return false }
      if (controller.signal.aborted) return false
      queryClient.setQueryData(queryKeys.runs(namespace), snapshot)
      cursor = snapshot.resourceVersion
      watchStart = snapshot.resourceVersion
      return true
    }
    const connect = async () => {
      while (!controller.signal.aborted) {
        try {
          const response = await api.watchRunSummaries(namespace, watchStart, controller.signal, resume ? cursor : undefined)
          if (!response.ok) {
            if (!everConnected && FALLBACK_STATUSES.has(response.status)) { setFallback(true); return }
            if (response.status === 401) notifyUnauthorized()
            if (response.status === 410) { resume = false; if (await relist()) continue; return }
            throw new ApiProblem({ type: 'about:blank', title: response.statusText || `Run watch failed (${response.status})`, status: response.status }, response.status)
          }
          everConnected = true; resume = true; setWatchError(undefined)
          await consumeSSE(response, async message => {
            if (message.event === 'run-relist' && !message.id) { resume = false; if (await relist()) throw new Relisted(); return }
            if (message.event === 'run-checkpoint') {
              const checkpoint = JSON.parse(message.data) as { resourceVersion: string }
              if (!message.id || checkpoint.resourceVersion !== message.id) throw new Error('Invalid Run watch checkpoint.')
              cursor = checkpoint.resourceVersion
              queryClient.setQueryData<RunSummaryList>(queryKeys.runs(namespace), old => old ? { ...old, resourceVersion: cursor } : old)
              return
            }
            if (message.event !== 'run') return
            const event = JSON.parse(message.data) as RunWatchEvent
            if (!message.id || event.resourceVersion !== message.id) throw new Error('Invalid Run watch event cursor.')
            queryClient.setQueryData<RunSummaryList>(queryKeys.runs(namespace), old => old ? applyRunEvent(old, event) : old)
            refreshMatchingDetail(queryClient, namespace, event.run)
            cursor = event.resourceVersion
          }, controller.signal)
        } catch (error) {
          if (controller.signal.aborted) return
          if (error instanceof Relisted) continue
          setWatchError(error instanceof Error ? error : new Error('Run watch failed.'))
        }
        await new Promise<void>(resolve => { retry = setTimeout(resolve, 1000) })
      }
    }
    void connect()
    return () => { unsubscribe(); controller.abort(); if (retry) clearTimeout(retry) }
  }, [fallback, namespace, query.isSuccess, queryClient])
  return { ...query, fallback, watchError }
}
