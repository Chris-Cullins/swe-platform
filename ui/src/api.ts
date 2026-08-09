import type { CreateRun, Environment, PortalServiceList, Problem, Run, RunList, RunSummaryList, Session } from './contracts'

export class ApiProblem extends Error {
  constructor(public readonly problem: Problem, public readonly status: number) {
    super(problem.detail || problem.title)
    this.name = 'ApiProblem'
  }
}

export class ResourceIdentityMismatch extends Error {
  constructor(resource: 'Run' | 'Environment') {
    super(`Control plane returned a different ${resource} identity`)
    this.name = 'ResourceIdentityMismatch'
  }
}

export class ResourceTransportError extends Error {
  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : 'Resource transport failed', { cause })
    this.name = 'ResourceTransportError'
  }
}

export function terminalRunIdentityError(error: unknown) {
  return error instanceof ResourceIdentityMismatch || error instanceof ApiProblem && (error.status === 404 || error.status === 409)
}

export function terminalEnvironmentIdentityError(error: unknown) {
  return error instanceof ResourceIdentityMismatch || error instanceof ApiProblem && error.status === 404
}

export function transientResourceError(error: unknown) {
  if (error instanceof ResourceTransportError) return true
  if (error instanceof ApiProblem) return error.status === 408 || error.status === 429 || error.status >= 500
  return false
}

export function retryTransientResourceError(failureCount: number, error: unknown) {
  return transientResourceError(error) && failureCount < 2
}

type UnauthorizedListener = () => void
const unauthorizedListeners = new Set<UnauthorizedListener>()
export const onUnauthorized = (listener: UnauthorizedListener) => {
  unauthorizedListeners.add(listener)
  return () => { unauthorizedListeners.delete(listener) }
}

export const notifyUnauthorized = () => unauthorizedListeners.forEach(listener => listener())

function asProblem(value: unknown, response: Response): Problem {
  const fallback = {
    type: 'about:blank',
    title: response.statusText || `Request failed (${response.status})`,
    status: response.status,
  }
  if (!value || typeof value !== 'object') return fallback
  const candidate = value as Record<string, unknown>
  return {
    type: typeof candidate.type === 'string' ? candidate.type : fallback.type,
    title: typeof candidate.title === 'string' ? candidate.title : fallback.title,
    status: typeof candidate.status === 'number' ? candidate.status : fallback.status,
    ...(typeof candidate.detail === 'string' ? { detail: candidate.detail } : {}),
  }
}

async function request<T>(path: string, init: RequestInit = {}, options?: { token?: string; notifyUnauthorized?: boolean }): Promise<T> {
  const headers = new Headers(init.headers)
  if (init.body !== undefined) headers.set('Content-Type', 'application/json')
  if (options?.token !== undefined) headers.set('Authorization', `Bearer ${options.token}`)
  let response: Response
  try {
    response = await fetch(path, { ...init, headers, credentials: 'same-origin' })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new ResourceTransportError(error)
  }
  if (!response.ok) {
    let body: unknown
    try { body = await response.json() } catch { body = undefined }
    if (response.status === 401 && options?.notifyUnauthorized !== false) {
      notifyUnauthorized()
    }
    throw new ApiProblem(asProblem(body, response), response.status)
  }
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

const base = (namespace: string) => `/api/v1/namespaces/${encodeURIComponent(namespace)}`

export interface RunListOptions { limit?: number; continue?: string; signal?: AbortSignal }
const MAX_RUN_LIST_PAGES = 100

export const api = {
  session: () => request<Session>('/api/v1/session'),
  login: (token: string) => request<Session>('/api/v1/session', { method: 'POST' }, { token, notifyUnauthorized: false }),
  logout: () => request<void>('/api/v1/session', { method: 'DELETE' }),
  runs: (namespace: string, options: RunListOptions = {}) => {
    const query = new URLSearchParams()
    if (options.limit !== undefined) {
      if (!Number.isInteger(options.limit) || options.limit < 1 || options.limit > 200) throw new RangeError('limit must be an integer from 1 to 200')
      query.set('limit', String(options.limit))
    }
    if (options.continue) query.set('continue', options.continue)
    const suffix = query.size ? `?${query}` : ''
    return request<RunList>(`${base(namespace)}/runs${suffix}`)
  },
  runSummaries: (namespace: string, options: RunListOptions = {}) => {
    const query = new URLSearchParams({ limit: String(options.limit ?? 200), view: 'summary' })
    if (options.continue) query.set('continue', options.continue)
    return request<RunSummaryList>(`${base(namespace)}/runs?${query}`, { signal: options.signal })
  },
  watchRunSummaries: (namespace: string, resourceVersion: string, signal: AbortSignal, lastEventID?: string) => {
    const query = new URLSearchParams({ watch: 'true', view: 'summary', resourceVersion })
    const headers: Record<string, string> = { Accept: 'text/event-stream' }
    if (lastEventID) headers['Last-Event-ID'] = lastEventID
    return fetch(`${base(namespace)}/runs?${query}`, {
      headers, credentials: 'same-origin', signal,
    })
  },
  run: (namespace: string, name: string, signal?: AbortSignal) => request<Run>(`${base(namespace)}/runs/${encodeURIComponent(name)}`, { signal }),
  runExact: async (namespace: string, name: string, runUID: string, signal?: AbortSignal) => {
    const run = await request<Run>(`${base(namespace)}/runs/${encodeURIComponent(name)}`, { headers: { 'SWE-Run-UID': runUID }, signal })
    if (run.uid !== runUID) throw new ResourceIdentityMismatch('Run')
    return run
  },
  createRun: (namespace: string, value: CreateRun) => request<Run>(`${base(namespace)}/runs`, { method: 'POST', body: JSON.stringify(value) }),
  cancelRun: (namespace: string, name: string, runUID: string) => request<Run>(`${base(namespace)}/runs/${encodeURIComponent(name)}/cancel`, { method: 'POST', body: JSON.stringify({ runUID }) }),
  environment: (namespace: string, name: string) => request<Environment>(`${base(namespace)}/environments/${encodeURIComponent(name)}`),
  environmentExact: async (namespace: string, name: string, environmentUID: string, signal?: AbortSignal) => {
    const environment = await request<Environment>(`${base(namespace)}/environments/${encodeURIComponent(name)}`, { signal })
    if (environment.uid !== environmentUID) throw new ResourceIdentityMismatch('Environment')
    return environment
  },
  portals: (namespace: string, run: string, runUID: string, environmentUID: string) => request<PortalServiceList>(`${base(namespace)}/runs/${encodeURIComponent(run)}/portals/${encodeURIComponent(runUID)}/${encodeURIComponent(environmentUID)}`),
  transcriptUrl: (namespace: string, name: string) => `${base(namespace)}/runs/${encodeURIComponent(name)}/transcript`,
  transcript: async (namespace: string, name: string, runUID: string, signal: AbortSignal, lastEventID?: string) => {
    const headers: Record<string, string> = { Accept: 'text/event-stream', 'SWE-Run-UID': runUID }
    if (lastEventID) headers['Last-Event-ID'] = lastEventID
    const response = await fetch(`${base(namespace)}/runs/${encodeURIComponent(name)}/transcript`, {
      headers, credentials: 'same-origin', signal,
    })
    if (response.status === 401) notifyUnauthorized()
    return response
  },
  terminalPath: (namespace: string, run: string, runUID: string, environmentUID: string) => `${base(namespace)}/runs/${encodeURIComponent(run)}/terminal/${encodeURIComponent(runUID)}/${encodeURIComponent(environmentUID)}`,
}

export async function listAllRuns(namespace: string): Promise<RunList> {
  const items: Run[] = []
  const seenCursors = new Set<string>()
  let cursor: string | undefined
  for (let pageNumber = 0; pageNumber < MAX_RUN_LIST_PAGES; pageNumber += 1) {
    const page = await api.runs(namespace, { limit: 200, ...(cursor ? { continue: cursor } : {}) })
    items.push(...page.items)
    if (!page.continue || seenCursors.has(page.continue)) return { items }
    seenCursors.add(page.continue)
    cursor = page.continue
  }
  return { items, continue: cursor }
}

const terminalStates = new Set(['Succeeded', 'Failed', 'Cancelled'])
export const isTerminal = (state?: string) => !!state && terminalStates.has(state)
export const fallbackPollInterval = 4000

export const queryKeys = {
  session: ['session'] as const,
  runs: (namespace: string) => ['runs', namespace] as const,
  runPrefix: (namespace: string, name: string) => ['run', namespace, name] as const,
  run: (namespace: string, name: string, uid: string) => ['run', namespace, name, uid] as const,
  runBootstrap: (namespace: string, name: string, request: string) => ['run-bootstrap', namespace, name, request] as const,
  environment: (namespace: string, name: string, uid: string) => ['environment', namespace, name, uid] as const,
  portals: (namespace: string, run: string, runUID: string, environmentUID: string) => ['portals', namespace, run, runUID, environmentUID] as const,
}
