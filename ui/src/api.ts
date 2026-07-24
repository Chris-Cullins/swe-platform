import type { CreateRun, Environment, Problem, Run, RunList, RunSummaryList, Session } from './contracts'

export class ApiProblem extends Error {
  constructor(public readonly problem: Problem, public readonly status: number) {
    super(problem.detail || problem.title)
    this.name = 'ApiProblem'
  }
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
  const response = await fetch(path, { ...init, headers, credentials: 'same-origin' })
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
  run: (namespace: string, name: string) => request<Run>(`${base(namespace)}/runs/${encodeURIComponent(name)}`),
  createRun: (namespace: string, value: CreateRun) => request<Run>(`${base(namespace)}/runs`, { method: 'POST', body: JSON.stringify(value) }),
  cancelRun: (namespace: string, name: string, runUID: string) => request<Run>(`${base(namespace)}/runs/${encodeURIComponent(name)}/cancel`, { method: 'POST', body: JSON.stringify({ runUID }) }),
  environment: (namespace: string, name: string) => request<Environment>(`${base(namespace)}/environments/${encodeURIComponent(name)}`),
  transcriptUrl: (namespace: string, name: string) => `${base(namespace)}/runs/${encodeURIComponent(name)}/transcript`,
  terminalPath: (namespace: string, environment: string) => `${base(namespace)}/environments/${encodeURIComponent(environment)}/terminal`,
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
  run: (namespace: string, name: string) => ['run', namespace, name] as const,
  environment: (namespace: string, name: string) => ['environment', namespace, name] as const,
}
