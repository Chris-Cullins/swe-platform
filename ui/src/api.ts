import type { CreateRun, Environment, Problem, Run, RunList, Session } from './contracts'

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
      unauthorizedListeners.forEach(listener => listener())
    }
    throw new ApiProblem(asProblem(body, response), response.status)
  }
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

const base = (namespace: string) => `/api/v1/namespaces/${encodeURIComponent(namespace)}`

export interface RunListOptions { limit?: number; continue?: string }

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
  run: (namespace: string, name: string) => request<Run>(`${base(namespace)}/runs/${encodeURIComponent(name)}`),
  createRun: (namespace: string, value: CreateRun) => request<Run>(`${base(namespace)}/runs`, { method: 'POST', body: JSON.stringify(value) }),
  cancelRun: (namespace: string, name: string) => request<Run>(`${base(namespace)}/runs/${encodeURIComponent(name)}/cancel`, { method: 'POST' }),
  environment: (namespace: string, name: string) => request<Environment>(`${base(namespace)}/environments/${encodeURIComponent(name)}`),
  transcriptUrl: (namespace: string, name: string) => `${base(namespace)}/runs/${encodeURIComponent(name)}/transcript`,
  terminalPath: (namespace: string, environment: string) => `${base(namespace)}/environments/${encodeURIComponent(environment)}/terminal`,
}

const terminalStates = new Set(['Succeeded', 'Failed', 'Cancelled'])
export const isTerminal = (state?: string) => !!state && terminalStates.has(state)
export const runPollInterval = (run?: Run) => run && !isTerminal(run.state) ? 4000 : false
export const listPollInterval = (list?: RunList) => Array.isArray(list?.items) && list.items.some(run => !isTerminal(run.state)) ? 4000 : false

export const queryKeys = {
  session: ['session'] as const,
  runs: (namespace: string) => ['runs', namespace] as const,
  run: (namespace: string, name: string) => ['run', namespace, name] as const,
  environment: (namespace: string, name: string) => ['environment', namespace, name] as const,
}
