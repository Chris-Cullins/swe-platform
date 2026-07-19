import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { App } from './App'
import type { Environment, Run } from './contracts'

const run: Run = {
  name: 'repair-ui', uid: 'run-uid', createdAt: '2026-07-19T12:00:00Z',
  intent: { selector: { project: 'platform', template: 'small' }, agent: 'amp', prompt: 'Repair UI' },
  cancelRequested: false, state: 'Running', environment: { name: 'repair-env', ownership: 'Owned' }, branch: 'agent/repair',
  usage: { cpuSeconds: 12.5, tokensIn: 101, tokensOut: 202 },
}
const environment: Environment = { name: 'repair-env', uid: 'env-uid', createdAt: '2026-07-19T12:00:01Z', project: 'platform', template: 'small', backend: 'pod', paused: false, phase: 'Running', ready: true }
const response = (body: unknown, status = 200) => new Response(status === 204 ? null : JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
function show(path: string) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  return { client, ...render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[path]}><App /></MemoryRouter></QueryClientProvider>) }
}

afterEach(() => vi.restoreAllMocks())
describe('App frozen API integration', () => {
  it('redirects a session 401 to login', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(response({ type: 'auth', title: 'Unauthorized', status: 401 }, 401))
    show('/namespaces/default/runs')
    expect(await screen.findByRole('heading', { name: 'SWE Operations' })).toBeInTheDocument()
  })

  it('uses the exact encoded namespace list API and renders accessible empty state', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => path === '/api/v1/session' ? response({ authenticated: true, username: 'alex' }) : response({ items: [] }))
    show('/namespaces/team%2Fa/runs')
    expect(await screen.findByText('No runs found.')).toHaveAttribute('role', 'status')
    expect(fetch).toHaveBeenCalledWith('/api/v1/namespaces/team%2Fa/runs', expect.anything())
  })

  it('shows accessible loading and problem error states', async () => {
    let resolveRuns!: (value: Response) => void
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      return new Promise<Response>(resolve => { resolveRuns = resolve })
    })
    show('/namespaces/default/runs')
    expect(await screen.findByText('Loading runs…')).toHaveAttribute('role', 'status')
    resolveRuns(response({ type: 'https://swe-platform.dev/problems/forbidden', title: 'Access denied', status: 403, detail: 'Missing list permission' }, 403))
    expect(await screen.findByRole('alert')).toHaveTextContent('Missing list permission')
  })

  it('returns to login when an ordinary API request reports an expired session', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => path === '/api/v1/session'
      ? response({ authenticated: true, username: 'alex' })
      : response({ type: 'https://swe-platform.dev/problems/unauthenticated', title: 'Session expired', status: 401 }, 401))
    const { client } = show('/namespaces/default/runs')
    client.setQueryData(['prior-user-data'], { prompt: 'secret task' })
    expect(await screen.findByLabelText('Access token')).toBeInTheDocument()
    expect(client.getQueryData(['prior-user-data'])).toBeUndefined()
  })

  it('renders exact Run usage, operational facts, environment status and ownership', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (String(path).includes('/environments/')) return response(environment)
      return response(run)
    })
    show('/namespaces/default/runs/repair-ui/overview')
    expect(await screen.findByRole('heading', { name: 'Operational conditions' })).toBeInTheDocument()
    await waitFor(() => expect(screen.getByText('Ready, active')).toBeInTheDocument())
    expect(screen.getByText('12.5')).toBeInTheDocument()
    expect(screen.getByText('101')).toBeInTheDocument()
    expect(screen.getByText('202')).toBeInTheDocument()
    expect(screen.getByText('Owned')).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: /changes/i })).not.toBeInTheDocument()
  })

  it('clears login token after an error and never accesses browser storage', async () => {
    const localGet = vi.spyOn(Storage.prototype, 'getItem')
    const localSet = vi.spyOn(Storage.prototype, 'setItem')
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(response({ type: 'auth', title: 'Bad token', status: 401 }, 401))
    show('/login')
    const token = screen.getByLabelText('Access token')
    await userEvent.type(token, 'super-secret')
    await userEvent.click(screen.getByRole('button', { name: 'Sign in' }))
    await screen.findByRole('alert')
    expect(token).toHaveValue('')
    expect(localGet).not.toHaveBeenCalled(); expect(localSet).not.toHaveBeenCalled()
  })

  it('creates the exact selector contract and invalidates the namespace feed', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs' && init?.method === 'POST') return response(run, 201)
      if (path === '/api/v1/namespaces/default/runs/repair-ui') return response(run)
      return response({ items: [] })
    })
    const { client } = show('/namespaces/default/runs/new')
    const invalidate = vi.spyOn(client, 'invalidateQueries')
    await screen.findByRole('heading', { name: 'New run' })
    await userEvent.type(screen.getByLabelText('Name'), 'repair-ui')
    await userEvent.clear(screen.getByLabelText('Agent'))
    await userEvent.type(screen.getByLabelText('Agent'), 'amp')
    await userEvent.type(screen.getByLabelText('Prompt / task'), '  Repair UI  ')
    await userEvent.type(screen.getByLabelText('Project reference'), 'platform')
    await userEvent.type(screen.getByLabelText('Template reference'), 'small')
    await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
    await waitFor(() => expect(fetch.mock.calls.some(call => call[0] === '/api/v1/namespaces/default/runs' && call[1]?.method === 'POST')).toBe(true))
    const createInit = fetch.mock.calls.find(call => call[0] === '/api/v1/namespaces/default/runs' && call[1]?.method === 'POST')?.[1]
    expect(JSON.parse(String(createInit?.body))).toEqual({
      name: 'repair-ui', selector: { project: 'platform', template: 'small' }, agent: 'amp', prompt: '  Repair UI  ',
    })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['runs', 'default'] })
    expect(await screen.findByRole('heading', { name: 'repair-ui' })).toBeInTheDocument()
  })

  it('cancels with an empty POST and invalidates list and detail data', async () => {
    const cancelled = { ...run, cancelRequested: true }
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (String(path).includes('/environments/')) return response(environment)
      if (String(path).endsWith('/cancel') && init?.method === 'POST') return response(cancelled)
      return response(run)
    })
    const { client } = show('/namespaces/default/runs/repair-ui/overview')
    const invalidate = vi.spyOn(client, 'invalidateQueries')
    await userEvent.click(await screen.findByRole('button', { name: 'Cancel run' }))
    await waitFor(() => expect(fetch).toHaveBeenCalledWith('/api/v1/namespaces/default/runs/repair-ui/cancel', expect.objectContaining({ method: 'POST' })))
    const cancelInit = fetch.mock.calls.find(call => String(call[0]).endsWith('/cancel'))?.[1]
    expect(cancelInit).not.toHaveProperty('body')
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['run', 'default', 'repair-ui'] })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['runs', 'default'] })
  })
})
