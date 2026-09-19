import { focusManager, onlineManager, QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, useLocation } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { App } from './App'
import { queryKeys } from './api'
import type { Environment, Run, RunSummary, RunWatchEvent } from './contracts'

const run: Run = {
  name: 'repair-ui', uid: 'run-uid', generation: 1, createdAt: '2026-07-19T12:00:00Z',
  intent: { selector: { project: 'platform', template: 'small' }, agent: 'amp', prompt: 'Repair UI', credentialProfile: 'amp-production' },
  cancelRequested: false, state: 'Running', startedAt: '2026-07-19T12:01:00Z', environment: { name: 'repair-env', uid: 'env-uid', ownership: 'Owned' }, terminalAvailable: true, branch: 'agent/repair',
  usage: { cpuSeconds: 12.5, tokensIn: 101, tokensOut: 202 },
}
const environment: Environment = { name: 'repair-env', uid: 'env-uid', createdAt: '2026-07-19T12:00:01Z', project: 'platform', template: 'small', backend: 'pod', paused: false, phase: 'Running', ready: true }
const response = (body: unknown, status = 200) => new Response(status === 204 ? null : JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
function LocationProbe() {
  const location = useLocation()
  return <output data-testid="location">{`${location.pathname}${location.search}${location.hash}`}</output>
}
function show(path: string, state?: unknown, providedClient?: QueryClient) {
  const client = providedClient || new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  const location = new URL(path, 'https://console.test')
  const entry = { pathname: location.pathname, search: location.search, hash: location.hash, state }
  return { client, ...render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[entry]}><LocationProbe /><App /></MemoryRouter></QueryClientProvider>) }
}

afterEach(() => { focusManager.setFocused(undefined); onlineManager.setOnline(true); vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllGlobals() })
describe('App frozen API integration', () => {
  it('lands on the default namespace Run feed from the root route', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => path === '/api/v1/session' ? response({ authenticated: true, username: 'alex' }) : response({ items: [] }))
    show('/')
    expect(await screen.findByText('No runs found.')).toBeInTheDocument()
    expect(screen.getByLabelText('Namespace')).toHaveValue('default')
    expect(screen.getByTestId('location')).toHaveTextContent('/namespaces/default/runs')
    expect(fetch).toHaveBeenCalledWith('/api/v1/namespaces/default/runs?limit=200&view=summary', expect.anything())
  })

  it('redirects a session 401 to login', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(response({ type: 'auth', title: 'Unauthorized', status: 401 }, 401))
    show('/namespaces/default/runs')
    expect(await screen.findByRole('heading', { name: 'SWE Operations' })).toBeInTheDocument()
  })

  it('defaults attention off and matches CLI membership for every state with both cancellation values', async () => {
    const cases: [string, boolean, boolean][] = [
      ['Failed', true, true], ['Succeeded', true, true], ['NeedsInput', true, false], ['Paused', true, false],
      ['Cancelled', false, false], ['Allocating', false, false], ['EnvironmentReady', false, false],
      ['AdapterAccepted', false, false], ['Running', false, false], ['', true, true], ['FutureState', true, true],
    ]
    const items = cases.flatMap(([state, uncancelled, cancelling], index) => [false, true].map(cancelRequested => ({
      ...run, name: `case-${index}-${cancelRequested}`, uid: `${index}-${cancelRequested}`, state, cancelRequested,
      agent: 'amp', promptPreview: 'Membership fixture', expected: cancelRequested ? cancelling : uncancelled,
    })))
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (String(path).includes('watch=true')) return new Response(new ReadableStream())
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items, resourceVersion: '1' })
      throw new Error(`Unexpected request: ${path}`)
    })
    show('/namespaces/default/runs')
    await screen.findByText(items[0].name)
    const checkbox = screen.getByRole('checkbox', { name: 'Attention candidates' })
    expect(checkbox).not.toBeChecked()
    expect(checkbox).toHaveAccessibleDescription(/reported state.*intentional pauses and retained successes.*not a review acknowledgement or an input channel/)
    expect(screen.getByRole('button', { name: 'Clear filters' })).toBeDisabled()
    for (const item of items) expect(screen.getByText(item.name)).toBeInTheDocument()
    await userEvent.click(checkbox)
    for (const item of items) expect(!!screen.queryByText(item.name)).toBe(item.expected)
    await userEvent.selectOptions(screen.getByLabelText('State'), 'Running')
    expect(screen.getByText('No runs match the filters.')).toBeInTheDocument()
    await userEvent.selectOptions(screen.getByLabelText('State'), 'Succeeded')
    for (const item of items) expect(!!screen.queryByText(item.name)).toBe(item.state === 'Succeeded')
    await userEvent.click(screen.getByRole('button', { name: 'Clear filters' }))
    expect(checkbox).not.toBeChecked()
    for (const item of items) expect(screen.getByText(item.name)).toBeInTheDocument()
    expect(fetch).toHaveBeenCalledTimes(3) // Session, one summary snapshot, one watch; no detail reads.
  })

  it('retains attention through live cancellation updates and reconnect without narrowing agent options', async () => {
    const paused: RunSummary = { ...run, name: 'paused-task', uid: 'paused', state: 'Paused', agent: 'amp', promptPreview: 'Paused task' }
    const active: RunSummary = { ...run, name: 'active-task', uid: 'active', agent: 'active-only', promptPreview: 'Active task' }
    let stream!: ReadableStreamDefaultController<Uint8Array>
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (String(path).includes('watch=true')) return new Response(new ReadableStream({ start(controller) { stream = controller } }))
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items: [paused, active], resourceVersion: '1' })
      throw new Error(`Unexpected request: ${path}`)
    })
    show('/namespaces/default/runs')
    await screen.findByText('paused-task')
    const checkbox = screen.getByRole('checkbox', { name: 'Attention candidates' })
    await userEvent.click(checkbox)
    expect(screen.queryByText('active-task')).not.toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'active-only' })).toBeInTheDocument()
    await act(async () => stream.enqueue(new TextEncoder().encode(`event: run\nid: 2\ndata: ${JSON.stringify({ type: 'MODIFIED', resourceVersion: '2', run: { ...paused, cancelRequested: true } })}\n\n`)))
    expect(await screen.findByText('No runs match the filters.')).toBeInTheDocument()
    vi.useFakeTimers()
    await act(async () => stream.error(new Error('Disconnected')))
    expect(screen.getByText('Live updates disconnected; reconnecting…')).toBeInTheDocument()
    expect(screen.getByText('No runs match the filters.')).toBeInTheDocument()
    expect(checkbox).toBeChecked()
    await act(async () => { await vi.advanceTimersByTimeAsync(1001) })
    expect(screen.queryByText('Live updates disconnected; reconnecting…')).not.toBeInTheDocument()
    expect(checkbox).toBeChecked()
    await act(async () => stream.enqueue(new TextEncoder().encode(`event: run\nid: 3\ndata: ${JSON.stringify({ type: 'MODIFIED', resourceVersion: '3', run: { ...paused, state: 'Succeeded', cancelRequested: true } })}\n\n`)))
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(screen.getByText('paused-task')).toBeInTheDocument()
    expect(fetch).toHaveBeenCalledTimes(4) // Only the existing watch reconnects; no relist or detail reads.
    expect(new Headers(fetch.mock.calls[3][1]?.headers).get('Last-Event-ID')).toBe('2')
  })

  it('filters the complete live feed with exact AND matches and preserves replacement UID navigation', async () => {
    const items: RunSummary[] = [
      { ...run, name: 'running-amp', uid: 'a', agent: 'amp', promptPreview: 'Running task' },
      { ...run, name: 'failed-codex', uid: 'b', state: 'Failed', agent: 'codex', promptPreview: 'Codex task' },
      { ...run, name: 'failed-amp', uid: 'c', state: 'Failed', agent: 'amp', promptPreview: 'Amp task' },
      { ...run, name: 'future', uid: 'd', state: 'FutureState', agent: 'future-agent', promptPreview: 'Future task' },
      { ...run, name: 'upper-amp', uid: 'e', state: 'Failed', agent: 'AMP', promptPreview: 'Case-sensitive agent' },
    ]
    let stream!: ReadableStreamDefaultController<Uint8Array>
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (String(path).includes('watch=true')) return new Response(new ReadableStream({ start(controller) { stream = controller } }))
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items: items.slice(0, 2), continue: 'next', resourceVersion: '1' })
      if (String(path).includes('continue=next')) return response({ items: items.slice(2), resourceVersion: '1' })
      if (path === '/api/v1/namespaces/default/runs/failed-amp') {
        expect(new Headers(init?.headers).get('SWE-Run-UID')).toBe('replacement-uid')
        return response({ ...run, name: 'failed-amp', uid: 'replacement-uid', state: 'Failed', environment: undefined })
      }
      throw new Error(`Unexpected request: ${path}`)
    })
    show('/namespaces/default/runs')
    await screen.findByText('future')
    expect(screen.getByText('FutureState')).toBeInTheDocument()
    await userEvent.click(screen.getByRole('checkbox', { name: 'Attention candidates' }))
    expect(screen.queryByText('running-amp')).not.toBeInTheDocument()
    await userEvent.selectOptions(screen.getByLabelText('State'), 'Failed')
    expect(screen.getByText('failed-codex')).toBeInTheDocument()
    expect(screen.queryByText('running-amp')).not.toBeInTheDocument()
    await userEvent.selectOptions(screen.getByLabelText('Agent'), 'amp')
    expect(screen.getByText('failed-amp')).toBeInTheDocument()
    expect(screen.queryByText('failed-codex')).not.toBeInTheDocument()
    expect(screen.queryByText('upper-amp')).not.toBeInTheDocument()
    let revision = 1
    const publish = async (type: RunWatchEvent['type'], summary: RunSummary) => {
      const resourceVersion = String(++revision)
      await act(async () => { stream.enqueue(new TextEncoder().encode(`event: run\nid: ${resourceVersion}\ndata: ${JSON.stringify({ type, resourceVersion, run: summary })}\n\n`)) })
    }
    await publish('MODIFIED', { ...items[2], state: 'Running' })
    expect(await screen.findByText('No runs match the filters.')).toBeInTheDocument()
    await publish('DELETED', items[2])
    await publish('DELETED', items[0])
    expect(screen.getByLabelText('Agent')).toHaveValue('amp')
    expect(screen.getByRole('option', { name: 'amp' })).toBeInTheDocument()
    expect(screen.queryByText('failed-codex')).not.toBeInTheDocument()
    for (const item of [items[1], items[3], items[4]]) await publish('DELETED', item)
    expect(await screen.findByText('No runs found.')).toBeInTheDocument()
    expect(screen.getByLabelText('Agent')).toHaveValue('amp')
    for (const item of [items[1], items[3]]) await publish('ADDED', item)
    expect(await screen.findByText('No runs match the filters.')).toBeInTheDocument()
    expect(screen.getByRole('checkbox', { name: 'Attention candidates' })).toBeChecked()
    await userEvent.click(screen.getByRole('button', { name: 'Clear filters' }))
    expect(screen.getByRole('checkbox', { name: 'Attention candidates' })).not.toBeChecked()
    expect(screen.getByLabelText('State')).toHaveValue('')
    expect(screen.getByLabelText('Agent')).toHaveValue('')
    expect(screen.getByText('failed-codex')).toBeInTheDocument()
    expect(screen.getByText('future')).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: 'amp' })).not.toBeInTheDocument()
    await publish('ADDED', { ...items[2], uid: 'replacement-uid' })
    await userEvent.selectOptions(screen.getByLabelText('State'), 'Failed')
    await userEvent.selectOptions(screen.getByLabelText('Agent'), 'amp')
    await userEvent.click(screen.getByRole('checkbox', { name: 'Attention candidates' }))
    expect(fetch).toHaveBeenCalledTimes(4) // Session, two pages, one watch; filters never fetch details.
    await userEvent.click(screen.getByRole('link', { name: /failed-amp/ }))
    expect(await screen.findByText('replacement-uid')).toBeInTheDocument()
    expect(fetch.mock.calls.filter(([path]) => String(path).includes('watch=true'))).toHaveLength(1)
  })

  it('switches to a valid namespace without leaking the previous namespace cache or filters', async () => {
    const otherRun = { ...run, name: 'argo-run', uid: 'argo-uid', intent: { ...run.intent, prompt: 'Argo namespace task' } }
    const lateRun = { ...run, name: 'late-default-run', uid: 'late-default-uid' }
    let defaultRequests = 0
    let resolveLateDefault!: (value: Response) => void
    let resolveOther!: (value: Response) => void
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') {
        defaultRequests += 1
        if (defaultRequests === 1) return response({ items: [run] })
        return new Promise<Response>(resolve => { resolveLateDefault = resolve })
      }
      if (path === '/api/v1/namespaces/swe-platform-system/runs?limit=200&view=summary') return new Promise<Response>(resolve => { resolveOther = resolve })
      throw new Error(`Unexpected request: ${path}`)
    })
    const { client } = show('/namespaces/default/runs')
    expect(await screen.findByText('repair-ui')).toBeInTheDocument()
    await userEvent.selectOptions(screen.getByLabelText('State'), 'Failed')
    await userEvent.selectOptions(screen.getByLabelText('Agent'), 'amp')
    await userEvent.click(screen.getByRole('checkbox', { name: 'Attention candidates' }))
    expect(screen.getByText('No runs match the filters.')).toBeInTheDocument()
    act(() => { void client.invalidateQueries({ queryKey: queryKeys.runs('default') }) })
    await waitFor(() => expect(defaultRequests).toBe(2))
    await userEvent.clear(screen.getByLabelText('Namespace'))
    await userEvent.type(screen.getByLabelText('Namespace'), 'swe-platform-system')
    await userEvent.click(screen.getByRole('button', { name: 'Switch' }))
    expect(await screen.findByText('Loading runs…')).toBeInTheDocument()
    expect(screen.getByLabelText('State')).toHaveValue('')
    expect(screen.getByLabelText('Agent')).toHaveValue('')
    expect(screen.getByRole('checkbox', { name: 'Attention candidates' })).not.toBeChecked()
    expect(screen.queryByRole('option', { name: 'amp' })).not.toBeInTheDocument()
    expect(screen.queryByText('repair-ui')).not.toBeInTheDocument()
    resolveOther(response({ items: [otherRun] }))
    expect(await screen.findByText('argo-run')).toBeInTheDocument()
    expect(screen.queryByText('repair-ui')).not.toBeInTheDocument()
    expect(screen.getByTestId('location')).toHaveTextContent('/namespaces/swe-platform-system/runs')
    expect(client.getQueryData(queryKeys.runs('swe-platform-system'))).toEqual(expect.objectContaining({ items: [expect.objectContaining({ name: 'argo-run', uid: 'argo-uid', agent: 'amp', promptPreview: 'Argo namespace task' })] }))
    resolveLateDefault(response({ items: [lateRun] }))
    await waitFor(() => expect(client.getQueryData(queryKeys.runs('default'))).toEqual(expect.objectContaining({ items: [expect.objectContaining({ name: 'repair-ui', uid: 'run-uid' })] })))
    expect(screen.getByText('argo-run')).toBeInTheDocument()
    expect(screen.queryByText('late-default-run')).not.toBeInTheDocument()
    expect(fetch).toHaveBeenCalledWith('/api/v1/namespaces/swe-platform-system/runs?limit=200&view=summary', expect.anything())
  })

  it.each(['', 'Default', 'team/a', 'https://evil.example', 'team?next=evil', 'team#fragment', '-team', `${'a'.repeat(64)}`])('rejects invalid namespace input %j without navigating or fetching', async invalid => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => path === '/api/v1/session' ? response({ authenticated: true, username: 'alex' }) : response({ items: [] }))
    show('/namespaces/default/runs')
    await screen.findByText('No runs found.')
    await userEvent.clear(screen.getByLabelText('Namespace'))
    if (invalid) await userEvent.type(screen.getByLabelText('Namespace'), invalid)
    await userEvent.click(screen.getByRole('button', { name: 'Switch' }))
    expect(screen.getByRole('alert')).toHaveTextContent(invalid ? 'valid Kubernetes DNS label' : 'Namespace is required')
    expect(screen.getByTestId('location')).toHaveTextContent('/namespaces/default/runs')
    expect(fetch).toHaveBeenCalledTimes(2)
  })

  it('rejects an invalid namespace deep link before issuing a namespace API request', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => path === '/api/v1/session' ? response({ authenticated: true, username: 'alex' }) : response({ items: [] }))
    show('/namespaces/team%2Fa/runs')
    expect(await screen.findByRole('alert')).toHaveTextContent('valid Kubernetes DNS label')
    expect(fetch).toHaveBeenCalledOnce()
  })

  it('rejects malformed namespace encoding without issuing a namespace API request', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined)
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => path === '/api/v1/session' ? response({ authenticated: true, username: 'alex' }) : response({ items: [] }))
    show('/namespaces/team%ZZ/runs')
    expect(await screen.findByRole('alert')).toHaveTextContent('valid Kubernetes DNS label')
    expect(fetch).toHaveBeenCalledOnce()
  })

  it('keeps polling an empty feed and discovers a run created elsewhere', async () => {
    vi.useFakeTimers()
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(response({ items: [run] }))
    const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } })
    client.setQueryData(queryKeys.session, { authenticated: true, username: 'alex' })
    client.setQueryData(queryKeys.runs('default'), { items: [] })
    render(<QueryClientProvider client={client}><MemoryRouter initialEntries={['/namespaces/default/runs']}><App /></MemoryRouter></QueryClientProvider>)
    expect(screen.getByText('No runs found.')).toBeInTheDocument()
    await act(async () => { await vi.advanceTimersByTimeAsync(4001); await Promise.resolve() })
    expect(screen.getByText('repair-ui')).toBeInTheDocument()
    expect(fetch).toHaveBeenCalledOnce()
  })

  it('recovers a transient initial summary failure without classifying it as legacy fallback', async () => {
    vi.useFakeTimers()
    let summaries = 0
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') {
        summaries += 1
        if (summaries === 1) return response({ title: 'Unavailable', status: 503 }, 503)
        return response({ items: [run], resourceVersion: '2' })
      }
      if (String(path).includes('watch=true')) return response({ title: 'Legacy', status: 501 }, 501)
      throw new Error(`Unexpected request: ${path}`)
    })
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    client.setQueryData(queryKeys.session, { authenticated: true, username: 'alex' })
    render(<QueryClientProvider client={client}><MemoryRouter initialEntries={['/namespaces/default/runs']}><App /></MemoryRouter></QueryClientProvider>)
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(screen.getByRole('alert')).toHaveTextContent('Unavailable')
    expect(screen.queryByText(/Live updates unavailable/)).not.toBeInTheDocument()
    await act(async () => { await vi.advanceTimersByTimeAsync(4001); await Promise.resolve(); await Promise.resolve() })
    expect(screen.getByText('repair-ui')).toBeInTheDocument()
    expect(fetch).toHaveBeenCalledWith(expect.stringContaining('watch=true'), expect.anything())
  })

  it('hides retained namespace data and actions when a watch reconnect loses authorization', async () => {
    vi.useFakeTimers()
    let watches = 0
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (String(path).includes('watch=true')) {
        watches += 1
        if (watches === 1) return new Response(new ReadableStream({ start: controller => controller.close() }), { headers: { 'Content-Type': 'text/event-stream' } })
        return response({ type: 'https://swe-platform.dev/problems/forbidden', title: 'Access denied', status: 403 }, 403)
      }
      throw new Error(`Unexpected request: ${path}`)
    })
    const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } })
    client.setQueryData(queryKeys.session, { authenticated: true, username: 'alex' })
    client.setQueryData(queryKeys.runs('default'), { items: [run], resourceVersion: '2' })
    render(<QueryClientProvider client={client}><MemoryRouter initialEntries={['/namespaces/default/runs']}><App /></MemoryRouter></QueryClientProvider>)
    expect(screen.getByText('repair-ui')).toBeInTheDocument()
    await act(async () => { await vi.advanceTimersByTimeAsync(1001) })
    expect(screen.getByRole('alert')).toHaveTextContent('Run watch failed (403)')
    expect(screen.queryByText('repair-ui')).not.toBeInTheDocument()
    expect(screen.queryByRole('link', { name: 'New run' })).not.toBeInTheDocument()
    expect(client.getQueryData(queryKeys.runs('default'))).toEqual(expect.objectContaining({ items: [expect.objectContaining({ uid: 'run-uid' })] }))
  })

  it('polls exact Run detail only while the summary feed uses compatibility fallback', async () => {
    vi.useFakeTimers()
    let details = 0
    const initial = { ...run, environment: undefined, generation: 1, state: 'Running' }
    const updated = { ...initial, generation: 2, state: 'Succeeded' }
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items: [initial] })
      if (path === '/api/v1/namespaces/default/runs/repair-ui') { details += 1; return response(details <= 2 ? initial : updated) }
      throw new Error(`Unexpected request: ${path}`)
    })
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    client.setQueryData(queryKeys.session, { authenticated: true, username: 'alex' })
    render(<QueryClientProvider client={client}><MemoryRouter initialEntries={['/namespaces/default/runs/repair-ui/overview']}><App /></MemoryRouter></QueryClientProvider>)
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(screen.getByText('Running', { selector: '.pill' })).toBeInTheDocument()
    await act(async () => { await vi.advanceTimersByTimeAsync(4001); await Promise.resolve(); await Promise.resolve() })
    expect(screen.getByText('Succeeded', { selector: '.pill' })).toBeInTheDocument()
    expect(details).toBe(3)
  })

  it('treats headerless discovery as non-renderable until exact identity confirmation', async () => {
    let resolveDiscovery!: (value: Response) => void
    let resolveExact!: (value: Response) => void
    const childRequests: string[] = []
    const headers: Array<string | null> = []
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ title: 'Forbidden', status: 403 }, 403)
      if (path === '/api/v1/namespaces/default/runs/repair-ui') {
        const uid = (init?.headers as Headers).get('SWE-Run-UID')
        headers.push(uid)
        if (!uid) return new Promise<Response>(resolve => { resolveDiscovery = resolve })
        return new Promise<Response>(resolve => { resolveExact = resolve })
      }
      childRequests.push(String(path))
      return response({})
    })
    const seededClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    seededClient.setQueryData(queryKeys.run('default', 'repair-ui', 'uid-a'), { ...run, uid: 'uid-a' })
    const { client } = show('/namespaces/default/runs/repair-ui/overview', undefined, seededClient)
    expect(await screen.findByText('Loading run…')).toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'repair-ui' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Cancel run' })).not.toBeInTheDocument()

    resolveDiscovery(response({ ...run, uid: 'uid-a' }))
    await waitFor(() => expect(headers).toEqual([null, 'uid-a']))
    expect(client.getQueryCache().findAll({ queryKey: ['run-bootstrap', 'default', 'repair-ui'] }).map(query => query.state.data)).toEqual(['uid-a'])
    expect(screen.queryByRole('heading', { name: 'repair-ui' })).not.toBeInTheDocument()
    expect(screen.queryByRole('navigation', { name: 'Run sections' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Cancel run' })).not.toBeInTheDocument()
    expect(childRequests).toEqual([])

    resolveExact(response({ title: 'Run identity conflict', status: 409, detail: 'the Run name now identifies UID B' }, 409))
    expect(await screen.findByRole('alert')).toHaveTextContent('UID B')
    expect(screen.queryByRole('navigation', { name: 'Run sections' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Cancel run' })).not.toBeInTheDocument()
    expect(childRequests).toEqual([])
    act(() => { focusManager.setFocused(false); focusManager.setFocused(true); onlineManager.setOnline(false); onlineManager.setOnline(true) })
    await Promise.resolve()
    expect(headers).toEqual([null, 'uid-a'])
  })

  it('recovers exact Run detail from a transient 503', async () => {
    vi.useFakeTimers()
    let details = 0
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items: [] })
      if (path === '/api/v1/namespaces/default/runs/repair-ui') {
        details += 1
        return details === 1 ? response({ title: 'Unavailable', status: 503 }, 503) : response(run)
      }
      throw new Error(`Unexpected request: ${path}`)
    })
    show('/namespaces/default/runs/repair-ui/overview', { runUID: 'run-uid' })
    await act(async () => { await vi.advanceTimersByTimeAsync(1100); await Promise.resolve() })
    expect(screen.getByRole('heading', { name: 'repair-ui' })).toBeInTheDocument()
    expect(details).toBe(2)
  })

  it('recovers an exact Run after a network failure but does not loop on malformed JSON', async () => {
    vi.useFakeTimers()
    let details = 0
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items: [] })
      if (path === '/api/v1/namespaces/default/runs/repair-ui') {
        details += 1
        if (details === 1) throw new TypeError('network unavailable')
        return response(run)
      }
      throw new Error(`Unexpected request: ${path}`)
    })
    const first = show('/namespaces/default/runs/repair-ui/overview', { runUID: 'run-uid' })
    await act(async () => { await vi.advanceTimersByTimeAsync(1100); await Promise.resolve() })
    expect(screen.getByRole('heading', { name: 'repair-ui' })).toBeInTheDocument()
    expect(details).toBe(2)
    first.unmount()

    details = 0
    fetch.mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items: [] })
      if (path === '/api/v1/namespaces/default/runs/repair-ui') { details += 1; return new Response('{') }
      throw new Error(`Unexpected request: ${path}`)
    })
    show('/namespaces/default/runs/repair-ui/overview', { runUID: 'run-uid' })
    await act(async () => { await vi.advanceTimersByTimeAsync(1100); await Promise.resolve() })
    expect(screen.getByRole('alert')).toBeInTheDocument()
    const settled = details
    act(() => { focusManager.setFocused(false); focusManager.setFocused(true); onlineManager.setOnline(false); onlineManager.setOnline(true) })
    await act(async () => { await vi.advanceTimersByTimeAsync(8001); await Promise.resolve() })
    expect(details).toBe(settled)
  })

  it('keeps the transcript stream mounted when cancellation only advances Run generation', async () => {
    const detail = { ...run, environment: undefined, generation: 1 }
    const transcriptRequests: RequestInit[] = []
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init = {}) => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items: [detail] })
      if (path === '/api/v1/namespaces/default/runs/repair-ui') return response(detail)
      if (path === '/api/v1/namespaces/default/runs/repair-ui/transcript') {
        transcriptRequests.push(init)
        return new Response(new ReadableStream())
      }
      throw new Error(`Unexpected request: ${path}`)
    })
    const { client } = show('/namespaces/default/runs/repair-ui/transcript')
    await waitFor(() => expect(transcriptRequests).toHaveLength(1))
    act(() => client.setQueryData(queryKeys.run('default', 'repair-ui', 'run-uid'), { ...detail, generation: 2, cancelRequested: true }))
    await waitFor(() => expect(client.getQueryData<Run>(queryKeys.run('default', 'repair-ui', 'run-uid'))?.generation).toBe(2))
    expect(transcriptRequests).toHaveLength(1)
    expect((transcriptRequests[0].signal as AbortSignal).aborted).toBe(false)
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
    let loggedIn = false
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
      if (path === '/api/v1/session' && init?.method === 'POST') { loggedIn = true; return response({ authenticated: true, username: 'alex' }) }
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (!loggedIn) return response({ type: 'https://swe-platform.dev/problems/unauthenticated', title: 'Session expired', status: 401 }, 401)
      return response(run)
    })
    const deepLink = '/namespaces/swe-platform-system/runs/repair-ui/overview?panel=usage#tokens'
    const { client } = show(deepLink)
    client.setQueryData(['prior-user-data'], { prompt: 'secret task' })
    const token = await screen.findByLabelText('Access token')
    expect(client.getQueryData(['prior-user-data'])).toBeUndefined()
    await userEvent.type(token, 'new-token')
    await userEvent.click(screen.getByRole('button', { name: 'Sign in' }))
    expect(await screen.findByRole('heading', { name: 'repair-ui' })).toBeInTheDocument()
    expect(screen.getByTestId('location')).toHaveTextContent(deepLink)
  })

  it('preserves an initial deep link through login and rejects external redirect state', async () => {
    let authenticated = false
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
      if (path === '/api/v1/session' && init?.method === 'POST') { authenticated = true; return response({ authenticated: true, username: 'alex' }) }
      if (path === '/api/v1/session' && !authenticated) return response({ type: 'auth', title: 'Unauthorized', status: 401 }, 401)
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      return response(run)
    })
    const deepLink = '/namespaces/swe-platform-system/runs/repair-ui/overview?panel=usage#tokens'
    const firstView = show(deepLink)
    const token = await screen.findByLabelText('Access token')
    await userEvent.type(token, 'token')
    await userEvent.click(screen.getByRole('button', { name: 'Sign in' }))
    expect(await screen.findByRole('heading', { name: 'repair-ui' })).toBeInTheDocument()
    expect(screen.getByTestId('location')).toHaveTextContent(deepLink)

    firstView.unmount()
    authenticated = false
    show('/login', { from: { pathname: '/\t/evil.example', search: '?steal=1' } })
    const externalToken = screen.getAllByLabelText('Access token').at(-1)!
    await userEvent.type(externalToken, 'token')
    await userEvent.click(screen.getAllByRole('button', { name: 'Sign in' }).at(-1)!)
    await waitFor(() => expect(screen.getAllByTestId('location').at(-1)).toHaveTextContent('/namespaces/default/runs'))
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
    expect(screen.getByText('2026-07-19T12:01:00Z')).toBeInTheDocument()
    expect(screen.getByText('Owned')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Changes' })).toHaveAttribute('href', '/namespaces/default/runs/repair-ui/changes')
  })

  it.each([
    { state: 'Failed', code: 'AdapterFailed', message: 'The agent reported a failure.', nextAction: 'Review the transcript for agent-reported details before starting another run.' },
    { state: 'Allocating', code: 'EnvironmentPreparing', message: 'Waiting for the environment to become ready.', nextAction: 'Wait for provisioning to finish. If this persists, ask an administrator to check the environment.' },
    { state: 'Paused', code: 'EnvironmentPaused', message: 'The environment is paused; workspace and transcript are retained.', nextAction: 'Check the environment hold with an administrator before resuming.' },
    { state: 'NeedsInput', code: 'AgentNeedsInput', message: 'The agent reported that it needs input.', nextAction: "Review the transcript for the agent's request. Input support depends on the adapter." },
  ])('shows $state diagnosis and next action through exact Run context', async ({ state, ...diagnostic }) => {
    const current = { ...run, state, diagnostic, cancelRequested: state === 'Allocating' }
    let exactConfirmed = false
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (String(path).includes('/environments/')) return response(environment)
      if (String(path).includes('/transcript')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
      if (new Headers(init?.headers).get('SWE-Run-UID') === run.uid) exactConfirmed = true
      return response(current)
    })
    show('/namespaces/default/runs/repair-ui/overview')
    expect(await screen.findByText(diagnostic.message)).toBeInTheDocument()
    expect(exactConfirmed).toBe(true)
    expect(screen.getByText(diagnostic.nextAction)).toBeInTheDocument()
    if (current.cancelRequested) expect(screen.getByText(/Cancellation requested. Waiting/)).toBeInTheDocument()
    await userEvent.click(screen.getByRole('link', { name: 'Review transcript' }))
    expect(await screen.findByText('No transcript events yet.')).toBeInTheDocument()
    expect(screen.getByTestId('location')).toHaveTextContent('/namespaces/default/runs/repair-ui/transcript')
    expect(screen.getByText(diagnostic.message)).toBeInTheDocument()
    await userEvent.click(screen.getByText('Task · amp'))
    expect(screen.getByText('Repair UI')).toBeVisible()
    expect(screen.getByRole('link', { name: 'Changes' })).toBeInTheDocument()
  })

  it('does not invent a missing diagnosis and removes the notice after a normal transition', async () => {
    let current: Run = { ...run, state: 'Failed', diagnostic: undefined }
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (String(path).includes('/environments/')) return response(environment)
      return response(current)
    })
    const { client } = show('/namespaces/default/runs/repair-ui/overview')
    expect(await screen.findByText('No current diagnostic is available.')).toBeInTheDocument()
    expect(screen.queryByText('The agent reported a failure.')).not.toBeInTheDocument()
    current = { ...run, state: 'Running' }
    act(() => { void client.invalidateQueries({ queryKey: queryKeys.run('default', run.name, run.uid) }) })
    await waitFor(() => expect(screen.queryByRole('region', { name: 'Run status' })).not.toBeInTheDocument())
    expect(screen.getByRole('button', { name: 'Cancel run' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Terminal' })).toBeInTheDocument()
  })

  it.each(['Running', 'Paused', 'Succeeded'] as const)('routes %s Changes through exact Run context while retaining task and cancellation state', async state => {
    let exactConfirmed = false
    const current = { ...run, state, cancelRequested: state === 'Running', terminalAvailable: state === 'Running' }
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
      const path = String(input)
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items: [] })
      if (path.includes('/changes?')) {
        expect(exactConfirmed).toBe(true)
        expect(new Headers(init?.headers).get('SWE-Run-UID')).toBe(run.uid)
        return response({ runUID: run.uid, revision: 1, state: 'clean', final: state === 'Succeeded', unavailable: false, total: 0, files: [] })
      }
      if (new Headers(init?.headers).get('SWE-Run-UID') === run.uid) exactConfirmed = true
      return response(current)
    })
    show('/namespaces/default/runs/repair-ui/changes')
    expect(await screen.findByText('No changes in this captured observation.')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Changes' })).toHaveAttribute('aria-current', 'page')
    expect(screen.getByText(/Pre-existing edits are part of the baseline, not attributed to this Run/)).toBeInTheDocument()
    await userEvent.click(screen.getByText('Task · amp'))
    expect(screen.getByText('Repair UI')).toBeVisible()
    if (state === 'Running') expect(screen.getByText(/Cancellation requested. Waiting for the agent to stop/)).toBeInTheDocument()
  })

  it('recovers Environment detail from 503 but stops on identity mismatch', async () => {
    vi.useFakeTimers()
    let environments = 0
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items: [] })
      if (path === '/api/v1/namespaces/default/runs/repair-ui') return response(run)
      if (String(path).includes('/environments/')) {
        environments += 1
        if (environments === 1) return response({ title: 'Unavailable', status: 503 }, 503)
        if (environments === 2) return response(environment)
        return response({ ...environment, uid: 'replacement-env-uid' })
      }
      throw new Error(`Unexpected request: ${path}`)
    })
    const { client } = show('/namespaces/default/runs/repair-ui/overview', { runUID: 'run-uid' })
    await act(async () => { await vi.advanceTimersByTimeAsync(1100); await Promise.resolve() })
    expect(screen.getByText('Ready, active')).toBeInTheDocument()
    act(() => { void client.invalidateQueries({ queryKey: queryKeys.environment('default', 'repair-env', 'env-uid') }) })
    await act(async () => { await vi.advanceTimersByTimeAsync(1); await Promise.resolve() })
    expect(screen.getByRole('alert')).toHaveTextContent('different Environment identity')
    const settled = environments
    act(() => { focusManager.setFocused(false); focusManager.setFocused(true); onlineManager.setOnline(false); onlineManager.setOnline(true) })
    await act(async () => { await vi.advanceTimersByTimeAsync(8001); await Promise.resolve() })
    expect(environments).toBe(settled)
  })

  it('hides and revokes terminal navigation when the exact association is unavailable', async () => {
    const released = { ...run, terminalAvailable: false, environment: { name: 'repair-env', ownership: 'Owned' as const } }
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      return response(released)
    })
    show('/namespaces/default/runs/repair-ui/terminal')
    expect(await screen.findByText('Terminal unavailable for this Run identity.')).toHaveAttribute('role', 'status')
    expect(screen.queryByRole('link', { name: 'Terminal' })).not.toBeInTheDocument()
  })

  it('lists authorized portals with honest state and a same-origin authenticated opener', async () => {
    const portalPath = '/api/v1/namespaces/default/runs/repair-ui/portals/run-uid/env-uid'
    const openPath = `${portalPath}/web/open`
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === portalPath) return response({ items: [
        { name: 'web', targetPort: 3000, status: 'Ready', url: 'https://locator.portal.test', openURL: openPath },
        { name: 'docs', targetPort: 4173, status: 'Paused', reason: 'Opening the portal wakes this idle Environment', url: 'https://docs.portal.test', openURL: `${portalPath}/docs/open` },
      ] })
      return response(run)
    })
    show('/namespaces/default/runs/repair-ui/portals')
    expect(await screen.findByText('web')).toBeInTheDocument()
    expect(screen.getByText('3000')).toBeInTheDocument()
    expect(screen.getByText('Ready')).toBeInTheDocument()
    expect(screen.getByText('Paused')).toBeInTheDocument()
    expect(screen.getByText('Opening the portal wakes this idle Environment')).toBeInTheDocument()
    const openers = screen.getAllByRole('button', { name: 'Open portal' })
    expect(openers[0].closest('form')).toHaveAttribute('action', openPath)
    expect(openers[0].closest('form')).toHaveAttribute('method', 'post')
    expect(openers[0].closest('form')).toHaveAttribute('target', '_blank')
    expect(fetch).toHaveBeenCalledWith(portalPath, expect.objectContaining({ credentials: 'same-origin' }))
  })

  it('does not disclose unauthorized portals or open an untrusted returned URL', async () => {
    const portalPath = '/api/v1/namespaces/default/runs/repair-ui/portals/run-uid/env-uid'
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === portalPath) return response({ items: [{ name: 'bad', targetPort: 3000, status: 'Failed', url: 'javascript:alert(1)', openURL: '//evil.test/open' }] })
      return response(run)
    })
    show('/namespaces/default/runs/repair-ui/portals')
    expect(await screen.findByText('bad')).toBeInTheDocument()
    expect(screen.getByText('Not configured')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Open portal' })).not.toBeInTheDocument()
  })

  it('represents an authorization-filtered empty portal list without guessing service names', async () => {
    const portalPath = '/api/v1/namespaces/default/runs/repair-ui/portals/run-uid/env-uid'
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === portalPath) return response({ items: [] })
      return response(run)
    })
    show('/namespaces/default/runs/repair-ui/portals')
    expect(await screen.findByText('No authorized declared services.')).toHaveAttribute('role', 'status')
    expect(screen.getByText('swe environment services')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'View task progress' })).toHaveAttribute('href', '/namespaces/default/runs/repair-ui/transcript')
    await userEvent.click(screen.getByText('Task · amp'))
    expect(screen.getByText('Repair UI')).toBeVisible()
    await userEvent.click(screen.getByRole('link', { name: 'Overview' }))
    expect(screen.getByText('Task · amp').parentElement).toHaveAttribute('open')
  })

  it('retries transient portal failures but stops polling an exact identity conflict', async () => {
    vi.useFakeTimers()
    const portalPath = '/api/v1/namespaces/default/runs/repair-ui/portals/run-uid/env-uid'
    let portals = 0
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs?limit=200&view=summary') return response({ items: [] })
      if (path === '/api/v1/namespaces/default/runs/repair-ui') return response(run)
      if (path === portalPath) {
        portals += 1
        if (portals === 1) return response({ title: 'Unavailable', status: 503 }, 503)
        if (portals === 2) return response({ items: [] })
        return response({ title: 'Run identity conflict', status: 409, detail: 'different Run' }, 409)
      }
      throw new Error(`Unexpected request: ${path}`)
    })
    const { client } = show('/namespaces/default/runs/repair-ui/portals', { runUID: 'run-uid' })
    await act(async () => { await vi.advanceTimersByTimeAsync(1100); await Promise.resolve() })
    expect(screen.getByText('No authorized declared services.')).toBeInTheDocument()
    act(() => { void client.invalidateQueries({ queryKey: queryKeys.portals('default', 'repair-ui', 'run-uid', 'env-uid') }) })
    await act(async () => { await vi.advanceTimersByTimeAsync(1); await Promise.resolve() })
    expect(screen.getByRole('alert')).toHaveTextContent('different Run')
    const settled = portals
    act(() => { focusManager.setFocused(false); focusManager.setFocused(true); onlineManager.setOnline(false); onlineManager.setOnline(true) })
    await act(async () => { await vi.advanceTimersByTimeAsync(8001); await Promise.resolve() })
    expect(portals).toBe(settled)
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

  it('creates the exact selector contract without racing the namespace watch', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs' && init?.method === 'POST') return response(run, 201)
      if (path === '/api/v1/namespaces/default/runs/repair-ui') return response(run)
      return response({ items: [] })
    })
    const { client } = show('/namespaces/default/runs/new')
    const invalidate = vi.spyOn(client, 'invalidateQueries')
    await screen.findByRole('heading', { name: 'New run' })
    await userEvent.clear(screen.getByLabelText('Name'))
    await userEvent.type(screen.getByLabelText('Name'), 'repair-ui')
    await userEvent.clear(screen.getByLabelText('Agent'))
    await userEvent.type(screen.getByLabelText('Agent'), 'amp')
    await userEvent.type(screen.getByLabelText('Credential profile'), '  amp-production  ')
    await userEvent.type(screen.getByLabelText('Prompt / task'), '  Repair UI  ')
    await userEvent.type(screen.getByLabelText('Project reference'), 'platform')
    await userEvent.type(screen.getByLabelText('Template reference'), 'small')
    await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
    await waitFor(() => expect(fetch.mock.calls.some(call => call[0] === '/api/v1/namespaces/default/runs' && call[1]?.method === 'POST')).toBe(true))
    const createInit = fetch.mock.calls.find(call => call[0] === '/api/v1/namespaces/default/runs' && call[1]?.method === 'POST')?.[1]
    expect(JSON.parse(String(createInit?.body))).toEqual({
      name: 'repair-ui', selector: { project: 'platform', template: 'small' }, agent: 'amp', prompt: '  Repair UI  ', credentialProfile: 'amp-production',
    })
    expect(invalidate).not.toHaveBeenCalledWith({ queryKey: ['runs', 'default'] })
    expect(await screen.findByRole('heading', { name: 'repair-ui' })).toBeInTheDocument()
  })

  it('does not leave the selected namespace when an old-namespace create completes', async () => {
    let resolveCreate!: (value: Response) => void
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (path === '/api/v1/namespaces/default/runs' && init?.method === 'POST') return new Promise<Response>(resolve => { resolveCreate = resolve })
      if (path === '/api/v1/namespaces/swe-platform-system/runs?limit=200&view=summary') return response({ items: [] })
      throw new Error(`Unexpected request: ${path}`)
    })
    const { client } = show('/namespaces/default/runs/new')
    client.setQueryData(queryKeys.runs('default'), { items: [] })
    const invalidate = vi.spyOn(client, 'invalidateQueries')
    await userEvent.clear(await screen.findByLabelText('Name'))
    await userEvent.type(await screen.findByLabelText('Name'), 'repair-ui')
    await userEvent.type(screen.getByLabelText('Prompt / task'), 'Repair UI')
    await userEvent.type(screen.getByLabelText('Template reference'), 'small')
    await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
    await waitFor(() => expect(fetch).toHaveBeenCalledWith('/api/v1/namespaces/default/runs', expect.objectContaining({ method: 'POST' })))
    await userEvent.clear(screen.getByLabelText('Namespace'))
    await userEvent.type(screen.getByLabelText('Namespace'), 'swe-platform-system')
    await userEvent.click(screen.getByRole('button', { name: 'Switch' }))
    expect(await screen.findByText('No runs found.')).toBeInTheDocument()
    resolveCreate(response(run, 201))
    await waitFor(() => expect(invalidate).not.toHaveBeenCalledWith({ queryKey: queryKeys.runs('default') }))
    expect(client.getQueryState(queryKeys.runs('default'))?.isInvalidated).toBe(false)
    expect(screen.getByTestId('location')).toHaveTextContent('/namespaces/swe-platform-system/runs')
    expect(screen.queryByRole('heading', { name: 'repair-ui' })).not.toBeInTheDocument()
  })

  it('cancels with an immutable UID fence and lets the watch update list data', async () => {
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
    expect(fetch.mock.calls.some(call => String(call[0]).endsWith('/cancel'))).toBe(false)
    expect(screen.getByRole('button', { name: 'Keep running' })).toHaveFocus()
    await userEvent.click(screen.getByRole('button', { name: 'Keep running' }))
    expect(screen.queryByRole('button', { name: 'Confirm cancellation' })).not.toBeInTheDocument()
    expect(fetch.mock.calls.some(call => String(call[0]).endsWith('/cancel'))).toBe(false)
    await userEvent.click(screen.getByRole('button', { name: 'Cancel run' }))
    await userEvent.click(screen.getByRole('button', { name: 'Confirm cancellation' }))
    await waitFor(() => expect(fetch).toHaveBeenCalledWith('/api/v1/namespaces/default/runs/repair-ui/cancel', expect.objectContaining({ method: 'POST' })))
    const cancelInit = fetch.mock.calls.find(call => String(call[0]).endsWith('/cancel'))?.[1]
    expect(JSON.parse(String(cancelInit?.body))).toEqual({ runUID: 'run-uid' })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['run', 'default', 'repair-ui', 'run-uid'], exact: true })
    expect(invalidate).not.toHaveBeenCalledWith({ queryKey: ['runs', 'default'] })
  })

  it('shows cancellation in flight, retains errors, and acknowledges the accepted request', async () => {
    let detail = run
    let resolveCancel!: (value: Response) => void
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (String(path).includes('/environments/')) return response(environment)
      if (String(path).includes('/portals/')) return response({ items: [] })
      if (String(path).endsWith('/cancel') && init?.method === 'POST') return new Promise(resolve => { resolveCancel = resolve })
      return response(detail)
    })
    show('/namespaces/default/runs/repair-ui/overview')
    await userEvent.click(await screen.findByRole('button', { name: 'Cancel run' }))
    await userEvent.click(screen.getByRole('button', { name: 'Confirm cancellation' }))
    expect(screen.getByRole('button', { name: 'Requesting cancellation…' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Keep running' })).toBeDisabled()
    resolveCancel(response({ title: 'Cancellation unavailable', status: 503 }, 503))
    expect(await screen.findByRole('alert')).toHaveTextContent('Cancellation unavailable')
    await userEvent.click(screen.getByRole('button', { name: 'Confirm cancellation' }))
    detail = { ...run, cancelRequested: true }
    resolveCancel(response(detail))
    expect(await screen.findByText('Cancellation requested. Waiting for the agent to stop.')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Cancel run|Confirm cancellation/ })).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('link', { name: 'Portals' }))
    expect(screen.getByText('Cancellation requested. Waiting for the agent to stop.')).toBeInTheDocument()
  })

  it.each(['Succeeded', 'Failed', 'Cancelled'])('does not offer cancellation or claim to be waiting on a %s run', async state => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
      if (path === '/api/v1/session') return response({ authenticated: true, username: 'alex' })
      if (String(path).includes('/environments/')) return response(environment)
      return response({ ...run, state, cancelRequested: true })
    })
    show('/namespaces/default/runs/repair-ui/overview')
    await screen.findByRole('heading', { name: 'repair-ui' })
    expect(screen.queryByRole('button', { name: 'Cancel run' })).not.toBeInTheDocument()
    expect(screen.queryByText('Cancellation requested. Waiting for the agent to stop.')).not.toBeInTheDocument()
  })
})
