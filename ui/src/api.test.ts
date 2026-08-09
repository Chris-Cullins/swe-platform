import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiProblem, api, fallbackPollInterval, listAllRuns, onUnauthorized, ResourceTransportError, retryTransientResourceError, transientResourceError } from './api'
import type { Run } from './contracts'

afterEach(() => vi.restoreAllMocks())
describe('API boundary', () => {
  it('encodes namespace and names and sends credentials', async () => { const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({ items: [] }))); await api.runs('team/a'); expect(fetch).toHaveBeenCalledWith('/api/v1/namespaces/team%2Fa/runs', expect.objectContaining({ credentials: 'same-origin' })) })
  it('uses bearer authorization only for login and never puts it in the body', async () => { const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => new Response(JSON.stringify({ authenticated: true }))); await api.login('secret'); const [, init] = fetch.mock.calls[0]; expect((init?.headers as Headers).get('Authorization')).toBe('Bearer secret'); expect(init?.body).toBeUndefined(); await api.session(); expect((fetch.mock.calls[1][1]?.headers as Headers).has('Authorization')).toBe(false) })
  it('sends cancel with an immutable UID fence', async () => { const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('{}')); await api.cancelRun('n', 'r', 'uid'); const [path, init] = fetch.mock.calls[0]; expect(path).toBe('/api/v1/namespaces/n/runs/r/cancel'); expect(init?.method).toBe('POST'); expect(JSON.parse(String(init?.body))).toEqual({ runUID: 'uid' }) })
  it('keeps discovery headerless and validates exact Run and Environment responses', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async path => new Response(String(path).includes('/environments/') ? '{"uid":"env-uid"}' : '{"uid":"run-uid"}'))
    const controller = new AbortController()
    await api.run('n', 'r', controller.signal)
    expect((fetch.mock.calls[0][1]?.headers as Headers).has('SWE-Run-UID')).toBe(false)
    await api.runExact('n', 'r', 'run-uid', controller.signal)
    expect((fetch.mock.calls[1][1]?.headers as Headers).get('SWE-Run-UID')).toBe('run-uid')
    await api.environmentExact('n', 'e', 'env-uid', controller.signal)
    fetch.mockResolvedValueOnce(new Response('{"uid":"replacement"}'))
    await expect(api.runExact('n', 'r', 'run-uid')).rejects.toThrow('different Run identity')
    fetch.mockResolvedValueOnce(new Response('{"uid":"replacement"}'))
    await expect(api.environmentExact('n', 'e', 'env-uid')).rejects.toThrow('different Environment identity')
  })
  it('notifies authentication state when a transcript session expires', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status: 401, statusText: 'Unauthorized' }))
    const listener = vi.fn()
    const unsubscribe = onUnauthorized(listener)
    await api.transcript('n', 'r', 'uid', new AbortController().signal)
    unsubscribe()
    expect(listener).toHaveBeenCalledOnce()
  })
  it('builds optional list pagination with URLSearchParams and validates limit', async () => { const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('{"items":[]}')); await api.runs('n', { limit: 200, continue: 'a+b' }); expect(fetch.mock.calls[0][0]).toBe('/api/v1/namespaces/n/runs?limit=200&continue=a%2Bb'); expect(() => api.runs('n', { limit: 201 })).toThrow(RangeError) })
  it('safely normalizes malformed errors into a typed problem', async () => { vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('not json', { status: 500, statusText: 'Oops' })); await expect(api.session()).rejects.toEqual(expect.objectContaining({ name: 'ApiProblem', status: 500, message: 'Oops', problem: { type: 'about:blank', title: 'Oops', status: 500 } })); await api.session().catch(error => expect(error).toBeInstanceOf(ApiProblem)) })
  it('retries typed network and retryable HTTP failures but not client-side errors', async () => {
    vi.spyOn(globalThis, 'fetch').mockRejectedValueOnce(new TypeError('network unavailable'))
    const network = await api.session().catch(error => error)
    expect(network).toBeInstanceOf(ResourceTransportError)
    expect(transientResourceError(network)).toBe(true)
    expect(retryTransientResourceError(0, new ApiProblem({ type: 'busy', title: 'busy', status: 429 }, 429))).toBe(true)
    expect(retryTransientResourceError(1, new ApiProblem({ type: 'unavailable', title: 'unavailable', status: 503 }, 503))).toBe(true)
    expect(retryTransientResourceError(2, network)).toBe(false)
    expect(transientResourceError(new Error('invalid response shape'))).toBe(false)
    expect(transientResourceError(new SyntaxError('malformed JSON'))).toBe(false)
  })
  it('follows list continuation cursors and aggregates pages', async () => {
    const first = { name: 'first' } as Run
    const second = { name: 'second' } as Run
    const fetch = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response(JSON.stringify({ items: [first], continue: 'next+page' })))
      .mockResolvedValueOnce(new Response(JSON.stringify({ items: [second] })))
    await expect(listAllRuns('team')).resolves.toEqual({ items: [first, second] })
    expect(fetch.mock.calls.map(call => call[0])).toEqual([
      '/api/v1/namespaces/team/runs?limit=200',
      '/api/v1/namespaces/team/runs?limit=200&continue=next%2Bpage',
    ])
  })
  it('keeps compatibility polling bounded at four seconds', () => { expect(fallbackPollInterval).toBe(4000) })
})
