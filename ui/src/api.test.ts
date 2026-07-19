import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiProblem, api, listAllRuns, listPollInterval, runPollInterval } from './api'
import type { Run } from './contracts'

afterEach(() => vi.restoreAllMocks())
describe('API boundary', () => {
  it('encodes namespace and names and sends credentials', async () => { const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({ items: [] }))); await api.runs('team/a'); expect(fetch).toHaveBeenCalledWith('/api/v1/namespaces/team%2Fa/runs', expect.objectContaining({ credentials: 'same-origin' })) })
  it('uses bearer authorization only for login and never puts it in the body', async () => { const fetch = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => new Response(JSON.stringify({ authenticated: true }))); await api.login('secret'); const [, init] = fetch.mock.calls[0]; expect((init?.headers as Headers).get('Authorization')).toBe('Bearer secret'); expect(init?.body).toBeUndefined(); await api.session(); expect((fetch.mock.calls[1][1]?.headers as Headers).has('Authorization')).toBe(false) })
  it('sends cancel as an empty POST', async () => { const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('{}')); await api.cancelRun('n', 'r'); const [path, init] = fetch.mock.calls[0]; expect(path).toBe('/api/v1/namespaces/n/runs/r/cancel'); expect(init?.method).toBe('POST'); expect(Object.hasOwn(init || {}, 'body')).toBe(false) })
  it('builds optional list pagination with URLSearchParams and validates limit', async () => { const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('{"items":[]}')); await api.runs('n', { limit: 200, continue: 'a+b' }); expect(fetch.mock.calls[0][0]).toBe('/api/v1/namespaces/n/runs?limit=200&continue=a%2Bb'); expect(() => api.runs('n', { limit: 201 })).toThrow(RangeError) })
  it('safely normalizes malformed errors into a typed problem', async () => { vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('not json', { status: 500, statusText: 'Oops' })); await expect(api.session()).rejects.toEqual(expect.objectContaining({ name: 'ApiProblem', status: 500, message: 'Oops', problem: { type: 'about:blank', title: 'Oops', status: 500 } })); await api.session().catch(error => expect(error).toBeInstanceOf(ApiProblem)) })
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
  it('polls feeds unconditionally but stops terminal detail polling', () => { const run = (state: string) => ({ state } as Run); expect(runPollInterval(run('Running'))).toBe(4000); expect(runPollInterval(run('Succeeded'))).toBe(false); expect(listPollInterval).toBe(4000) })
})
