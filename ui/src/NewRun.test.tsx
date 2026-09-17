import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Link, MemoryRouter, useLocation } from 'react-router'
import { afterEach, expect, it, vi } from 'vitest'
import { App } from './App'

const response = (body: unknown) => new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } })
function Navigation() {
  const location = useLocation()
  return <><output data-testid="location">{location.pathname}</output><Link to="/namespaces/other/runs/new">Other form</Link><Link to="/namespaces/other/runs">Leave form</Link></>
}
function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={['/namespaces/default/runs/new']}><Navigation /><App /></MemoryRouter></QueryClientProvider>)
}
afterEach(() => vi.restoreAllMocks())

it('retains its generated name and exact intent across edits, validation and lost-response retry', async () => {
  const requests: string[] = []
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
    if (path === '/api/v1/session') return response({ authenticated: true })
    if (init?.method === 'POST') {
      requests.push(String(init.body))
      throw new TypeError('Response lost')
    }
    return response({ items: [] })
  })
  show()
  const name = (await screen.findByLabelText('Name') as HTMLInputElement).value
  expect(name).toMatch(/^run-[a-f0-9]{32}$/)
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  expect(await screen.findByRole('alert')).toBeInTheDocument()
  expect(requests).toEqual([])
  expect(screen.getByLabelText('Name')).toHaveValue(name)
  await userEvent.type(screen.getByLabelText('Prompt / task'), '  Private task  ')
  await userEvent.type(screen.getByLabelText('Project reference'), 'private-project')
  await userEvent.type(screen.getByLabelText('Credential profile'), 'private-profile')
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('Response lost')
  expect(requests).toHaveLength(1)
  expect(screen.getByLabelText('Name')).toHaveValue(name)
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  await waitFor(() => expect(requests).toHaveLength(2))
  expect(requests[1]).toBe(requests[0])
  expect(JSON.parse(requests[0])).toEqual({ name, agent: 'claude-code', prompt: '  Private task  ', selector: { project: 'private-project' }, credentialProfile: 'private-profile' })
  expect(screen.getByLabelText('Name')).toHaveValue(name)
  await userEvent.clear(screen.getByLabelText('Name'))
  await userEvent.type(screen.getByLabelText('Name'), 'INVALID NAME')
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  expect(requests).toHaveLength(2)
  expect(screen.getByLabelText('Name')).toHaveValue('INVALID NAME')
  await userEvent.clear(screen.getByLabelText('Name'))
  await userEvent.type(screen.getByLabelText('Name'), 'my-manual-run')
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  await waitFor(() => expect(requests).toHaveLength(3))
  expect(JSON.parse(requests[2]).name).toBe('my-manual-run')
})

it('starts fresh on direct namespace changes and new forms, fencing a late pending response', async () => {
  let resolveCreate!: (value: Response) => void
  const requests: string[] = []
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
    if (path === '/api/v1/session') return response({ authenticated: true })
    if (init?.method === 'POST') {
      requests.push(String(path))
      return new Promise<Response>(resolve => { resolveCreate = resolve })
    }
    return response({ items: [] })
  })
  show()
  const oldName = (await screen.findByLabelText('Name') as HTMLInputElement).value
  await userEvent.type(screen.getByLabelText('Prompt / task'), 'Old task')
  await userEvent.type(screen.getByLabelText('Template reference'), 'small')
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  await waitFor(() => expect(requests).toEqual(['/api/v1/namespaces/default/runs']))
  expect(screen.getByRole('button', { name: 'Create run' })).toBeDisabled()
  await userEvent.click(screen.getByRole('link', { name: 'Other form' }))
  const newName = (await screen.findByLabelText('Name') as HTMLInputElement).value
  expect(newName).toMatch(/^run-[a-f0-9]{32}$/)
  expect(newName).not.toBe(oldName)
  expect(screen.getByLabelText('Prompt / task')).toHaveValue('')
  expect(screen.getByLabelText('Template reference')).toHaveValue('')
  await act(async () => { resolveCreate(response({ name: oldName, uid: 'old-uid' })) })
  expect(screen.getByTestId('location')).toHaveTextContent('/namespaces/other/runs/new')
  expect(screen.getByLabelText('Name')).toHaveValue(newName)
  expect(requests).toHaveLength(1)
  await userEvent.click(screen.getByRole('link', { name: 'Leave form' }))
  await userEvent.click(await screen.findByRole('link', { name: 'New run' }))
  expect(await screen.findByLabelText('Name')).not.toHaveValue(newName)
})
