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

const project = { namespace: 'default', name: 'platform', uid: 'PROJECT_uid:original', generation: 1, defaultTemplate: 'small' }

it('pins discovered identity through validation, replacement refresh and exact lost-response retries, with deliberate reselect', async () => {
  const requests: string[] = []
  let current = project
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
    if (path === '/api/v1/session') return response({ authenticated: true })
    if (String(path).includes('/projects?')) return response({ items: [current] })
    if (init?.method === 'POST') { requests.push(String(init.body)); throw new TypeError('Response lost') }
    return response({ items: [] })
  })
  show()
  await userEvent.click(await screen.findByRole('button', { name: `Select platform UID ${project.uid}` }))
  const name = (screen.getByLabelText('Name') as HTMLInputElement).value
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('Prompt is required')
  await userEvent.type(screen.getByLabelText('Prompt / task'), '  exact task  ')
  await userEvent.type(screen.getByLabelText('Template reference'), 'explicit-template')
  await userEvent.type(screen.getByLabelText('Credential profile'), 'profile-a')
  await userEvent.selectOptions(screen.getByLabelText('Git credential'), 'GitHubApp')
  await userEvent.type(screen.getByLabelText('Existing environment reference'), 'existing')
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('without an existing Environment')
  expect(requests).toHaveLength(0)
  await userEvent.clear(screen.getByLabelText('Existing environment reference'))
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  await screen.findByRole('button', { name: 'Retry previous exact request' })
  expect(JSON.parse(requests[0])).toEqual({ name, agent: 'claude-code', prompt: '  exact task  ', selector: { project: project.name, projectUID: project.uid, template: 'explicit-template' }, credentialProfile: 'profile-a', repositoryCredential: 'GitHubApp' })
  current = { ...project, uid: 'replacement', defaultTemplate: 'large' }
  await userEvent.click(screen.getByRole('button', { name: 'Restart Project discovery' }))
  await screen.findByRole('button', { name: 'Select platform UID replacement' })
  expect(screen.getByText(project.uid)).toBeInTheDocument()
  await userEvent.clear(screen.getByLabelText('Prompt / task'))
  await userEvent.type(screen.getByLabelText('Prompt / task'), 'revised task')
  await userEvent.click(screen.getByRole('button', { name: 'Retry previous exact request' }))
  await waitFor(() => expect(requests).toHaveLength(2))
  expect(requests[1]).toBe(requests[0])
  await userEvent.click(screen.getByRole('button', { name: 'Select platform UID replacement' }))
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  await waitFor(() => expect(requests).toHaveLength(3))
  expect(JSON.parse(requests[2])).toEqual({ ...JSON.parse(requests[0]), prompt: 'revised task', selector: { project: project.name, projectUID: 'replacement', template: 'explicit-template' } })
})

it('bounds discovery by page and exposes expiry, denial, transient failure and empty recovery without clearing selection', async () => {
  let status = 200
  let empty = false
  const paths: string[] = []
  vi.spyOn(globalThis, 'fetch').mockImplementation(async path => {
    if (path === '/api/v1/session') return response({ authenticated: true })
    if (String(path).includes('/projects?')) {
      paths.push(String(path))
      if (status !== 200) return new Response('{}', { status })
      return response(empty ? { items: [] } : { items: [project], continue: 'next+page' })
    }
    return response({ items: [] })
  })
  show()
  await userEvent.click(await screen.findByRole('button', { name: `Select platform UID ${project.uid}` }))
  expect(paths).toHaveLength(1)
  status = 410
  await userEvent.click(screen.getByRole('button', { name: 'Next Project page' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('Project page expired')
  expect(paths[1]).toBe('/api/v1/namespaces/default/projects?limit=50&continue=next%2Bpage')
  status = 403
  await userEvent.click(screen.getByRole('button', { name: 'Restart Project discovery' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('discovery denied')
  expect(screen.getByText(project.uid)).toBeInTheDocument()
  expect(screen.getByLabelText('Project reference')).toBeDisabled()
  status = 503
  await userEvent.click(screen.getByRole('button', { name: 'Retry Project page' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('discovery unavailable')
  status = 200; empty = true
  await userEvent.click(screen.getByRole('button', { name: 'Retry Project page' }))
  await screen.findByText('No Projects on this page.')
  expect(screen.getByText(project.uid)).toBeInTheDocument()
  await userEvent.click(screen.getByRole('button', { name: 'Clear exact Project selection' }))
  expect(screen.getByLabelText('Project reference')).toBeEnabled()
  expect(screen.queryByText(project.uid)).not.toBeInTheDocument()
  empty = false
  await userEvent.click(screen.getByRole('button', { name: 'Restart Project discovery' }))
  await userEvent.click(await screen.findByRole('button', { name: 'Next Project page' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('repeated cursor reached')
  expect(screen.queryByRole('button', { name: 'Next Project page' })).not.toBeInTheDocument()
})

it('discards late discovery on namespace change and blocks navigation when old-server success drops exact UID', async () => {
  let resolvePage!: (value: Response) => void
  const requests: string[] = []
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
    if (path === '/api/v1/session') return response({ authenticated: true })
    if (String(path).includes('/default/projects?')) return new Promise(resolve => { resolvePage = resolve })
    if (String(path).includes('/other/projects?')) return response({ items: [{ ...project, namespace: 'other', uid: 'other-uid' }] })
    if (init?.method === 'POST') {
      requests.push(String(init.body))
      const { name, ...intent } = JSON.parse(String(init.body))
      delete intent.selector.projectUID
      return response({ name, uid: 'created-uid', intent })
    }
    return response({ items: [] })
  })
  show()
  await screen.findByText('Loading Projects…')
  await userEvent.click(screen.getByRole('link', { name: 'Other form' }))
  await userEvent.click(await screen.findByRole('button', { name: 'Select platform UID other-uid' }))
  await act(async () => resolvePage(response({ items: [project] })))
  expect(screen.queryByText(project.uid)).not.toBeInTheDocument()
  await userEvent.type(screen.getByLabelText('Prompt / task'), 'task')
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('Create outcome uncertain')
  expect(screen.getByTestId('location')).toHaveTextContent('/namespaces/other/runs/new')
  await userEvent.click(screen.getByRole('button', { name: 'Retry previous exact request' }))
  await waitFor(() => expect(requests).toHaveLength(2))
  expect(requests[1]).toBe(requests[0])
})

it('fences a late exact create success after namespace switch even when unrelated Run-list reads are denied', async () => {
  let finish!: () => void
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (path, init) => {
    if (path === '/api/v1/session') return response({ authenticated: true })
    if (String(path).includes('/projects?')) return response({ items: String(path).includes('/default/') ? [project] : [] })
    if (init?.method === 'POST') {
      const { name, ...intent } = JSON.parse(String(init.body))
      return new Promise(resolve => { finish = () => resolve(response({ name, uid: 'created-uid', intent })) })
    }
    return new Response('{}', { status: 403 })
  })
  show()
  await userEvent.click(await screen.findByRole('button', { name: `Select platform UID ${project.uid}` }))
  await userEvent.type(screen.getByLabelText('Prompt / task'), 'task')
  await userEvent.click(screen.getByRole('button', { name: 'Create run' }))
  await waitFor(() => expect(finish).toBeDefined())
  await userEvent.click(screen.getByRole('link', { name: 'Other form' }))
  await screen.findByText('No Projects on this page.')
  expect(screen.queryByText('Exact Project: platform')).not.toBeInTheDocument()
  await act(async () => finish())
  expect(screen.getByTestId('location')).toHaveTextContent('/namespaces/other/runs/new')
  expect(screen.getByLabelText('Prompt / task')).toHaveValue('')
})
