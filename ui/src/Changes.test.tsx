import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { RunChangesView } from './Changes'
import type { Run, RunChanges } from './contracts'

const run = { name: 'review', uid: 'run-uid', state: 'Succeeded' } as Run
const snapshot: RunChanges = { runUID: run.uid, revision: 4, state: 'changed', capturedAt: '2026-09-05T12:00:00Z', final: true, unavailable: false, total: 3, files: [
  { path: 'src/main.go', kind: 'modified', state: 'text' },
  { path: 'image.png', kind: 'added', state: 'binary' },
  { path: 'large.txt', kind: 'modified', state: 'oversized' },
] }
function mount() { return render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><RunChangesView namespace="project" run={run} /></QueryClientProvider>) }
afterEach(() => vi.unstubAllGlobals())

describe('Run Changes', () => {
  it('pins file requests to the selected Run UID and observation, with explicit unsupported states', async () => {
    const fetch = vi.fn(async (input: string, init: RequestInit) => {
      expect(new Headers(init.headers).get('SWE-Run-UID')).toBe('run-uid')
      const url = new URL(input, 'http://test')
      const path = url.searchParams.get('path')
      if (path) expect(url.searchParams.get('revision')).toBe('4')
      return new Response(JSON.stringify(path ? { ...snapshot, files: snapshot.files.filter(file => file.path === path).map(file => ({ ...file, ...(file.state === 'text' ? { diff: '--- a/src/main.go\n+++ b/src/main.go\n-old\n+new\n' } : {}) })) } : snapshot))
    })
    vi.stubGlobal('fetch', fetch); mount()
    await userEvent.click(await screen.findByRole('button', { name: /modified src\/main.go/ }))
    expect(screen.getByLabelText('Observation revision')).toHaveTextContent('Revision 4')
    expect(await screen.findByLabelText('Diff for src/main.go')).toHaveTextContent('+new')
    await userEvent.click(screen.getByRole('button', { name: /added image.png binary/ }))
    expect(await screen.findByText(/Binary file changed/)).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: /modified large.txt oversized/ }))
    expect(await screen.findByText(/exceeds the review limit/)).toBeInTheDocument()
  })

  it.each(['clean', 'unavailable'] as const)('shows explicit %s results without inventing a diff', async state => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ ...snapshot, state, total: 0, files: [] }))))
    mount()
    expect(await screen.findByText(state === 'clean' ? /No changes in this captured observation/ : /Comparison unavailable/)).toBeInTheDocument()
    expect(screen.queryByLabelText('File diff')).not.toBeInTheDocument()
  })

  it('labels paused captures as retained and incomplete', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ ...snapshot, final: false, unavailable: true }))))
    mount()
    expect(await screen.findByText(/Latest capture unavailable/)).toBeInTheDocument()
    expect(screen.getByLabelText('Observation revision')).toHaveTextContent('Revision 4')
    expect(screen.getByText(/Pausing retains this review/)).toBeInTheDocument()
  })

  it.each([0, 12])('does not claim verified capture evidence for unavailable revision %s', async revision => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ ...snapshot, revision, state: 'unavailable', capturedAt: '0001-01-01T00:00:00Z', unavailable: true, total: 0, files: [] }))))
    mount()
    expect(await screen.findByLabelText('Observation revision')).toHaveTextContent(revision ? 'Revision 12' : 'No captured revision')
    expect(screen.getByText(/Comparison unavailable/)).toBeInTheDocument()
    expect(screen.queryByText(/Captured /)).not.toBeInTheDocument()
    expect(screen.queryByText(/No changes in this captured observation/)).not.toBeInTheDocument()
  })

  it('pins next/previous pages and files, then refreshes to the returned revision without old selection or diff', async () => {
    const files = Array.from({ length: 51 }, (_, i) => ({ path: `file-${i}.go`, kind: 'modified', state: 'text' } as const))
    let revision = 37
    const requests: { offset: string | null; revision: string | null; path: string | null }[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: string, init: RequestInit) => {
      expect(new Headers(init.headers).get('SWE-Run-UID')).toBe('run-uid')
      const url = new URL(input, 'http://test')
      expect(url.pathname).toBe('/api/v1/namespaces/project/runs/review/changes')
      const request = { offset: url.searchParams.get('offset'), revision: url.searchParams.get('revision'), path: url.searchParams.get('path') }
      requests.push(request)
      const offset = Number(request.offset)
      return new Response(JSON.stringify({ ...snapshot, revision, total: 51, next: offset ? undefined : 50, files: request.path
        ? [{ ...files.find(file => file.path === request.path), diff: `+revision-${revision}-diff` }]
        : files.slice(offset, offset + 50) }))
    }))
    mount()
    expect(await screen.findByLabelText('Observation revision')).toHaveTextContent('Revision 37')
    await userEvent.click(screen.getByRole('button', { name: 'modified file-0.go' }))
    expect(await screen.findByLabelText('Diff for file-0.go')).toHaveTextContent('+revision-37-diff')
    await userEvent.click(screen.getByRole('button', { name: 'Next files' }))
    await screen.findByRole('button', { name: 'modified file-50.go' })
    expect(screen.queryByLabelText('Diff for file-0.go')).not.toBeInTheDocument()
    expect(screen.getByText('51–51 of 51')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Next files' })).toBeDisabled()
    await userEvent.click(screen.getByRole('button', { name: 'modified file-50.go' }))
    expect(await screen.findByLabelText('Diff for file-50.go')).toHaveTextContent('+revision-37-diff')
    await userEvent.click(screen.getByRole('button', { name: 'Previous files' }))
    await screen.findByRole('button', { name: 'modified file-0.go' })
    expect(screen.queryByLabelText('Diff for file-50.go')).not.toBeInTheDocument()
    expect(screen.getByText('1–50 of 51')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Previous files' })).toBeDisabled()
    expect(requests).toEqual([
      { offset: null, revision: null, path: null },
      { offset: null, revision: '37', path: 'file-0.go' },
      { offset: '50', revision: '37', path: null },
      { offset: null, revision: '37', path: 'file-50.go' },
      { offset: null, revision: '37', path: null },
    ])
    await userEvent.click(screen.getByRole('button', { name: 'modified file-0.go' }))
    await screen.findByLabelText('Diff for file-0.go')
    revision = 83
    await userEvent.click(screen.getByRole('button', { name: 'Refresh' }))
    expect(await screen.findByText('Revision 83')).toBeInTheDocument()
    expect(requests.at(-1)).toEqual({ offset: null, revision: null, path: null })
    expect(screen.queryByLabelText('Diff for file-0.go')).not.toBeInTheDocument()
    expect(screen.getByText('Select a file to review its diff.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'modified file-0.go' })).toHaveAttribute('aria-pressed', 'false')
    await userEvent.click(screen.getByRole('button', { name: 'modified file-0.go' }))
    expect(await screen.findByLabelText('Diff for file-0.go')).toHaveTextContent('+revision-83-diff')
    expect(requests.at(-1)).toEqual({ offset: null, revision: '83', path: 'file-0.go' })
  })

  it('rejects a stale page and only loads a new revision after explicit refresh', async () => {
    let revision = 9
    vi.stubGlobal('fetch', vi.fn(async (input: string, init: RequestInit) => {
      expect(new Headers(init.headers).get('SWE-Run-UID')).toBe('run-uid')
      const url = new URL(input, 'http://test')
      if (url.searchParams.has('offset')) {
        expect(url.searchParams.get('revision')).toBe('9')
        revision = 14
        return new Response('{}', { status: 409 })
      }
      expect(url.searchParams.has('revision')).toBe(false)
      return new Response(JSON.stringify({ ...snapshot, revision, next: 50, total: 51 }))
    }))
    mount()
    await screen.findByText('Revision 9')
    await userEvent.click(screen.getByRole('button', { name: 'Next files' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('Refresh the file list')
    expect(screen.queryByLabelText('Observation revision')).not.toBeInTheDocument()
    expect(screen.queryByRole('navigation', { name: 'Changed files' })).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: 'Refresh' }))
    expect(await screen.findByText('Revision 14')).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.getByText('Select a file to review its diff.')).toBeInTheDocument()
  })

  it('rejects a same-name replacement response', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ ...snapshot, runUID: 'replacement' }))))
    mount()
    expect(await screen.findByRole('alert')).toHaveTextContent('different Run identity')
    expect(screen.queryByRole('button', { name: /src\/main.go/ })).not.toBeInTheDocument()
  })

  it('rejects a stale file revision and refreshes without retaining its diff', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: string) => new URL(input, 'http://test').searchParams.has('path')
      ? new Response('{}', { status: 409 }) : new Response(JSON.stringify(snapshot))))
    mount()
    await userEvent.click(await screen.findByRole('button', { name: /modified src\/main.go/ }))
    expect(await screen.findByRole('alert')).toHaveTextContent('Refresh the file list')
    await userEvent.click(screen.getByRole('button', { name: 'Refresh' }))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.getByText('Select a file to review its diff.')).toBeInTheDocument()
  })

  it('shows denied review access without displaying files', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ detail: 'Run Changes access denied' }), { status: 403 })))
    mount()
    expect(await screen.findByRole('alert')).toHaveTextContent('Run Changes access denied')
    expect(screen.queryByRole('navigation', { name: 'Changed files' })).not.toBeInTheDocument()
  })
})
