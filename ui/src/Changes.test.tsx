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
    expect(screen.getByText(/Pausing retains this review/)).toBeInTheDocument()
  })

  it('rejects a same-name replacement response', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ ...snapshot, runUID: 'replacement' }))))
    mount()
    expect(await screen.findByRole('alert')).toHaveTextContent('different Run identity')
    expect(screen.queryByRole('button', { name: /src\/main.go/ })).not.toBeInTheDocument()
  })
})
