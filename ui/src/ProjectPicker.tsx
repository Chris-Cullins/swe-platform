import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, ApiProblem } from './api'
import type { Project } from './contracts'

export function ProjectPicker({ namespace, selected, disabled, onSelect }: {
  namespace: string
  selected?: Project
  disabled: boolean
  onSelect: (project?: Project) => void
}) {
  const [page, setPage] = React.useState({ cursor: '', number: 1, revision: 0 })
  const query = useQuery({
    queryKey: ['project-discovery', namespace, page],
    queryFn: ({ signal }) => api.projects(namespace, page.cursor, signal),
    retry: false, gcTime: 0, staleTime: Infinity,
    refetchOnWindowFocus: false, refetchOnReconnect: false,
  })
  const expired = query.error instanceof ApiProblem && query.error.status === 410
  const denied = query.error instanceof ApiProblem && query.error.status === 403
  const next = query.data?.continue
  const blockedNext = next === page.cursor || page.number >= 100
  return <section className="project-picker" aria-label="Project discovery">
    <h2>Discover a Project</h2>
    <p className="hint">Choose a name and exact UID. Discovery is not a readiness check. The default Template name is resolved at allocation, not pinned here.</p>
    {query.isFetching ? <p role="status">Loading Projects…</p> : query.isError ? <p role="alert">{expired
      ? 'Project page expired. Restart discovery; your selected UID is unchanged.'
      : denied ? 'Project discovery denied. Your selected UID is unchanged; no name-only fallback was made.'
        : 'Project discovery unavailable. Retry or restart; your selected UID is unchanged.'}</p>
      : <>
        {!query.data?.items.length && <p>No Projects on this page.</p>}
        {query.data?.items.map(project => <div className="project-option" key={project.uid}>
          <div><strong>{project.name}</strong><br /><code>{project.uid}</code><p className="hint">Default Template: {project.defaultTemplate || 'None'}</p></div>
          <button type="button" disabled={disabled || selected?.uid === project.uid} onClick={() => onSelect(project)} aria-label={`Select ${project.name} UID ${project.uid}`}>{selected?.uid === project.uid ? 'Selected' : 'Select'}</button>
        </div>)}
        {next && blockedNext && <p role="alert">Discovery page limit or repeated cursor reached. Restart discovery.</p>}
        {next && !blockedNext && <button type="button" disabled={disabled} onClick={() => setPage({ ...page, cursor: next, number: page.number + 1 })}>Next Project page</button>}
      </>}
    <div className="project-actions">
      <button type="button" disabled={disabled || query.isFetching} onClick={() => setPage({ cursor: '', number: 1, revision: page.revision + 1 })}>Restart Project discovery</button>
      {query.isError && !expired && <button type="button" disabled={disabled || query.isFetching} onClick={() => void query.refetch()}>Retry Project page</button>}
    </div>
    {selected && <div role="status"><strong>Exact Project: {selected.name}</strong><br /><code>{selected.uid}</code><p>Selection survives refresh and retries. Selecting another Project deliberately revises intent.</p><button type="button" disabled={disabled} onClick={() => onSelect()}>Clear exact Project selection</button></div>}
  </section>
}
