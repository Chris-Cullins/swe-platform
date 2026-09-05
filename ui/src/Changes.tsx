import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useOutletContext, useParams } from 'react-router'
import { ApiProblem, getRunChanges, retryTransientResourceError } from './api'
import type { ChangedFile, Run } from './contracts'
import './changes.css'

const explanations: Record<ChangedFile['state'], string> = {
  text: 'No text differences; file mode changed.',
  binary: 'Binary file changed. A text diff is not available.',
  oversized: 'File content or diff exceeds the review limit. Changes cannot be fully determined for this file.',
  unavailable: 'File content could not be safely read. It may be a symlink, submodule, special file, or no longer accessible.',
}

export default function Changes() {
  const run = useOutletContext<Run>()
  const { namespace = '' } = useParams()
  return <RunChangesView key={`${namespace}/${run.uid}`} namespace={namespace} run={run} />
}

export function RunChangesView({ namespace, run }: { namespace: string; run: Run }) {
  const [page, setPage] = useState({ offset: 0, revision: 0 })
  const [selected, setSelected] = useState<{ path: string; revision: number }>()
  const list = useQuery({
    queryKey: ['changes', namespace, run.name, run.uid, page],
    queryFn: ({ signal }) => getRunChanges(namespace, run.name, run.uid, { ...page, signal }),
    retry: retryTransientResourceError,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  })
  const detail = useQuery({
    queryKey: ['changes-file', namespace, run.name, run.uid, selected],
    queryFn: ({ signal }) => getRunChanges(namespace, run.name, run.uid, { ...selected, signal }),
    enabled: !!selected,
    retry: retryTransientResourceError,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  })
  const refresh = () => {
    setSelected(undefined)
    if (page.offset || page.revision) setPage({ offset: 0, revision: 0 })
    else void list.refetch()
  }
  const failure = (error: Error) => error instanceof ApiProblem && error.status === 409
    ? 'The Run identity or captured changes changed. Refresh the file list; reselect the Run if its identity changed.'
    : error.message
  const snapshot = list.data
  const file = detail.data?.files[0]
  return <section className="changes-review" aria-label="Run changes">
    <div className="changes-heading"><div><h2>Workspace changes</h2><p className="hint">Compared with the workspace before this Run started, including pre-existing edits. Read-only review; nothing is committed or published.</p></div><button onClick={refresh} disabled={list.isFetching}>Refresh</button></div>
    {list.isPending && <p role="status">Loading changed files…</p>}
    {list.error && <p role="alert">{failure(list.error)}</p>}
    {snapshot && !list.error && <>
      <div className="changes-summary"><strong>{snapshot.total} {snapshot.total === 1 ? 'file' : 'files'}</strong><span>{snapshot.final ? 'Final capture outcome' : 'Retained observation'}</span>{snapshot.capturedAt && !snapshot.capturedAt.startsWith('0001-') && <time dateTime={snapshot.capturedAt}>Captured {new Date(snapshot.capturedAt).toLocaleString()}</time>}</div>
      {snapshot.unavailable && <p role="status" className="changes-warning">Latest capture unavailable. Any files shown are from the last verified observation, not a complete final result.</p>}
      {!snapshot.final && <p className="hint">The workspace may have changed after this capture. Pausing retains this review but may prevent a final capture. Refresh to load the latest retained observation.</p>}
      {snapshot.state === 'unavailable' && <p role="status">Comparison unavailable: no usable Run-start baseline or workspace capture. A missing baseline is never treated as an empty repository.</p>}
      {snapshot.state === 'clean' && <p role="status">No changes in this captured observation.</p>}
      {snapshot.files.length > 0 && <div className="changes-layout">
        <nav aria-label="Changed files" className="changes-files">{snapshot.files.map(item => <button key={item.path} className={selected?.path === item.path ? 'selected' : ''} onClick={() => setSelected({ path: item.path, revision: snapshot.revision })} aria-pressed={selected?.path === item.path}>
          <span className={`change-kind change-${item.kind}`}>{item.kind}</span><code>{item.path}</code>{item.state !== 'text' && <small>{item.state}</small>}
        </button>)}</nav>
        <div className="changes-file-detail" aria-label="File diff">
          {!selected && <p className="hint">Select a file to review its diff.</p>}
          {selected && <h3>{selected.path}</h3>}
          {detail.isFetching && selected && <p role="status">Loading diff…</p>}
          {detail.error && <p role="alert">{failure(detail.error)}</p>}
          {file && !detail.error && (file.diff ? <pre className="changes-diff" tabIndex={0} aria-label={`Diff for ${file.path}`}><code>{file.diff.split('\n').map((line, index) => <span key={index} className={line.startsWith('+') ? 'diff-add' : line.startsWith('-') ? 'diff-remove' : line.startsWith('@@') ? 'diff-hunk' : ''}>{line}{'\n'}</span>)}</code></pre> : <p role="status">{explanations[file.state]}</p>)}
        </div>
      </div>}
      {(page.offset > 0 || snapshot.next) && <div className="changes-pagination"><button disabled={!page.offset} onClick={() => { setSelected(undefined); setPage({ offset: Math.max(0, page.offset - 50), revision: snapshot.revision }) }}>Previous files</button><span>{page.offset + 1}–{page.offset + snapshot.files.length} of {snapshot.total}</span><button disabled={!snapshot.next} onClick={() => { setSelected(undefined); setPage({ offset: snapshot.next || 0, revision: snapshot.revision }) }}>Next files</button></div>}
      <p className="hint">Review limits: 4,096 paths, 256 KiB per file, 16 MiB of captured content, 512 KiB per diff. Binary, oversized, and unreadable files are explicit; renames appear as added and deleted files.</p>
    </>}
  </section>
}
