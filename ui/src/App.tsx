import React, { lazy, Suspense } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Link, Navigate, NavLink, Outlet, Route, Routes, useLocation,
  useNavigate, useOutletContext, useParams,
} from 'react-router'
import {
  api, ApiProblem, fallbackPollInterval, isTerminal, onUnauthorized, queryKeys,
  retryTransientResourceError, transientResourceError,
} from './api'
import type { CreateRun, Environment, PortalServiceList, Run, Selector } from './contracts'
import { useRunFeed } from './runFeed'
import { Transcript } from './Transcript'

const Terminal = lazy(() => import('./Terminal'))
const DNS_SUBDOMAIN = /^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/
const DNS_LABEL = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/
const MAX_CREATE_BODY_BYTES = 1024 * 1024
const hasASCIIControl = (value: string) => [...value].some(character => character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127)

// Exported for focused URL-boundary tests.
// eslint-disable-next-line react-refresh/only-export-components
export function validateNamespace(value: string): string | undefined {
  if (!value) return 'Namespace is required.'
  if (value.length > 63 || !DNS_LABEL.test(value)) {
    return 'Namespace must be a valid Kubernetes DNS label (lowercase letters, digits, and hyphens; maximum 63 characters).'
  }
}

// Exported for contract-level validation tests.
// eslint-disable-next-line react-refresh/only-export-components
export function validateCreateRun(value: CreateRun): string | undefined {
  if (!value.name) return 'Name is required.'
  if (value.name.length > 253 || !DNS_SUBDOMAIN.test(value.name) || value.name.split('.').some(part => part.length > 63 || !DNS_LABEL.test(part))) {
    return 'Name must be a valid DNS subdomain (lowercase letters, digits, dots, and hyphens; maximum 253 characters).'
  }
  if (!value.agent) return 'Agent is required.'
  if (value.agent.length > 128) return 'Agent must be at most 128 characters.'
  if (!value.prompt.trim()) return 'Prompt is required.'
  const refs = Object.entries(value.selector).filter((entry): entry is [string, string] => !!entry[1])
  if (!refs.length) return 'Choose an environment, project, or template.'
  if (value.selector.environment && (value.selector.project || value.selector.template)) return 'Environment cannot be combined with project or template.'
  if (refs.some(([, ref]) => ref.length > 253 || !DNS_SUBDOMAIN.test(ref) || ref.split('.').some(part => part.length > 63 || !DNS_LABEL.test(part)))) {
    return 'Selector references must be valid DNS subdomains with at most 253 characters.'
  }
  if (value.credentialProfile && (value.credentialProfile.length > 253 || !DNS_SUBDOMAIN.test(value.credentialProfile) || value.credentialProfile.split('.').some(part => part.length > 63 || !DNS_LABEL.test(part)))) return 'Credential profile must be a valid DNS subdomain with at most 253 characters.'
  if (new TextEncoder().encode(JSON.stringify(value)).length > MAX_CREATE_BODY_BYTES) return 'Create request must be at most 1 MiB.'
}

const Busy = ({ label = 'Loading' }: { label?: string }) => <p role="status">{label}…</p>
const Failure = ({ error }: { error: Error }) => <div role="alert"><strong>Request failed</strong><p>{error.message}</p></div>
type RunFeed = ReturnType<typeof useRunFeed>
const RunFeedContext = React.createContext<RunFeed | undefined>(undefined)
const useActiveRunFeed = () => {
  const feed = React.useContext(RunFeedContext)
  if (!feed) throw new Error('Run feed is unavailable')
  return feed
}

function RunFeedOutlet({ namespace }: { namespace: string }) {
  const feed = useRunFeed(namespace)
  if (feed.watchError instanceof ApiProblem && feed.watchError.status === 403) {
    return <main><Failure error={feed.watchError} /></main>
  }
  return <RunFeedContext.Provider value={feed}><Outlet /></RunFeedContext.Provider>
}
const age = (createdAt: string) => {
  const minutes = Math.max(0, Math.floor((Date.now() - new Date(createdAt).getTime()) / 60_000))
  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
}

function loginDestination(state: unknown) {
  if (!state || typeof state !== 'object' || !('from' in state)) return '/'
  const from = (state as { from?: unknown }).from
  if (!from || typeof from !== 'object') return '/'
  const candidate = from as { pathname?: unknown; search?: unknown; hash?: unknown }
  if (typeof candidate.pathname !== 'string' || !candidate.pathname.startsWith('/') || candidate.pathname.startsWith('//') || candidate.pathname.includes('\\') || candidate.pathname.includes('?') || candidate.pathname.includes('#') || hasASCIIControl(candidate.pathname)) return '/'
  const search = typeof candidate.search === 'string' && (candidate.search === '' || candidate.search.startsWith('?')) ? candidate.search : ''
  const hash = typeof candidate.hash === 'string' && (candidate.hash === '' || candidate.hash.startsWith('#')) ? candidate.hash : ''
  if (hasASCIIControl(search) || hasASCIIControl(hash) || search.includes('#')) return '/'
  try {
    if (new URL(`${candidate.pathname}${search}${hash}`, window.location.origin).origin !== window.location.origin) return '/'
  } catch { return '/' }
  return { pathname: candidate.pathname, search, hash }
}

function Login() {
  const [token, setToken] = React.useState('')
  const location = useLocation()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const mutation = useMutation({
    mutationFn: api.login,
    onSettled: () => setToken(''),
    onSuccess: session => {
      queryClient.setQueryData(queryKeys.session, session)
      navigate(loginDestination(location.state), { replace: true })
    },
  })
  return <main className="login"><form onSubmit={event => { event.preventDefault(); mutation.mutate(token) }}>
    <h1>SWE Operations</h1>
    <label>Access token<input aria-label="Access token" type="password" autoComplete="off" required value={token} onChange={event => setToken(event.target.value)} autoFocus /></label>
    <button disabled={mutation.isPending}>Sign in</button>
    {mutation.isError && <Failure error={mutation.error} />}
  </form></main>
}

function Auth() {
  const location = useLocation()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  React.useEffect(() => onUnauthorized(() => {
    queryClient.clear()
    navigate('/login', { state: { from: location }, replace: true })
  }), [location, navigate, queryClient])
  const session = useQuery({ queryKey: queryKeys.session, queryFn: api.session, retry: false })
  if (session.isPending) return <main><Busy label="Checking session" /></main>
  if (session.error instanceof ApiProblem && session.error.status === 401) return <Navigate to="/login" state={{ from: location }} replace />
  if (session.error) return <main><Failure error={session.error} /></main>
  return <Outlet />
}

function Shell() {
  const { namespace = 'default' } = useParams()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [nextNamespace, setNextNamespace] = React.useState(namespace)
  const [validation, setValidation] = React.useState('')
  React.useEffect(() => { setNextNamespace(namespace); setValidation('') }, [namespace])
  const logout = useMutation({ mutationFn: api.logout, onSuccess: () => { queryClient.clear(); navigate('/login', { replace: true }) } })
  const routeError = validateNamespace(namespace)
  return <><header>
    <Link to={`/namespaces/${encodeURIComponent(namespace)}/runs`} className="brand">SWE Operations</Link>
    <form className="namespace-switcher" onSubmit={event => {
      event.preventDefault()
      const error = validateNamespace(nextNamespace)
      setValidation(error || '')
      if (!error) navigate(`/namespaces/${encodeURIComponent(nextNamespace)}/runs`)
    }}>
      <label htmlFor="namespace">Namespace</label>
      <input id="namespace" value={nextNamespace} onChange={event => setNextNamespace(event.target.value)} aria-invalid={!!validation} aria-describedby={validation ? 'namespace-error' : undefined} />
      <button>Switch</button>
      {validation && <span id="namespace-error" role="alert">{validation}</span>}
    </form>
    <button onClick={() => logout.mutate()}>Log out</button>
  </header>{routeError ? <main><Failure error={new Error(routeError)} /></main> : <RunFeedOutlet namespace={namespace} />}</>
}

function RunList() {
  const { namespace = '' } = useParams()
  const query = useActiveRunFeed()
  return <main>
    <div className="title"><div><h1>Runs</h1><p>Agent tasks in {namespace}</p></div><Link className="button" to="new">New run</Link></div>
    {query.fallback && <p className="hint" role="status">Live updates unavailable; refreshing every 4 seconds.</p>}
    {!query.fallback && query.watchError && <p className="hint" role="status">Live updates disconnected; reconnecting…</p>}
    {query.isPending ? <Busy label="Loading runs" /> : query.error ? <Failure error={query.error} /> : !query.data.items.length ? <p role="status">No runs found.</p> :
      <div className="cards">{query.data.items.map(run => <Link className="card" key={run.uid} to={`${encodeURIComponent(run.name)}/overview`} state={{ runUID: run.uid }}>
        <div><strong>{run.name}</strong><span className="pill">{run.state}</span></div>
        <p>{run.promptPreview}</p><dl><dt>Agent</dt><dd>{run.agent}</dd><dt>Environment</dt><dd>{run.environment?.name || 'Allocating'}</dd><dt>Age</dt><dd title={run.createdAt}>{age(run.createdAt)}</dd></dl>
      </Link>)}</div>}
  </main>
}

type RunForm = { name: string; agent: string; prompt: string; credentialProfile: string; repositoryCredential: string; environment: string; project: string; template: string }
function NewRun() {
  const { namespace = '' } = useParams()
  const navigate = useNavigate()
  const [form, setForm] = React.useState<RunForm>({ name: '', agent: 'claude-code', prompt: '', credentialProfile: '', repositoryCredential: '', environment: '', project: '', template: '' })
  const [validation, setValidation] = React.useState('')
  const mutation = useMutation({
    mutationFn: (value: CreateRun) => api.createRun(namespace, value),
  })
  const field = (key: keyof RunForm, label: string, area = false) => <label>{label}{area
    ? <textarea value={form[key]} onChange={event => setForm({ ...form, [key]: event.target.value })} />
    : <input value={form[key]} onChange={event => setForm({ ...form, [key]: event.target.value })} />}</label>
  return <main><h1>New run</h1><form className="runform" onSubmit={event => {
    event.preventDefault()
    const selector: Selector = {}
    for (const key of ['environment', 'project', 'template'] as const) if (form[key].trim()) selector[key] = form[key].trim()
    const credentialProfile = form.credentialProfile.trim()
    const repositoryCredential = form.repositoryCredential.trim()
    const value: CreateRun = { name: form.name.trim(), agent: form.agent.trim(), prompt: form.prompt, selector, ...(credentialProfile ? { credentialProfile } : {}), ...(repositoryCredential ? { repositoryCredential: repositoryCredential as 'GitHubApp' } : {}) }
    const error = validateCreateRun(value)
    setValidation(error || '')
    if (!error) mutation.mutate(value, { onSuccess: run => navigate(`/namespaces/${encodeURIComponent(namespace)}/runs/${encodeURIComponent(run.name)}/overview`, { state: { runUID: run.uid } }) })
  }}>
    {field('name', 'Name')}{field('agent', 'Agent')}{field('credentialProfile', 'Credential profile')}
    <label>Git credential<select value={form.repositoryCredential} onChange={event => setForm({ ...form, repositoryCredential: event.target.value })}><option value="">None</option><option value="GitHubApp">GitHub App</option></select></label>{field('prompt', 'Prompt / task', true)}
    {field('project', 'Project reference')}{field('template', 'Template reference')}{field('environment', 'Existing environment reference')}
    <p className="hint">Use an existing environment alone, or provide a project and/or template.</p>
    {validation && <p role="alert">{validation}</p>}{mutation.isError && <Failure error={mutation.error} />}
    <button disabled={mutation.isPending}>Create run</button>
  </form></main>
}

function useRun() {
  const { namespace = '', run = '' } = useParams()
  const feed = useActiveRunFeed()
  const location = useLocation()
  const bootstrapRequest = React.useId()
  const selectedUID = location.state && typeof location.state === 'object' && 'runUID' in location.state && typeof location.state.runUID === 'string' && location.state.runUID
    ? location.state.runUID
    : undefined
  const [locked, setLocked] = React.useState<{ namespace: string; name: string; uid: string } | undefined>(
    selectedUID ? { namespace, name: run, uid: selectedUID } : undefined,
  )
  const [confirmed, setConfirmed] = React.useState<{ namespace: string; name: string; uid: string }>()
  const matchingLockedUID = locked?.namespace === namespace && locked.name === run ? locked.uid : undefined
  const runUID = selectedUID || matchingLockedUID
  React.useEffect(() => {
    if (selectedUID) setLocked({ namespace, name: run, uid: selectedUID })
  }, [namespace, run, selectedUID])
  const query = useQuery<Run | string, Error>({
    queryKey: runUID ? queryKeys.run(namespace, run, runUID) : queryKeys.runBootstrap(namespace, run, bootstrapRequest),
    queryFn: async ({ signal }) => {
      if (runUID) {
        const exact = await api.runExact(namespace, run, runUID, signal)
        setConfirmed({ namespace, name: run, uid: runUID })
        return exact
      }
      const discovered = await api.run(namespace, run, signal)
      setLocked({ namespace, name: run, uid: discovered.uid })
      return discovered.uid
    },
    retry: retryTransientResourceError,
    refetchInterval: current => feed.fallback && !!runUID && (!current.state.error || transientResourceError(current.state.error)) ? fallbackPollInterval : false,
    refetchOnWindowFocus: current => !current.state.error || transientResourceError(current.state.error),
    refetchOnReconnect: current => !current.state.error || transientResourceError(current.state.error),
  })
  const confirmedRun = runUID && confirmed?.namespace === namespace && confirmed.name === run && confirmed.uid === runUID && typeof query.data !== 'string'
    ? query.data
    : undefined
  return { ...query, isPending: !query.error && (!confirmedRun || query.isPending), data: confirmedRun }
}

function Detail() {
  const { namespace = '', run = '' } = useParams()
  const query = useRun()
  if (query.isPending) return <main><Busy label="Loading run" /></main>
  if (query.error) return <main><Link to={`/namespaces/${encodeURIComponent(namespace)}/runs`}>← Runs</Link><Failure error={query.error} /><p>Select a run from the list to try again.</p></main>
  if (!query.data) return <main><Busy label="Confirming run identity" /></main>
  return <main><div className="title"><div><Link to={`/namespaces/${encodeURIComponent(namespace)}/runs`}>← Runs</Link><h1>{run}</h1></div><span className="pill">{query.data.state}</span></div>
    <details className="card" key={`task:${query.data.uid}`}><summary>Task · {query.data.intent.agent}</summary><pre>{query.data.intent.prompt}</pre></details>
    {query.data.cancelRequested && !isTerminal(query.data.state) && <p role="status">Cancellation requested. Waiting for the agent to stop.</p>}
    <nav aria-label="Run sections"><NavLink to="overview">Overview</NavLink><NavLink to="transcript">Transcript</NavLink>{query.data.environment?.uid && <NavLink to="portals">Portals</NavLink>}{query.data.terminalAvailable && query.data.environment?.uid && <NavLink to="terminal">Terminal</NavLink>}</nav>
    <Outlet key={query.data.uid} context={query.data} />
  </main>
}

function useOutletRun() { return useOutletContext<Run>() }

function Overview() {
  const run = useOutletRun()
  const { namespace = '' } = useParams()
  const queryClient = useQueryClient()
  const [confirmCancel, setConfirmCancel] = React.useState(false)
  const keepRunning = React.useRef<HTMLButtonElement>(null)
  React.useEffect(() => { if (confirmCancel) keepRunning.current?.focus() }, [confirmCancel])
  const environmentUID = run.environment?.uid || ''
  const environment = useQuery<Environment, Error>({
    queryKey: queryKeys.environment(namespace, run.environment?.name || '', environmentUID),
    queryFn: ({ signal }) => api.environmentExact(namespace, run.environment!.name, environmentUID, signal),
    enabled: !!environmentUID,
    retry: retryTransientResourceError,
    refetchInterval: current => !isTerminal(run.state) && (!current.state.error || transientResourceError(current.state.error)) ? 4000 : false,
    refetchOnWindowFocus: current => !current.state.error || transientResourceError(current.state.error),
    refetchOnReconnect: current => !current.state.error || transientResourceError(current.state.error),
  })
  const cancel = useMutation({
    mutationFn: () => api.cancelRun(namespace, run.name, run.uid),
    onSuccess: updated => {
      const key = queryKeys.run(namespace, run.name, run.uid)
      queryClient.setQueryData<Run>(key, current => current?.uid === run.uid && updated.uid === run.uid ? updated : current)
      queryClient.invalidateQueries({ queryKey: key, exact: true })
    },
  })
  const env = environment.data
  return <section>
    <dl className="facts"><dt>State</dt><dd>{run.state}</dd><dt>Agent</dt><dd>{run.intent.agent}</dd><dt>Credential profile</dt><dd>{run.intent.credentialProfile || '—'}</dd><dt>Git credential</dt><dd>{run.intent.repositoryCredential || '—'}</dd><dt>Git credential status</dt><dd>{run.repositoryCredentialReason || '—'}</dd><dt>Prompt</dt><dd>{run.intent.prompt}</dd><dt>Created</dt><dd>{run.createdAt}</dd><dt>Started</dt><dd>{run.startedAt || '—'}</dd><dt>Finished</dt><dd>{run.finishedAt || '—'}</dd><dt>UID</dt><dd>{run.uid}</dd><dt>Branch</dt><dd>{run.branch || '—'}</dd></dl>
    <h2>Usage</h2><dl className="facts"><dt>CPU seconds</dt><dd>{run.usage.cpuSeconds}</dd><dt>Tokens in</dt><dd>{run.usage.tokensIn}</dd><dt>Tokens out</dt><dd>{run.usage.tokensOut}</dd></dl>
    <h2>Operational conditions</h2><table><thead><tr><th>Exposed fact</th><th>Status</th></tr></thead><tbody>
      <tr><td>Cancellation requested</td><td>{run.cancelRequested ? 'Yes' : 'No'}</td></tr>
      <tr><td>Environment allocated</td><td>{run.environment ? 'Yes' : 'No'}</td></tr>
      {environmentUID && <><tr><td>Environment ready</td><td>{environment.isPending ? 'Loading…' : env?.ready ? 'Yes' : 'No'}</td></tr><tr><td>Environment paused</td><td>{environment.isPending ? 'Loading…' : env?.paused ? 'Yes' : 'No'}</td></tr></>}
    </tbody></table>
    <h2>Environment</h2>{!run.environment ? <p>Not allocated.</p> : !environmentUID ? <p role="status">Exact Environment identity unavailable.</p> : environment.error ? <Failure error={environment.error} /> : <dl className="facts"><dt>Name</dt><dd>{run.environment.name}</dd><dt>Ownership</dt><dd>{run.environment.ownership}</dd><dt>Phase</dt><dd>{env?.phase || 'Loading…'}</dd><dt>Backend</dt><dd>{env?.backend || 'Loading…'}</dd><dt>Template</dt><dd>{env?.template || 'Loading…'}</dd><dt>Status</dt><dd>{env ? `${env.ready ? 'Ready' : 'Not ready'}, ${env.paused ? 'paused' : 'active'}` : 'Loading…'}</dd></dl>}
    {!isTerminal(run.state) && !run.cancelRequested && (confirmCancel ? <section className="card" aria-label="Confirm cancellation">
      <h2>Cancel {run.name}?</h2>
      <p>This requests that the agent stop its current task. It cannot be undone.</p>
      <div className="title"><button ref={keepRunning} disabled={cancel.isPending} onClick={() => { setConfirmCancel(false); cancel.reset() }}>Keep running</button><button className="danger" disabled={cancel.isPending} onClick={() => cancel.mutate()}>{cancel.isPending ? 'Requesting cancellation…' : 'Confirm cancellation'}</button></div>
    </section> : <button className="danger" onClick={() => setConfirmCancel(true)}>Cancel run</button>)}{cancel.isError && <Failure error={cancel.error} />}
  </section>
}

function TranscriptRoute() { const run = useOutletRun(); const { namespace = '' } = useParams(); return <Transcript key={`${namespace}/${run.name}/${run.uid}`} namespace={namespace} run={run.name} identity={run.uid} /> }
function TerminalRoute() { const run = useOutletRun(); const { namespace = '' } = useParams(); return run.terminalAvailable && run.environment?.uid ? <Suspense fallback={<Busy label="Loading terminal" />}><Terminal key={`${namespace}/${run.name}/${run.uid}/${run.environment.uid}`} namespace={namespace} run={run.name} runUID={run.uid} environment={run.environment.name} environmentUID={run.environment.uid} /></Suspense> : <p role="status">Terminal unavailable for this Run identity.</p> }

function safePortalURL(value?: string) {
  if (!value) return undefined
  try {
    const url = new URL(value)
    return url.protocol === 'https:' || url.protocol === 'http:' ? url.href : undefined
  } catch { return undefined }
}

function Portals() {
  const run = useOutletRun()
  const { namespace = '' } = useParams()
  const environmentUID = run.environment?.uid || ''
  const query = useQuery<PortalServiceList, Error>({
    queryKey: queryKeys.portals(namespace, run.name, run.uid, environmentUID),
    queryFn: () => api.portals(namespace, run.name, run.uid, environmentUID),
    enabled: !!environmentUID,
    retry: retryTransientResourceError,
    refetchInterval: current => !current.state.error || transientResourceError(current.state.error) ? 4000 : false,
    refetchOnWindowFocus: current => !current.state.error || transientResourceError(current.state.error),
    refetchOnReconnect: current => !current.state.error || transientResourceError(current.state.error),
  })
  if (!environmentUID) return <p role="status">Portals unavailable for this Run identity.</p>
  if (query.isPending) return <Busy label="Loading portals" />
  if (query.error) return <Failure error={query.error} />
  if (!query.data.items.length) return <section><p role="status">No authorized declared services.</p><p>Only declared HTTP services you can access appear here. If your task needs a preview, declare it in <code>.swe/services.yaml</code> in the repository or use <code>swe environment services</code> from the CLI.</p><Link to="../transcript">View task progress</Link></section>
  return <section>
    <p className="hint">Links are stable gateway locators, not credentials. Opening an idle portal wakes its Environment. Portal sessions are host-local.</p>
    <table><thead><tr><th>Service</th><th>Port</th><th>Status</th><th>Portal</th></tr></thead><tbody>{query.data.items.map(service => {
      const url = safePortalURL(service.url)
      const openURL = service.openURL?.startsWith('/') && !service.openURL.startsWith('//') ? service.openURL : undefined
      return <tr key={service.name}><td>{service.name}</td><td>{service.targetPort}</td><td><span className={`service-status service-${service.status.toLowerCase()}`}>{service.status}</span>{service.reason && <small>{service.reason}</small>}</td><td>{url && openURL ? <><form className="portal-open" action={openURL} method="post" target="_blank" rel="noopener"><button>Open portal</button></form><small>{url}</small></> : 'Not configured'}</td></tr>
    })}</tbody></table>
  </section>
}

export function App() {
  return <Routes>
    <Route path="/login" element={<Login />} />
    <Route element={<Auth />}>
      <Route path="/" element={<Navigate to="/namespaces/default/runs" replace />} />
      <Route path="/namespaces/:namespace" element={<Shell />}>
        <Route path="runs" element={<RunList />} /><Route path="runs/new" element={<NewRun />} />
        <Route path="runs/:run" element={<Detail />}>
          <Route index element={<Navigate to="overview" replace />} /><Route path="overview" element={<Overview />} />
          <Route path="transcript" element={<TranscriptRoute />} /><Route path="portals" element={<Portals />} /><Route path="terminal" element={<TerminalRoute />} />
          <Route path="changes/*" element={<Navigate to="../overview" replace />} />
        </Route>
      </Route>
    </Route>
    <Route path="*" element={<Navigate to="/" replace />} />
  </Routes>
}
