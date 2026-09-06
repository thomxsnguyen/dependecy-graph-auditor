import { useCallback, useEffect, useMemo, useState, type FormEvent } from "react"
import { Activity, AlertTriangle, Check, Clock3, Database, Play, RefreshCw, RotateCcw, Search, Square, X } from "lucide-react"
import { JobApi } from "../data/JobApi"
import type { DLQEntry, JobDetail, JobPage, JobStatus, UpdateResult } from "../types/jobs"

const statuses: JobStatus[] = ["pending", "running", "waiting", "retry_scheduled", "completed", "failed", "dead_lettered", "cancelled"]
const terminal = new Set<JobStatus>(["completed", "failed", "dead_lettered", "cancelled"])

function shortID(id: string) { return id.slice(0, 8) }
function when(value?: string) { return value ? new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium" }).format(new Date(value)) : "—" }
function pretty(value: unknown) { return JSON.stringify(value ?? {}, null, 2) }
function label(value: string) { return value.replaceAll("_", " ") }

export function OperationsDashboard() {
  const [page, setPage] = useState<JobPage>({ jobs: [], counts: {} })
  const [query, setQuery] = useState("")
  const [status, setStatus] = useState<JobStatus | "">("")
  const [selected, setSelected] = useState<JobDetail | null>(null)
  const [dlq, setDLQ] = useState<DLQEntry[]>([])
  const [view, setView] = useState<"jobs" | "dlq">("jobs")
  const [error, setError] = useState("")
  const [busy, setBusy] = useState(false)
  const [repositoryURL, setRepositoryURL] = useState("https://github.com/example/project")
	const selectedID = selected?.job.id

  const load = useCallback(async () => {
    try {
      const [jobs, dead] = await Promise.all([JobApi.list(query, status), JobApi.listDLQ()])
      setPage(jobs)
      setDLQ(dead.entries)
      setError("")
      if (selectedID) setSelected(await JobApi.get(selectedID))
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "The queue could not be loaded.")
    }
  }, [query, selectedID, status])

  useEffect(() => {
		const initial = window.setTimeout(() => void load(), 0)
    const timer = window.setInterval(() => void load(), 3000)
		return () => { window.clearTimeout(initial); window.clearInterval(timer) }
  }, [load])

  const act = async (action: () => Promise<unknown>, refresh = true) => {
    setBusy(true)
    try { await action(); if (refresh) await load() } catch (cause) { setError(cause instanceof Error ? cause.message : "Action failed.") }
    finally { setBusy(false) }
  }

	const loadMore = async () => {
		if (!page.nextCursor) return
		try {
			const next = await JobApi.list(query, status, page.nextCursor)
			setPage({ ...next, jobs: [...page.jobs, ...next.jobs] })
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : "More jobs could not be loaded.")
		}
	}

  const submitDemo = (payload: object) => act(async () => {
    const response = await JobApi.submit("demo", payload)
    setSelected(await JobApi.get(response.job.id))
  })

  const submitRepository = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (busy) return
    const submitter = (event.nativeEvent as SubmitEvent).submitter as HTMLButtonElement | null
    const type = submitter?.value === "dependency_update_scan" ? "dependency_update_scan" : "dependency_audit"
    void act(async () => {
      const payload = type === "dependency_update_scan" ? { repositoryUrl: repositoryURL } : { repositoryUrl: repositoryURL, ref: "main" }
      const response = await JobApi.submit(type, payload)
      setView("jobs")
      setSelected(await JobApi.get(response.job.id))
      // Selecting the new root refreshes the list through the existing effect.
    }, false)
  }

  const visibleEvents = useMemo(() => selected?.events ?? [], [selected])

  return (
    <main className="dashboard">
      <header className="topbar">
        <div><p className="eyebrow">Distributed job queue</p><h1>Operations dashboard</h1></div>
        <div className="health"><span className="health__dot" /> Service polling every 3s</div>
      </header>

      {error && <div className="notice" role="alert"><AlertTriangle size={18} />{error}<button aria-label="Dismiss error" onClick={() => setError("")}><X size={16}/></button></div>}

      <section className="summary" aria-label="Queue totals">
        {statuses.map((item) => <button key={item} className={`metric ${status === item ? "metric--active" : ""}`} onClick={() => { setStatus(status === item ? "" : item); setView("jobs") }}>
          <span>{label(item)}</span><strong>{page.counts[item] ?? 0}</strong>
        </button>)}
      </section>

      <section className="submit-panel">
        <div><h2>Submit work</h2><p>Use deterministic controls to demonstrate success, retry, cancellation, and DLQ behavior.</p></div>
        <div className="demo-actions">
          <button onClick={() => void submitDemo({ durationMs: 800, result: { message: "completed" } })}><Play size={16}/> Success</button>
          <button onClick={() => void submitDemo({ durationMs: 0, transientFailures: 2 })}><RefreshCw size={16}/> Retry twice</button>
          <button onClick={() => void submitDemo({ durationMs: 0, permanentFailure: true })}><AlertTriangle size={16}/> Permanent fail</button>
          <button onClick={() => void submitDemo({ durationMs: 60000 })}><Clock3 size={16}/> Long running</button>
        </div>
        <form className="audit-form" onSubmit={submitRepository}>
          <label htmlFor="repository">GitHub repository</label>
          <input id="repository" type="url" value={repositoryURL} onChange={(event) => setRepositoryURL(event.target.value)} required />
          <div className="repository-actions">
            <button type="submit" value="dependency_audit" disabled={busy}><Database size={16}/> Audit dependencies</button>
            <button type="submit" value="dependency_update_scan" disabled={busy}><RefreshCw size={16}/> Check for updates</button>
          </div>
        </form>
      </section>

      <nav className="tabs" aria-label="Queue views">
        <button className={view === "jobs" ? "active" : ""} onClick={() => setView("jobs")}>Jobs</button>
        <button className={view === "dlq" ? "active" : ""} onClick={() => setView("dlq")}>Dead letter queue <span>{dlq.length}</span></button>
      </nav>

      {view === "jobs" ? <section className="workspace">
        <div className="list-panel">
          <div className="list-toolbar">
            <label className="search"><Search size={16}/><input aria-label="Search jobs" placeholder="Search ID or type" value={query} onChange={(event) => setQuery(event.target.value)} /></label>
            <button className="icon-button" aria-label="Refresh jobs" onClick={() => void load()}><RefreshCw size={17}/></button>
          </div>
          <div className="table-wrap"><table><thead><tr><th>Job</th><th>Status</th><th>Attempts</th><th>Created</th><th>Worker</th></tr></thead>
            <tbody>{page.jobs.map((item) => <tr key={item.id} className={selected?.job.id === item.id ? "selected" : ""} onClick={() => void JobApi.get(item.id).then(setSelected)}>
              <td><strong>{item.type}</strong><code>{shortID(item.id)}</code></td><td><Status value={item.status}/></td><td>{item.attempts} / {item.maxAttempts}</td><td>{when(item.createdAt)}</td><td>{item.lockedBy ?? "—"}</td>
            </tr>)}</tbody></table>{page.jobs.length === 0 && <Empty text="No jobs match this view."/>}{page.nextCursor && <button className="load-more" onClick={() => void loadMore()}>Load more jobs</button>}</div>
        </div>
        <DetailPanel detail={selected} busy={busy} events={visibleEvents} onCancel={() => selected && void act(() => JobApi.cancel(selected.job.id))} onRetry={() => selected && void act(async () => { const response = await JobApi.retry(selected.job.id); setSelected(await JobApi.get(response.job.id)) })}/>
      </section> : <section className="dlq-panel"><h2>Dead letter queue</h2><p>Jobs appear here only after exhausting transient retries.</p>
        {dlq.length === 0 ? <Empty text="The dead letter queue is empty."/> : dlq.map((entry) => <article className="dlq-row" key={entry.id}><div><strong>{entry.jobType}</strong><code>{shortID(entry.jobId)}</code><p>{entry.error}</p></div><div><Status value="dead_lettered"/><span>{when(entry.deadAt)}</span>{entry.replayedAsJobId ? <span>Replayed as {shortID(entry.replayedAsJobId)}</span> : <button disabled={busy} onClick={() => void act(() => JobApi.replay(entry.id))}><RotateCcw size={15}/> Replay</button>}</div></article>)}</section>}
    </main>
  )
}

function Status({ value }: { value: JobStatus }) { return <span className={`status status--${value}`}>{value === "completed" ? <Check size={13}/> : value === "running" ? <Activity size={13}/> : null}{label(value)}</span> }
function Empty({ text }: { text: string }) { return <div className="empty"><Square size={20}/><p>{text}</p></div> }

function DetailPanel({ detail, busy, events, onCancel, onRetry }: { detail: JobDetail | null; busy: boolean; events: JobDetail["events"]; onCancel: () => void; onRetry: () => void }) {
  if (!detail) return <aside className="detail-panel detail-panel--empty"><Activity size={26}/><h2>Select a job</h2><p>Inspect its payload, lease, attempts, events, and durable result.</p></aside>
  const canCancel = !terminal.has(detail.job.status)
  const canRetry = detail.job.status === "failed" || detail.job.status === "dead_lettered"
  return <aside className="detail-panel"><div className="detail-heading"><div><p className="eyebrow">{detail.job.type}</p><h2>{shortID(detail.job.id)}</h2></div><Status value={detail.job.status}/></div>
    <div className="detail-actions">{canCancel && <button disabled={busy} onClick={onCancel}><Square size={15}/> Cancel</button>}{canRetry && <button disabled={busy} onClick={onRetry}><RotateCcw size={15}/> Retry as new job</button>}</div>
    <dl className="facts"><div><dt>Worker</dt><dd>{detail.job.lockedBy ?? "Unassigned"}</dd></div><div><dt>Lease expires</dt><dd>{when(detail.job.lockedUntil)}</dd></div><div><dt>Attempts</dt><dd>{detail.job.attempts} / {detail.job.maxAttempts}</dd></div><div><dt>Created</dt><dd>{when(detail.job.createdAt)}</dd></div></dl>
    {detail.job.lastError && <div className="error-box"><strong>{detail.job.lastErrorKind}</strong><p>{detail.job.lastError}</p></div>}
    {detail.job.type === "dependency_update_scan" && detail.job.id === detail.job.rootJobId && <DependencyUpdates detail={detail}/> }
    <h3>Attempts</h3><div className="attempts">{detail.attempts.length === 0 ? <p>No attempt has started.</p> : detail.attempts.map((attempt) => <div key={attempt.attempt}><strong>#{attempt.attempt} · {label(attempt.status)}</strong><span>{attempt.workerId} · {when(attempt.startedAt)}</span>{attempt.error && <p>{attempt.errorKind}: {attempt.error}</p>}</div>)}</div>
    <h3>Lifecycle</h3><ol className="timeline">{events.map((event) => <li key={event.id}><span/><div><strong>{label(event.type)}</strong><small>{when(event.occurredAt)}{event.workerId ? ` · ${event.workerId}` : ""}</small></div></li>)}</ol>
    <details><summary>Payload</summary><pre>{pretty(detail.job.payload)}</pre></details>
    {detail.result !== undefined && <details open><summary>Result</summary><pre>{pretty(detail.result)}</pre></details>}
    {detail.auditResults && detail.auditResults.length > 0 && <details open><summary>Audit packages ({detail.auditResults.length})</summary><div className="audit-results">{detail.auditResults.map((row) => <div key={`${row.ecosystem}:${row.name}@${row.version}`}><strong>{row.name}</strong><span>{row.ecosystem} · {row.version}</span><span>{row.license}</span><span>{row.verdict}</span></div>)}</div></details>}
  </aside>
}

const updateBadges: Record<UpdateResult["staleness"], { order: number; text: string; status: string }> = {
  major: { order: 0, text: "🔴 Major", status: "failed" },
  minor: { order: 1, text: "🟡 Minor", status: "retry_scheduled" },
  patch: { order: 2, text: "🟢 Patch", status: "completed" },
  up_to_date: { order: 3, text: "✅ Up to date", status: "completed" },
  unknown: { order: 4, text: "Unknown", status: "cancelled" },
}

function DependencyUpdates({ detail }: { detail: JobDetail }) {
  const rows = [...(detail.updateResults ?? [])].sort((a, b) =>
    updateBadges[a.staleness].order - updateBadges[b.staleness].order ||
    a.ecosystem.localeCompare(b.ecosystem) || a.name.localeCompare(b.name) || a.jobId.localeCompare(b.jobId))
  const counts = detail.childCounts ?? {}
  const total = Object.values(counts).reduce((sum, count) => sum + (count ?? 0), 0)
  const completed = counts.completed ?? 0
  const succeeded = detail.job.status === "completed"
  const incomplete = terminal.has(detail.job.status) && !succeeded
  const allUpToDate = succeeded && rows.length > 0 && rows.length === total && rows.every((row) => row.staleness === "up_to_date")

  return <section className="dependency-updates" aria-labelledby="dependency-updates-heading">
    <h3 id="dependency-updates-heading">Dependency updates</h3>
    <p>Current version is resolved from the manifest at scan time; it may differ from the installed version.</p>
    <div role="status">
      {incomplete && <p><strong>Incomplete</strong> · {counts.failed ?? 0} failed · {counts.dead_lettered ?? 0} dead-lettered · {counts.cancelled ?? 0} cancelled</p>}
      {!terminal.has(detail.job.status) && total === 0 && <p>Scanning dependency manifests…</p>}
      {total > 0 && <p>{completed} of {total} checks completed</p>}
      {succeeded && total === 0 && rows.length === 0 && <p>No direct dependencies found.</p>}
      {allUpToDate && <p>All checked dependencies are up to date.</p>}
    </div>
    {rows.length > 0 && <div className="table-wrap" tabIndex={0} role="region" aria-label="Package update results">
      <table aria-label="Dependency updates"><thead><tr><th scope="col">Package</th><th scope="col">Ecosystem</th><th scope="col">Current version</th><th scope="col">Latest version</th><th scope="col">Update</th></tr></thead>
        <tbody>{rows.map((row) => <tr key={row.jobId}>
          <td>{row.name}</td><td>{row.ecosystem}</td><td>{row.currentVersion}</td><td>{row.latestVersion}</td>
          <td><span className={`status status--${updateBadges[row.staleness].status}`}>{updateBadges[row.staleness].text}</span></td>
        </tr>)}</tbody>
      </table>
    </div>}
  </section>
}
