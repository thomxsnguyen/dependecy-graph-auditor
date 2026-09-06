# Distributed Job Queue Service Transition Plan

## Objective

Transition the current repository into a deployable **Distributed Job Queue
Service** with an operational dashboard and dependency auditing as its reference
workload.

```text
API server  ->  PostgreSQL queue  <-  Worker processes
                         |
                         v
                Operations dashboard
```

The API accepts and manages jobs, PostgreSQL owns durable state, workers execute
jobs independently, and the dashboard provides operational visibility and
recovery controls.

## Scope statement

> Build a durable, PostgreSQL-backed distributed job queue with an HTTP API,
> independently scalable workers, operational visibility, and dependency
> auditing as one reference workload.

A feature is in scope only when it directly helps demonstrate:

- Job submission and lifecycle management
- Worker coordination
- Persistence
- Retries and dead-letter handling
- Crash recovery
- Operational visibility
- Deployment or production-quality verification

File architecture inference and file-graph presentation are outside this scope.

## Target deliverable

```text
cmd/api       - accepts and manages jobs
cmd/worker    - claims and processes jobs
PostgreSQL    - durable queue, attempts, leases, results, and DLQ
web           - job operations dashboard
```

The dependency auditor remains a supported job type. Its results may be shown
as summaries and tables. Any dependency graph is an optional result view rather
than the main product.

## Target HTTP API

```http
POST   /api/jobs
GET    /api/jobs
GET    /api/jobs/{id}
POST   /api/jobs/{id}/cancel
POST   /api/jobs/{id}/retry

GET    /api/dlq
POST   /api/dlq/{id}/replay
GET    /health
GET    /ready
GET    /metrics
```

Example submission:

```json
{
  "type": "dependency_audit",
  "payload": {
    "repositoryUrl": "https://github.com/example/project",
    "ref": "main"
  },
  "maxAttempts": 5
}
```

The API should return `202 Accepted` immediately with a durable job identifier.

## Current repository assessment

### Keep

- `internal/job`
- `internal/queue`
- `internal/worker`
- `internal/store`
- `internal/dlq`
- `internal/auditor`
- `internal/depfile`
- `internal/gomod`
- `internal/pypi`
- `internal/semver`
- The parts of `internal/github` required to retrieve dependency manifests
- `cmd/auditor` temporarily, until dependency auditing runs through the worker

### Adapt

- Expand the PostgreSQL schema with leases, lifecycle timestamps,
  cancellation, idempotency, results, and attempts.
- Expand the store contract with listing, lookup, cancellation, replay,
  heartbeat, and operational query methods.
- Give each worker a stable identity and implement leases and heartbeats.
- Replace global recovery of running jobs with recovery of expired leases only.
- Introduce explicit transient, permanent, and cancelled failure categories.
- Persist dependency-audit results instead of relying on in-memory stores.
- Replace the fixture-backed graph interface with a live operations console.
- Expand Docker Compose to run the API, workers, dashboard, and PostgreSQL.

### Replace

| Current capability | Target capability |
| --- | --- |
| `cmd/filegraph-api` | `cmd/api` |
| Synchronous `POST /api/file-graphs` | Asynchronous `POST /api/jobs` |
| Fixture-backed audit UI | Live job API client |
| Dependency canvas as homepage | Job table and operational summary |
| Simulated recent activity | Persisted job-attempt and event history |

### Delete after replacement

- `cmd/filegraph-api`
- `internal/filegraph`
- `internal/httpapi/filegraph.go`
- File-graph frontend components
- File-graph data sources and fixtures
- File classification and hierarchy logic
- File-graph tests
- File-graph documentation
- Generated `*-file-graph.json` artifacts

Do not delete `internal/github` wholesale. Dependency auditing still requires
repository URL validation and remote manifest retrieval.

## Transition phases

### Phase 0: Freeze the current baseline

Status: complete.

1. Resolve the current uncommitted UI changes intentionally.
2. Verify that Go and frontend tests pass.
3. Create a baseline commit or tag.
4. Stop adding features to the file graph.

Exit condition:

```text
The existing system builds and has a recoverable Git checkpoint.
```

### Phase 1: Define contracts before implementation

Status: complete. The accepted contract is recorded in
[`docs/adr/0001-job-service-contracts.md`](adr/0001-job-service-contracts.md).

Write an architecture decision record defining:

- Job states and valid transitions
- API request and response formats
- Lease and heartbeat behavior
- Retryable versus permanent failures
- Cancellation semantics
- Idempotency behavior
- Parent and child jobs for dependency audits
- Result persistence
- DLQ replay behavior

Recommended states:

```text
pending
running
retry_scheduled
completed
failed
dead_lettered
cancelled
```

Exit condition:

```text
The API, database, and workers agree on one lifecycle model.
```

### Phase 2: Evolve PostgreSQL and the store

Status: complete.

Expand persistence while keeping the existing CLI operational. Expected data
areas include:

```text
jobs
job_attempts
job_events
job_results
workers
audit_results
dlq
```

Important job fields include:

```text
id
type
payload
status
attempts
max_attempts
scheduled_at
locked_by
locked_until
heartbeat_at
idempotency_key
cancel_requested_at
last_error
created_at
started_at
completed_at
```

Implement and test lease-safe store operations before changing worker behavior.

Exit condition:

```text
The database can represent every required lifecycle transition.
```

### Phase 3: Add the API alongside the existing server

Status: complete.

Create `cmd/api` without deleting `cmd/filegraph-api`.

Implement endpoints in this order:

1. `GET /health`
2. `GET /ready`
3. `POST /api/jobs`
4. `GET /api/jobs/{id}`
5. `GET /api/jobs`
6. Job cancellation
7. Job retry
8. DLQ listing and replay
9. Metrics

Both old and new servers may coexist temporarily.

Exit condition:

```text
A client can submit a job and inspect its durable state.
```

### Phase 4: Establish an independent worker

Status: complete.

Create `cmd/worker` and move queue execution out of `cmd/auditor`.

Implement:

1. Worker registration and identity
2. Atomic job claiming with a lease
3. Lease heartbeat renewal
4. Expired-lease recovery
5. Bounded concurrency
6. Typed transient and permanent failures
7. Scheduled retries with exponential backoff and jitter
8. DLQ transitions
9. Cancellation checks
10. Graceful shutdown and draining

Start with a controlled `demo` handler that can succeed, sleep, fail
transiently for a configured number of attempts, and fail permanently.

Exit condition:

```text
Two worker processes safely share work and recover an abandoned job.
```

### Phase 5: Move dependency auditing behind the queue

Status: complete.

Convert the auditor into a registered `dependency_audit` handler.

Preserve:

- Manifest parsing
- npm, PyPI, and Go dependency resolution
- License and policy evaluation
- Self-feeding child jobs

Change:

- Associate every job with an audit ID.
- Persist packages, relationships, violations, and summaries.
- Classify GitHub and registry failures using the retry policy.
- Make child-job creation idempotent.
- Return durable results through the job API.

Keep `cmd/auditor` as a compatibility tool until this path is proven. It may
then be removed or converted into an API client.

Exit condition:

```text
Submitting dependency_audit through the API produces a durable result.
```

### Phase 6: Replace the frontend

Status: complete.

Build the operations dashboard against the new API while leaving the old graph
code untouched until the replacement is complete.

Build only:

- Queue totals by status
- Searchable and paginated job table
- Live running jobs
- Selected-job details
- Attempt and lifecycle event timeline
- Payload and result details
- Worker and lease information
- Cancel and retry actions
- DLQ inspection and replay
- Polling or server-sent updates

Avoid charts, elaborate animations, worker topology maps, generalized workflow
design, and unrelated analytics.

Exit condition:

```text
The complete reliability demo can be performed through the operations dashboard.
```

### Phase 7: Cut over and remove the file graph

Status: complete.

After the new dashboard covers the complete demo:

1. Make `cmd/api` the only HTTP entrypoint.
2. Remove the Files mode from the frontend.
3. Remove `POST /api/file-graphs`.
4. Remove `cmd/filegraph-api`.
5. Remove `internal/filegraph`.
6. Remove file-graph frontend code.
7. Remove file-graph tests and fixtures.
8. Remove obsolete documentation and generated reports.
9. Search for remaining references and review each result.
10. Remove unused Go and npm dependencies.

Verification search:

```bash
rg -i "filegraph|file graph|file-graphs|file dependency" .
```

Validation:

```bash
go mod tidy
go test -race ./...

cd web
npm install
npm run typecheck
npm run lint
npm test
npm run build
```

Exit condition:

```text
No production path, documentation, fixture, or test describes the removed feature.
```

### Phase 8: Package the deliverable

Status: complete.

Complete:

- API Dockerfile
- Worker Dockerfile
- Frontend Dockerfile
- Complete Docker Compose stack
- Database migrations and migration procedure
- Health and readiness checks
- Environment variable documentation
- Metrics endpoint
- Architecture documentation
- Delivery-guarantee and failure-mode documentation
- Demo script
- Integration and crash-recovery tests

Exit condition:

```bash
docker compose up --build
```

starts the complete system.

## Required verification

The final test suite should prove:

- Concurrent workers never claim the same active lease.
- Expired leases are reclaimed.
- Retries survive restarts.
- Duplicate submissions are idempotent.
- Permanent errors are not retried.
- Exhausted jobs enter the DLQ.
- DLQ jobs can be replayed.
- Cancellation works.
- Graceful shutdown drains active work.
- Queue state and history survive process restarts.

## Final demonstration

1. Start PostgreSQL, the API, the dashboard, and two workers.
2. Submit several jobs.
3. Show work distributed between workers.
4. Kill one worker during execution.
5. Show its lease expire and another worker reclaim the job.
6. Trigger a transient failure and show automatic retry.
7. Trigger a permanent failure and show it enter the DLQ.
8. Replay the DLQ job from the dashboard.
9. Restart the stack and show that job history remains available.

## Scope guardrails

Explicitly defer:

- File dependency visualization
- Runtime call-graph analysis
- Arbitrary workflow design
- Kafka, Redis, or multiple queue backends
- Kubernetes operators
- Multi-region replication
- Complex authentication systems
- Priority and fair-share scheduling
- Cron scheduling
- Webhook ecosystems
- Additional dependency ecosystems
- Advanced analytics dashboards

These items may appear under future work, but they should not be implemented as
part of this deliverable.

## Recommended milestone commits

```text
Define job lifecycle and service boundaries
Add lease-aware job persistence
Add asynchronous job API
Add independently runnable worker
Add demo job handler and failure controls
Run dependency audits through durable jobs
Replace graph UI with operations dashboard
Remove obsolete graph vertical slice
Cover concurrency and crash recovery
Package deployable compose stack
Document architecture and demo workflow
```

## Transition rule

Do not begin by deleting the file graph. Freeze it, build the API and worker
beside it, prove the replacement dashboard, and then remove the old vertical
slice in one focused cleanup milestone.
