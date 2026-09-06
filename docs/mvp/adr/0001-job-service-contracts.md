# ADR 0001: Distributed Job Service Contracts

- Status: Accepted
- Date: 2026-09-04
- Scope: Phase 1 only
- Decision owners: API, worker, persistence, and dashboard implementations

## Context

The repository currently contains a durable queue, a bounded worker pool, a
dependency-auditor CLI, and a synchronous file-graph HTTP service. The target
deliverable separates job submission from execution:

```text
API server  ->  PostgreSQL queue  <-  Worker processes
                         |
                         v
                Operations dashboard
```

Before changing the database or runtime, the API, workers, persistence layer,
and dashboard need one shared contract. This ADR defines that contract. It does
not implement it.

## Decision summary

- PostgreSQL is the source of truth for jobs and lifecycle history.
- Delivery is **at least once**.
- Workers claim jobs with expiring leases and fencing tokens.
- A claimed attempt is counted when execution begins.
- Only transient failures are retried automatically.
- Retry scheduling is durable and does not sleep inside a worker.
- Cancellation is cooperative for running jobs and immediate for queued jobs.
- API and child-job submission support idempotency.
- Dependency audits use a root job with internal child jobs and a `waiting`
  state while descendants finish.
- Results and lifecycle events are persisted.
- Manual retry and DLQ replay create new jobs and preserve original history.

## Non-goals

This contract does not introduce:

- Priority queues or fair-share scheduling
- Cron or recurring jobs
- Kafka, Redis, or another queue backend
- Multi-region coordination
- Arbitrary job dependency graphs
- A generalized workflow designer
- Authentication or multi-tenant authorization
- File dependency visualization
- Runtime call-graph analysis

## Terminology

| Term | Meaning |
| --- | --- |
| Job | A durable unit of work submitted to the service |
| Attempt | One worker execution of a job |
| Lease | A time-bounded worker claim on a job |
| Fencing token | A unique token that prevents a stale worker from committing after losing its lease |
| Root job | A user-submitted job that owns a logical operation |
| Child job | Internal work created by a root job or another child |
| Terminal state | `completed`, `failed`, `dead_lettered`, or `cancelled` |
| DLQ entry | An immutable snapshot of a job that exhausted transient retries |

## Delivery guarantee

The service provides **at-least-once delivery**. A worker may execute a handler
more than once when it loses its lease, crashes after an external side effect,
or cannot persist its completion.

Therefore:

- Handlers must be idempotent.
- External side effects should use the job ID or a derived operation key as an
  idempotency key when the downstream system supports it.
- The service does not claim exactly-once execution.
- A stale worker may continue computing, but its fenced persistence writes must
  be rejected after its lease is lost.

## Job model

Every job has these logical fields:

| Field | Contract |
| --- | --- |
| `id` | Server-generated immutable identifier |
| `type` | Registered handler name |
| `payload` | Immutable JSON input |
| `status` | Current lifecycle state |
| `attempts` | Number of attempts that have started |
| `maxAttempts` | Maximum automatic execution attempts |
| `scheduledAt` | Earliest time the job may be claimed |
| `idempotencyKey` | Optional submission deduplication key |
| `requestHash` | Canonical hash used to detect key reuse with different input |
| `rootJobId` | Root operation identifier; self for root jobs |
| `parentJobId` | Direct parent for internal child jobs, otherwise absent |
| `internal` | Whether the job is hidden from the default public job list |
| `lockedBy` | Current worker identifier while leased |
| `leaseToken` | Current fencing token while leased |
| `lockedUntil` | Lease expiration time |
| `cancelRequestedAt` | Time cancellation was requested, if any |
| `lastErrorKind` | Most recent normalized failure category |
| `lastError` | Safe, bounded failure summary |
| `createdAt` | Creation time |
| `startedAt` | First attempt start time |
| `completedAt` | Terminal transition time |

Payloads and results are JSON. The implementation must reject unsupported job
types before persistence.

## Lifecycle states

The service uses these states:

| State | Meaning |
| --- | --- |
| `pending` | Durable and eligible when `scheduledAt <= now()` |
| `running` | Owned by a worker with a live lease |
| `waiting` | Root job has produced children and is waiting for them; it has no worker lease |
| `retry_scheduled` | Transient failure occurred and a future retry is scheduled |
| `completed` | Work and required descendants completed successfully |
| `failed` | A permanent failure ended processing |
| `dead_lettered` | Transient failures exhausted `maxAttempts` |
| `cancelled` | Cancellation reached a terminal state |

`waiting` exists only for root jobs coordinating internal child work. It is not
a general workflow dependency mechanism.

### Valid transitions

```text
submission
    -> pending

pending
    -> running
    -> cancelled

retry_scheduled
    -> running
    -> cancelled

running
    -> completed
    -> waiting
    -> retry_scheduled
    -> failed
    -> dead_lettered
    -> cancelled
    -> pending          (expired lease recovery)

waiting
    -> completed
    -> failed
    -> dead_lettered
    -> cancelled
```

Terminal states never transition in place. Manual retry or replay creates a new
job linked to the terminal job.

### Transition invariants

- Only `pending` and due `retry_scheduled` jobs may be claimed.
- Only `running` jobs have `lockedBy`, `leaseToken`, and `lockedUntil`.
- `waiting` jobs never hold a worker lease.
- A worker may update a running job only with the current fencing token.
- `completedAt` is set exactly once on entry to a terminal state.
- A completed job cannot contain an error.
- `attempts` cannot exceed `maxAttempts` through automatic retries.
- Every state transition creates an immutable lifecycle event.

## Claiming, leases, and heartbeats

### Worker identity

Each worker process receives a stable identifier for its lifetime. The default
format is a server-generated UUID with an optional human-readable prefix such
as the hostname. Worker identity is never inferred solely from a process ID.

### Atomic claim

A worker claims one eligible job in a short PostgreSQL transaction using row
locking with `FOR UPDATE SKIP LOCKED`. The transaction:

1. Selects the oldest eligible `pending` or due `retry_scheduled` job.
2. Changes it to `running`.
3. Increments `attempts`.
4. Sets `startedAt` if this is the first attempt.
5. Sets `lockedBy`, a new random `leaseToken`, and `lockedUntil`.
6. Creates a `started` attempt and lifecycle event.
7. Returns the claimed job and fencing token.

No network or handler work occurs inside the claim transaction.

### Lease timing

Initial defaults:

- Lease duration: 30 seconds
- Heartbeat interval: 10 seconds
- Recovery scan interval: 5 seconds

These values are configurable, but the heartbeat interval must remain less than
half the lease duration.

### Heartbeat

A heartbeat extends `lockedUntil` only when all of these still match:

- Job ID
- `running` status
- Worker ID
- Fencing token

If no row is updated, the worker has lost ownership. It must cancel the handler
context and must not persist a result or failure transition.

### Fenced completion

Completion and failure writes use the same ownership predicate as heartbeat.
A worker whose lease expired cannot overwrite a result produced by a newer
attempt.

### Expired lease recovery

The recovery process handles `running` jobs whose `lockedUntil` is in the past:

1. Mark the current attempt `abandoned`.
2. Record a `lease_expired` lifecycle event.
3. Clear lease fields.
4. If cancellation was requested, transition to `cancelled`.
5. If attempts remain, transition to `pending` for immediate reclaim.
6. If attempts are exhausted, transition to `dead_lettered` and create a DLQ
   entry.

Recovery never resets all running jobs during process startup. Only expired
leases are recoverable.

## Failure classification and retries

Handlers return or wrap one of three categories:

| Kind | Meaning | Automatic behavior |
| --- | --- | --- |
| `transient` | A later attempt may succeed | Retry with durable backoff when attempts remain |
| `permanent` | The same input should not be attempted again | Transition directly to `failed` |
| `cancelled` | Execution stopped because cancellation was requested | Transition to `cancelled` |

Unknown errors are treated as permanent. This avoids retry storms caused by
unclassified programming or validation failures.

Typical transient failures include:

- Network timeouts
- Temporary DNS failures
- HTTP `429`
- HTTP `502`, `503`, and `504`
- Temporary database connectivity failures after the handler has started

Typical permanent failures include:

- Invalid payloads
- Unsupported job types
- Malformed manifests
- Repository not found
- Authentication or authorization failures
- Unsupported dependency formats

### Backoff

Retry delay uses capped exponential backoff with full jitter:

```text
maximumDelay = min(baseDelay * 2^(attempts - 1), retryCap)
delay        = random duration in [0, maximumDelay]
```

Initial defaults:

- Base delay: 1 second
- Retry cap: 5 minutes
- Default maximum attempts: 5

The chosen retry time is written to `scheduledAt`, the job moves to
`retry_scheduled`, and the worker becomes available immediately. Workers never
sleep while waiting for a retry time.

## Cancellation semantics

`POST /api/jobs/{id}/cancel` is idempotent.

### Queued jobs

For `pending` and `retry_scheduled` jobs, cancellation transitions directly to
`cancelled` in one transaction.

### Running jobs

For `running` jobs:

1. Set `cancelRequestedAt`.
2. Record a `cancellation_requested` event.
3. The owning worker observes cancellation during heartbeat polling.
4. The worker cancels the handler context.
5. The fenced worker transition records `cancelled`.

If the worker disappears, expired-lease recovery completes cancellation.

### Waiting root jobs

Cancelling a `waiting` root job marks the root and all non-terminal descendants
for cancellation in one transaction. Queued descendants become `cancelled`;
running descendants follow cooperative cancellation.

### Terminal jobs

- Cancelling an already `cancelled` job returns the existing representation.
- Cancelling any other terminal job returns `409 Conflict`.

Handlers are required to observe context cancellation. The service cannot
guarantee interruption of an external side effect already accepted downstream.

## Idempotency

### Public submission

`POST /api/jobs` accepts an optional `Idempotency-Key` header.

- The key is trimmed, bounded, and treated as an opaque case-sensitive value.
- The server stores a canonical hash of job type, payload, and `maxAttempts`.
- A new key creates a job and returns `202 Accepted`.
- Repeating the same key and request returns the original job with `200 OK`.
- Reusing the key with different request content returns `409 Conflict`.
- Concurrent requests with the same key are serialized by a unique database
  constraint.

Keys are retained with job history for this deliverable. Automatic expiration
is deferred.

### Internal child jobs

Dependency-audit child jobs require deterministic idempotency keys. The key is
derived from the root audit ID, ecosystem, normalized package coordinate, and
operation type. Discovery of the same package through multiple parents creates
relationships but does not create duplicate package-processing jobs.

## Parent and child jobs for dependency audits

The public job type is `dependency_audit`. Package-resolution work uses an
internal `audit_package` type.

### Root behavior

1. The root `dependency_audit` job validates its immutable payload.
2. It creates an audit result record and idempotent seed child jobs.
3. It transitions from `running` to `waiting` and releases its lease.
4. Children may create more `audit_package` children using deterministic keys.
5. A transaction updates audit progress whenever a child becomes terminal.
6. When no active descendants remain, the root is finalized.

### Root outcome

- Policy violations are successful audit findings; they do not fail the job.
- The root becomes `completed` when traversal finishes and a report is stored,
  even when violations exist.
- A permanent orchestration or manifest failure makes the root `failed`.
- An exhausted infrastructure failure that prevents a complete report makes
  the root `dead_lettered` and creates a root DLQ entry.
- Cancelling the root cancels the complete audit operation.

### Child visibility

Internal children are excluded from the default `GET /api/jobs` response. They
remain queryable through root-job detail so the dashboard can show progress and
attempt history without overwhelming the main job table.

This model supports the reference workload only; it is not a general-purpose
DAG scheduler.

## Result persistence

### Generic results

Each completed public job may have one immutable JSON result associated with
its job ID. Result writes occur in the same transaction as the transition to
`completed`.

The generic result record includes:

- Job ID
- Result schema version
- JSON result
- Creation time

Failed, dead-lettered, and cancelled jobs do not have successful result rows.
Their diagnostics remain in attempts and lifecycle events.

### Dependency-audit results

Dependency audits additionally persist normalized data for operational queries
and report reconstruction:

- Audit metadata and status
- Resolved packages and versions
- Dependency relationships
- Policy violations
- Diagnostics
- Final summary

The final generic job result references the audit ID and contains a bounded
summary. Large package inventories and graphs are retrieved through dedicated
audit-result queries rather than duplicated into the job row.

### Immutability and payload safety

- Original job payloads are immutable.
- Successful results are immutable.
- Lifecycle events and attempts are append-only.
- Public API responses must not expose secrets or raw internal stack traces.
- Stored error messages and JSON payloads have enforced size limits.

## DLQ behavior and replay

### Entry creation

A DLQ entry is created atomically with a job transition to `dead_lettered`. It
captures:

- Original job ID and type
- Root and parent identifiers
- Original payload
- Attempt count
- Last transient error kind and safe message
- Dead-letter time
- Replay linkage, when replayed

Permanent failures use `failed`, not the DLQ. The DLQ is reserved for retryable
work that exhausted its automatic attempt budget or repeatedly lost leases.

### Replay

`POST /api/dlq/{id}/replay` creates a new `pending` job. It never mutates the
original job back into a non-terminal state.

The replay transaction:

1. Locks the DLQ entry.
2. Rejects a second replay when an active replay already exists.
3. Creates a new job with a new ID and reset attempt count.
4. Copies the original type and payload.
5. Records `replayedFromJobId` on the new job.
6. Records `replayedAsJobId` and `replayedAt` on the DLQ entry.
7. Creates submission and replay lifecycle events.

A replay may accept a new `maxAttempts` within normal validation bounds, but it
cannot change the job type or payload. Correcting invalid input requires a new
job submission, not replay.

If a replayed job later exhausts its retries, it creates a new DLQ entry with
its own history.

## HTTP API contract

All API responses use JSON except `/metrics`. All timestamps use RFC 3339 with
UTC offsets. Unknown JSON fields are rejected on mutation endpoints. Request
bodies are size-limited.

### Error shape

```json
{
  "error": {
    "code": "job_not_found",
    "message": "The requested job does not exist."
  }
}
```

`code` is stable for clients. `message` is safe for display and does not contain
stack traces, SQL, credentials, or internal filesystem paths.

### Submit a job

```http
POST /api/jobs
Idempotency-Key: optional-client-key
Content-Type: application/json
```

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

Validation rules:

- `type` must be `demo` or `dependency_audit` for this deliverable.
- `payload` must satisfy the selected handler schema.
- `maxAttempts` defaults to 5 and must be between 1 and 20.
- Payload size is limited to 64 KiB.

New submission response: `202 Accepted`.

```json
{
  "job": {
    "id": "job-id",
    "type": "dependency_audit",
    "status": "pending",
    "attempts": 0,
    "maxAttempts": 5,
    "createdAt": "2026-09-04T18:00:00Z"
  }
}
```

Idempotent duplicate response: `200 OK` with the original job.

### List jobs

```http
GET /api/jobs?status=running&type=demo&limit=50&cursor=opaque
```

- Results use cursor pagination ordered by creation time and job ID descending.
- Default limit is 50; maximum is 100.
- `status`, `type`, and a bounded text query are optional filters.
- Internal child jobs are excluded by default.

```json
{
  "jobs": [],
  "nextCursor": null
}
```

### Get job detail

```http
GET /api/jobs/{id}
```

Returns the job, attempts, lifecycle events, result summary, and child-status
counts. Unknown IDs return `404 Not Found`.

### Cancel a job

```http
POST /api/jobs/{id}/cancel
```

Returns `200 OK` with the current job after applying or recording cancellation.
Invalid terminal transitions return `409 Conflict`.

### Retry a terminal job

```http
POST /api/jobs/{id}/retry
Content-Type: application/json
```

```json
{
  "maxAttempts": 5
}
```

- Allowed source states are `failed` and `dead_lettered`.
- A new job is created with a new ID and `retriedFromJobId` linkage.
- Type and payload cannot be changed.
- The response is `202 Accepted` with the new job.
- Retrying `completed`, active, or cancelled jobs returns `409 Conflict`.

### List DLQ entries

```http
GET /api/dlq?limit=50&cursor=opaque
```

Returns cursor-paginated DLQ entries ordered newest first.

### Replay a DLQ entry

```http
POST /api/dlq/{id}/replay
Content-Type: application/json
```

The optional body may set `maxAttempts` only. Success returns `202 Accepted`
with the newly created job.

### Health

`GET /health` is a liveness check. It does not depend on PostgreSQL and returns
`200 OK` while the HTTP process is functioning.

### Readiness

`GET /ready` checks that required configuration is valid and PostgreSQL can
serve a bounded query. It returns `200 OK` when ready and `503 Service
Unavailable` otherwise.

### Metrics

`GET /metrics` returns Prometheus text exposition. Initial metrics are limited
to:

- Jobs by current status
- Job submissions
- Attempts by outcome and job type
- Queue wait duration
- Handler duration
- Retry scheduling count
- Lease expiration count
- DLQ entry and replay counts
- Worker heartbeat freshness

Metrics must not contain job IDs, repository URLs, package names, or other
unbounded labels.

## Demo handler contract

The `demo` job is deterministic and contains only the controls needed to prove
queue behavior:

```json
{
  "durationMs": 3000,
  "transientFailures": 2,
  "permanentFailure": false,
  "result": { "message": "optional bounded value" }
}
```

Rules:

- `durationMs` is bounded.
- `transientFailures` means the first N attempts return a transient error.
- `permanentFailure` produces a permanent failure on the first attempt.
- The handler observes context cancellation.
- Behavior depends only on persisted attempt number and immutable payload, so it
  remains deterministic across process restarts.

## Dashboard contract

The operations dashboard consumes only the public HTTP API. It does not read
PostgreSQL directly.

It displays:

- Counts by job status
- Searchable, paginated public jobs
- Running jobs and worker lease information
- Job payload and result summary
- Attempts and lifecycle timeline
- Cancellation and manual retry actions
- DLQ inspection and replay

Polling is the initial update mechanism. Server-sent events may replace polling
later without changing persisted contracts, but are not required for the
deliverable.

## Compatibility outcome

The queued audit handler and operations dashboard replaced the temporary CLI
and graph-server compatibility paths during the final cutover. The retained
GitHub client retrieves dependency manifests only.

## Consequences

### Benefits

- API, worker, database, and dashboard share explicit state semantics.
- Fencing prevents stale workers from corrupting newer attempts.
- Durable retry scheduling releases worker capacity.
- Immutable retries and replays preserve operational history.
- The dependency auditor remains a meaningful self-feeding workload without
  turning the service into a general workflow engine.

### Costs

- `waiting` adds one root-only state.
- Lease heartbeats and fencing add database writes and worker complexity.
- At-least-once delivery requires idempotent handlers.
- Persisted attempts and events require retention planning in a future phase.

## Phase 1 acceptance criteria

- Job states and valid transitions are explicit.
- API request, response, pagination, and error shapes are explicit.
- Claim, lease, heartbeat, fencing, and recovery behavior are explicit.
- Failure categories and retry behavior are explicit.
- Cancellation behavior is explicit for every non-terminal state.
- Public and internal idempotency rules are explicit.
- Dependency-audit parent and child behavior is explicit.
- Generic and audit-specific result persistence is explicit.
- DLQ creation and replay behavior are explicit.
- Non-goals prevent implementation scope creep.
- No runtime code, migrations, frontend code, or deployment configuration is
  changed by this ADR.
