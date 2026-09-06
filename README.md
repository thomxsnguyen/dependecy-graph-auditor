# Distributed Job Queue Service

A deployable PostgreSQL-backed job queue with a Go API, independently scalable
workers, an operations dashboard, dependency auditing, and package-graph
analysis as reference workloads.

```text
Browser → Dashboard → API → PostgreSQL ← Worker 1..N
```

## Start the complete application

Docker Compose is the supported delivery path:

```bash
cp .env.example .env          # edit GITHUB_TOKEN to raise GitHub API rate limits
docker compose up --build
```

Open the dashboard at [http://localhost:3000](http://localhost:3000). The API is
available at `http://localhost:8080`.

Compose starts PostgreSQL, applies every migration, starts the API, starts two
workers (each with `WORKER_CONCURRENCY=4`), and serves the dashboard. Job state
persists in the `pg_data` volume across restarts. Use `docker compose down -v`
only when you intentionally want to delete that history.

## Run components locally

Start only the database and run migrations:

```bash
docker compose up -d postgres migrate
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable'
```

Then use separate terminals:

```bash
go run ./cmd/api
go run ./cmd/worker
go run ./cmd/worker    # optional second worker for concurrency testing
```

Run the dashboard in development mode:

```bash
cd web
npm install
npm run dev
```

## Environment variables

### API (`cmd/api`)

| Variable           | Default        | Description                      |
|--------------------|----------------|----------------------------------|
| `DATABASE_URL`     | *(required)*   | PostgreSQL connection string     |
| `API_ADDR`         | `0.0.0.0:8080` | Bind address                     |
| `SHUTDOWN_TIMEOUT` | `10s`          | Graceful-shutdown window         |

### Worker (`cmd/worker`)

| Variable                    | Default              | Description                                      |
|-----------------------------|----------------------|--------------------------------------------------|
| `DATABASE_URL`              | *(required)*         | PostgreSQL connection string                     |
| `WORKER_ID`                 | `<hostname>-<uuid8>` | Unique identifier shown in heartbeat rows        |
| `WORKER_CONCURRENCY`        | `10`                 | Maximum simultaneous job goroutines              |
| `WORKER_LEASE_DURATION`     | `30s`                | How long a claimed job is locked to this worker  |
| `WORKER_HEARTBEAT_INTERVAL` | `10s`                | How often the worker renews its lease            |
| `WORKER_RECOVERY_INTERVAL`  | `5s`                 | How often expired leases are reclaimed           |
| `WORKER_POLL_INTERVAL`      | `250ms`              | How often the worker polls for pending jobs      |
| `SHUTDOWN_TIMEOUT`          | `25s`                | Grace period to finish in-flight jobs on SIGTERM |
| `GITHUB_TOKEN`              | *(optional)*         | Raises GitHub API rate limits for audit jobs     |

## HTTP API

All endpoints are served by `cmd/api` on port `8080`.

| Method | Path                        | Description                                              |
|--------|-----------------------------|----------------------------------------------------------|
| `GET`  | `/health`                   | Always `200 OK`                                          |
| `GET`  | `/ready`                    | `503` if PostgreSQL is unavailable                       |
| `GET`  | `/metrics`                  | Prometheus-style text metrics                            |
| `POST` | `/api/jobs`                 | Submit a new job (`202 Accepted`)                        |
| `GET`  | `/api/jobs`                 | List jobs (filter: `status`, `type`, `q`, `cursor`, `limit`) |
| `GET`  | `/api/jobs/{id}`            | Get a single job and its children                        |
| `POST` | `/api/jobs/{id}/cancel`     | Cancel a pending or waiting job                          |
| `POST` | `/api/jobs/{id}/retry`      | Re-queue a failed or dead-lettered job                   |
| `GET`  | `/api/dlq`                  | List dead-lettered entries (`cursor`, `limit`)           |
| `POST` | `/api/dlq/{id}/replay`      | Re-submit a DLQ entry as a new job                       |

Pass `Idempotency-Key: <string>` (≤ 128 chars) on `POST /api/jobs` to
deduplicate submissions. A duplicate key with the same body returns the existing
job (`200 OK`); a mismatched body returns `409 Conflict`.

## Job types

### `demo`

Controlled workload for testing queue mechanics.

```bash
curl -i -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"type":"demo","payload":{"durationMs":1000},"maxAttempts":5}'
```

| Payload field       | Type | Description                                            |
|---------------------|------|--------------------------------------------------------|
| `durationMs`        | int  | Simulated work time (0–120 000 ms)                     |
| `transientFailures` | int  | Transient errors to return before succeeding (0–20)    |
| `permanentFailure`  | bool | If `true`, the job always fails permanently            |
| `result`            | any  | Arbitrary JSON stored as the job result                |

### `dependency_audit`

Scans a public GitHub repository for supported dependency manifests and fans out
one child audit job per direct dependency. Supports `package.json` (npm),
`pyproject.toml` / `requirements.txt` (PyPI), and `go.mod` (Go modules).

```bash
curl -i -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"dependency_audit","payload":{"repositoryUrl":"https://github.com/owner/repo","ref":"main"},"maxAttempts":3}'
```

| Payload field   | Type   | Description                                                      |
|-----------------|--------|------------------------------------------------------------------|
| `repositoryUrl` | string | `https://github.com/owner/repository`                            |
| `ref`           | string | Branch, tag, or commit SHA (optional; defaults to default branch)|

### `audit_npm_package`

Audits a single npm package against the license policy. Spawned automatically
by `dependency_audit`; can also be submitted directly.

### `audit_pypi_package`

Audits a single PyPI package (resolved for Python 3.12 / Linux). Spawned
automatically by `dependency_audit`.

### `audit_go_module`

Audits a single Go module. Spawned automatically by `dependency_audit`.

## Job lifecycle

```
pending → running → completed
                 ↘ retry_scheduled → pending (exponential back-off)
                 ↘ failed → dead_lettered
pending/running → cancelled
pending         → waiting  (child waiting for parent)
```

| Status            | Meaning                                          |
|-------------------|--------------------------------------------------|
| `pending`         | Ready to be claimed by a worker                  |
| `running`         | Claimed and actively being processed             |
| `waiting`         | Child job waiting for its parent to complete     |
| `retry_scheduled` | Transient failure; will retry after back-off     |
| `completed`       | Finished successfully                            |
| `failed`          | Exhausted all attempts                           |
| `dead_lettered`   | Moved to the DLQ after final failure             |
| `cancelled`       | Cancelled by an operator                         |

## Verification

```bash
# Unit tests + race detector
go test -race ./...

# Integration tests (requires a running PostgreSQL instance)
DATABASE_URL="$DATABASE_URL" go test -race -tags=integration ./internal/store/postgres

# Frontend checks
cd web
npm run typecheck
npm run lint
npm test
npm run build
```

See [architecture](docs/mvp/architecture.md), [reliability guarantees](docs/mvp/reliability.md),
[test strategy](docs/mvp/testing.md), and the [demo runbook](docs/mvp/demo.md).
