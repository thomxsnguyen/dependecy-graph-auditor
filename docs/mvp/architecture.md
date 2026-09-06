# Service architecture

The product has four runtime components:

```text
Operations dashboard -> Job API -> PostgreSQL <- Worker processes
```

- `cmd/api` validates submissions and exposes job, cancellation, retry, DLQ,
  health, readiness, and metrics endpoints. It never executes jobs.
- PostgreSQL is the queue and system of record for jobs, attempts, immutable
  events, leases, results, workers, audit data, and DLQ entries.
- `cmd/worker` atomically claims due work with `FOR UPDATE SKIP LOCKED`. Each
  process has bounded concurrency and can scale independently from the API.
- `web` polls the public API and supplies only operational controls.

## Execution flow

1. The API inserts a `pending` job and returns its ID with `202 Accepted`.
2. One worker claims it, increments the attempt, and receives a fencing token.
3. The worker renews its lease while the handler runs.
4. Success, classified failure, and child creation are committed transactionally.
5. A transient failure becomes a durable `retry_scheduled` job or a DLQ entry.
6. A recovery scan reclaims only `running` jobs whose leases have expired.

Dependency audits are the one self-feeding reference workload. A public root
job scans `package.json`, `pyproject.toml`, `requirements.txt`, and `go.mod`,
then creates idempotent internal package jobs for npm, PyPI, and Go. Normalized
package findings and relationships remain queryable from the root job detail.

The exact lifecycle and API contracts are defined in
[ADR 0001](adr/0001-job-service-contracts.md).
