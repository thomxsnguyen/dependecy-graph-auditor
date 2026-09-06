# Reliability guarantees and failure modes

## Guarantee

Delivery is **at least once**, not exactly once. Leases make abandoned work
available again; a handler can therefore execute more than once. Handlers and
external effects must use stable idempotency keys.

## Coordination

- Claims use row locks with `SKIP LOCKED`, so active workers do not claim the
  same job.
- Every lease has a random fencing token. Completion, failure, and heartbeat
  writes require the current worker ID and token.
- A stale worker cannot overwrite a later attempt even if it finishes late.
- Recovery touches only expired leases; it never resets all running work.

## Failures

- Transient failures use exponential backoff with jitter and consume an attempt.
- Permanent failures move directly to `failed` and are not retried.
- Exhausted transient failures atomically create a `dead_lettered` job and DLQ
  snapshot.
- DLQ replay and manual retry create new linked jobs; original history is
  immutable.
- Queued cancellation is immediate. Running cancellation is cooperative and is
  observed at the next lease heartbeat.
- On SIGTERM, workers stop claiming, drain active handlers, and keep renewing
  leases. At the configured deadline, unfinished work is released as a
  transient retry.

PostgreSQL is a deliberate single dependency for this deliverable. If it is
unavailable, readiness fails, submissions fail, and workers retain no unsafe
in-memory acknowledgement state.
