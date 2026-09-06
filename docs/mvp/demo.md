# Demo runbook

1. Start the stack with `docker compose up --build` and open
   `http://localhost:3000`.
2. Submit several **Success** and **Long running** demo jobs. Select running rows
   to show work split between `worker-1` and `worker-2`.
3. Stop the worker owning a long job with `docker compose kill worker-1`. After
   its 15-second lease expires, show the other worker reclaiming the job.
4. Use **Retry twice** and show two `retry_scheduled` events followed by success.
5. For DLQ behavior, submit this bounded transient failure:

   ```bash
   curl -X POST http://localhost:8080/api/jobs \
     -H 'Content-Type: application/json' \
     -d '{"type":"demo","payload":{"transientFailures":10},"maxAttempts":2}'
   ```

6. Open **Dead letter queue**, inspect the error, and replay the entry.
7. Use **Permanent fail** and show that it enters `failed` without automatic
   retries or a DLQ entry.
8. Submit a public repository through **Audit dependencies** and inspect child
   counts and persisted npm, PyPI, or Go results.
9. Run `docker compose restart`, refresh the dashboard, and show the same job,
   attempt, event, result, and DLQ history.
