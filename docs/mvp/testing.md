# Test strategy

Run unit and component verification:

```bash
go test -race ./...
cd web
npm run typecheck
npm run lint
npm test
npm run build
```

PostgreSQL integration tests use isolated schemas and apply every migration:

```bash
docker compose up -d postgres migrate
DATABASE_URL='postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable' \
  go test -race -tags=integration ./internal/store/postgres
```

The integration suite covers persistence across helper-process restarts,
idempotent submission, mutually exclusive lease claims, stale fencing-token
rejection, durable retry scheduling, cancellation, expired-lease reclamation,
DLQ creation, and single replay. Worker unit tests cover successful execution
and bounded graceful shutdown. Dashboard tests cover queue totals, job detail,
lifecycle history, and controlled demo submission.
