# Outdated Dependency Report — Operations Dashboard Integration

Status: implemented. Scan submission, root-scoped update results, and the dashboard report are available in the source code.

Backend implementation: [outdated-dependency-report.md](./outdated-dependency-report.md).
Feature specification: [outdated-dependency-report-plan.md](./outdated-dependency-report-plan.md).

## Objective

Allow an operator to submit a `dependency_update_scan` from the existing operations dashboard and view its package update results in the selected job's detail panel.

## Scope

Implement only:

1. A scan submission action using the existing repository URL input.
2. Aggregated child results in the existing job-detail API response.
3. A package update table with staleness badges, urgency ordering, and progress/empty/error states.

Reuse the existing layout, styles, API client, three-second polling, and job lifecycle controls. Do not add a separate page, endpoint, dependency, database migration, registry lookup, or background process. Transitive scanning, security advisories, automated upgrades, exports, historical comparisons, notifications, and dashboard redesign are outside scope.

## Implementation checklist

- [x] Extend the Go job-detail response with typed update results.
- [x] Load completed package update results for the selected scan root.
- [x] Extend the TypeScript API client and response types.
- [x] Add the dashboard scan submission action.
- [x] Render the package update table and scan states in the detail panel.
- [x] Verify API isolation, submission, rendering, ordering, and existing audit behavior.

## 1. Aggregate results in the existing API

Files: `internal/job/service.go` and `internal/store/postgres/service.go`.

Add an `UpdateResult` response type with these JSON fields:

| Field | Meaning |
| --- | --- |
| `jobId` | ID of the child job; stable row identity |
| `ecosystem` | `npm`, `pypi`, or `go` |
| `name` | Package or module name |
| `currentVersion` | Version resolved by the existing child handler |
| `latestVersion` | Latest version returned by the existing child handler |
| `staleness` | `major`, `minor`, `patch`, `up_to_date`, or `unknown` |

Add an optional `updateResults` array to `job.Detail`. Preserve the existing `result`, `childCounts`, `auditResults`, and other response fields.

In `Store.Get`, populate this array only when the requested job is a `dependency_update_scan` root. Read stored results through the existing tables:

```sql
SELECT j.id, r.result
FROM jobs j
JOIN job_results r ON r.job_id = j.id
WHERE j.root_job_id = $1
  AND j.parent_job_id = $1
  AND j.type = 'package_update_check'
  AND j.internal = TRUE
  AND j.status = 'completed'
ORDER BY j.id;
```

Bind `$1` to the selected root ID. Decode each stored result into the response type and attach its child job ID. Check query, row-scan, JSON-decode, and row-iteration errors; do not silently present a failed query as an empty report.

Return completed rows while other children are still running, and retain those rows when the root fails or is cancelled. Pending and failed children have no successful update result; represent their progress through the existing `childCounts` rather than inventing version values.

The frontend must treat an absent or empty `updateResults` field as an empty array. Do not use `GET /api/jobs?q={rootId}` to obtain child results: the list API excludes internal jobs and does not search root relationships.

## 2. Extend frontend types and submission

Files: `web/src/types/jobs.ts`, `web/src/data/JobApi.ts`, and `web/src/app/OperationsDashboard.tsx`.

- Add `dependency_update_scan` to the accepted `JobApi.submit` types.
- Add the `UpdateResult` interface and optional `updateResults` field to `JobDetail`, matching the API contract above.
- Add a **Check for updates** action beside the existing repository audit action, using the same URL input and required URL validation.
- Submit `dependency_update_scan` with `{ repositoryUrl }`. Omit `ref` so the scan uses the repository's default branch. No branch-selection UI is required.
- Use the existing busy/error handling, disable duplicate scan submissions while the request is in flight, and select the returned root job on success.
- Keep the existing audit submission action working with its current payload.

Use the existing refresh cycle to update the selected job and report. Do not add another timer or change worker behavior.

## 3. Render the report in the detail panel

Files: `web/src/app/OperationsDashboard.tsx` and, only where needed, `web/src/styles/globals.css`.

Render a **Dependency updates** section only for a selected `dependency_update_scan` root. Use a semantic table with these columns:

| Column | Source |
| --- | --- |
| Package | `name` |
| Ecosystem | `ecosystem` |
| Current version | `currentVersion` |
| Latest version | `latestVersion` |
| Update | Text badge derived from `staleness` |

Use `jobId` as the row key. Sort a copy of the rows in this fixed order, then by ecosystem, package name, and job ID for deterministic ties:

| Order | Staleness | Badge |
| --- | --- | --- |
| 1 | `major` | 🔴 Major |
| 2 | `minor` | 🟡 Minor |
| 3 | `patch` | 🟢 Patch |
| 4 | `up_to_date` | ✅ Up to date |
| 5 | `unknown` | Unknown |

Use text as well as color; these badges describe version gaps, not security severity. No interactive sorting or additional report filters are required.

Include a short explanation: **Current version is resolved from the manifest at scan time; it may differ from the installed version.** Preserve `unknown` as an explicit value rather than labelling it up to date.

## 4. Progress, empty, and error states

Reuse `job.status`, `childCounts`, and the existing root error display:

- While work is active, show completed checks versus total children and any available result rows. Before children are created, show **Scanning dependency manifests…**.
- After successful completion with zero dependencies, show **No direct dependencies found.** Do not show this message while a scan is still starting.
- When all result rows are up to date, show **All checked dependencies are up to date.** Only show this after successful scan completion with at least one result and no unknown rows.
- If the root fails, is dead-lettered, or is cancelled, retain successful rows and label the report **Incomplete**. Use child counts to show failed/dead-lettered/cancelled checks alongside the root's existing error.
- Keep API failures in the existing dashboard error notice. A failed refresh must not become an empty successful report.

Use the existing table overflow pattern for narrow panels and existing controls for cancellation and retry.

## 5. Verification

Extend existing tests rather than adding test infrastructure:

| Area | Required checks |
| --- | --- |
| PostgreSQL integration | Root-scoped aggregation; no results from another scan or audit; completed rows only; partial results retained; job IDs preserved; empty scan |
| API response | New fields serialize correctly; existing job/audit response fields remain available |
| Dashboard | Correct scan type/payload; submitted job selected; refresh adds rows; urgency order and badges; unknown versions; active/empty/incomplete states; audit submission still works |

Use `internal/store/postgres/update_scan_integration_test.go`, `internal/httpapi/jobs_test.go`, and `web/tests/OperationsDashboard.test.tsx` as the existing test locations.

Run backend checks:

```bash
go test -race ./internal/httpapi/... ./internal/handlers/...
go test -race -tags=integration ./internal/store/postgres
```

Run frontend checks from `web`:

```bash
npm run typecheck
npm run lint
npm test
npm run build
```

For a manual check, rebuild the existing Docker Compose stack, open the operations dashboard, submit a public repository with supported manifests, and verify progress and package rows in its detail panel. Confirm an ordinary dependency audit still submits and displays its existing report.

## Verification completed

- Passed the backend API/handler tests and PostgreSQL integration suite with race detection.
- Passed frontend type checking, lint, all 12 UI tests, and the production build.
- Checked scan submission and report rendering in Chromium at desktop and mobile widths with API fixtures; no browser errors. The table scrolls within the detail panel on narrow screens.
- The running Docker application was not redeployed, and the public-repository manual smoke procedure was not run as part of this change.

## Completion criteria

An operator can submit a scan and inspect completed package update results in the existing dashboard without using curl or SQL. The report stays scoped to the selected root and communicates unfinished or failed checks without presenting them as up-to-date dependencies.
