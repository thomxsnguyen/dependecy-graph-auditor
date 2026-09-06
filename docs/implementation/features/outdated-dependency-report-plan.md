# Outdated Dependency Report — Feature Plan

## Goal

Add a `dependency_update_scan` job type that fetches a repo's dependency
manifests (npm, PyPI, Go — already supported), then fans out one
`package_update_check` child job per direct dependency. Each child resolves
the declared version range to the current pinned version, fetches the latest
published version from the same registry, compares the two with semver, and
classifies the gap as `patch`, `minor`, or `major`.

The result is a prioritised, cross-ecosystem upgrade list delivered seconds
after submission — no installation, no CI config, no Dependabot setup required.

---

## Why this is a natural extension

Almost every piece of needed infrastructure already exists:

| Need | Already there |
|---|---|
| Fetch manifests from GitHub | `DependencyAuditHandler` + `GitHubClient.FetchManifest` |
| Parse npm / PyPI / Go manifests | `internal/depfile` (ParsePackageJSON, ParsePyProject, ParseRequirements, ParseGoMod) |
| Query npm registry | `auditor.NpmClient` — `fetchAllVersions` already returns every version |
| Query PyPI registry | `pypi.Client` |
| Query Go module proxy | `gomod.Client` |
| Resolve version ranges | `internal/semver.ParseRange` + `semver.Resolve` |
| Fan-out pattern | Identical to `dependency_audit` |
| Idempotent child deduplication | `sha256(rootJobID + ecosystem + name + range)` key |
| Result storage per job | `job_results` JSONB table |
| Error classification | `classifyServiceError` (transient/permanent) |

New code needed: a `LatestVersion` method on each registry client (~30 lines
each), a staleness classifier (~20 lines), and two handler files (~80 lines
each).

---

## Architecture

```
POST /api/jobs
  { "type": "dependency_update_scan",
    "payload": { "repositoryUrl": "https://github.com/owner/repo", "ref": "main" } }
        │
        ▼
  dependency_update_scan job  (root, public)
        │  fetches package.json, pyproject.toml, requirements.txt, go.mod
        │  creates one child per direct dependency
        ▼
  package_update_check jobs × N  (internal, idempotent, parallel)
        │  resolves declared range → current pinned version  (existing logic)
        │  fetches latest published version from registry    (new: LatestVersion)
        │  compares with semver → staleness: patch / minor / major / up_to_date
        │  stores JSON result in job_results
        ▼
  GET /api/jobs/{rootId}
        │  childCounts shows live progress as results land
        │  child results assembled at query time → prioritised upgrade list
        ▼
  Dashboard: sortable table  (major → minor → patch → up_to_date)
```

---

## New job types

### `dependency_update_scan` (root, public)

**Payload** — identical to `dependency_audit`:

```json
{
  "repositoryUrl": "https://github.com/owner/repo",
  "ref": "main"
}
```

**Handler logic** (mirrors `DependencyAuditHandler` exactly):

1. Parse `repositoryUrl` with `ParseRepositoryURL`.
2. Attempt `FetchManifest` for each supported path
   (`package.json`, `pyproject.toml`, `requirements.txt`, `go.mod`).
3. Parse each found manifest with the existing `depfile` parsers.
4. For each direct dependency, emit one `package_update_check` child job with
   an idempotency key of `sha256(rootJobID + "\x00" + ecosystem + "\x00" + name + "\x00" + versionRange)`.
5. Return summary result JSON + child submissions.

**Result JSON**

```json
{
  "repositoryUrl": "https://github.com/owner/repo",
  "ref":           "main",
  "dependencyCount": 47,
  "auditId":       "<rootJobId>"
}
```

---

### `package_update_check` (internal, child)

**Payload** (set by root handler):

```json
{
  "ecosystem":    "npm",
  "name":         "lodash",
  "versionRange": "^4.17.0"
}
```

**Handler logic**:

1. Resolve `versionRange` → exact current version (reuse existing registry
   client `FetchPackage` — it already does this).
2. Call `LatestVersion(ctx, name)` on the same registry client (new method).
3. Parse both versions with `semver`; compute staleness:
   - Same version → `up_to_date`
   - Different patch only → `patch`
   - Different minor → `minor`
   - Different major → `major`
4. Return result JSON.

**Result JSON**

```json
{
  "ecosystem":      "npm",
  "name":           "lodash",
  "currentVersion": "4.17.11",
  "latestVersion":  "4.17.21",
  "staleness":      "patch"
}
```

**Staleness urgency mapping** (for dashboard sorting):

| Staleness   | Urgency | Action |
|-------------|---------|--------|
| `major`     | 🔴 High  | Breaking changes likely — plan upgrade |
| `minor`     | 🟡 Medium | New features available — schedule upgrade |
| `patch`     | 🟢 Low   | Bug/security fixes — upgrade when convenient |
| `up_to_date`| ✅ None  | No action needed |

---

## New registry methods needed

Add `LatestVersion(ctx context.Context, name string) (string, error)` to each
registry client. This is the only genuinely new code required beyond the
handlers.

### npm (`internal/auditor/registry.go`)

`NpmClient` already calls `fetchAllVersions` which returns every published
version. Latest = `semver.Resolve(constraint(">=0.0.0"), allVersions)`. Zero
new HTTP calls — just reuse what's already fetched in the resolve step, or hit
`GET /registry.npmjs.org/{name}/latest` (returns `{"version": "x.y.z"}`).

### PyPI (`internal/pypi/`)

`GET https://pypi.org/pypi/{name}/json` already returns
`{"info": {"version": "<latest>"}}` — the existing client likely already
fetches this. Latest = `response.info.version`.

### Go module proxy (`internal/gomod/`)

`GET https://proxy.golang.org/{module}/@latest` returns
`{"Version": "v1.2.3"}`. One new endpoint method.

---

## Files to create / modify

### New files

| File | Purpose |
|---|---|
| `internal/handlers/dependency_update_scan.go` | Root handler — fan-out per dependency |
| `internal/handlers/package_update_check.go` | Child handler — resolve + latest + diff |

### Modified files

| File | Change |
|---|---|
| `internal/auditor/registry.go` | Add `LatestVersion` to `NpmClient` and `RegistryClient` interface |
| `internal/pypi/` | Add `LatestVersion` method |
| `internal/gomod/` | Add `LatestVersion` method |
| `cmd/worker/main.go` | Register two new handlers |
| `internal/httpapi/jobs.go` | Add `dependency_update_scan` to `validateSubmission` |

No new DB migration — results use the existing `job_results` JSONB table.

---

## Staleness computation

```
current = semver.Parse(resolvedVersion)   // e.g. 4.17.11
latest  = semver.Parse(latestVersion)     // e.g. 4.17.21

if latest.Major() > current.Major() → "major"
if latest.Minor() > current.Minor() → "minor"
if latest.Patch() > current.Patch() → "patch"
else                                → "up_to_date"
```

The `Masterminds/semver` library (already a dependency) exposes `.Major()`,
`.Minor()`, `.Patch()` — no new dependencies needed.

---

## Error handling

| Scenario | Classification | Behaviour |
|---|---|---|
| Registry rate limit / 429 | Transient | Retried with exponential backoff |
| Network timeout | Transient | Retried |
| Package not found (404) | Permanent | Child job fails; others continue |
| Version range unparseable | Permanent | Stored as `unknown` staleness |
| Repository not found | Permanent | Root job fails immediately |
| No supported manifest found | Permanent | Root job fails with clear message |

---

## Scope boundaries

**In scope**

- Direct dependencies only (no transitive graph — that's what `dependency_audit` is for).
- Public repositories and public registries only.
- npm, PyPI, Go — the three ecosystems already supported.

**Out of scope**

- Security advisories / CVE lookup (separate feature).
- Transitive dependency updates.
- Private registries.
- Automated PR creation (this is a read-only scan tool).

---

## Verification plan

```bash
# Unit tests for new handler code
go test -race ./internal/handlers/...

# Unit tests for LatestVersion methods
go test -race ./internal/auditor/... ./internal/pypi/... ./internal/gomod/...

# End-to-end: submit against a real repo with known deps
curl -i -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: update-scan-1' \
  -d '{"type":"dependency_update_scan","payload":{"repositoryUrl":"https://github.com/octocat/Hello-World"},"maxAttempts":3}'

# Watch live progress as child results land
curl -s http://localhost:8080/api/jobs/{rootId} | jq '.childCounts'

# Demonstrate crash recovery
docker compose kill worker-1
# After WORKER_LEASE_DURATION (30s) worker-2 reclaims orphaned package_update_check jobs
# No results are duplicated — idempotency keys prevent double-writes
```

**Expected dashboard output**: a table sorted major → minor → patch → up_to_date,
showing ecosystem, package name, current version, latest version, and staleness badge.

---

## Implementation order

1. Add `LatestVersion` to `NpmClient` + interface (with unit tests).
2. Add `LatestVersion` to PyPI and Go module clients (with unit tests).
3. Write staleness classifier (pure function, easy to unit test in isolation).
4. `internal/handlers/package_update_check.go` — child handler (mock registry client).
5. `internal/handlers/dependency_update_scan.go` — root handler (mirrors `DependencyAuditHandler`).
6. Wire up in `cmd/worker/main.go` and `internal/httpapi/jobs.go`.
7. End-to-end test against a real public repo.
8. Dashboard table view (phase 2, separate).
