# Outdated Dependency Report — Implementation Guide

Feature specification: [outdated-dependency-report-plan.md](./outdated-dependency-report-plan.md).

This document defines the implementation steps for the existing Go service. The companion plan owns the feature rationale, architecture overview, urgency table, and scope. The backend implementation is complete. The steps below record the implementation contract.

## Implementation checklist

- [x] 1. Define latest-version contracts and update registry test doubles.
- [x] 2. Implement npm latest-version lookup.
- [x] 3. Implement PyPI latest-version lookup.
- [x] 4. Implement Go proxy latest-version lookup.
- [x] 5. Implement and test the staleness classifier.
- [x] 6. Implement the `package_update_check` child handler with internal ecosystem routing.
- [x] 7. Implement the `dependency_update_scan` root handler using the existing audit manifest flow.
- [x] 8. Register the two handlers in the worker.
- [x] 9. Add public submission validation and preserve update results during root completion.
- [x] 10. Run focused tests and verify scan completion, idempotency, and crash recovery.

## Verification completed

Validated with `go test -race ./...` and `go test -race -tags=integration ./...`.
The PostgreSQL tests cover API submission, direct-child deduplication, successful/failed/empty scans, preserved root summaries, concurrent child completion, and audit compatibility. Recovery is verified by killing a helper worker during a child lookup, expiring its isolated test lease, reclaiming it, and checking fencing and one stored result per child.

Registry requests use HTTP fixtures; the public-repository Docker smoke procedure below remains a manual demonstration. No running application services were redeployed.

## 1. Registry contracts

In `internal/auditor/registry.go`, extend `RegistryClient`:

```go
type RegistryClient interface {
    FetchPackage(ctx context.Context, name, version string) (*PackageMetadata, error)
    LatestVersion(ctx context.Context, name string) (string, error)
}
```

Implement the new method on npm and PyPI clients and update every fake implementing this interface in the same change.

The Go client does **not** implement `auditor.RegistryClient`: it exposes `Fetch`, returning `gomod.Metadata`, rather than `FetchPackage`. Keep that distinction. Define this narrow interface in the update handler:

```go
type GoLatestClient interface {
    LatestVersion(ctx context.Context, modulePath string) (string, error)
}

type UpdateCheckRegistries struct {
    NPM  auditor.RegistryClient
    PyPI auditor.RegistryClient
    Go   GoLatestClient
}

type PackageUpdateCheckHandler struct {
    Registries UpdateCheckRegistries
}
```

Latest lookup prefers a stable release. npm and PyPI may fall back to a prerelease when no stable release exists; Go uses the proxy's `@latest` result. Return an error if no usable version exists.

## 2. npm latest version

File: `internal/auditor/registry.go`.

```go
func (c *NpmClient) LatestVersion(ctx context.Context, name string) (string, error)
```

1. Call the existing `fetchAllVersions(ctx, name)`.
2. Parse `">=0.0.0"` with `internal/semver.ParseRange` and pass the constraint and available versions to `internal/semver.Resolve`.
3. If there is no stable match, parse candidates with the existing Masterminds/semver library and select the highest valid version using `GreaterThan`. Return its `Original()` string.
4. Wrap failures with package context, retaining the underlying error.

Do not detect prereleases by searching for hyphens: use parsed version semantics. The stable constraint alone does not implement prerelease fallback.

**Request accounting:** this reuses an existing endpoint and helper, not an already-retained response. The current client does not cache `fetchAllVersions`; calling `FetchPackage` followed by `LatestVersion` fetches the version list again. Do not claim zero additional HTTP requests. Shared caching or a combined lookup API is outside this implementation.

Tests in `internal/auditor/registry_test.go`: highest stable release, stable preferred over a newer prerelease, prerelease-only fallback, malformed/empty candidates, and HTTP failures. Use an HTTP test server to exercise the concrete method.

## 3. PyPI latest version

File: `internal/pypi/client.go`.

```go
func (c *Client) LatestVersion(ctx context.Context, name string) (string, error) {
    versions, err := c.fetchAvailableVersions(ctx, NormalizeName(name))
    if err != nil {
        return "", fmt.Errorf("pypi: latest version for %s: %w", name, err)
    }
    latest, err := ResolveVersion("", versions)
    if err != nil {
        return "", fmt.Errorf("pypi: latest version for %s: %w", name, err)
    }
    return latest, nil
}
```

The existing `fetchAvailableVersions` reads `/pypi/{name}/json`, excluding invalid releases, releases without files, and releases whose files are all yanked. `ResolveVersion("", versions)` already accepts an empty constraint, prefers stable releases, and supports prerelease fallback.

Use this existing release-list behavior instead of adding an `info.version` lookup. No new endpoint is required, but a separate method call makes another request; the client does not retain the earlier response.

Tests in `internal/pypi/client_test.go`: normalized package names, highest stable version, yanked releases excluded, prerelease fallback, and empty/error responses.

## 4. Go proxy latest version

File: `internal/gomod/client.go`.

```go
func (c *Client) LatestVersion(ctx context.Context, modulePath string) (string, error)
```

Follow `Client.Fetch` conventions:

1. Escape the module path with `module.EscapePath` and validate the configured base URL.
2. Request `/{escapedModulePath}/@latest` with the supplied context, existing HTTP client defaults, and JSON headers.
3. Reuse proxy error kinds, including rate limiting, timeout, not-found, and HTTP status failures.
4. Read with the existing response-size limit and decode the `Version` field.
5. Reject a missing or invalid version using the existing coordinate validation before returning it.

This adds one endpoint. Do not introduce `NewUpdateClient` or force the Go client to implement `FetchPackage`.

The current version comes directly from the validated `go.mod` requirement. Lookup is scoped to that module path; discovering a different major-version module path is outside scope.

Tests in `internal/gomod/client_test.go`: escaped path, valid version, missing/invalid version, 404/410, 429, timeout, and oversized/invalid JSON responses.

## 5. Staleness classifier

New file: `internal/updater/staleness.go`.

```go
package updater

import ms "github.com/Masterminds/semver/v3"

type Staleness string

const (
    StalenessUpToDate Staleness = "up_to_date"
    StalenessPatch    Staleness = "patch"
    StalenessMinor    Staleness = "minor"
    StalenessMajor    Staleness = "major"
    StalenessUnknown  Staleness = "unknown"
)

func Classify(current, latest string) Staleness {
    c, err := ms.NewVersion(current)
    if err != nil {
        return StalenessUnknown
    }
    l, err := ms.NewVersion(latest)
    if err != nil {
        return StalenessUnknown
    }
    if !l.GreaterThan(c) {
        return StalenessUpToDate
    }
    if l.Major() > c.Major() {
        return StalenessMajor
    }
    if l.Minor() > c.Minor() {
        return StalenessMinor
    }
    if l.Patch() > c.Patch() {
        return StalenessPatch
    }
    return StalenessUnknown // newer version differs only in prerelease ordering
}
```

Test equality, each numeric increment, latest older than current, malformed inputs, and prerelease-only differences in `internal/updater/staleness_test.go`.

This classifier does not implement full PEP 440 comparison. Resolved versions that cannot be represented by this semver classifier produce `unknown`. A failure to resolve the declared range is a failed child job, not a successful `unknown` result.

## 6. Child handler

New file: `internal/handlers/package_update_check.go`.

Use exactly one job type, `package_update_check`, and the registry structure from step 1. Switch internally on the payload's `ecosystem`; do not register ecosystem-suffixed job types.

Payload fields: `ecosystem`, `name`, `versionRange`.

1. Decode and validate the payload. Reject unsupported ecosystems and empty package names as permanent failures. An empty PyPI constraint is valid.
2. For npm/PyPI, call the selected client's `FetchPackage` and use `meta.Version` as the current version. Ignore its dependency map; emit no grandchildren.
3. For Go, validate the package/version coordinate and use the exact requirement as the current version.
4. Call the selected client's `LatestVersion`.
5. Classify and marshal the result fields: `ecosystem`, `name`, `currentVersion`, `latestVersion`, `staleness`.
6. Return `job.HandlerResult{Result: result}` with no children.

For npm/PyPI, reuse `classifyError` from `dependency_audit.go`. For Go, preserve typed `Retryable()` handling as in `go_service_handler.go`. Handle JSON marshal errors explicitly.

For a range, `currentVersion` means the highest release satisfying the declaration at scan time. It is not an installed or lockfile-pinned version. Lockfile inspection is outside scope.

Tests: routing across all three ecosystems, result fields, invalid payloads, lookup failures and retry classification, `unknown` results, and no child submissions.

## 7. Root handler

New file: `internal/handlers/dependency_update_scan.go`.

```go
type DependencyUpdateScanHandler struct{ GitHub ManifestFetcher }
```

Reuse `DependencyAuditPayload`, `ManifestFetcher`, repository parsing, manifest paths, Python target, and depfile parsers from `DependencyAuditHandler`. Extract the common manifest-loading flow into a package-private helper used by both handlers so it does not become a second drifting implementation. Preserve existing audit behavior.

Direct-dependency selection needs one explicit adjustment: `ParseGoMod` currently includes `// indirect` requirements and discards that flag. Preserve the flag in parser output (for example, an `Indirect bool` on `depfile.Dependency`) and skip flagged entries only in the update scan. The audit must continue receiving all its existing seeds.

For each selected dependency, submit an internal `package_update_check` child with the payload from step 6, inherited `MaxAttempts`, `RootJobID`, and `ParentJobID`. Reuse the existing hash construction:

```go
sum := sha256.Sum256([]byte(rootJobID + "\x00" + ecosystem + "\x00" + name + "\x00" + versionRange))
key := "update-child:" + hex.EncodeToString(sum[:])
```

Use a shared helper for this construction with audit/update prefixes. Deduplicate identical coordinates before submission so `dependencyCount` matches the logical child count.

Return a summary containing `repositoryUrl`, `ref`, `dependencyCount`, and `auditId` (the root ID). Missing individual manifests are skipped; no supported manifest is a permanent failure. A supported manifest with zero direct dependencies is a valid empty scan.

Tests: supported manifests, Go indirect filtering, duplicate coordinates, stable keys on retries, missing versus invalid manifests, empty scans, and unchanged audit behavior.

## 8. Worker registration

File: `cmd/worker/main.go`. Add these entries to the existing handler map:

```go
"dependency_update_scan": handlers.DependencyUpdateScanHandler{GitHub: githubClient},
"package_update_check": handlers.PackageUpdateCheckHandler{
    Registries: handlers.UpdateCheckRegistries{
        NPM:  auditor.NewNpmClient(),
        PyPI: pypi.NewClient(mustPythonTarget()),
        Go:   gomod.NewClient(),
    },
},
```

## 9. Submission validation and result completion

In `internal/httpapi/jobs.go`, add a `dependency_update_scan` case using the same repository URL/ref validation as `dependency_audit`. Update the unsupported-type error message. Keep `package_update_check` internal; public submissions must still reject it.

The current `internal/store/postgres/service.go` finalizer builds an audit-specific summary from `audit_results`. Add a narrow branch for update-scan roots that preserves their update summary when children finish, while retaining existing terminal-status and retry behavior. Do not let an update scan finish with an unrelated package/license audit summary. Update-scan finalizers lock the root row before counting active children so simultaneous completions cannot leave the root waiting.

Child results already fit `job_results`; no migration is needed. The current root detail API does not assemble update rows, and the list API excludes internal jobs. Verify child results via their IDs or database inspection during this phase. A dashboard table and an aggregated report query remain phase 2.

Tests: accepted root submission, rejected public child submission, preserved update summary, correct root status on child success/failure, empty-scan completion, and unchanged audit finalization.

## 10. Verification and recovery smoke test

Run the existing focused tests with the new cases:

```bash
go test -race ./internal/auditor/... ./internal/pypi/... ./internal/gomod/... ./internal/updater/... ./internal/depfile/... ./internal/handlers/... ./internal/httpapi/...
```

Run PostgreSQL integration tests using the repository's existing integration-test setup for completion and idempotency changes.

For the manual smoke test:

1. Start the stack with `docker compose up --build -d`.
2. Choose a public repository/ref verified to contain supported manifests and direct dependencies. Do not assume `octocat/Hello-World` contains them.
3. Submit a `dependency_update_scan` to `POST /api/jobs` with `repositoryUrl`, optional `ref`, `maxAttempts: 3`, and a unique `Idempotency-Key`. Record `.job.id`.
4. Poll `GET /api/jobs/{id}` for root status and `childCounts`. Inspect child rows/results by root ID in PostgreSQL; `q={rootId}` does not search parent/root relationships and cannot expose internal children.
5. For recovery, submit another scan and confirm a child is running on `worker-1` before executing `docker compose kill worker-1`. Killing after completion does not test recovery.
6. Wait for the configured lease expiry plus recovery interval. Confirm `worker-2` reclaims the interrupted job and the root reaches its expected terminal state. If the workload finishes too quickly, use the existing integration-test controls to make interruption deterministic.
7. Confirm one logical child per idempotency key and one stored result per child after recovery. Repeat the original submission with the same key and payload and confirm it returns the same root.
8. Restart the stopped worker with `docker compose start worker-1`.

## Files affected

| File | Planned change |
| --- | --- |
| `internal/auditor/registry.go` | Extend interface and implement npm latest lookup |
| `internal/pypi/client.go` | Implement PyPI latest lookup |
| `internal/gomod/client.go` | Implement Go latest endpoint |
| `internal/updater/staleness.go` | Add classifier |
| `internal/handlers/package_update_check.go` | Add internally routed child handler |
| `internal/handlers/dependency_update_scan.go` | Add root handler |
| `internal/handlers/dependency_audit.go` | Extract shared manifest/key helpers without changing audit behavior |
| `internal/depfile/depfile.go`, `internal/depfile/go.go` | Preserve indirect requirement metadata |
| `cmd/worker/main.go` | Register two handlers |
| `internal/httpapi/jobs.go` | Validate public scan submission |
| `internal/store/postgres/service.go` | Preserve update summary during root finalization |
| Corresponding test files and registry fakes | Verify new behavior and existing audit compatibility |

No new dependencies, transitive traversal, private-registry support, security advisory lookup, automatic upgrades, dashboard work, or job-service redesign are included.
