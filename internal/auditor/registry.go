package auditor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	ms "github.com/Masterminds/semver/v3"

	"github.com/thomxsnguyen/mini-distributed-job-api/internal/semver"
)

// PackageMetadata is the data returned by the registry for a single (name, version) pair.
// Dependencies maps direct dependency names to their declared version ranges
// (e.g. "^4.18.0") — range resolution for children happens lazily when each
// child job is processed.
type PackageMetadata struct {
	Name         string
	Version      string
	License      string
	Dependencies map[string]string // dep name → version range
}

// RegistryClient fetches package metadata from an external registry (e.g., npm).
// Implementations must resolve version ranges to exact versions internally so
// that callers always receive a fully-resolved PackageMetadata.
// Implementations must be safe for concurrent calls from multiple worker goroutines.
type RegistryClient interface {
	// FetchPackage returns the resolved metadata for name at the given version range.
	FetchPackage(ctx context.Context, name, version string) (*PackageMetadata, error)
	// LatestVersion prefers the highest stable release, falling back to prereleases.
	LatestVersion(ctx context.Context, name string) (string, error)
}

// npmVersionMeta is the subset of an npm package version object we need.
// https://registry.npmjs.org/{name}/{version}
type npmVersionMeta struct {
	Name         string            `json:"name"`
	Version      string            `json:"version"`
	License      string            `json:"license"`
	Dependencies map[string]string `json:"dependencies"` // dep name → version range
}

// npmPackageMeta is the full package document returned by the unversioned endpoint.
// https://registry.npmjs.org/{name}
// We only use the top-level "versions" map to enumerate available versions for
// range resolution.
type npmPackageMeta struct {
	Versions map[string]struct{} `json:"versions"` // keys are exact versions
}

// NpmClient is a RegistryClient that speaks to the public npm registry.
// It is safe for concurrent use by multiple worker goroutines; http.Client
// handles connection pooling internally.
type NpmClient struct {
	http    *http.Client
	baseURL string // injectable for testing; production value is https://registry.npmjs.org
}

// NewNpmClient returns an NpmClient with sensible timeouts.
func NewNpmClient() *NpmClient {
	return &NpmClient{
		http: &http.Client{
			Timeout: 15 * time.Second,
		},
		baseURL: "https://registry.npmjs.org",
	}
}

// FetchPackage fetches and returns the resolved metadata for name@version.
//
// The version string coming from a dependency map is a range (e.g. "^4.18.0").
// FetchPackage resolves it to the highest satisfying exact version before
// fetching that version's own metadata.
//
// The returned PackageMetadata.Dependencies map contains the raw range strings
// exactly as the registry returns them — range resolution for those children
// happens lazily when each child job is processed.
func (c *NpmClient) FetchPackage(ctx context.Context, name, version string) (*PackageMetadata, error) {
	// Step 1 — resolve the version range to an exact version.
	exact, err := c.resolveVersion(ctx, name, version)
	if err != nil {
		return nil, fmt.Errorf("npm: resolve %s@%s: %w", name, version, err)
	}

	// Step 2 — fetch that exact version's metadata.
	meta, err := c.fetchVersionMeta(ctx, name, exact)
	if err != nil {
		return nil, fmt.Errorf("npm: fetch %s@%s: %w", name, exact, err)
	}

	return &PackageMetadata{
		Name:         meta.Name,
		Version:      meta.Version,
		License:      meta.License,
		Dependencies: meta.Dependencies,
	}, nil
}

// resolveVersion resolves a version range string to the highest matching exact version.
// If rangeStr is already an exact version (no operator characters), it is returned as-is
// after verifying it parses — this avoids an unnecessary network round-trip.
func (c *NpmClient) resolveVersion(ctx context.Context, name, rangeStr string) (string, error) {
	constraint, err := semver.ParseRange(rangeStr)
	if err != nil {
		// Fall back: treat the string as a literal tag (e.g. "latest", "next").
		// Return it unchanged and let fetchVersionMeta fail loudly if the tag is bad.
		return rangeStr, nil
	}

	// Fetch all available versions for this package.
	available, err := c.fetchAllVersions(ctx, name)
	if err != nil {
		return "", err
	}

	return semver.Resolve(constraint, available)
}

// fetchAllVersions returns the list of all published versions for name by
// hitting the unversioned registry endpoint and extracting the "versions" map keys.
func (c *NpmClient) fetchAllVersions(ctx context.Context, name string) ([]string, error) {
	url := fmt.Sprintf("%s/%s", c.baseURL, name)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("npm: build request for %s: %w", name, err)
	}
	// Ask npm for a compact registry response — dramatically smaller than the full doc.
	req.Header.Set("Accept", "application/vnd.npm.install-v1+json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("npm: GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("npm: GET %s: unexpected status %d", url, resp.StatusCode)
	}

	// The compact manifest has the same "versions" key structure as the full doc.
	var doc struct {
		Versions map[string]json.RawMessage `json:"versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("npm: decode versions for %s: %w", name, err)
	}

	versions := make([]string, 0, len(doc.Versions))
	for v := range doc.Versions {
		versions = append(versions, v)
	}
	return versions, nil
}

// fetchVersionMeta fetches the metadata object for an exact (name, version) pair.
func (c *NpmClient) fetchVersionMeta(ctx context.Context, name, version string) (*npmVersionMeta, error) {
	url := fmt.Sprintf("%s/%s/%s", c.baseURL, name, version)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("npm: build request for %s@%s: %w", name, version, err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("npm: GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("npm: %s@%s not found (404)", name, version)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("npm: GET %s: unexpected status %d", url, resp.StatusCode)
	}

	var meta npmVersionMeta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, fmt.Errorf("npm: decode metadata for %s@%s: %w", name, version, err)
	}
	return &meta, nil
}

// LatestVersion reuses the compact manifest endpoint. Each call fetches a fresh list.
func (c *NpmClient) LatestVersion(ctx context.Context, name string) (string, error) {
	versions, err := c.fetchAllVersions(ctx, name)
	if err != nil {
		return "", fmt.Errorf("npm: latest version for %s: %w", name, err)
	}
	constraint, err := semver.ParseRange(">=0.0.0")
	if err != nil {
		return "", err
	}
	if latest, err := semver.Resolve(constraint, versions); err == nil {
		return latest, nil
	}
	var latest *ms.Version
	for _, raw := range versions {
		candidate, err := ms.NewVersion(raw)
		if err == nil && (latest == nil || candidate.GreaterThan(latest)) {
			latest = candidate
		}
	}
	if latest == nil {
		return "", fmt.Errorf("npm: latest version for %s: no usable releases", name)
	}
	return latest.Original(), nil
}
