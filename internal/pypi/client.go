package pypi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/thomxsnguyen/mini-distributed-job-api/internal/auditor"
)

const (
	defaultBaseURL      = "https://pypi.org"
	defaultTimeout      = 15 * time.Second
	defaultMaxBodyBytes = int64(5 << 20)
)

// Client implements auditor.RegistryClient using public PyPI JSON metadata.
type Client struct {
	HTTPClient *http.Client
	BaseURL    string
	Target     Target
}

// NewClient constructs the production PyPI client for a deterministic target.
func NewClient(target Target) *Client {
	return &Client{Target: target}
}

type releaseFile struct {
	Yanked bool `json:"yanked"`
}

type projectResponse struct {
	Releases map[string][]releaseFile `json:"releases"`
}

type releaseResponse struct {
	Info struct {
		Name              string   `json:"name"`
		Version           string   `json:"version"`
		License           string   `json:"license"`
		LicenseExpression string   `json:"license_expression"`
		Classifiers       []string `json:"classifiers"`
		RequiresDist      []string `json:"requires_dist"`
	} `json:"info"`
}

// FetchPackage resolves a PEP 440 constraint and returns exact release metadata.
func (c *Client) FetchPackage(ctx context.Context, name, constraint string) (*auditor.PackageMetadata, error) {
	normalizedName := NormalizeName(name)
	available, err := c.fetchAvailableVersions(ctx, normalizedName)
	if err != nil {
		return nil, fmt.Errorf("pypi: resolve %s%s: %w", normalizedName, displayConstraint(constraint), err)
	}
	exact, err := ResolveVersion(constraint, available)
	if err != nil {
		return nil, fmt.Errorf("pypi: resolve %s%s: %w", normalizedName, displayConstraint(constraint), err)
	}
	release, err := c.fetchRelease(ctx, normalizedName, exact)
	if err != nil {
		return nil, fmt.Errorf("pypi: fetch %s@%s: %w", normalizedName, exact, err)
	}
	if strings.TrimSpace(release.Info.Name) == "" || strings.TrimSpace(release.Info.Version) == "" {
		return nil, fmt.Errorf("pypi: fetch %s@%s: response is missing package name or version", normalizedName, exact)
	}
	if _, err := ParseVersion(release.Info.Version); err != nil {
		return nil, fmt.Errorf("pypi: fetch %s@%s: invalid resolved version %q", normalizedName, exact, release.Info.Version)
	}

	dependencies, err := c.parseReleaseDependencies(release.Info.RequiresDist)
	if err != nil {
		return nil, fmt.Errorf("pypi: parse dependencies for %s@%s: %w", normalizedName, exact, err)
	}
	resolvedName := NormalizeName(release.Info.Name)
	if resolvedName == "" {
		resolvedName = normalizedName
	}
	return &auditor.PackageMetadata{
		Name:         resolvedName,
		Version:      release.Info.Version,
		License:      pythonLicense(release.Info.LicenseExpression, release.Info.License, release.Info.Classifiers),
		Dependencies: dependencies,
	}, nil
}

func (c *Client) fetchAvailableVersions(ctx context.Context, name string) ([]string, error) {
	var response projectResponse
	if err := c.getJSON(ctx, []string{"pypi", name, "json"}, &response); err != nil {
		return nil, err
	}
	versions := make([]string, 0, len(response.Releases))
	for version, files := range response.Releases {
		if _, err := ParseVersion(version); err != nil || len(files) == 0 {
			continue
		}
		allYanked := true
		for _, file := range files {
			if !file.Yanked {
				allYanked = false
				break
			}
		}
		if !allYanked {
			versions = append(versions, version)
		}
	}
	return versions, nil
}

func (c *Client) fetchRelease(ctx context.Context, name, version string) (*releaseResponse, error) {
	var response releaseResponse
	if err := c.getJSON(ctx, []string{"pypi", name, version, "json"}, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) getJSON(ctx context.Context, pathSegments []string, destination any) error {
	baseURL := c.BaseURL
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	endpoint, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("invalid PyPI base URL: %w", err)
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/" + strings.Join(pathSegments, "/")

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("create PyPI request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "mini-distributed-job-api")
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("PyPI request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return fmt.Errorf("PyPI resource %q was not found (404)", endpoint.Path)
	}
	if response.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("PyPI rate limit exceeded for %q (429)", endpoint.Path)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("PyPI returned status %d for %q", response.StatusCode, endpoint.Path)
	}

	limited := io.LimitReader(response.Body, defaultMaxBodyBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read PyPI response: %w", err)
	}
	if int64(len(data)) > defaultMaxBodyBytes {
		return fmt.Errorf("PyPI metadata exceeds the 5 MiB limit")
	}
	if err := json.Unmarshal(data, destination); err != nil {
		return fmt.Errorf("decode PyPI response: %w", err)
	}
	return nil
}

func (c *Client) parseReleaseDependencies(values []string) (map[string]string, error) {
	constraints := make(map[string][]string)
	for _, value := range values {
		requirement, err := ParseRequirement(value, c.Target)
		if err != nil {
			return nil, fmt.Errorf("Requires-Dist %q: %w", value, err)
		}
		if !requirement.Active {
			continue
		}
		constraint := strings.TrimSpace(requirement.Constraint)
		if constraint != "" && !stringSliceContains(constraints[requirement.Name], constraint) {
			constraints[requirement.Name] = append(constraints[requirement.Name], constraint)
		} else if _, exists := constraints[requirement.Name]; !exists {
			constraints[requirement.Name] = nil
		}
	}
	dependencies := make(map[string]string, len(constraints))
	for name, values := range constraints {
		sort.Strings(values)
		dependencies[name] = strings.Join(values, ",")
	}
	return dependencies, nil
}

func pythonLicense(expression, legacy string, classifiers []string) string {
	if value := strings.TrimSpace(expression); value != "" {
		return value
	}
	if value := normalizeLicense(strings.TrimSpace(legacy)); value != "" {
		return value
	}
	for _, classifier := range classifiers {
		if value := classifierLicense(classifier); value != "" {
			return value
		}
	}
	return "UNKNOWN"
}

func normalizeLicense(value string) string {
	switch strings.ToLower(value) {
	case "mit", "mit license":
		return "MIT"
	case "apache-2.0", "apache 2.0", "apache software license":
		return "Apache-2.0"
	case "isc", "isc license":
		return "ISC"
	case "bsd-2-clause", "2-clause bsd", "bsd 2-clause":
		return "BSD-2-Clause"
	case "bsd-3-clause", "3-clause bsd", "bsd 3-clause", "bsd":
		return "BSD-3-Clause"
	default:
		return value
	}
}

func classifierLicense(classifier string) string {
	lower := strings.ToLower(classifier)
	switch {
	case strings.Contains(lower, "mit license"):
		return "MIT"
	case strings.Contains(lower, "apache software license"):
		return "Apache-2.0"
	case strings.Contains(lower, "isc license"):
		return "ISC"
	case strings.Contains(lower, "bsd license"):
		return "BSD-3-Clause"
	case strings.Contains(lower, "mozilla public license 2.0"):
		return "MPL-2.0"
	case strings.Contains(lower, "gnu general public license v3"):
		return "GPL-3.0-only"
	default:
		return ""
	}
}

func displayConstraint(constraint string) string {
	if strings.TrimSpace(constraint) == "" {
		return ""
	}
	return " " + constraint
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// LatestVersion returns the highest non-yanked stable release, or a prerelease
// if no stable release exists, using the existing PEP 440 resolver.
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
