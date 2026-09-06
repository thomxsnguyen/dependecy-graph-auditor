package gomod

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/mod/module"
)

const (
	defaultBaseURL      = "https://proxy.golang.org"
	defaultTimeout      = 15 * time.Second
	defaultMaxBodyBytes = int64(5 << 20)
)

// ErrorKind classifies a proxy failure for the worker retry boundary.
type ErrorKind string

const (
	ErrorNotFound         ErrorKind = "not_found"
	ErrorRateLimited      ErrorKind = "rate_limited"
	ErrorHTTPStatus       ErrorKind = "http_status"
	ErrorTimeout          ErrorKind = "timeout"
	ErrorRequest          ErrorKind = "request"
	ErrorResponseTooLarge ErrorKind = "response_too_large"
	ErrorDecode           ErrorKind = "decode"
)

// ProxyError describes a failed exact-coordinate metadata request without
// retaining or exposing the proxy response body.
type ProxyError struct {
	Kind       ErrorKind
	ModulePath string
	Version    string
	StatusCode int
	Err        error
}

func (e *ProxyError) Error() string {
	coordinate := e.ModulePath + "@" + e.Version
	if e.StatusCode != 0 {
		return fmt.Sprintf("go proxy: fetch %s: %s (HTTP %d)", coordinate, e.Kind, e.StatusCode)
	}
	if e.Err != nil {
		return fmt.Sprintf("go proxy: fetch %s: %s: %v", coordinate, e.Kind, e.Err)
	}
	return fmt.Sprintf("go proxy: fetch %s: %s", coordinate, e.Kind)
}

func (e *ProxyError) Unwrap() error { return e.Err }

// Retryable reports whether the existing worker retry lifecycle may retry the
// failure. The client itself never retries.
func (e *ProxyError) Retryable() bool {
	switch e.Kind {
	case ErrorRateLimited, ErrorTimeout, ErrorRequest:
		return true
	case ErrorHTTPStatus:
		return e.StatusCode == http.StatusRequestTimeout || e.StatusCode >= 500
	default:
		return false
	}
}

// Client fetches exact .mod metadata from the public Go module proxy protocol.
type Client struct {
	HTTPClient *http.Client
	BaseURL    string
}

// NewClient constructs a client with production defaults.
func NewClient() *Client { return &Client{} }

// Fetch retrieves and parses one exact module coordinate.
func (c *Client) Fetch(ctx context.Context, modulePath, version string) (Metadata, error) {
	if err := ValidateCoordinate(modulePath, version); err != nil {
		return Metadata{}, fmt.Errorf("go proxy: invalid coordinate %s@%s: %w", modulePath, version, err)
	}
	escapedPath, err := module.EscapePath(modulePath)
	if err != nil {
		return Metadata{}, fmt.Errorf("go proxy: escape module path %q: %w", modulePath, err)
	}
	escapedVersion, err := module.EscapeVersion(version)
	if err != nil {
		return Metadata{}, fmt.Errorf("go proxy: escape module version %q: %w", version, err)
	}

	baseURL := c.BaseURL
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	if err := validateBaseURL(baseURL); err != nil {
		return Metadata{}, err
	}
	endpoint := strings.TrimRight(baseURL, "/") + "/" + escapedPath + "/@v/" + escapedVersion + ".mod"

	requestContext, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, endpoint, nil)
	if err != nil {
		return Metadata{}, newProxyError(ErrorRequest, modulePath, version, 0, err)
	}
	request.Header.Set("Accept", "text/plain")
	request.Header.Set("User-Agent", "mini-distributed-job-api")

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return Metadata{}, classifyRequestError(modulePath, version, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		kind := ErrorHTTPStatus
		switch response.StatusCode {
		case http.StatusNotFound, http.StatusGone:
			kind = ErrorNotFound
		case http.StatusTooManyRequests:
			kind = ErrorRateLimited
		}
		return Metadata{}, newProxyError(kind, modulePath, version, response.StatusCode, nil)
	}

	data, err := io.ReadAll(io.LimitReader(response.Body, defaultMaxBodyBytes+1))
	if err != nil {
		return Metadata{}, classifyRequestError(modulePath, version, err)
	}
	if int64(len(data)) > defaultMaxBodyBytes {
		return Metadata{}, newProxyError(ErrorResponseTooLarge, modulePath, version, 0, fmt.Errorf(".mod response exceeds 5 MiB"))
	}
	metadata, err := ParseMetadata(data)
	if err != nil {
		return Metadata{}, newProxyError(ErrorDecode, modulePath, version, 0, err)
	}
	if metadata.ModulePath != modulePath {
		return Metadata{}, newProxyError(ErrorDecode, modulePath, version, 0,
			fmt.Errorf("module directive declares %q", metadata.ModulePath))
	}
	return metadata, nil
}

func validateBaseURL(baseURL string) error {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("go proxy: invalid base URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("go proxy: invalid base URL")
	}
	return nil
}

func classifyRequestError(modulePath, version string, err error) error {
	kind := ErrorRequest
	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &networkError) && networkError.Timeout()) {
		kind = ErrorTimeout
	}
	return newProxyError(kind, modulePath, version, 0, err)
}

func newProxyError(kind ErrorKind, modulePath, version string, statusCode int, err error) *ProxyError {
	return &ProxyError{Kind: kind, ModulePath: modulePath, Version: version, StatusCode: statusCode, Err: err}
}

// LatestVersion returns the proxy's latest release for this module path.
func (c *Client) LatestVersion(ctx context.Context, modulePath string) (string, error) {
	escapedPath, err := module.EscapePath(modulePath)
	if err != nil {
		return "", fmt.Errorf("go proxy: escape module path %q: %w", modulePath, err)
	}
	baseURL := c.BaseURL
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	if err := validateBaseURL(baseURL); err != nil {
		return "", err
	}
	endpoint := strings.TrimRight(baseURL, "/") + "/" + escapedPath + "/@latest"
	requestContext, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", newProxyError(ErrorRequest, modulePath, "@latest", 0, err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "mini-distributed-job-api")
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return "", classifyRequestError(modulePath, "@latest", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		kind := ErrorHTTPStatus
		switch response.StatusCode {
		case http.StatusNotFound, http.StatusGone:
			kind = ErrorNotFound
		case http.StatusTooManyRequests:
			kind = ErrorRateLimited
		}
		return "", newProxyError(kind, modulePath, "@latest", response.StatusCode, nil)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, defaultMaxBodyBytes+1))
	if err != nil {
		return "", classifyRequestError(modulePath, "@latest", err)
	}
	if int64(len(data)) > defaultMaxBodyBytes {
		return "", newProxyError(ErrorResponseTooLarge, modulePath, "@latest", 0, fmt.Errorf("latest response exceeds 5 MiB"))
	}
	var result struct {
		Version string `json:"Version"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return "", newProxyError(ErrorDecode, modulePath, "@latest", 0, err)
	}
	if err := ValidateCoordinate(modulePath, result.Version); err != nil {
		return "", newProxyError(ErrorDecode, modulePath, "@latest", 0, err)
	}
	return result.Version, nil
}
