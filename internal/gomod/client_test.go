package gomod

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientFetchesEscapedExactModResource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Errorf("method: got %s", request.Method)
		}
		if request.URL.EscapedPath() != "/example.com/!acme/!widget/@v/v1.2.3-!r!c1.mod" {
			t.Errorf("path: got %q", request.URL.EscapedPath())
		}
		if request.Header.Get("Accept") != "text/plain" {
			t.Errorf("Accept: got %q", request.Header.Get("Accept"))
		}
		_, _ = writer.Write([]byte(`
module example.com/Acme/Widget
go 1.22
require example.com/child v1.0.0
`))
	}))
	defer server.Close()

	client := &Client{HTTPClient: server.Client(), BaseURL: server.URL}
	metadata, err := client.Fetch(context.Background(), "example.com/Acme/Widget", "v1.2.3-RC1")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.ModulePath != "example.com/Acme/Widget" || len(metadata.Requirements) != 1 {
		t.Fatalf("metadata: %+v", metadata)
	}
}

func TestClientClassifiesHTTPFailuresWithoutResponseBodies(t *testing.T) {
	tests := []struct {
		status    int
		kind      ErrorKind
		retryable bool
	}{
		{status: http.StatusNotFound, kind: ErrorNotFound},
		{status: http.StatusGone, kind: ErrorNotFound},
		{status: http.StatusTooManyRequests, kind: ErrorRateLimited, retryable: true},
		{status: http.StatusBadRequest, kind: ErrorHTTPStatus},
		{status: http.StatusInternalServerError, kind: ErrorHTTPStatus, retryable: true},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				writer.WriteHeader(tt.status)
				_, _ = writer.Write([]byte("secret response body"))
			}))
			defer server.Close()

			client := &Client{HTTPClient: server.Client(), BaseURL: server.URL}
			_, err := client.Fetch(context.Background(), "example.com/module", "v1.0.0")
			var proxyError *ProxyError
			if !errors.As(err, &proxyError) {
				t.Fatalf("error: got %T %v", err, err)
			}
			if proxyError.Kind != tt.kind || proxyError.StatusCode != tt.status || proxyError.Retryable() != tt.retryable {
				t.Fatalf("proxy error: got %+v retryable=%v", proxyError, proxyError.Retryable())
			}
			if strings.Contains(err.Error(), "secret response body") {
				t.Fatalf("error exposed response body: %v", err)
			}
			if requests.Load() != 1 {
				t.Fatalf("requests: got %d, want 1 (client must not retry)", requests.Load())
			}
		})
	}
}

func TestClientClassifiesTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = writer.Write([]byte("module example.com/module\n"))
	}))
	defer server.Close()

	client := &Client{
		HTTPClient: &http.Client{Timeout: 5 * time.Millisecond},
		BaseURL:    server.URL,
	}
	_, err := client.Fetch(context.Background(), "example.com/module", "v1.0.0")
	var proxyError *ProxyError
	if !errors.As(err, &proxyError) || proxyError.Kind != ErrorTimeout || !proxyError.Retryable() {
		t.Fatalf("error: got %T %v", err, err)
	}
}

func TestClientEnforcesModResponseLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write(bytes.Repeat([]byte("x"), int(defaultMaxBodyBytes+1)))
	}))
	defer server.Close()

	client := &Client{HTTPClient: server.Client(), BaseURL: server.URL}
	_, err := client.Fetch(context.Background(), "example.com/module", "v1.0.0")
	var proxyError *ProxyError
	if !errors.As(err, &proxyError) || proxyError.Kind != ErrorResponseTooLarge || proxyError.Retryable() {
		t.Fatalf("error: got %T %v", err, err)
	}
}

func TestClientClassifiesDecodeAndModuleMismatch(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed", body: "module (\n"},
		{name: "module mismatch", body: "module example.com/other\ngo 1.22\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				_, _ = writer.Write([]byte(tt.body))
			}))
			defer server.Close()
			client := &Client{HTTPClient: server.Client(), BaseURL: server.URL}
			_, err := client.Fetch(context.Background(), "example.com/module", "v1.0.0")
			var proxyError *ProxyError
			if !errors.As(err, &proxyError) || proxyError.Kind != ErrorDecode || proxyError.Retryable() {
				t.Fatalf("error: got %T %v", err, err)
			}
		})
	}
}

func TestClientRejectsInvalidCoordinatesBeforeRequest(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	client := &Client{HTTPClient: server.Client(), BaseURL: server.URL}

	for _, coordinate := range [][2]string{
		{"example.com/module", "^1.2.3"},
		{"example.com/module", "v1.2"},
		{"example.com/module/v2", "v1.2.3"},
	} {
		if _, err := client.Fetch(context.Background(), coordinate[0], coordinate[1]); err == nil {
			t.Fatalf("coordinate %s@%s: expected error", coordinate[0], coordinate[1])
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("requests: got %d, want 0", requests.Load())
	}
}

func TestLatestVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/example.com/!acme/!widget/@latest" || r.Header.Get("Accept") != "application/json" {
			t.Errorf("request=%s headers=%v", r.URL.Path, r.Header)
		}
		_, _ = w.Write([]byte(`{"Version":"v1.2.3"}`))
	}))
	defer server.Close()
	c := &Client{HTTPClient: server.Client(), BaseURL: server.URL}
	got, err := c.LatestVersion(context.Background(), "example.com/Acme/Widget")
	if err != nil || got != "v1.2.3" {
		t.Fatalf("got %q err %v", got, err)
	}
}

func TestLatestVersionFailures(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		kind       ErrorKind
		retry      bool
	}{
		{"missing", `{}`, 200, ErrorDecode, false}, {"invalid version", `{"Version":"latest"}`, 200, ErrorDecode, false},
		{"major mismatch", `{"Version":"v2.0.0"}`, 200, ErrorDecode, false}, {"invalid JSON", `{`, 200, ErrorDecode, false},
		{"oversized", strings.Repeat("x", int(defaultMaxBodyBytes+1)), 200, ErrorResponseTooLarge, false},
		{"not found", "", 404, ErrorNotFound, false}, {"gone", "", 410, ErrorNotFound, false},
		{"rate limit", "", 429, ErrorRateLimited, true}, {"server error", "", 503, ErrorHTTPStatus, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			c := &Client{HTTPClient: server.Client(), BaseURL: server.URL}
			_, err := c.LatestVersion(context.Background(), "example.com/module")
			var pe *ProxyError
			if !errors.As(err, &pe) || pe.Kind != tc.kind || pe.Retryable() != tc.retry {
				t.Fatalf("error=%v want %s retry=%v", err, tc.kind, tc.retry)
			}
		})
	}
	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
		defer server.Close()
		c := &Client{HTTPClient: server.Client(), BaseURL: server.URL}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := c.LatestVersion(ctx, "example.com/module")
		var pe *ProxyError
		if !errors.As(err, &pe) || pe.Kind != ErrorTimeout {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("invalid base URL", func(t *testing.T) {
		c := &Client{BaseURL: "file:///tmp/proxy"}
		if _, err := c.LatestVersion(context.Background(), "example.com/module"); err == nil {
			t.Fatal("expected rejection")
		}
	})
}
