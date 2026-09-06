package pypi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thomxsnguyen/mini-distributed-job-api/internal/auditor"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
)

func TestClientResolvesPyPIReleaseMetadata(t *testing.T) {
	target := testTarget(t, "3.12", "linux")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/pypi/demo-package/json":
			_, _ = io.WriteString(writer, `{"releases":{"1.0.0":[{"yanked":false}],"1.5.0":[{"yanked":false}],"2.0.0":[{"yanked":true}]}}`)
		case "/pypi/demo-package/1.5.0/json":
			_, _ = io.WriteString(writer, `{"info":{"name":"Demo_Package","version":"1.5.0","license_expression":"MIT","requires_dist":["urllib3>=2","colorama; sys_platform == 'win32'"]}}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client := &Client{HTTPClient: server.Client(), BaseURL: server.URL, Target: target}
	metadata, err := client.FetchPackage(context.Background(), "Demo.Package", ">=1,<2")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Name != "demo-package" || metadata.Version != "1.5.0" || metadata.License != "MIT" {
		t.Fatalf("metadata: %+v", metadata)
	}
	if len(metadata.Dependencies) != 1 || metadata.Dependencies["urllib3"] != ">=2" {
		t.Fatalf("dependencies: %+v", metadata.Dependencies)
	}
}

func TestClientErrorsAndLimits(t *testing.T) {
	target := testTarget(t, "3.12", "linux")
	for _, status := range []int{http.StatusNotFound, http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(status)
			}))
			defer server.Close()
			client := &Client{HTTPClient: server.Client(), BaseURL: server.URL, Target: target}
			if _, err := client.FetchPackage(context.Background(), "demo", ">=1"); err == nil || !strings.Contains(err.Error(), "demo") {
				t.Fatalf("error: %v", err)
			}
		})
	}

	t.Run("oversized", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprint(writer, strings.Repeat("x", (5<<20)+1))
		}))
		defer server.Close()
		client := &Client{HTTPClient: server.Client(), BaseURL: server.URL, Target: target}
		if _, err := client.FetchPackage(context.Background(), "demo", ""); err == nil || !strings.Contains(err.Error(), "5 MiB") {
			t.Fatalf("error: %v", err)
		}
	})
}

func TestClientDoesNotRetryHTTPFailure(t *testing.T) {
	target := testTarget(t, "3.12", "linux")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client := &Client{HTTPClient: server.Client(), BaseURL: server.URL, Target: target}
	_, _ = client.FetchPackage(context.Background(), "demo", ">=1")
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests: got %d, want 1", got)
	}
}

func TestClientHonorsContextDeadline(t *testing.T) {
	target := testTarget(t, "3.12", "linux")
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	client := &Client{HTTPClient: server.Client(), BaseURL: server.URL, Target: target}
	if _, err := client.FetchPackage(ctx, "demo", ">=1"); err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("error: %v", err)
	}
}

func TestPyPIClientIntegratesWithExistingAuditHandler(t *testing.T) {
	target := testTarget(t, "3.12", "linux")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		responses := map[string]string{
			"/pypi/parent/json":       `{"releases":{"1.0.0":[{"yanked":false}]}}`,
			"/pypi/parent/1.0.0/json": `{"info":{"name":"parent","version":"1.0.0","license_expression":"MIT","requires_dist":["child>=2"]}}`,
			"/pypi/child/json":        `{"releases":{"2.1.0":[{"yanked":false}]}}`,
			"/pypi/child/2.1.0/json":  `{"info":{"name":"child","version":"2.1.0","license_expression":"Apache-2.0","requires_dist":[]}}`,
		}
		body, ok := responses[request.URL.Path]
		if !ok {
			http.NotFound(writer, request)
			return
		}
		_, _ = io.WriteString(writer, body)
	}))
	defer server.Close()

	packages := auditor.NewPackageStore()
	edges := auditor.NewEdgeStore()
	handler := auditor.NewAuditHandler(
		&Client{HTTPClient: server.Client(), BaseURL: server.URL, Target: target},
		auditor.LicensePolicy{}, packages, edges,
	)
	seedPayload, err := json.Marshal(auditor.AuditPayload{Name: "parent", Version: ">=1", ParentName: "python-app"})
	if err != nil {
		t.Fatal(err)
	}
	children, err := handler.Handle(context.Background(), job.NewJob("audit_package", seedPayload))
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 {
		t.Fatalf("child jobs: got %d, want 1", len(children))
	}
	if _, err := handler.Handle(context.Background(), children[0]); err != nil {
		t.Fatal(err)
	}
	if len(packages.All()) != 2 || len(edges.All()) != 2 {
		t.Fatalf("packages=%+v edges=%+v", packages.All(), edges.All())
	}
	for _, pkg := range packages.All() {
		if pkg.Verdict != auditor.VerdictPass {
			t.Errorf("package did not reach existing license policy: %+v", pkg)
		}
	}
}

func TestPythonLicensePrecedence(t *testing.T) {
	if got := pythonLicense("Apache-2.0", "MIT", []string{"License :: OSI Approved :: ISC License"}); got != "Apache-2.0" {
		t.Fatalf("expression: got %q", got)
	}
	if got := pythonLicense("", "MIT License", nil); got != "MIT" {
		t.Fatalf("legacy: got %q", got)
	}
	if got := pythonLicense("", "", []string{"License :: OSI Approved :: MIT License"}); got != "MIT" {
		t.Fatalf("classifier: got %q", got)
	}
	if got := pythonLicense("", "", nil); got != "UNKNOWN" {
		t.Fatalf("fallback: got %q", got)
	}
}

func TestLatestVersion(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"stable and yanked", `{"releases":{"1.9":[{"yanked":false}],"1.10":[{"yanked":false}],"2.0":[{"yanked":true}],"3.0rc1":[{"yanked":false}],"4.0":[],"invalid":[{"yanked":false}]}}`, "1.10"},
		{"prerelease only", `{"releases":{"2.0rc1":[{"yanked":false}],"2.0rc2":[{"yanked":false}]}}`, "2.0rc2"},
		{"partly yanked", `{"releases":{"1.0":[{"yanked":true},{"yanked":false}]}}`, "1.0"},
		{"empty", `{"releases":{}}`, ""}, {"invalid JSON", `{`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/pypi/demo-package/json" {
					t.Errorf("path=%s", r.URL.Path)
				}
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			c := &Client{HTTPClient: server.Client(), BaseURL: server.URL}
			got, err := c.LatestVersion(context.Background(), "Demo.Package")
			if got != tc.want || (err != nil) != (tc.want == "") {
				t.Fatalf("got %q err %v want %q", got, err, tc.want)
			}
		})
	}
	for _, status := range []int{404, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			c := &Client{HTTPClient: server.Client(), BaseURL: server.URL}
			if _, err := c.LatestVersion(context.Background(), "demo"); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
}
