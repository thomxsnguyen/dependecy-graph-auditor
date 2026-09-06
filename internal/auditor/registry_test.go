package auditor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNpmLatestVersion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		versions []string
		want     string
	}{
		{"highest stable", []string{"1.9.0", "1.10.0", "1.2.0"}, "1.10.0"},
		{"prefer stable", []string{"1.0.0", "2.0.0-rc.1"}, "1.0.0"},
		{"prerelease fallback", []string{"2.0.0-rc.1", "2.0.0-rc.2", "broken"}, "2.0.0-rc.2"},
		{"hyphen in build metadata", []string{"1.0.0+build-one", "0.9.0"}, "1.0.0+build-one"},
		{"invalid ignored", []string{"bad", "1.0.0"}, "1.0.0"},
		{"invalid only", []string{"bad"}, ""}, {"empty", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/@scope/demo" || r.Header.Get("Accept") != "application/vnd.npm.install-v1+json" {
					t.Errorf("request %s headers %v", r.URL.Path, r.Header)
				}
				versions := map[string]any{}
				for _, v := range tc.versions {
					versions[v] = struct{}{}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"versions": versions})
			}))
			defer server.Close()
			c := &NpmClient{http: server.Client(), baseURL: server.URL}
			got, err := c.LatestVersion(context.Background(), "@scope/demo")
			if got != tc.want || (err != nil) != (tc.want == "") {
				t.Fatalf("got %q err %v want %q", got, err, tc.want)
			}
		})
	}
}

func TestNpmLatestVersionHTTPFailures(t *testing.T) {
	for _, status := range []int{404, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			c := &NpmClient{http: server.Client(), baseURL: server.URL}
			if _, err := c.LatestVersion(context.Background(), "demo"); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
}
