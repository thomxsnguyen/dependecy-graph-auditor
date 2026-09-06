package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	githubsource "github.com/thomxsnguyen/mini-distributed-job-api/internal/github"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
)

func scanJob() job.Job {
	return job.Job{ID: "root", RootJobID: "root", Type: "dependency_update_scan", MaxAttempts: 4, Payload: []byte(`{"repositoryUrl":"https://github.com/acme/service","ref":"main"}`)}
}

func TestUpdateScanDirectDependenciesAndDeduplication(t *testing.T) {
	fetcher := manifestFetcherStub{
		"package.json":     `{"name":"web","dependencies":{"demo":"^1.0.0"},"devDependencies":{"demo":"^1.0.0","dev":"1.0.0"}}`,
		"pyproject.toml":   "[project]\nname = \"service\"\nversion = \"1.0\"\ndependencies = [\"requests==2.0.0\"]\n",
		"requirements.txt": "requests==2.0.0\n",
		"go.mod":           "module example.com/service\ngo 1.23\nrequire (\nexample.com/direct v1.2.3\nexample.com/indirect v1.0.0 // indirect\n)\n",
	}
	h := DependencyUpdateScanHandler{GitHub: fetcher}
	value := scanJob()
	result, err := h.Handle(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	again, err := h.Handle(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, again) {
		t.Fatal("retry changed result/keys")
	}
	if len(result.Children) != 4 {
		t.Fatalf("children=%+v", result.Children)
	}
	seen := map[string]bool{}
	for _, child := range result.Children {
		var p updateCheckPayload
		if err := json.Unmarshal(child.Payload, &p); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte("root\x00" + p.Ecosystem + "\x00" + p.Name + "\x00" + p.VersionRange))
		if child.Type != "package_update_check" || !child.Internal || child.RootJobID != "root" || child.ParentJobID != "root" || child.MaxAttempts != 4 || child.IdempotencyKey != "update-child:"+hex.EncodeToString(sum[:]) {
			t.Fatalf("child=%+v", child)
		}
		if seen[child.IdempotencyKey] || p.Name == "example.com/indirect" {
			t.Fatalf("duplicate or indirect child=%+v", p)
		}
		seen[child.IdempotencyKey] = true
	}
	var summary struct {
		RepositoryURL   string `json:"repositoryUrl"`
		Ref             string
		DependencyCount int
		AuditID         string
	}
	if err := json.Unmarshal(result.Result, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.RepositoryURL != "https://github.com/acme/service" || summary.Ref != "main" || summary.DependencyCount != 4 || summary.AuditID != "root" {
		t.Fatalf("summary=%+v", summary)
	}
	// The shared flow must retain indirect requirements and the original key namespace for audits.
	audit, err := (DependencyAuditHandler{GitHub: fetcher}).Handle(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	foundIndirect := false
	for _, child := range audit.Children {
		var p struct{ Name, Version string }
		if err := json.Unmarshal(child.Payload, &p); err != nil {
			t.Fatal(err)
		}
		if p.Name == "example.com/indirect" {
			foundIndirect = true
			sum := sha256.Sum256([]byte("root\x00go\x00example.com/indirect\x00v1.0.0"))
			if child.Type != "audit_go_module" || child.IdempotencyKey != "audit-child:"+hex.EncodeToString(sum[:]) {
				t.Fatalf("audit child=%+v", child)
			}
		}
	}
	if !foundIndirect {
		t.Fatal("audit lost indirect requirement")
	}
}

type checkingManifestFetcher struct {
	t   *testing.T
	err error
}

func (f checkingManifestFetcher) FetchManifest(_ context.Context, repo githubsource.Repository, path, ref string) ([]byte, error) {
	if repo.Name != "service" || ref != "main" {
		f.t.Errorf("repository=%+v ref=%q", repo, ref)
	}
	return nil, f.err
}

func TestUpdateScanEmptyAndErrors(t *testing.T) {
	empty, err := (DependencyUpdateScanHandler{GitHub: manifestFetcherStub{"package.json": `{"name":"empty"}`}}).Handle(context.Background(), scanJob())
	if err != nil || len(empty.Children) != 0 {
		t.Fatalf("empty=%+v err=%v", empty, err)
	}
	for _, tc := range []struct {
		name    string
		fetcher ManifestFetcher
		payload string
		kind    job.ErrorKind
	}{
		{"missing manifests", manifestFetcherStub{}, "", job.ErrorPermanent},
		{"invalid manifest", manifestFetcherStub{"package.json": "{"}, "", job.ErrorPermanent},
		{"malformed payload", manifestFetcherStub{}, "{", job.ErrorPermanent},
		{"invalid repository", manifestFetcherStub{}, `{"repositoryUrl":"https://example.com/repo"}`, job.ErrorPermanent},
		{"fetch rate limit", checkingManifestFetcher{t, fmt.Errorf("status 429")}, "", job.ErrorTransient},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := scanJob()
			if tc.payload != "" {
				value.Payload = []byte(tc.payload)
			}
			_, err := (DependencyUpdateScanHandler{GitHub: tc.fetcher}).Handle(context.Background(), value)
			if err == nil || job.KindOf(err) != tc.kind {
				t.Fatalf("err=%v kind=%s", err, job.KindOf(err))
			}
		})
	}
}
