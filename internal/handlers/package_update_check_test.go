package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/thomxsnguyen/mini-distributed-job-api/internal/auditor"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/gomod"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/updater"
)

type updateRegistryStub struct {
	current, latest         string
	fetchErr, latestErr     error
	fetchCalls, latestCalls int
	name, versionRange      string
}

func (s *updateRegistryStub) FetchPackage(_ context.Context, name, versionRange string) (*auditor.PackageMetadata, error) {
	s.fetchCalls++
	s.name = name
	s.versionRange = versionRange
	if s.fetchErr != nil {
		return nil, s.fetchErr
	}
	return &auditor.PackageMetadata{Version: s.current, Dependencies: map[string]string{"transitive": "1.0.0"}}, nil
}
func (s *updateRegistryStub) LatestVersion(_ context.Context, name string) (string, error) {
	s.latestCalls++
	s.name = name
	return s.latest, s.latestErr
}

func updateJob(t *testing.T, ecosystem, name, versionRange string) job.Job {
	t.Helper()
	body, err := json.Marshal(updateCheckPayload{Ecosystem: ecosystem, Name: name, VersionRange: versionRange})
	if err != nil {
		t.Fatal(err)
	}
	return job.Job{Type: "package_update_check", Payload: body}
}

func TestUpdateCheckRoutesAndEmitsNoChildren(t *testing.T) {
	for _, tc := range []struct {
		ecosystem, name, versionRange, current, latest string
		want                                           updater.Staleness
	}{
		{"npm", "demo", "^1.0.0", "1.5.0", "2.0.0", updater.StalenessMajor},
		{"pypi", "demo", "", "1.0.0", "1.1.0", updater.StalenessMinor},
		{"go", "example.com/demo", "v1.0.0", "v1.0.0", "v1.0.1", updater.StalenessPatch},
		{"pypi", "demo", ">=1", "1!1.0", "1!2.0", updater.StalenessUnknown},
	} {
		t.Run(tc.ecosystem+tc.current, func(t *testing.T) {
			npm, pypi, goClient := &updateRegistryStub{}, &updateRegistryStub{}, &updateRegistryStub{}
			registries := map[string]*updateRegistryStub{"npm": npm, "pypi": pypi, "go": goClient}
			selected := registries[tc.ecosystem]
			selected.current = tc.current
			selected.latest = tc.latest
			h := PackageUpdateCheckHandler{Registries: UpdateCheckRegistries{NPM: npm, PyPI: pypi, Go: goClient}}
			result, err := h.Handle(context.Background(), updateJob(t, tc.ecosystem, tc.name, tc.versionRange))
			if err != nil {
				t.Fatal(err)
			}
			var got updateCheckResult
			if err := json.Unmarshal(result.Result, &got); err != nil {
				t.Fatal(err)
			}
			if got.Ecosystem != tc.ecosystem || got.Name != tc.name || got.CurrentVersion != tc.current || got.LatestVersion != tc.latest || got.Staleness != tc.want || len(result.Children) != 0 {
				t.Fatalf("result=%+v children=%v", got, result.Children)
			}
			for ecosystem, registry := range registries {
				wantFetch, wantLatest := 0, 0
				if ecosystem == tc.ecosystem {
					wantLatest = 1
					if ecosystem != "go" {
						wantFetch = 1
					}
				}
				if registry.fetchCalls != wantFetch || registry.latestCalls != wantLatest {
					t.Fatalf("%s calls=%d/%d", ecosystem, registry.fetchCalls, registry.latestCalls)
				}
			}
			if selected.name != tc.name || (tc.ecosystem != "go" && selected.versionRange != tc.versionRange) {
				t.Fatalf("lookup=%+v", selected)
			}
		})
	}
}

func TestUpdateCheckRejectsInvalidPayload(t *testing.T) {
	for _, body := range []string{`{`, `null`, `{}`, `{"ecosystem":"ruby","name":"demo","versionRange":"1"}`, `{"ecosystem":"npm","name":" ","versionRange":"1"}`, `{"ecosystem":"npm","name":"demo"}`, `{"ecosystem":"go","name":"example.com/demo","versionRange":"^1"}`} {
		t.Run(body, func(t *testing.T) {
			registry := &updateRegistryStub{}
			h := PackageUpdateCheckHandler{Registries: UpdateCheckRegistries{NPM: registry, PyPI: registry, Go: registry}}
			_, err := h.Handle(context.Background(), job.Job{Payload: []byte(body)})
			if err == nil || job.KindOf(err) != job.ErrorPermanent || registry.fetchCalls+registry.latestCalls != 0 {
				t.Fatalf("err=%v calls=%+v", err, registry)
			}
		})
	}
}

func TestUpdateCheckErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name, ecosystem string
		fetch           bool
		err             error
		kind            job.ErrorKind
	}{
		{"npm resolve failure", "npm", true, errors.New("no version satisfies constraint"), job.ErrorPermanent},
		{"npm rate limit", "npm", false, errors.New("unexpected status 429"), job.ErrorTransient},
		{"pypi server failure", "pypi", true, errors.New("PyPI returned status 503"), job.ErrorTransient},
		{"pypi latest not found", "pypi", false, errors.New("not found (404)"), job.ErrorPermanent},
		{"go rate limit", "go", false, fmt.Errorf("wrapped: %w", &gomod.ProxyError{Kind: gomod.ErrorRateLimited}), job.ErrorTransient},
		{"go timeout", "go", false, &gomod.ProxyError{Kind: gomod.ErrorTimeout}, job.ErrorTransient},
		{"go not found", "go", false, &gomod.ProxyError{Kind: gomod.ErrorNotFound}, job.ErrorPermanent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := &updateRegistryStub{current: "1.0.0", latest: "2.0.0"}
			if tc.fetch {
				registry.fetchErr = tc.err
			} else {
				registry.latestErr = tc.err
			}
			h := PackageUpdateCheckHandler{Registries: UpdateCheckRegistries{NPM: registry, PyPI: registry, Go: registry}}
			result, err := h.Handle(context.Background(), updateJob(t, tc.ecosystem, "example.com/demo", "v1.0.0"))
			if !errors.Is(err, tc.err) || job.KindOf(err) != tc.kind || len(result.Result) != 0 {
				t.Fatalf("err=%v kind=%s result=%s", err, job.KindOf(err), result.Result)
			}
			if tc.fetch && registry.latestCalls != 0 {
				t.Fatal("latest called after failed resolution")
			}
		})
	}
}
