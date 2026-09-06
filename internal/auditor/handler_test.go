package auditor_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/thomxsnguyen/mini-distributed-job-api/internal/auditor"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
)

// ---------------------------------------------------------------------------
// Mock registry
// ---------------------------------------------------------------------------

// mockRegistry is a deterministic, in-memory RegistryClient for use in tests.
// Configure Packages to map "name@version" → PackageMetadata that FetchPackage
// will return. Set Err to simulate a registry failure.
type mockRegistry struct {
	Packages map[string]*auditor.PackageMetadata
	Err      error
}

func (m *mockRegistry) FetchPackage(_ context.Context, name, version string) (*auditor.PackageMetadata, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	key := name + "@" + version
	meta, ok := m.Packages[key]
	if !ok {
		return nil, errors.New("mock: package not found: " + key)
	}
	return meta, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newAuditJob builds a minimal "audit_package" job for the given name@version.
func newAuditJob(t *testing.T, name, version string) job.Job {
	t.Helper()
	return newAuditJobWithParent(t, name, version, "", "")
}

func newAuditJobWithParent(t *testing.T, name, version, parentName, parentVersion string) job.Job {
	t.Helper()
	payload, err := json.Marshal(auditor.AuditPayload{
		Name:          name,
		Version:       version,
		ParentName:    parentName,
		ParentVersion: parentVersion,
	})
	if err != nil {
		t.Fatalf("newAuditJob marshal: %v", err)
	}
	return job.Job{
		ID:      job.NewJobID(),
		Type:    "audit_package",
		Payload: payload,
		Status:  job.StatusPending,
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestHandleHappyPath verifies that a package with two dependencies produces
// two parent-aware child jobs and one stored package node. Edges are recorded
// later, after each child resolves.
func TestHandleHappyPath(t *testing.T) {
	reg := &mockRegistry{
		Packages: map[string]*auditor.PackageMetadata{
			"express@4.18.2": {
				Name:    "express",
				Version: "4.18.2",
				License: "MIT",
				Dependencies: map[string]string{
					"body-parser": "1.20.1",
					"qs":          "6.11.0",
				},
			},
		},
	}
	pkgs := auditor.NewPackageStore()
	edges := auditor.NewEdgeStore()
	h := auditor.NewAuditHandler(reg, auditor.LicensePolicy{}, pkgs, edges)

	j := newAuditJob(t, "express", "4.18.2")
	newJobs, err := h.Handle(context.Background(), j)

	if err != nil {
		t.Fatalf("Handle: unexpected error: %v", err)
	}
	if len(newJobs) != 2 {
		t.Fatalf("Handle: expected 2 child jobs, got %d", len(newJobs))
	}
	if len(pkgs.All()) != 1 {
		t.Fatalf("Handle: expected 1 package in store, got %d", len(pkgs.All()))
	}
	if len(edges.All()) != 0 {
		t.Fatalf("Handle: expected no unresolved edges in store, got %d", len(edges.All()))
	}
	for _, child := range newJobs {
		var payload auditor.AuditPayload
		if err := json.Unmarshal(child.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.ParentName != "express" || payload.ParentVersion != "4.18.2" {
			t.Errorf("child parent: got %s@%s, want express@4.18.2", payload.ParentName, payload.ParentVersion)
		}
	}
}

func TestHandleRecordsResolvedIncomingEdgeAndRootMapping(t *testing.T) {
	reg := &mockRegistry{Packages: map[string]*auditor.PackageMetadata{
		"react@^19.1.0": {
			Name: "react", Version: "19.2.8", License: "MIT", Dependencies: map[string]string{},
		},
	}}
	packages := auditor.NewPackageStore()
	edges := auditor.NewEdgeStore()
	h := auditor.NewAuditHandler(reg, auditor.LicensePolicy{}, packages, edges)

	_, err := h.Handle(context.Background(), newAuditJobWithParent(
		t, "react", "^19.1.0", "personal-portfolio", "",
	))
	if err != nil {
		t.Fatal(err)
	}

	want := auditor.DependencyEdge{
		FromName: "personal-portfolio", ToName: "react", ToVersion: "19.2.8",
	}
	got := edges.All()
	if len(got) != 1 || got[0] != want {
		t.Fatalf("edges: got %+v, want [%+v]", got, want)
	}

	report := auditor.GenerateReport(packages, edges, "personal-portfolio")
	markdown, err := auditor.GenerateMarkdownReport(auditor.MarkdownReportInput{
		Root: "personal-portfolio", Packages: packages.All(), Edges: got, Report: report,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(markdown, "react@^19.1.0") {
		t.Fatalf("Markdown contains unresolved range node:\n%s", markdown)
	}
	for _, coordinate := range []string{"personal-portfolio", "react@19.2.8"} {
		if !strings.Contains(markdown, coordinate) {
			t.Errorf("Markdown missing node %q:\n%s", coordinate, markdown)
		}
	}
}

// TestHandleVerdictPolicyViolation verifies that a disallowed license produces
// a VerdictPolicyViolation on the stored package.
func TestHandleVerdictPolicyViolation(t *testing.T) {
	reg := &mockRegistry{
		Packages: map[string]*auditor.PackageMetadata{
			"evil-lib@0.1.0": {
				Name:         "evil-lib",
				Version:      "0.1.0",
				License:      "GPL-3.0",
				Dependencies: map[string]string{},
			},
		},
	}
	pkgs := auditor.NewPackageStore()
	h := auditor.NewAuditHandler(reg, auditor.LicensePolicy{}, pkgs, auditor.NewEdgeStore())

	_, err := h.Handle(context.Background(), newAuditJob(t, "evil-lib", "0.1.0"))
	if err != nil {
		t.Fatalf("Handle: unexpected error: %v", err)
	}

	all := pkgs.All()
	if len(all) != 1 {
		t.Fatalf("expected 1 package, got %d", len(all))
	}
	if all[0].Verdict != auditor.VerdictPolicyViolation {
		t.Errorf("verdict: got %q, want %q", all[0].Verdict, auditor.VerdictPolicyViolation)
	}
}

// TestHandleDeduplicatesAlreadySeenPackage verifies that when the package is
// already in the store, Handle returns nil jobs and nil error — no duplicate
// edges are written and no child jobs are produced.
func TestHandleDeduplicatesAlreadySeenPackage(t *testing.T) {
	reg := &mockRegistry{
		Packages: map[string]*auditor.PackageMetadata{
			"lodash@4.17.21": {
				Name:         "lodash",
				Version:      "4.17.21",
				License:      "MIT",
				Dependencies: map[string]string{"dep-a": "1.0.0"},
			},
		},
	}
	pkgs := auditor.NewPackageStore()
	edges := auditor.NewEdgeStore()
	// Pre-populate the store to simulate a prior worker having audited lodash.
	pkgs.Add(auditor.Package{Name: "lodash", Version: "4.17.21"})

	h := auditor.NewAuditHandler(reg, auditor.LicensePolicy{}, pkgs, edges)
	newJobs, err := h.Handle(context.Background(), newAuditJob(t, "lodash", "4.17.21"))

	if err != nil {
		t.Fatalf("Handle (dedup): unexpected error: %v", err)
	}
	if newJobs != nil {
		t.Errorf("Handle (dedup): expected nil child jobs, got %v", newJobs)
	}
	if len(edges.All()) != 0 {
		t.Errorf("Handle (dedup): expected 0 edges written, got %d", len(edges.All()))
	}
}

// TestHandleEnqueuesRelationshipsBeforeExactResolution verifies that requested
// ranges are not compared with exact package-store coordinates. Each declared
// relationship must resolve so its incoming exact edge can be recorded.
func TestHandleEnqueuesRelationshipsBeforeExactResolution(t *testing.T) {
	reg := &mockRegistry{
		Packages: map[string]*auditor.PackageMetadata{
			"a@1.0.0": {
				Name:    "a",
				Version: "1.0.0",
				License: "MIT",
				Dependencies: map[string]string{
					"b": "2.0.0", // already seen
					"c": "3.0.0", // new
				},
			},
		},
	}
	pkgs := auditor.NewPackageStore()
	edges := auditor.NewEdgeStore()
	// Pre-populate b as already audited.
	pkgs.Add(auditor.Package{Name: "b", Version: "2.0.0"})

	h := auditor.NewAuditHandler(reg, auditor.LicensePolicy{}, pkgs, edges)
	newJobs, err := h.Handle(context.Background(), newAuditJob(t, "a", "1.0.0"))

	if err != nil {
		t.Fatalf("Handle: unexpected error: %v", err)
	}
	if len(newJobs) != 2 {
		t.Fatalf("Handle: expected 2 child jobs, got %d", len(newJobs))
	}
	if len(edges.All()) != 0 {
		t.Errorf("Handle: expected no pre-resolution edges, got %d", len(edges.All()))
	}
}

func TestHandlePreservesIncomingEdgesForDeduplicatedPackage(t *testing.T) {
	reg := &mockRegistry{Packages: map[string]*auditor.PackageMetadata{
		"shared@^3.0.0": {
			Name: "shared", Version: "3.1.0", License: "MIT", Dependencies: map[string]string{},
		},
	}}
	packages := auditor.NewPackageStore()
	edges := auditor.NewEdgeStore()
	h := auditor.NewAuditHandler(reg, auditor.LicensePolicy{}, packages, edges)

	firstJobs, err := h.Handle(context.Background(), newAuditJobWithParent(t, "shared", "^3.0.0", "left", "1.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	secondJobs, err := h.Handle(context.Background(), newAuditJobWithParent(t, "shared", "^3.0.0", "right", "2.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	if len(firstJobs) != 0 || secondJobs != nil {
		t.Fatalf("unexpected child expansion: first=%v second=%v", firstJobs, secondJobs)
	}

	got := edges.All()
	want := map[auditor.DependencyEdge]bool{
		{FromName: "left", FromVersion: "1.0.0", ToName: "shared", ToVersion: "3.1.0"}:  true,
		{FromName: "right", FromVersion: "2.0.0", ToName: "shared", ToVersion: "3.1.0"}: true,
	}
	if len(got) != len(want) {
		t.Fatalf("edges: got %+v, want two diamond edges", got)
	}
	for _, edge := range got {
		if !want[edge] {
			t.Errorf("unexpected edge: %+v", edge)
		}
	}
}

func TestHandleLegacyPayloadWithoutParentRemainsValid(t *testing.T) {
	reg := &mockRegistry{Packages: map[string]*auditor.PackageMetadata{
		"legacy@1.0.0": {
			Name: "legacy", Version: "1.0.0", License: "MIT", Dependencies: map[string]string{},
		},
	}}
	edges := auditor.NewEdgeStore()
	h := auditor.NewAuditHandler(reg, auditor.LicensePolicy{}, auditor.NewPackageStore(), edges)

	if _, err := h.Handle(context.Background(), newAuditJob(t, "legacy", "1.0.0")); err != nil {
		t.Fatal(err)
	}
	if len(edges.All()) != 0 {
		t.Fatalf("legacy payload unexpectedly created an incoming edge: %+v", edges.All())
	}
}

// TestHandleRegistryErrorPropagates verifies that a registry failure is
// returned as an error and nothing is written to the stores.
func TestHandleRegistryErrorPropagates(t *testing.T) {
	reg := &mockRegistry{Err: errors.New("registry unavailable")}
	pkgs := auditor.NewPackageStore()
	edges := auditor.NewEdgeStore()
	h := auditor.NewAuditHandler(reg, auditor.LicensePolicy{}, pkgs, edges)

	_, err := h.Handle(context.Background(), newAuditJob(t, "some-pkg", "1.0.0"))

	if err == nil {
		t.Fatal("Handle: expected error from failing registry, got nil")
	}
	if len(pkgs.All()) != 0 {
		t.Errorf("Handle: expected 0 packages after registry error, got %d", len(pkgs.All()))
	}
}

// TestHandleBadPayloadReturnsError verifies that a malformed JSON payload
// returns an error without calling the registry.
func TestHandleBadPayloadReturnsError(t *testing.T) {
	// reg.Err is nil but Packages is empty — if registry is called, it will error
	// with "package not found", but we expect the payload error to come first.
	reg := &mockRegistry{Packages: map[string]*auditor.PackageMetadata{}}
	h := auditor.NewAuditHandler(reg, auditor.LicensePolicy{}, auditor.NewPackageStore(), auditor.NewEdgeStore())

	bad := job.Job{
		ID:      job.NewJobID(),
		Type:    "audit_package",
		Payload: []byte("not-valid-json"),
	}
	_, err := h.Handle(context.Background(), bad)
	if err == nil {
		t.Fatal("Handle: expected unmarshal error, got nil")
	}
}

func (*mockRegistry) LatestVersion(context.Context, string) (string, error) {
	return "", errors.New("latest lookup not configured in audit test")
}
