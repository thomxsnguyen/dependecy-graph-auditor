//go:build integration

package postgres_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/thomxsnguyen/mini-distributed-job-api/internal/auditor"
	githubsource "github.com/thomxsnguyen/mini-distributed-job-api/internal/github"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/handlers"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/httpapi"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
	storepg "github.com/thomxsnguyen/mini-distributed-job-api/internal/store/postgres"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/worker"
)

type updateManifests map[string]string

func (f updateManifests) FetchManifest(_ context.Context, _ githubsource.Repository, path, _ string) ([]byte, error) {
	if body, ok := f[path]; ok {
		return []byte(body), nil
	}
	return nil, fmt.Errorf("manifest %s was not found", path)
}

type updateSmokeRegistry struct{}

func (updateSmokeRegistry) FetchPackage(context.Context, string, string) (*auditor.PackageMetadata, error) {
	return &auditor.PackageMetadata{Version: "1.2.0", Dependencies: map[string]string{"transitive": "1.0.0"}}, nil
}
func (updateSmokeRegistry) LatestVersion(context.Context, string) (string, error) {
	return "2.0.0", nil
}

func claimUpdateJob(t *testing.T, s *storepg.Store, workerID string) job.Job {
	t.Helper()
	value, found, err := s.Claim(context.Background(), workerID, time.Minute)
	if err != nil || !found {
		t.Fatalf("claim: found=%v err=%v", found, err)
	}
	return value
}

func startUpdateRoot(t *testing.T, s *storepg.Store, manifest string) (job.Job, job.HandlerResult) {
	t.Helper()
	ctx := context.Background()
	// Exercise public API submission and repeated idempotent submission.
	body := []byte(`{"type":"dependency_update_scan","payload":{"repositoryUrl":"https://github.com/acme/service","ref":"main"},"maxAttempts":2}`)
	var root job.Job
	for i := 0; i < 2; i++ {
		request := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
		request.Header.Set("Idempotency-Key", "update-scan-integration")
		response := httptest.NewRecorder()
		httpapi.NewJobAPI(s, nil).Routes().ServeHTTP(response, request)
		wantStatus := http.StatusAccepted
		if i > 0 {
			wantStatus = http.StatusOK
		}
		if response.Code != wantStatus {
			t.Fatalf("submit: %d %s", response.Code, response.Body.String())
		}
		var result struct{ Job job.Job }
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if i > 0 && result.Job.ID != root.ID {
			t.Fatal("duplicate root")
		}
		root = result.Job
	}
	claimed := claimUpdateJob(t, s, "root-worker")
	if claimed.ID != root.ID {
		t.Fatalf("claimed=%s root=%s", claimed.ID, root.ID)
	}
	result, err := (handlers.DependencyUpdateScanHandler{GitHub: updateManifests{"package.json": manifest}}).Handle(ctx, claimed)
	if err != nil {
		t.Fatal(err)
	}
	// The store must also deduplicate repeated child submissions at the atomic boundary.
	if len(result.Children) > 0 {
		result.Children = append(result.Children, result.Children[0])
	}
	if err := s.Complete(ctx, claimed, result); err != nil {
		t.Fatal(err)
	}
	return root, result
}

func assertUpdateSummary(t *testing.T, detail job.Detail, want json.RawMessage) {
	t.Helper()
	var gotValue, wantValue any
	if err := json.Unmarshal(detail.Result, &gotValue); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("summary=%s want %s", detail.Result, want)
	}
}

func TestUpdateScanFinalization(t *testing.T) {
	for _, outcome := range []string{"success", "permanent failure", "exhausted retry", "empty"} {
		t.Run(outcome, func(t *testing.T) {
			db := setupIntegrationDB(t)
			s := storepg.New(db.pool)
			ctx := context.Background()
			manifest := `{"name":"web","dependencies":{"demo":"^1.0.0"}}`
			if outcome == "empty" {
				manifest = `{"name":"web"}`
			}
			root, rootResult := startUpdateRoot(t, s, manifest)
			detail, err := s.Get(ctx, root.ID)
			if err != nil {
				t.Fatal(err)
			}
			assertUpdateSummary(t, detail, rootResult.Result)
			wantStatus := job.StatusCompleted
			if outcome != "empty" {
				if detail.Job.Status != job.StatusWaiting {
					t.Fatalf("root status=%s", detail.Job.Status)
				}
				child := claimUpdateJob(t, s, "child-worker")
				if child.Type != "package_update_check" || child.RootJobID != root.ID || !child.Internal {
					t.Fatalf("child=%+v", child)
				}
				switch outcome {
				case "success":
					result, err := (handlers.PackageUpdateCheckHandler{Registries: handlers.UpdateCheckRegistries{NPM: updateSmokeRegistry{}}}).Handle(ctx, child)
					if err != nil {
						t.Fatal(err)
					}
					if err := s.Complete(ctx, child, result); err != nil {
						t.Fatal(err)
					}
					stored, err := s.Get(ctx, child.ID)
					if err != nil {
						t.Fatal(err)
					}
					var row map[string]string
					if err := json.Unmarshal(stored.Result, &row); err != nil {
						t.Fatal(err)
					}
					if row["currentVersion"] != "1.2.0" || row["latestVersion"] != "2.0.0" || row["staleness"] != "major" {
						t.Fatalf("result=%s", stored.Result)
					}
				case "permanent failure":
					wantStatus = job.StatusFailed
					if err := s.Fail(ctx, child, job.ErrorPermanent, "not found", time.Time{}); err != nil {
						t.Fatal(err)
					}
				case "exhausted retry":
					wantStatus = job.StatusDeadLettered
					if err := s.Fail(ctx, child, job.ErrorTransient, "rate limited", time.Now().Add(-time.Second)); err != nil {
						t.Fatal(err)
					}
					child = claimUpdateJob(t, s, "retry-worker")
					if err := s.Fail(ctx, child, job.ErrorTransient, "rate limited", time.Time{}); err != nil {
						t.Fatal(err)
					}
				}
			}
			detail, err = s.Get(ctx, root.ID)
			if err != nil {
				t.Fatal(err)
			}
			if detail.Job.Status != wantStatus {
				t.Fatalf("status=%s want=%s", detail.Job.Status, wantStatus)
			}
			assertUpdateSummary(t, detail, rootResult.Result)
			var children int
			if err := db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE root_job_id=$1 AND id<>$1`, root.ID).Scan(&children); err != nil {
				t.Fatal(err)
			}
			wantChildren := 1
			if outcome == "empty" {
				wantChildren = 0
			}
			if children != wantChildren {
				t.Fatalf("children=%d want=%d", children, wantChildren)
			}
			var auditRows int
			if err := db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM audit_results WHERE root_job_id=$1`, root.ID).Scan(&auditRows); err != nil {
				t.Fatal(err)
			}
			if auditRows != 0 {
				t.Fatal("update scan wrote audit results")
			}
		})
	}
}

func TestAuditFinalizationStillProducesAuditSummary(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	s := storepg.New(db.pool)
	root, _, err := s.Submit(ctx, job.Submission{Type: "dependency_audit", Payload: json.RawMessage(`{}`), MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	claimed := claimUpdateJob(t, s, "audit-worker")
	if err := s.Complete(ctx, claimed, job.HandlerResult{Children: []job.Submission{{Type: "audit_npm_package", Payload: json.RawMessage(`{"name":"demo","version":"1.0.0"}`), IdempotencyKey: "audit-child-test"}}}); err != nil {
		t.Fatal(err)
	}
	child := claimUpdateJob(t, s, "audit-worker")
	if err := s.Complete(ctx, child, job.HandlerResult{Result: json.RawMessage(`{"ecosystem":"npm","name":"demo","version":"1.0.0","license":"MIT","verdict":"allowed"}`)}); err != nil {
		t.Fatal(err)
	}
	detail, err := s.Get(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	var result struct{ Packages, Violations int }
	if err := json.Unmarshal(detail.Result, &result); err != nil {
		t.Fatal(err)
	}
	if detail.Job.Status != job.StatusCompleted || result.Packages != 1 || result.Violations != 0 {
		t.Fatalf("detail=%+v", detail)
	}
}

// The subprocess blocks inside a real child handler so killing it leaves a live lease.
type interruptedUpdateRegistry struct{ updateSmokeRegistry }

func (interruptedUpdateRegistry) LatestVersion(context.Context, string) (string, error) {
	fmt.Println("READY")
	select {}
}

func TestUpdateScanCrashHelper(t *testing.T) {
	if os.Getenv("UPDATE_SCAN_CRASH_HELPER") != "1" {
		return
	}
	ctx := context.Background()
	pool, err := openSchemaPool(ctx, os.Getenv("UPDATE_SCAN_DATABASE_URL"), os.Getenv("UPDATE_SCAN_SCHEMA"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	service := worker.NewService(storepg.New(pool), "crashed-worker", map[string]job.ServiceHandler{
		"package_update_check": handlers.PackageUpdateCheckHandler{Registries: handlers.UpdateCheckRegistries{NPM: interruptedUpdateRegistry{}}},
	}, worker.ServiceOptions{Concurrency: 1})
	if err := service.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateScanCrashRecovery(t *testing.T) {
	db := setupIntegrationDB(t)
	s := storepg.New(db.pool)
	ctx := context.Background()
	root, rootResult := startUpdateRoot(t, s, `{"name":"web","dependencies":{"demo":"^1.0.0"}}`)
	cmd := exec.Command(os.Args[0], "-test.run=^TestUpdateScanCrashHelper$")
	cmd.Env = append(os.Environ(), "UPDATE_SCAN_CRASH_HELPER=1", "UPDATE_SCAN_DATABASE_URL="+db.url, "UPDATE_SCAN_SCHEMA="+db.schema)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	ready := make(chan bool, 1)
	go func() { scanner := bufio.NewScanner(stdout); ready <- scanner.Scan() && scanner.Text() == "READY" }()
	select {
	case ok := <-ready:
		if !ok {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("helper not ready: %s", stderr.String())
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("helper timed out")
	}
	var childID string
	if err := db.pool.QueryRow(ctx, `SELECT id FROM jobs WHERE root_job_id=$1 AND internal=TRUE AND status='running' AND locked_by='crashed-worker'`, root.ID).Scan(&childID); err != nil {
		t.Fatal(err)
	}
	oldDetail, err := s.Get(ctx, childID)
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("helper exited without being killed")
	}
	// Expire only the isolated test job's lease to avoid a wall-clock sleep.
	if _, err := db.pool.Exec(ctx, `UPDATE jobs SET locked_until=NOW()-INTERVAL '1 second' WHERE id=$1`, childID); err != nil {
		t.Fatal(err)
	}
	recovered := storepg.New(db.pool)
	count, err := recovered.ReclaimExpired(ctx)
	if err != nil || count != 1 {
		t.Fatalf("reclaimed=%d err=%v", count, err)
	}
	child := claimUpdateJob(t, recovered, "recovery-worker")
	if child.ID != childID || child.Attempts != 2 {
		t.Fatalf("recovered=%+v", child)
	}
	result, err := (handlers.PackageUpdateCheckHandler{Registries: handlers.UpdateCheckRegistries{NPM: updateSmokeRegistry{}}}).Handle(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Complete(ctx, oldDetail.Job, result); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("stale completion=%v", err)
	}
	if err := recovered.Complete(ctx, child, result); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Complete(ctx, child, result); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("duplicate completion=%v", err)
	}
	detail, err := recovered.Get(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Job.Status != job.StatusCompleted {
		t.Fatalf("root status=%s", detail.Job.Status)
	}
	assertUpdateSummary(t, detail, rootResult.Result)
	var children, results int
	if err := db.pool.QueryRow(ctx, `SELECT COUNT(*),COUNT(r.job_id) FROM jobs j LEFT JOIN job_results r ON r.job_id=j.id WHERE j.root_job_id=$1 AND j.internal=TRUE`, root.ID).Scan(&children, &results); err != nil {
		t.Fatal(err)
	}
	if children != 1 || results != 1 {
		t.Fatalf("children=%d results=%d", children, results)
	}
}

func TestUpdateScanConcurrentChildCompletion(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	s := storepg.New(db.pool)
	manifest := `{"name":"web","dependencies":{`
	const count = 8
	for i := 0; i < count; i++ {
		if i > 0 {
			manifest += ","
		}
		manifest += fmt.Sprintf(`"demo-%d":"1.0.0"`, i)
	}
	manifest += "}}"
	root, rootResult := startUpdateRoot(t, s, manifest)
	children := make([]job.Job, count)
	for i := range children {
		children[i] = claimUpdateJob(t, s, fmt.Sprintf("worker-%d", i))
	}
	start := make(chan struct{})
	done := make(chan error, count)
	for _, child := range children {
		go func() {
			<-start
			result, err := (handlers.PackageUpdateCheckHandler{Registries: handlers.UpdateCheckRegistries{NPM: updateSmokeRegistry{}}}).Handle(ctx, child)
			if err == nil {
				err = s.Complete(ctx, child, result)
			}
			done <- err
		}()
	}
	close(start)
	for range children {
		if err := <-done; err != nil {
			t.Errorf("complete: %v", err)
		}
	}
	detail, err := s.Get(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Job.Status != job.StatusCompleted || detail.ChildCounts[job.StatusCompleted] != count {
		t.Fatalf("root=%s children=%v", detail.Job.Status, detail.ChildCounts)
	}
	assertUpdateSummary(t, detail, rootResult.Result)
}
