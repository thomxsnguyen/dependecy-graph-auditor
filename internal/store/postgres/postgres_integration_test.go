//go:build integration

package postgres_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/auditor"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/dlq"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/queue"
	storepg "github.com/thomxsnguyen/mini-distributed-job-api/internal/store/postgres"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/worker"
)

const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"

type integrationDB struct {
	url    string
	schema string
	pool   *pgxpool.Pool
}

func setupIntegrationDB(t *testing.T) *integrationDB {
	t.Helper()
	ctx := context.Background()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = defaultDatabaseURL
	}

	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("open Postgres admin pool: %v", err)
	}
	if err := admin.Ping(ctx); err != nil {
		admin.Close()
		t.Fatalf("Postgres is required for integration tests: %v", err)
	}

	schema := fmt.Sprintf("phase4_test_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.Close()
		t.Fatalf("create test schema: %v", err)
	}

	pool, err := openSchemaPool(ctx, url, schema)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+identifier+" CASCADE")
		admin.Close()
		t.Fatalf("open test schema pool: %v", err)
	}

	_, filename, _, _ := runtime.Caller(0)
	migrationPaths, err := filepath.Glob(filepath.Join(filepath.Dir(filename), "../../../db/migrations/*.sql"))
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	for _, migrationPath := range migrationPaths {
		migration, err := os.ReadFile(migrationPath)
		if err != nil {
			t.Fatalf("read migration: %v", err)
		}
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", filepath.Base(migrationPath), err)
		}
	}

	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE")
		admin.Close()
	})
	return &integrationDB{url: url, schema: schema, pool: pool}
}

func openSchemaPool(ctx context.Context, url, schema string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func runProcessHelper(t *testing.T, db *integrationDB, action, id string, scheduledAt time.Time) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPhase4ProcessHelper$")
	cmd.Env = append(os.Environ(),
		"PHASE4_HELPER=1",
		"PHASE4_DATABASE_URL="+db.url,
		"PHASE4_SCHEMA="+db.schema,
		"PHASE4_ACTION="+action,
		"PHASE4_JOB_ID="+id,
		"PHASE4_SCHEDULED_AT="+scheduledAt.Format(time.RFC3339Nano),
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("helper process failed: %v\n%s", err, output)
	}
}

func runAndKillHelper(t *testing.T, db *integrationDB, id string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPhase4ProcessHelper$")
	cmd.Env = append(os.Environ(),
		"PHASE4_HELPER=1",
		"PHASE4_DATABASE_URL="+db.url,
		"PHASE4_SCHEMA="+db.schema,
		"PHASE4_ACTION=running",
		"PHASE4_JOB_ID="+id,
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "READY" {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("crash helper did not become ready: %s", stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("crash helper exited normally; expected forced termination")
	}
}

func TestPhase4ProcessHelper(t *testing.T) {
	if os.Getenv("PHASE4_HELPER") != "1" {
		return
	}
	ctx := context.Background()
	pool, err := openSchemaPool(ctx, os.Getenv("PHASE4_DATABASE_URL"), os.Getenv("PHASE4_SCHEMA"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	s := storepg.New(pool)
	j := job.NewJob("integration", json.RawMessage(`{"source":"helper"}`))
	j.ID = os.Getenv("PHASE4_JOB_ID")
	// Keep immediate acquisition deterministic even if the application and
	// database clocks differ by a few milliseconds.
	j.ScheduledAt = time.Now().Add(-time.Second)
	if err := s.CreateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	j, found, err := s.AcquireJob(ctx)
	if err != nil || !found {
		t.Fatalf("acquire helper job: found=%v err=%v", found, err)
	}

	switch os.Getenv("PHASE4_ACTION") {
	case "running":
		fmt.Println("READY")
		select {}
	case "retry":
		scheduledAt, err := time.Parse(time.RFC3339Nano, os.Getenv("PHASE4_SCHEDULED_AT"))
		if err != nil {
			t.Fatal(err)
		}
		j.Attempts = 1
		j.ScheduledAt = scheduledAt
		if err := s.RetryJob(ctx, j); err != nil {
			t.Fatal(err)
		}
	case "dlq":
		j.Attempts = j.MaxAttempts
		persistentDLQ := dlq.New(s)
		persistentDLQ.Publish(j, errors.New("persistent failure"))
		if len(persistentDLQ.Entries()) != 1 {
			t.Fatal("persistent DLQ entry was not written")
		}
	default:
		t.Fatalf("unknown helper action %q", os.Getenv("PHASE4_ACTION"))
	}
}

type handlerFunc func(context.Context, job.Job) ([]job.Job, error)

func (f handlerFunc) Handle(ctx context.Context, j job.Job) ([]job.Job, error) {
	return f(ctx, j)
}

func waitDone(t *testing.T, p *worker.Pool) {
	t.Helper()
	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("pool did not finish")
	}
}

func runPool(t *testing.T, p *worker.Pool, q *queue.Queue) {
	t.Helper()
	p.Start(context.Background())
	waitDone(t, p)
	q.Close()
	p.Wait()
}

func TestCrashAndResume(t *testing.T) {
	db := setupIntegrationDB(t)
	id := fmt.Sprintf("crash-%d", time.Now().UnixNano())
	runAndKillHelper(t, db, id)

	s := storepg.New(db.pool)
	q := queue.New(4, s)
	var calls int
	p := worker.NewWithOptions(1, q, handlerFunc(func(context.Context, job.Job) ([]job.Job, error) {
		calls++
		return nil, nil
	}), worker.WithStore(s), worker.WithPollInterval(5*time.Millisecond))
	runPool(t, p, q)

	var status job.Status
	if err := db.pool.QueryRow(context.Background(), "SELECT status FROM jobs WHERE id=$1", id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || status != job.StatusCompleted {
		t.Fatalf("resumed job calls=%d status=%q", calls, status)
	}
}

func TestDurableBackoffSurvivesRestart(t *testing.T) {
	db := setupIntegrationDB(t)
	id := fmt.Sprintf("backoff-%d", time.Now().UnixNano())
	scheduledAt := time.Now().Add(300 * time.Millisecond)
	runProcessHelper(t, db, "retry", id, scheduledAt)

	s := storepg.New(db.pool)
	q := queue.New(4, s)
	var mu sync.Mutex
	var handledAt time.Time
	p := worker.NewWithOptions(1, q, handlerFunc(func(context.Context, job.Job) ([]job.Job, error) {
		mu.Lock()
		handledAt = time.Now()
		mu.Unlock()
		return nil, nil
	}), worker.WithStore(s), worker.WithPollInterval(5*time.Millisecond))
	runPool(t, p, q)

	mu.Lock()
	actual := handledAt
	mu.Unlock()
	if actual.Before(scheduledAt) {
		t.Fatalf("job handled at %v before scheduled_at %v", actual, scheduledAt)
	}
	var attempts int
	var status job.Status
	if err := db.pool.QueryRow(context.Background(),
		"SELECT attempts, status FROM jobs WHERE id=$1", id,
	).Scan(&attempts, &status); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || status != job.StatusCompleted {
		t.Fatalf("durable retry attempts=%d status=%q", attempts, status)
	}
}

func TestDLQPersistsAcrossRestart(t *testing.T) {
	db := setupIntegrationDB(t)
	id := fmt.Sprintf("dlq-%d", time.Now().UnixNano())
	runProcessHelper(t, db, "dlq", id, time.Time{})

	entries := dlq.New(storepg.New(db.pool)).Entries()
	if len(entries) != 1 {
		t.Fatalf("DLQ entries=%d, want 1", len(entries))
	}
	if entries[0].Job.ID != id || entries[0].Err != "persistent failure" {
		t.Fatalf("unexpected DLQ entry: %+v", entries[0])
	}
}

type smokeRegistry struct{}

func (smokeRegistry) FetchPackage(context.Context, string, string) (*auditor.PackageMetadata, error) {
	return &auditor.PackageMetadata{
		Name:         "smoke-package",
		Version:      "1.0.0",
		License:      "MIT",
		Dependencies: map[string]string{},
	}, nil
}

func TestAuditorDurableStorageSmoke(t *testing.T) {
	db := setupIntegrationDB(t)
	s := storepg.New(db.pool)
	q := queue.New(4, s)
	packages := auditor.NewPackageStore()
	handler := auditor.NewAuditHandler(
		smokeRegistry{},
		auditor.LicensePolicy{},
		packages,
		auditor.NewEdgeStore(),
	)
	p := worker.NewWithOptions(1, q, handler,
		worker.WithStore(s),
		worker.WithDLQ(dlq.New(s)),
		worker.WithPollInterval(5*time.Millisecond),
	)
	payload, err := json.Marshal(auditor.AuditPayload{Name: "smoke-package", Version: "1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	j := job.NewJob("audit_package", payload)
	p.Submit(j)
	runPool(t, p, q)

	if len(packages.All()) != 1 {
		t.Fatalf("audited packages=%d, want 1", len(packages.All()))
	}
	var status job.Status
	if err := db.pool.QueryRow(context.Background(), "SELECT status FROM jobs WHERE id=$1", j.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != job.StatusCompleted {
		t.Fatalf("durable smoke job status=%q", status)
	}
}

func TestServiceIdempotencyAndAtomicClaim(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	s := storepg.New(db.pool)
	input := job.Submission{Type: "demo", Payload: json.RawMessage(`{"durationMs":0}`), MaxAttempts: 3, IdempotencyKey: "same-request"}
	first, created, err := s.Submit(ctx, input)
	if err != nil || !created {
		t.Fatalf("first submit: created=%v err=%v", created, err)
	}
	second, created, err := s.Submit(ctx, input)
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("duplicate submit: id=%q created=%v err=%v", second.ID, created, err)
	}
	input.Payload = json.RawMessage(`{"durationMs":1}`)
	if _, _, err := s.Submit(ctx, input); !errors.Is(err, job.ErrIdempotencyConflict) {
		t.Fatalf("conflicting submit: %v", err)
	}

	type claimResult struct {
		value job.Job
		found bool
		err   error
	}
	claims := make(chan claimResult, 2)
	var claimers sync.WaitGroup
	for _, workerID := range []string{"worker-one", "worker-two"} {
		claimers.Add(1)
		go func() {
			defer claimers.Done()
			value, found, err := s.Claim(ctx, workerID, time.Minute)
			claims <- claimResult{value: value, found: found, err: err}
		}()
	}
	claimers.Wait()
	close(claims)
	var claimed job.Job
	foundCount := 0
	for result := range claims {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.found {
			claimed = result.value
			foundCount++
		}
	}
	if foundCount != 1 {
		t.Fatalf("successful concurrent claims=%d", foundCount)
	}
	if claimed.Attempts != 1 || claimed.LockedBy == "" || claimed.LeaseToken == "" || claimed.LockedUntil == nil {
		t.Fatalf("claim returned stale lease state: %+v", claimed)
	}
	owned, err := s.Heartbeat(ctx, claimed, time.Minute)
	if err != nil || !owned {
		t.Fatalf("heartbeat with returned lease: owned=%v err=%v", owned, err)
	}
	stale := claimed
	stale.LeaseToken = "wrong"
	if err := s.Complete(ctx, stale, job.HandlerResult{Result: json.RawMessage(`{"ok":true}`)}); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("stale completion: %v", err)
	}
	if err := s.Complete(ctx, claimed, job.HandlerResult{Result: json.RawMessage(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	detail, err := s.Get(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Job.Status != job.StatusCompleted || len(detail.Attempts) != 1 || len(detail.Result) == 0 {
		t.Fatalf("unexpected detail: %+v", detail)
	}
}

func TestServiceRetryCancellationAndDLQReplay(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	s := storepg.New(db.pool)
	retryJob, _, err := s.Submit(ctx, job.Submission{Type: "demo", Payload: json.RawMessage(`{}`), MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	claimed, _, err := s.Claim(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != retryJob.ID {
		t.Fatalf("claimed %s", claimed.ID)
	}
	if err := s.Fail(ctx, claimed, job.ErrorTransient, "temporary", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	detail, err := s.Get(ctx, retryJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Job.Status != job.StatusRetryScheduled {
		t.Fatalf("status=%s", detail.Job.Status)
	}
	cancelled, err := s.Cancel(ctx, retryJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != job.StatusCancelled {
		t.Fatalf("cancel status=%s", cancelled.Status)
	}

	dead, _, err := s.Submit(ctx, job.Submission{Type: "demo", Payload: json.RawMessage(`{}`), MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, _, err = s.Claim(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != dead.ID {
		t.Fatalf("claimed %s", claimed.ID)
	}
	if err := s.Fail(ctx, claimed, job.ErrorTransient, "exhausted", time.Time{}); err != nil {
		t.Fatal(err)
	}
	page, err := s.ListDLQ(ctx, 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 {
		t.Fatalf("dlq entries=%d", len(page.Entries))
	}
	replayed, err := s.ReplayDLQ(ctx, page.Entries[0].ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Status != job.StatusPending || replayed.ReplayedFromJobID != dead.ID {
		t.Fatalf("replayed=%+v", replayed)
	}
	if _, err := s.ReplayDLQ(ctx, page.Entries[0].ID, 2); !errors.Is(err, job.ErrConflict) {
		t.Fatalf("second replay: %v", err)
	}
}

func TestServicePermanentFailureIsNotRetried(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	s := storepg.New(db.pool)
	value, _, err := s.Submit(ctx, job.Submission{Type: "demo", Payload: json.RawMessage(`{}`), MaxAttempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	claimed, found, err := s.Claim(ctx, "worker", time.Minute)
	if err != nil || !found {
		t.Fatalf("claim found=%v err=%v", found, err)
	}
	if err := s.Fail(ctx, claimed, job.ErrorPermanent, "invalid input", time.Time{}); err != nil {
		t.Fatal(err)
	}
	detail, err := s.Get(ctx, value.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Job.Status != job.StatusFailed || detail.Job.Attempts != 1 {
		t.Fatalf("job=%+v", detail.Job)
	}
	if _, found, err := s.Claim(ctx, "worker", time.Minute); err != nil || found {
		t.Fatalf("permanent failure reclaimed: found=%v err=%v", found, err)
	}
}

func TestServiceReclaimsOnlyExpiredLease(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	s := storepg.New(db.pool)
	first, _, _ := s.Submit(ctx, job.Submission{Type: "demo", Payload: json.RawMessage(`{}`), MaxAttempts: 2})
	second, _, _ := s.Submit(ctx, job.Submission{Type: "demo", Payload: json.RawMessage(`{}`), MaxAttempts: 2})
	claimedFirst, _, err := s.Claim(ctx, "worker-one", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claimedSecond, _, err := s.Claim(ctx, "worker-two", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.pool.Exec(ctx, "UPDATE jobs SET locked_until=NOW()-INTERVAL '1 second' WHERE id=$1", claimedFirst.ID)
	if err != nil {
		t.Fatal(err)
	}
	count, err := s.ReclaimExpired(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("reclaimed=%d", count)
	}
	detailFirst, _ := s.Get(ctx, first.ID)
	detailSecond, _ := s.Get(ctx, second.ID)
	statuses := map[string]job.Status{detailFirst.Job.ID: detailFirst.Job.Status, detailSecond.Job.ID: detailSecond.Job.Status}
	if statuses[claimedFirst.ID] != job.StatusPending || statuses[claimedSecond.ID] != job.StatusRunning {
		t.Fatalf("statuses=%v", statuses)
	}
}

func (smokeRegistry) LatestVersion(context.Context, string) (string, error) {
	return "", errors.New("latest lookup not configured in audit test")
}
