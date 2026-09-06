package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/auditor"
	githubsource "github.com/thomxsnguyen/mini-distributed-job-api/internal/github"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/gomod"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/handlers"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/pypi"
	storepg "github.com/thomxsnguyen/mini-distributed-job-api/internal/store/postgres"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/worker"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		hostname, _ := os.Hostname()
		workerID = hostname + "-" + uuid.NewString()[:8]
	}
	concurrency := 10
	if raw := os.Getenv("WORKER_CONCURRENCY"); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			concurrency = value
		}
	}
	store := storepg.New(pool)
	githubClient := &githubsource.GitHubClient{Token: os.Getenv("GITHUB_TOKEN")}
	handlers := map[string]job.ServiceHandler{
		"demo":                   worker.DemoHandler{},
		"dependency_update_scan": handlers.DependencyUpdateScanHandler{GitHub: githubClient},
		"package_update_check": handlers.PackageUpdateCheckHandler{Registries: handlers.UpdateCheckRegistries{
			NPM: auditor.NewNpmClient(), PyPI: pypi.NewClient(mustPythonTarget()), Go: gomod.NewClient(),
		}},
		"dependency_audit":   handlers.DependencyAuditHandler{GitHub: githubClient},
		"audit_npm_package":  auditor.AuditPackageServiceHandler{Registry: auditor.NewNpmClient(), Policy: auditor.LicensePolicy{}, JobType: "audit_npm_package", Ecosystem: "npm"},
		"audit_pypi_package": auditor.AuditPackageServiceHandler{Registry: pypi.NewClient(mustPythonTarget()), Policy: auditor.LicensePolicy{}, JobType: "audit_pypi_package", Ecosystem: "pypi"},
		"audit_go_module":    handlers.GoModuleServiceHandler{Client: gomod.NewClient()},
	}
	service := worker.NewService(store, workerID, handlers, worker.ServiceOptions{
		Concurrency: concurrency, LeaseDuration: durationEnv("WORKER_LEASE_DURATION", 30*time.Second),
		HeartbeatEvery:   durationEnv("WORKER_HEARTBEAT_INTERVAL", 10*time.Second),
		RecoveryInterval: durationEnv("WORKER_RECOVERY_INTERVAL", 5*time.Second),
		PollInterval:     durationEnv("WORKER_POLL_INTERVAL", 250*time.Millisecond),
		ShutdownTimeout:  durationEnv("SHUTDOWN_TIMEOUT", 25*time.Second),
	})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("worker %s starting with concurrency %d", workerID, concurrency)
	if err := service.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		log.Fatalf("%s must be a positive duration", name)
	}
	return value
}

func mustPythonTarget() pypi.Target {
	target, err := pypi.NewTarget("3.12", "linux")
	if err != nil {
		panic(err)
	}
	return target
}
