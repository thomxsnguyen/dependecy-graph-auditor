package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
)

type apiStoreStub struct {
	job.ServiceStore
	submitted job.Submission
	detail    job.Detail
}

func (s *apiStoreStub) Submit(_ context.Context, input job.Submission) (job.Job, bool, error) {
	s.submitted = input
	return job.Job{ID: "job-1", Type: input.Type, Status: job.StatusPending, MaxAttempts: input.MaxAttempts}, true, nil
}

func (s *apiStoreStub) Metrics(context.Context) (job.Metrics, error) {
	return job.Metrics{Counts: job.Counts{job.StatusPending: 2}, Attempts: map[job.MetricKey]int64{}}, nil
}

func TestSubmitJobReturnsAccepted(t *testing.T) {
	store := &apiStoreStub{}
	request := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewBufferString(`{"type":"demo","payload":{"durationMs":0}}`))
	request.Header.Set("Idempotency-Key", "demo-one")
	recorder := httptest.NewRecorder()
	NewJobAPI(store, nil).Routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if store.submitted.Type != "demo" || store.submitted.IdempotencyKey != "demo-one" || store.submitted.MaxAttempts != job.DefaultMaxAttempts {
		t.Fatalf("submission=%+v", store.submitted)
	}
}

func TestSubmitRejectsUnsupportedJob(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewBufferString(`{"type":"unknown","payload":{}}`))
	recorder := httptest.NewRecorder()
	NewJobAPI(&apiStoreStub{}, nil).Routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestMetricsUsesBoundedLabels(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	NewJobAPI(&apiStoreStub{}, nil).Routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `job_queue_jobs{status="pending"} 2`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestSubmitDependencyUpdateScan(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"valid", `{"type":"dependency_update_scan","payload":{"repositoryUrl":"https://github.com/acme/service","ref":"main"}}`, http.StatusAccepted},
		{"bad repository", `{"type":"dependency_update_scan","payload":{"repositoryUrl":"https://example.com/acme/service"}}`, http.StatusBadRequest},
		{"missing payload", `{"type":"dependency_update_scan"}`, http.StatusBadRequest},
		{"internal child", `{"type":"package_update_check","payload":{"ecosystem":"npm","name":"demo","versionRange":"1.0.0"}}`, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &apiStoreStub{}
			request := httptest.NewRequest(http.MethodPost, "/api/jobs", strings.NewReader(tc.body))
			request.Header.Set("Idempotency-Key", "update-test")
			recorder := httptest.NewRecorder()
			NewJobAPI(store, nil).Routes().ServeHTTP(recorder, request)
			if recorder.Code != tc.status {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if tc.status == http.StatusAccepted && (store.submitted.Type != "dependency_update_scan" || store.submitted.Internal || store.submitted.IdempotencyKey != "update-test") {
				t.Fatalf("submission=%+v", store.submitted)
			}
			if tc.status != http.StatusAccepted && store.submitted.Type != "" {
				t.Fatal("invalid job reached store")
			}
		})
	}
}

func (s *apiStoreStub) Get(context.Context, string) (job.Detail, error) { return s.detail, nil }

func TestJobDetailSerializesUpdateResults(t *testing.T) {
	store := &apiStoreStub{detail: job.Detail{
		Job:    job.Job{ID: "root", RootJobID: "root", Type: "dependency_update_scan", Status: job.StatusWaiting},
		Result: json.RawMessage(`{"dependencyCount":2}`), ChildCounts: map[job.Status]int{job.StatusCompleted: 1, job.StatusPending: 1},
		AuditResults:  []job.AuditResult{{Name: "existing-audit-field"}},
		UpdateResults: []job.UpdateResult{{JobID: "child", Ecosystem: "npm", Name: "demo", CurrentVersion: "1.0.0", LatestVersion: "2.0.0", Staleness: "major"}},
	}}
	response := httptest.NewRecorder()
	NewJobAPI(store, nil).Routes().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/jobs/root", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d", response.Code)
	}
	var body struct {
		Job           job.Job             `json:"job"`
		Result        json.RawMessage     `json:"result"`
		ChildCounts   map[string]int      `json:"childCounts"`
		AuditResults  []job.AuditResult   `json:"auditResults"`
		UpdateResults []map[string]string `json:"updateResults"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Job.ID != "root" || len(body.Result) == 0 || body.ChildCounts["completed"] != 1 || len(body.AuditResults) != 1 || len(body.UpdateResults) != 1 {
		t.Fatalf("response=%s", response.Body.String())
	}
	for key, want := range map[string]string{"jobId": "child", "ecosystem": "npm", "name": "demo", "currentVersion": "1.0.0", "latestVersion": "2.0.0", "staleness": "major"} {
		if body.UpdateResults[0][key] != want {
			t.Fatalf("field %s=%q want=%q", key, body.UpdateResults[0][key], want)
		}
	}
}
