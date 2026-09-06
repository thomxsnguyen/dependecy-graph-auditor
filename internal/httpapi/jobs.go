package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	githubsource "github.com/thomxsnguyen/mini-distributed-job-api/internal/github"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
)

const maxJobRequestBytes = 64 << 10

type JobAPI struct {
	store job.ServiceStore
	ready func(context.Context) error
}

func NewJobAPI(store job.ServiceStore, ready func(context.Context) error) *JobAPI {
	return &JobAPI{store: store, ready: ready}
}

func (api *JobAPI) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", api.health)
	mux.HandleFunc("GET /ready", api.readiness)
	mux.HandleFunc("GET /metrics", api.metrics)
	mux.HandleFunc("POST /api/jobs", api.submit)
	mux.HandleFunc("GET /api/jobs", api.list)
	mux.HandleFunc("GET /api/jobs/{id}", api.get)
	mux.HandleFunc("POST /api/jobs/{id}/cancel", api.cancel)
	mux.HandleFunc("POST /api/jobs/{id}/retry", api.retry)
	mux.HandleFunc("GET /api/dlq", api.listDLQ)
	mux.HandleFunc("POST /api/dlq/{id}/replay", api.replayDLQ)
	return mux
}

type submitRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"maxAttempts,omitempty"`
}

type retryRequest struct {
	MaxAttempts int `json:"maxAttempts,omitempty"`
}

func decodeBody(writer http.ResponseWriter, request *http.Request, output any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxJobRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("request must contain one JSON value")
	}
	return nil
}

func validateSubmission(input submitRequest) error {
	if input.MaxAttempts == 0 {
		input.MaxAttempts = job.DefaultMaxAttempts
	}
	if input.MaxAttempts < 1 || input.MaxAttempts > 20 {
		return errors.New("maxAttempts must be between 1 and 20")
	}
	switch input.Type {
	case "demo":
		var payload struct {
			DurationMS        int             `json:"durationMs"`
			TransientFailures int             `json:"transientFailures"`
			PermanentFailure  bool            `json:"permanentFailure"`
			Result            json.RawMessage `json:"result"`
		}
		if len(input.Payload) == 0 || json.Unmarshal(input.Payload, &payload) != nil {
			return errors.New("payload must be a valid demo object")
		}
		if payload.DurationMS < 0 || payload.DurationMS > 120000 || payload.TransientFailures < 0 || payload.TransientFailures > 20 {
			return errors.New("demo controls are outside allowed bounds")
		}
	case "dependency_audit", "dependency_update_scan":
		var payload struct {
			RepositoryURL string `json:"repositoryUrl"`
			Ref           string `json:"ref"`
		}
		if len(input.Payload) == 0 || json.Unmarshal(input.Payload, &payload) != nil {
			if input.Type == "dependency_update_scan" {
				return errors.New("payload must be a valid dependency update scan object")
			}
			return errors.New("payload must be a valid dependency audit object")
		}
		if _, err := githubsource.ParseRepositoryURL(strings.TrimSpace(payload.RepositoryURL)); err != nil {
			return errors.New("repositoryUrl must be https://github.com/owner/repository")
		}
	default:
		return errors.New("type must be demo, dependency_audit or dependency_update_scan")
	}
	return nil
}

func requestHash(input submitRequest) string {
	var payload any
	canonical := input.Payload
	if json.Unmarshal(input.Payload, &payload) == nil {
		if encoded, err := json.Marshal(payload); err == nil {
			canonical = encoded
		}
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", input.Type, input.MaxAttempts, canonical)))
	return hex.EncodeToString(sum[:])
}

func (api *JobAPI) submit(writer http.ResponseWriter, request *http.Request) {
	var input submitRequest
	if err := decodeBody(writer, request, &input); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_request", "Enter one valid JSON request object.")
		return
	}
	if input.MaxAttempts == 0 {
		input.MaxAttempts = job.DefaultMaxAttempts
	}
	if err := validateSubmission(input); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_job", err.Error())
		return
	}
	idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) > 128 {
		writeAPIError(writer, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key must be at most 128 characters.")
		return
	}
	value, created, err := api.store.Submit(request.Context(), job.Submission{Type: input.Type, Payload: input.Payload, MaxAttempts: input.MaxAttempts,
		IdempotencyKey: idempotencyKey, RequestHash: requestHash(input)})
	if errors.Is(err, job.ErrIdempotencyConflict) {
		writeAPIError(writer, http.StatusConflict, "idempotency_conflict", err.Error())
		return
	}
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, "internal_error", "The job could not be submitted.")
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	writeJSON(writer, status, map[string]any{"job": value})
}

func (api *JobAPI) list(writer http.ResponseWriter, request *http.Request) {
	limit, _ := strconv.Atoi(request.URL.Query().Get("limit"))
	status := job.Status(request.URL.Query().Get("status"))
	if status != "" && !validStatus(status) {
		writeAPIError(writer, http.StatusBadRequest, "invalid_query", "The job status is invalid.")
		return
	}
	query := request.URL.Query().Get("q")
	if len(query) > 128 {
		writeAPIError(writer, http.StatusBadRequest, "invalid_query", "The search query must be at most 128 characters.")
		return
	}
	page, err := api.store.List(request.Context(), job.ListFilter{Status: status, Type: request.URL.Query().Get("type"), Query: query, Limit: limit, Cursor: request.URL.Query().Get("cursor")})
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_query", "The job filters or cursor are invalid.")
		return
	}
	writeJSON(writer, http.StatusOK, page)
}

func (api *JobAPI) get(writer http.ResponseWriter, request *http.Request) {
	detail, err := api.store.Get(request.Context(), request.PathValue("id"))
	if handleStoreError(writer, err) {
		return
	}
	writeJSON(writer, http.StatusOK, detail)
}

func (api *JobAPI) cancel(writer http.ResponseWriter, request *http.Request) {
	value, err := api.store.Cancel(request.Context(), request.PathValue("id"))
	if handleStoreError(writer, err) {
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"job": value})
}

func (api *JobAPI) retry(writer http.ResponseWriter, request *http.Request) {
	input := retryRequest{}
	if request.ContentLength != 0 {
		if err := decodeBody(writer, request, &input); err != nil {
			writeAPIError(writer, http.StatusBadRequest, "invalid_request", "Enter one valid JSON request object.")
			return
		}
	}
	if input.MaxAttempts < 0 || input.MaxAttempts > 20 {
		writeAPIError(writer, http.StatusBadRequest, "invalid_job", "maxAttempts must be between 1 and 20.")
		return
	}
	value, err := api.store.Retry(request.Context(), request.PathValue("id"), input.MaxAttempts)
	if handleStoreError(writer, err) {
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]any{"job": value})
}

func (api *JobAPI) listDLQ(writer http.ResponseWriter, request *http.Request) {
	limit, _ := strconv.Atoi(request.URL.Query().Get("limit"))
	page, err := api.store.ListDLQ(request.Context(), limit, request.URL.Query().Get("cursor"))
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_query", "The DLQ cursor is invalid.")
		return
	}
	writeJSON(writer, http.StatusOK, page)
}

func (api *JobAPI) replayDLQ(writer http.ResponseWriter, request *http.Request) {
	id, err := strconv.ParseInt(request.PathValue("id"), 10, 64)
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_request", "The DLQ entry ID is invalid.")
		return
	}
	input := retryRequest{}
	if request.ContentLength != 0 {
		if err := decodeBody(writer, request, &input); err != nil {
			writeAPIError(writer, http.StatusBadRequest, "invalid_request", "Enter one valid JSON request object.")
			return
		}
	}
	if input.MaxAttempts < 0 || input.MaxAttempts > 20 {
		writeAPIError(writer, http.StatusBadRequest, "invalid_job", "maxAttempts must be between 1 and 20.")
		return
	}
	value, err := api.store.ReplayDLQ(request.Context(), id, input.MaxAttempts)
	if handleStoreError(writer, err) {
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]any{"job": value})
}

func validStatus(status job.Status) bool {
	switch status {
	case job.StatusPending, job.StatusRunning, job.StatusWaiting, job.StatusRetryScheduled,
		job.StatusCompleted, job.StatusFailed, job.StatusDeadLettered, job.StatusCancelled:
		return true
	default:
		return false
	}
}

func (api *JobAPI) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}
func (api *JobAPI) readiness(writer http.ResponseWriter, request *http.Request) {
	if api.ready != nil && api.ready(request.Context()) != nil {
		writeAPIError(writer, http.StatusServiceUnavailable, "not_ready", "PostgreSQL is unavailable.")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
}
func (api *JobAPI) metrics(writer http.ResponseWriter, request *http.Request) {
	metrics, err := api.store.Metrics(request.Context())
	if err != nil {
		writeAPIError(writer, http.StatusServiceUnavailable, "metrics_unavailable", "Metrics are unavailable.")
		return
	}
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, status := range []job.Status{job.StatusPending, job.StatusRunning, job.StatusWaiting, job.StatusRetryScheduled, job.StatusCompleted, job.StatusFailed, job.StatusDeadLettered, job.StatusCancelled} {
		_, _ = fmt.Fprintf(writer, "job_queue_jobs{status=%q} %d\n", status, metrics.Counts[status])
	}
	_, _ = fmt.Fprintf(writer, "job_queue_submissions_total %d\n", metrics.Submissions)
	for key, count := range metrics.Attempts {
		_, _ = fmt.Fprintf(writer, "job_queue_attempts_total{type=%q,outcome=%q} %d\n", key.JobType, key.Outcome, count)
	}
	_, _ = fmt.Fprintf(writer, "job_queue_wait_seconds_avg %g\n", metrics.AverageQueueWaitSeconds)
	_, _ = fmt.Fprintf(writer, "job_queue_handler_seconds_avg %g\n", metrics.AverageHandlerSeconds)
	_, _ = fmt.Fprintf(writer, "job_queue_retries_total %d\n", metrics.Retries)
	_, _ = fmt.Fprintf(writer, "job_queue_lease_expirations_total %d\n", metrics.LeaseExpirations)
	_, _ = fmt.Fprintf(writer, "job_queue_dlq_entries_total %d\n", metrics.DLQEntries)
	_, _ = fmt.Fprintf(writer, "job_queue_dlq_replays_total %d\n", metrics.DLQReplays)
	_, _ = fmt.Fprintf(writer, "job_queue_workers_fresh %d\n", metrics.WorkersFresh)
}

func handleStoreError(writer http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, job.ErrNotFound) {
		writeAPIError(writer, http.StatusNotFound, "job_not_found", "The requested resource does not exist.")
		return true
	}
	if errors.Is(err, job.ErrConflict) {
		writeAPIError(writer, http.StatusConflict, "job_conflict", "The requested transition is not allowed.")
		return true
	}
	writeAPIError(writer, http.StatusInternalServerError, "internal_error", "The request could not be completed.")
	return true
}
func writeAPIError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
