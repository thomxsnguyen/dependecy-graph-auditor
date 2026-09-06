package job

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type ErrorKind string

const (
	ErrorTransient ErrorKind = "transient"
	ErrorPermanent ErrorKind = "permanent"
	ErrorCancelled ErrorKind = "cancelled"
)

var (
	ErrNotFound            = errors.New("job not found")
	ErrConflict            = errors.New("job state conflict")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different input")
	ErrLeaseLost           = errors.New("job lease lost")
)

type Submission struct {
	Type              string
	Payload           json.RawMessage
	MaxAttempts       int
	IdempotencyKey    string
	RequestHash       string
	RootJobID         string
	ParentJobID       string
	Internal          bool
	RetriedFromJobID  string
	ReplayedFromJobID string
}

type Attempt struct {
	Attempt    int        `json:"attempt"`
	WorkerID   string     `json:"workerId"`
	Status     string     `json:"status"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	ErrorKind  ErrorKind  `json:"errorKind,omitempty"`
	Error      string     `json:"error,omitempty"`
}

type Event struct {
	ID         int64           `json:"id"`
	Type       string          `json:"type"`
	Attempt    int             `json:"attempt,omitempty"`
	WorkerID   string          `json:"workerId,omitempty"`
	Details    json.RawMessage `json:"details,omitempty"`
	OccurredAt time.Time       `json:"occurredAt"`
}

type Detail struct {
	Job                Job                 `json:"job"`
	Attempts           []Attempt           `json:"attempts"`
	Events             []Event             `json:"events"`
	Result             json.RawMessage     `json:"result,omitempty"`
	ChildCounts        map[Status]int      `json:"childCounts,omitempty"`
	UpdateResults      []UpdateResult      `json:"updateResults,omitempty"`
	AuditResults       []AuditResult       `json:"auditResults,omitempty"`
	AuditRelationships []AuditRelationship `json:"auditRelationships,omitempty"`
}

// UpdateResult is a stored direct dependency check exposed on its scan root.
type UpdateResult struct {
	JobID          string `json:"jobId"`
	Ecosystem      string `json:"ecosystem"`
	Name           string `json:"name"`
	CurrentVersion string `json:"currentVersion"`
	LatestVersion  string `json:"latestVersion"`
	Staleness      string `json:"staleness"`
}

type AuditRelationship struct {
	Ecosystem     string `json:"ecosystem"`
	ParentName    string `json:"parentName"`
	ParentVersion string `json:"parentVersion,omitempty"`
	ChildName     string `json:"childName"`
	ChildVersion  string `json:"childVersion"`
}

type AuditResult struct {
	Ecosystem     string `json:"ecosystem"`
	Name          string `json:"name"`
	Version       string `json:"version"`
	License       string `json:"license"`
	Verdict       string `json:"verdict"`
	ParentName    string `json:"parentName,omitempty"`
	ParentVersion string `json:"parentVersion,omitempty"`
}

type ListFilter struct {
	Status Status
	Type   string
	Query  string
	Limit  int
	Cursor string
}

type ListPage struct {
	Jobs       []Job  `json:"jobs"`
	NextCursor string `json:"nextCursor,omitempty"`
	Counts     Counts `json:"counts"`
}

type Counts map[Status]int

type MetricKey struct {
	JobType string
	Outcome string
}

type Metrics struct {
	Counts                  Counts
	Submissions             int64
	Attempts                map[MetricKey]int64
	AverageQueueWaitSeconds float64
	AverageHandlerSeconds   float64
	Retries                 int64
	LeaseExpirations        int64
	DLQEntries              int64
	DLQReplays              int64
	WorkersFresh            int64
}

type DLQEntry struct {
	ID              int64           `json:"id"`
	JobID           string          `json:"jobId"`
	JobType         string          `json:"jobType"`
	Payload         json.RawMessage `json:"payload"`
	Attempts        int             `json:"attempts"`
	ErrorKind       ErrorKind       `json:"errorKind"`
	Error           string          `json:"error"`
	DeadAt          time.Time       `json:"deadAt"`
	ReplayedAt      *time.Time      `json:"replayedAt,omitempty"`
	ReplayedAsJobID string          `json:"replayedAsJobId,omitempty"`
}

type DLQPage struct {
	Entries    []DLQEntry `json:"entries"`
	NextCursor string     `json:"nextCursor,omitempty"`
}

type HandlerResult struct {
	Result   json.RawMessage
	Children []Submission
}

type ServiceHandler interface {
	Handle(context.Context, Job) (HandlerResult, error)
}

type ClassifiedError struct {
	Kind ErrorKind
	Err  error
}

func (e *ClassifiedError) Error() string { return e.Err.Error() }
func (e *ClassifiedError) Unwrap() error { return e.Err }

func Failure(kind ErrorKind, err error) error {
	if err == nil {
		return nil
	}
	return &ClassifiedError{Kind: kind, Err: err}
}

func KindOf(err error) ErrorKind {
	var classified *ClassifiedError
	if errors.As(err, &classified) {
		return classified.Kind
	}
	if errors.Is(err, context.Canceled) {
		return ErrorCancelled
	}
	return ErrorPermanent
}

type ServiceStore interface {
	Submit(context.Context, Submission) (Job, bool, error)
	Get(context.Context, string) (Detail, error)
	List(context.Context, ListFilter) (ListPage, error)
	Counts(context.Context) (Counts, error)
	Metrics(context.Context) (Metrics, error)
	Cancel(context.Context, string) (Job, error)
	Retry(context.Context, string, int) (Job, error)
	ListDLQ(context.Context, int, string) (DLQPage, error)
	ReplayDLQ(context.Context, int64, int) (Job, error)

	RegisterWorker(context.Context, string) error
	WorkerHeartbeat(context.Context, string) error
	Claim(context.Context, string, time.Duration) (Job, bool, error)
	Heartbeat(context.Context, Job, time.Duration) (bool, error)
	Complete(context.Context, Job, HandlerResult) error
	Fail(context.Context, Job, ErrorKind, string, time.Time) error
	ReclaimExpired(context.Context) (int, error)
}
