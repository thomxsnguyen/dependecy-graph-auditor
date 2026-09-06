package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/thomxsnguyen/mini-distributed-job-api/internal/job"
)

const serviceJobColumns = `id, type, payload, status, attempts, max_attempts,
	scheduled_at, root_job_id, COALESCE(parent_job_id, ''), internal,
	COALESCE(idempotency_key, ''), COALESCE(request_hash, ''), COALESCE(locked_by, ''),
	COALESCE(lease_token, ''), locked_until, heartbeat_at, cancel_requested_at,
	COALESCE(last_error_kind, ''), COALESCE(last_error, ''), created_at, started_at,
	completed_at, COALESCE(retried_from_job_id, ''), COALESCE(replayed_from_job_id, '')`

type scanner interface{ Scan(...any) error }

func scanServiceJob(row scanner) (job.Job, error) {
	var value job.Job
	var payload []byte
	err := row.Scan(
		&value.ID, &value.Type, &payload, &value.Status, &value.Attempts,
		&value.MaxAttempts, &value.ScheduledAt, &value.RootJobID,
		&value.ParentJobID, &value.Internal, &value.IdempotencyKey,
		&value.RequestHash, &value.LockedBy, &value.LeaseToken,
		&value.LockedUntil, &value.HeartbeatAt, &value.CancelRequestedAt,
		&value.LastErrorKind, &value.LastError, &value.CreatedAt,
		&value.StartedAt, &value.CompletedAt, &value.RetriedFromJobID,
		&value.ReplayedFromJobID,
	)
	value.Payload = payload
	return value, err
}

func submissionHash(value job.Submission) string {
	if value.RequestHash != "" {
		return value.RequestHash
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", value.Type, value.MaxAttempts, value.Payload)))
	return hex.EncodeToString(sum[:])
}

func (s *Store) Submit(ctx context.Context, input job.Submission) (job.Job, bool, error) {
	if input.MaxAttempts == 0 {
		input.MaxAttempts = job.DefaultMaxAttempts
	}
	id := job.NewJobID()
	rootID := input.RootJobID
	if rootID == "" {
		rootID = id
	}
	requestHash := submissionHash(input)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return job.Job{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
		INSERT INTO jobs (
			id, type, payload, status, attempts, max_attempts, scheduled_at,
			root_job_id, parent_job_id, internal, idempotency_key, request_hash,
			retried_from_job_id, replayed_from_job_id
		) VALUES ($1, $2, $3::jsonb, $4, 0, $5, NOW(), $6, NULLIF($7, ''), $8,
			NULLIF($9, ''), $10, NULLIF($11, ''), NULLIF($12, ''))
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
	`, id, input.Type, jsonValue(input.Payload), job.StatusPending, input.MaxAttempts,
		rootID, input.ParentJobID, input.Internal, input.IdempotencyKey, requestHash,
		input.RetriedFromJobID, input.ReplayedFromJobID)
	if err != nil {
		return job.Job{}, false, fmt.Errorf("submit job: %w", err)
	}
	created := tag.RowsAffected() == 1
	if !created {
		existing, err := scanServiceJob(tx.QueryRow(ctx,
			"SELECT "+serviceJobColumns+" FROM jobs WHERE idempotency_key=$1", input.IdempotencyKey))
		if err != nil {
			return job.Job{}, false, fmt.Errorf("load idempotent job: %w", err)
		}
		if existing.RequestHash != requestHash {
			return job.Job{}, false, job.ErrIdempotencyConflict
		}
		return existing, false, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO job_events (job_id, event_type, details)
		VALUES ($1, 'submitted', '{}'::jsonb)`, id); err != nil {
		return job.Job{}, false, err
	}
	createdJob, err := scanServiceJob(tx.QueryRow(ctx,
		"SELECT "+serviceJobColumns+" FROM jobs WHERE id=$1", id))
	if err != nil {
		return job.Job{}, false, err
	}
	return createdJob, true, tx.Commit(ctx)
}

func (s *Store) Get(ctx context.Context, id string) (job.Detail, error) {
	value, err := scanServiceJob(s.pool.QueryRow(ctx,
		"SELECT "+serviceJobColumns+" FROM jobs WHERE id=$1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Detail{}, job.ErrNotFound
	}
	if err != nil {
		return job.Detail{}, err
	}
	detail := job.Detail{Job: value, Attempts: []job.Attempt{}, Events: []job.Event{}}
	rows, err := s.pool.Query(ctx, `SELECT attempt, worker_id, status, started_at,
		finished_at, COALESCE(error_kind, ''), COALESCE(error, '')
		FROM job_attempts WHERE job_id=$1 ORDER BY attempt`, id)
	if err != nil {
		return job.Detail{}, err
	}
	for rows.Next() {
		var attempt job.Attempt
		if err := rows.Scan(&attempt.Attempt, &attempt.WorkerID, &attempt.Status,
			&attempt.StartedAt, &attempt.FinishedAt, &attempt.ErrorKind, &attempt.Error); err != nil {
			rows.Close()
			return job.Detail{}, err
		}
		detail.Attempts = append(detail.Attempts, attempt)
	}
	rows.Close()
	rows, err = s.pool.Query(ctx, `SELECT id, event_type, COALESCE(attempt, 0),
		COALESCE(worker_id, ''), COALESCE(details, '{}'::jsonb), occurred_at
		FROM job_events WHERE job_id=$1 ORDER BY occurred_at, id`, id)
	if err != nil {
		return job.Detail{}, err
	}
	for rows.Next() {
		var event job.Event
		var details []byte
		if err := rows.Scan(&event.ID, &event.Type, &event.Attempt, &event.WorkerID,
			&details, &event.OccurredAt); err != nil {
			rows.Close()
			return job.Detail{}, err
		}
		event.Details = details
		detail.Events = append(detail.Events, event)
	}
	rows.Close()
	var result []byte
	if err := s.pool.QueryRow(ctx, `SELECT result FROM job_results WHERE job_id=$1`, id).Scan(&result); err == nil {
		detail.Result = result
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return job.Detail{}, err
	}
	detail.ChildCounts = map[job.Status]int{}
	rows, err = s.pool.Query(ctx, `SELECT status, COUNT(*) FROM jobs
		WHERE root_job_id=$1 AND id<>$1 GROUP BY status`, value.RootJobID)
	if err != nil {
		return job.Detail{}, err
	}
	for rows.Next() {
		var status job.Status
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return job.Detail{}, err
		}
		detail.ChildCounts[status] = count
	}
	rows.Close()
	detail.AuditResults = []job.AuditResult{}
	rows, err = s.pool.Query(ctx, `SELECT ecosystem,package_name,package_version,license,verdict,
		COALESCE(parent_name,''),COALESCE(parent_version,'') FROM audit_results
		WHERE root_job_id=$1 ORDER BY package_name,package_version,parent_name`, value.RootJobID)
	if err != nil {
		return job.Detail{}, err
	}
	for rows.Next() {
		var result job.AuditResult
		if err := rows.Scan(&result.Ecosystem, &result.Name, &result.Version, &result.License, &result.Verdict,
			&result.ParentName, &result.ParentVersion); err != nil {
			rows.Close()
			return job.Detail{}, err
		}
		detail.AuditResults = append(detail.AuditResults, result)
	}
	rows.Close()
	detail.AuditRelationships = []job.AuditRelationship{}
	rows, err = s.pool.Query(ctx, `SELECT ecosystem,parent_name,parent_version,child_name,child_version
		FROM audit_relationships WHERE root_job_id=$1 ORDER BY ecosystem,parent_name,child_name`, value.RootJobID)
	if err != nil {
		return job.Detail{}, err
	}
	for rows.Next() {
		var relationship job.AuditRelationship
		if err := rows.Scan(&relationship.Ecosystem, &relationship.ParentName, &relationship.ParentVersion,
			&relationship.ChildName, &relationship.ChildVersion); err != nil {
			rows.Close()
			return job.Detail{}, err
		}
		detail.AuditRelationships = append(detail.AuditRelationships, relationship)
	}
	rows.Close()
	return detail, nil
}

func encodeCursor(created time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(created.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", job.ErrConflict
	}
	parts := strings.SplitN(string(data), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", job.ErrConflict
	}
	created, err := time.Parse(time.RFC3339Nano, parts[0])
	return created, parts[1], err
}

func (s *Store) List(ctx context.Context, filter job.ListFilter) (job.ListPage, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	query := "SELECT " + serviceJobColumns + " FROM jobs WHERE internal=FALSE"
	args := []any{}
	add := func(clause string, value any) {
		args = append(args, value)
		query += fmt.Sprintf(clause, len(args))
	}
	if filter.Status != "" {
		add(" AND status=$%d", filter.Status)
	}
	if filter.Type != "" {
		add(" AND type=$%d", filter.Type)
	}
	if filter.Query != "" {
		args = append(args, filter.Query)
		position := len(args)
		query += fmt.Sprintf(" AND (id ILIKE '%%' || $%d || '%%' OR type ILIKE '%%' || $%d || '%%')", position, position)
	}
	if filter.Cursor != "" {
		created, id, err := decodeCursor(filter.Cursor)
		if err != nil {
			return job.ListPage{}, err
		}
		args = append(args, created, id)
		query += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return job.ListPage{}, err
	}
	defer rows.Close()
	page := job.ListPage{Jobs: []job.Job{}}
	for rows.Next() {
		value, err := scanServiceJob(rows)
		if err != nil {
			return job.ListPage{}, err
		}
		page.Jobs = append(page.Jobs, value)
	}
	if len(page.Jobs) > limit {
		last := page.Jobs[limit-1]
		page.NextCursor = encodeCursor(last.CreatedAt, last.ID)
		page.Jobs = page.Jobs[:limit]
	}
	if err := rows.Err(); err != nil {
		return job.ListPage{}, err
	}
	rows.Close()
	page.Counts, err = s.Counts(ctx)
	if err != nil {
		return job.ListPage{}, err
	}
	return page, nil
}

func (s *Store) Counts(ctx context.Context) (job.Counts, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, COUNT(*) FROM jobs WHERE internal=FALSE GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := job.Counts{}
	for rows.Next() {
		var status job.Status
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	return counts, rows.Err()
}

func (s *Store) Metrics(ctx context.Context) (job.Metrics, error) {
	counts, err := s.Counts(ctx)
	if err != nil {
		return job.Metrics{}, err
	}
	metrics := job.Metrics{Counts: counts, Attempts: map[job.MetricKey]int64{}}
	err = s.pool.QueryRow(ctx, `SELECT
		(SELECT COUNT(*) FROM job_events WHERE event_type='submitted'),
		COALESCE((SELECT AVG(EXTRACT(EPOCH FROM (started_at-created_at))) FROM jobs WHERE started_at IS NOT NULL),0),
		COALESCE((SELECT AVG(EXTRACT(EPOCH FROM (finished_at-started_at))) FROM job_attempts WHERE finished_at IS NOT NULL),0),
		(SELECT COUNT(*) FROM job_events WHERE event_type='retry_scheduled'),
		(SELECT COUNT(*) FROM job_events WHERE event_type='lease_expired'),
		(SELECT COUNT(*) FROM dlq d JOIN jobs j ON j.id=d.job_id WHERE j.internal=FALSE),
		(SELECT COUNT(*) FROM dlq d JOIN jobs j ON j.id=d.job_id WHERE j.internal=FALSE AND d.replayed_at IS NOT NULL),
		(SELECT COUNT(*) FROM workers WHERE heartbeat_at > NOW()-INTERVAL '30 seconds')`).Scan(
		&metrics.Submissions, &metrics.AverageQueueWaitSeconds, &metrics.AverageHandlerSeconds,
		&metrics.Retries, &metrics.LeaseExpirations, &metrics.DLQEntries, &metrics.DLQReplays,
		&metrics.WorkersFresh)
	if err != nil {
		return job.Metrics{}, err
	}
	rows, err := s.pool.Query(ctx, `SELECT j.type,a.status,COUNT(*) FROM job_attempts a
		JOIN jobs j ON j.id=a.job_id GROUP BY j.type,a.status`)
	if err != nil {
		return job.Metrics{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var key job.MetricKey
		var count int64
		if err := rows.Scan(&key.JobType, &key.Outcome, &count); err != nil {
			return job.Metrics{}, err
		}
		metrics.Attempts[key] = count
	}
	return metrics, rows.Err()
}

func (s *Store) RegisterWorker(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO workers (id) VALUES ($1)
		ON CONFLICT (id) DO UPDATE SET started_at=NOW(), heartbeat_at=NOW()`, id)
	return err
}

func (s *Store) WorkerHeartbeat(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE workers SET heartbeat_at=NOW() WHERE id=$1`, id)
	return err
}

func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (job.Job, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return job.Job{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	token := job.NewJobID()
	value, err := scanServiceJob(tx.QueryRow(ctx, `
		UPDATE jobs SET status=$3, attempts=attempts+1,
			started_at=COALESCE(started_at, NOW()), locked_by=$4,
			lease_token=$5, locked_until=NOW()+make_interval(secs => $6), heartbeat_at=NOW()
		WHERE id=(
			SELECT id FROM jobs
			WHERE status IN ($1, $2) AND scheduled_at <= NOW()
			ORDER BY scheduled_at, created_at, id
			FOR UPDATE SKIP LOCKED LIMIT 1
		)
		RETURNING `+serviceJobColumns,
		job.StatusPending, job.StatusRetryScheduled, job.StatusRunning,
		workerID, token, lease.Seconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, false, nil
	}
	if err != nil {
		return job.Job{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO job_attempts
		(job_id, attempt, worker_id, lease_token, status) VALUES ($1,$2,$3,$4,'running')`,
		value.ID, value.Attempts, workerID, token); err != nil {
		return job.Job{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO job_events
		(job_id,event_type,attempt,worker_id,details) VALUES ($1,'started',$2,$3,'{}')`,
		value.ID, value.Attempts, workerID); err != nil {
		return job.Job{}, false, err
	}
	return value, true, tx.Commit(ctx)
}

func (s *Store) Heartbeat(ctx context.Context, value job.Job, lease time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE jobs SET locked_until=NOW()+make_interval(secs => $1),
		heartbeat_at=NOW() WHERE id=$2 AND status=$3 AND locked_by=$4 AND lease_token=$5
		AND cancel_requested_at IS NULL`,
		lease.Seconds(), value.ID, job.StatusRunning, value.LockedBy, value.LeaseToken)
	return tag.RowsAffected() == 1, err
}

func insertSubmission(ctx context.Context, tx pgx.Tx, input job.Submission) error {
	if input.MaxAttempts == 0 {
		input.MaxAttempts = job.DefaultMaxAttempts
	}
	id := job.NewJobID()
	_, err := tx.Exec(ctx, `INSERT INTO jobs
		(id,type,payload,status,attempts,max_attempts,scheduled_at,root_job_id,parent_job_id,internal,idempotency_key,request_hash)
		VALUES ($1,$2,$3::jsonb,$4,0,$5,NOW(),$6,$7,$8,$9,$10)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
		id, input.Type, jsonValue(input.Payload), job.StatusPending, input.MaxAttempts,
		input.RootJobID, input.ParentJobID, input.Internal, input.IdempotencyKey, submissionHash(input))
	return err
}

func (s *Store) Complete(ctx context.Context, value job.Job, result job.HandlerResult) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	status := job.StatusCompleted
	if len(result.Children) > 0 && value.ID == value.RootJobID {
		status = job.StatusWaiting
	}
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status=$1, locked_by=NULL, lease_token=NULL,
		locked_until=NULL, heartbeat_at=NULL, completed_at=CASE WHEN $1=$2 THEN NOW() ELSE NULL END
		WHERE id=$3 AND status=$4 AND locked_by=$5 AND lease_token=$6`,
		status, job.StatusCompleted, value.ID, job.StatusRunning, value.LockedBy, value.LeaseToken)
	if err != nil || tag.RowsAffected() != 1 {
		if err != nil {
			return err
		}
		return job.ErrLeaseLost
	}
	if _, err := tx.Exec(ctx, `UPDATE job_attempts SET status='completed', finished_at=NOW()
		WHERE job_id=$1 AND attempt=$2 AND lease_token=$3`, value.ID, value.Attempts, value.LeaseToken); err != nil {
		return err
	}
	for _, child := range result.Children {
		child.RootJobID = value.RootJobID
		child.ParentJobID = value.ID
		child.Internal = true
		if err := insertAuditRelationship(ctx, tx, child); err != nil {
			return err
		}
		if err := insertSubmission(ctx, tx, child); err != nil {
			return err
		}
	}
	if len(result.Result) > 0 && string(result.Result) != "null" && (status == job.StatusCompleted || value.Type == "dependency_update_scan") {
		if _, err := tx.Exec(ctx, `INSERT INTO job_results (job_id,result) VALUES ($1,$2::jsonb)
			ON CONFLICT (job_id) DO NOTHING`, value.ID, string(result.Result)); err != nil {
			return err
		}
	}
	if strings.HasPrefix(value.Type, "audit_") && len(result.Result) > 0 {
		var row struct {
			Ecosystem, Name, Version, License, Verdict, ParentName, ParentVersion string
		}
		if json.Unmarshal(result.Result, &row) == nil {
			_, err = tx.Exec(ctx, `INSERT INTO audit_results
				(root_job_id,ecosystem,package_name,package_version,license,verdict,parent_name,parent_version)
				VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''))
				ON CONFLICT DO NOTHING`, value.RootJobID, row.Ecosystem, row.Name, row.Version, row.License,
				row.Verdict, row.ParentName, row.ParentVersion)
			if err != nil {
				return err
			}
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO job_events
		(job_id,event_type,attempt,worker_id,details) VALUES ($1,$2,$3,$4,'{}')`,
		value.ID, string(status), value.Attempts, value.LockedBy); err != nil {
		return err
	}
	if status == job.StatusCompleted && value.ID != value.RootJobID {
		if err := finalizeRoot(ctx, tx, value.RootJobID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func insertAuditRelationship(ctx context.Context, tx pgx.Tx, child job.Submission) error {
	ecosystem := strings.TrimPrefix(child.Type, "audit_")
	ecosystem = strings.TrimSuffix(ecosystem, "_package")
	ecosystem = strings.TrimSuffix(ecosystem, "_module")
	if ecosystem != "npm" && ecosystem != "pypi" && ecosystem != "go" {
		return nil
	}
	var payload struct {
		Name               string `json:"name"`
		Version            string `json:"version"`
		ParentNameSnake    string `json:"parent_name"`
		ParentVersionSnake string `json:"parent_version"`
		ParentNameCamel    string `json:"parentName"`
		ParentVersionCamel string `json:"parentVersion"`
	}
	if err := json.Unmarshal(child.Payload, &payload); err != nil {
		return nil
	}
	parentName := firstNonEmpty(payload.ParentNameSnake, payload.ParentNameCamel)
	parentVersion := firstNonEmpty(payload.ParentVersionSnake, payload.ParentVersionCamel)
	if parentName == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO audit_relationships
		(root_job_id,ecosystem,parent_name,parent_version,child_name,child_version)
		VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, child.RootJobID, ecosystem,
		parentName, parentVersion, payload.Name, payload.Version)
	return err
}

func finalizeRoot(ctx context.Context, tx pgx.Tx, rootID string) error {
	var rootType string
	if err := tx.QueryRow(ctx, `SELECT type FROM jobs WHERE id=$1`, rootID).Scan(&rootType); err != nil {
		return err
	}
	if rootType == "dependency_update_scan" {
		// Serialize update finalizers so simultaneous child completions cannot
		// each see another uncommitted child and leave the root waiting forever.
		var lockedID string
		if err := tx.QueryRow(ctx, `SELECT id FROM jobs WHERE id=$1 FOR UPDATE`, rootID).Scan(&lockedID); err != nil {
			return err
		}
	}
	var active int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE root_job_id=$1 AND id<>$1
		AND status IN ('pending','running','retry_scheduled','waiting')`, rootID).Scan(&active); err != nil {
		return err
	}
	if active != 0 {
		return nil
	}
	var failed, dead int
	if err := tx.QueryRow(ctx, `SELECT
		COUNT(*) FILTER (WHERE status IN ('failed','dead_lettered','cancelled')),
		COUNT(*) FILTER (WHERE status='dead_lettered')
		FROM jobs WHERE root_job_id=$1 AND id<>$1`, rootID).Scan(&failed, &dead); err != nil {
		return err
	}
	status := job.StatusCompleted
	if dead > 0 {
		status = job.StatusDeadLettered
	} else if failed > 0 {
		status = job.StatusFailed
	}
	result := ""
	failureMessage := "one or more audit tasks failed"
	exhaustedMessage := "one or more audit tasks exhausted retries"
	if rootType == "dependency_update_scan" {
		failureMessage = "one or more update checks failed"
		exhaustedMessage = "one or more update checks exhausted retries"
	} else {
		var packages, violations int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE verdict='policy_violation')
			FROM audit_results WHERE root_job_id=$1`, rootID).Scan(&packages, &violations); err != nil {
			return err
		}
		result = fmt.Sprintf(`{"auditId":%q,"packages":%d,"violations":%d,"failedChildren":%d}`, rootID, packages, violations, failed)
	}
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status=$1, completed_at=NOW(),
		last_error=CASE WHEN $1<>$2 THEN $7 ELSE NULL END,
		last_error_kind=CASE WHEN $1=$3 THEN 'transient' WHEN $1=$4 THEN 'permanent' ELSE NULL END
		WHERE id=$5 AND status=$6`, status, job.StatusCompleted, job.StatusDeadLettered,
		job.StatusFailed, rootID, job.StatusWaiting, failureMessage)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	if status == job.StatusCompleted && rootType != "dependency_update_scan" {
		_, err := tx.Exec(ctx, `INSERT INTO job_results (job_id,result) VALUES ($1,$2::jsonb)
			ON CONFLICT (job_id) DO NOTHING`, rootID, result)
		if err != nil {
			return err
		}
	}
	if status == job.StatusDeadLettered {
		_, err := tx.Exec(ctx, `INSERT INTO dlq
			(job_id,job_type,payload,attempts,error,error_kind,root_job_id)
			SELECT id,type,payload,attempts,$2,'transient',root_job_id
			FROM jobs WHERE id=$1`, rootID, exhaustedMessage)
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO job_events (job_id,event_type,details)
		VALUES ($1,$2,'{}')`, rootID, string(status))
	return err
}

func boundedError(message string) string {
	if len(message) > 2048 {
		return message[:2048]
	}
	return message
}

func (s *Store) Fail(ctx context.Context, value job.Job, kind job.ErrorKind, message string, retryAt time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	message = boundedError(message)
	status := job.StatusFailed
	if kind == job.ErrorCancelled {
		status = job.StatusCancelled
	}
	if kind == job.ErrorTransient {
		if value.Attempts < value.MaxAttempts {
			status = job.StatusRetryScheduled
		} else {
			status = job.StatusDeadLettered
		}
	}
	completed := status != job.StatusRetryScheduled
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status=$1, scheduled_at=CASE WHEN $1=$2 THEN $3 ELSE scheduled_at END,
		locked_by=NULL, lease_token=NULL, locked_until=NULL, heartbeat_at=NULL,
		last_error_kind=$4,last_error=$5,completed_at=CASE WHEN $6 THEN NOW() ELSE NULL END
		WHERE id=$7 AND status=$8 AND locked_by=$9 AND lease_token=$10`, status,
		job.StatusRetryScheduled, retryAt, kind, message, completed, value.ID,
		job.StatusRunning, value.LockedBy, value.LeaseToken)
	if err != nil || tag.RowsAffected() != 1 {
		if err != nil {
			return err
		}
		return job.ErrLeaseLost
	}
	_, err = tx.Exec(ctx, `UPDATE job_attempts SET status=$1,finished_at=NOW(),error_kind=$2,error=$3
		WHERE job_id=$4 AND attempt=$5 AND lease_token=$6`, status, kind, message,
		value.ID, value.Attempts, value.LeaseToken)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO job_events
		(job_id,event_type,attempt,worker_id,details) VALUES ($1,$2,$3,$4,jsonb_build_object('errorKind',$5::text,'error',$6::text))`,
		value.ID, string(status), value.Attempts, value.LockedBy, kind, message)
	if err != nil {
		return err
	}
	if status == job.StatusDeadLettered {
		_, err = tx.Exec(ctx, `INSERT INTO dlq
			(job_id,job_type,payload,attempts,error,error_kind,root_job_id,parent_job_id)
			VALUES ($1,$2,$3::jsonb,$4,$5,$6,$7,NULLIF($8,''))`, value.ID, value.Type,
			jsonValue(value.Payload), value.Attempts, message, kind, value.RootJobID, value.ParentJobID)
		if err != nil {
			return err
		}
	}
	if completed && value.ID != value.RootJobID {
		if err := finalizeRoot(ctx, tx, value.RootJobID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) Cancel(ctx context.Context, id string) (job.Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return job.Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	value, err := scanServiceJob(tx.QueryRow(ctx, "SELECT "+serviceJobColumns+" FROM jobs WHERE id=$1 FOR UPDATE", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrNotFound
	}
	if err != nil {
		return job.Job{}, err
	}
	cancelledNow := false
	switch value.Status {
	case job.StatusPending, job.StatusRetryScheduled:
		_, err = tx.Exec(ctx, `UPDATE jobs SET status=$1,cancel_requested_at=NOW(),completed_at=NOW()
			WHERE id=$2`, job.StatusCancelled, id)
		cancelledNow = true
	case job.StatusRunning:
		_, err = tx.Exec(ctx, `UPDATE jobs SET cancel_requested_at=COALESCE(cancel_requested_at,NOW()) WHERE id=$1`, id)
	case job.StatusWaiting:
		_, err = tx.Exec(ctx, `UPDATE jobs SET status=$1,cancel_requested_at=NOW(),completed_at=NOW()
			WHERE root_job_id=$2 AND status IN ($3,$4,$5)`, job.StatusCancelled, value.RootJobID,
			job.StatusPending, job.StatusRetryScheduled, job.StatusWaiting)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE jobs SET cancel_requested_at=COALESCE(cancel_requested_at,NOW())
				WHERE root_job_id=$1 AND status=$2`, value.RootJobID, job.StatusRunning)
		}
		cancelledNow = true
	case job.StatusCancelled:
		return value, tx.Commit(ctx)
	default:
		return job.Job{}, job.ErrConflict
	}
	if err != nil {
		return job.Job{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO job_events (job_id,event_type,details)
		VALUES ($1,'cancellation_requested','{}')`, id)
	if err != nil {
		return job.Job{}, err
	}
	if cancelledNow {
		if _, err := tx.Exec(ctx, `INSERT INTO job_events (job_id,event_type,details)
			VALUES ($1,'cancelled','{}')`, id); err != nil {
			return job.Job{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, err
	}
	return scanServiceJob(s.pool.QueryRow(ctx, "SELECT "+serviceJobColumns+" FROM jobs WHERE id=$1", id))
}

func (s *Store) Retry(ctx context.Context, id string, maxAttempts int) (job.Job, error) {
	detail, err := s.Get(ctx, id)
	if err != nil {
		return job.Job{}, err
	}
	if detail.Job.Status != job.StatusFailed && detail.Job.Status != job.StatusDeadLettered {
		return job.Job{}, job.ErrConflict
	}
	if maxAttempts == 0 {
		maxAttempts = detail.Job.MaxAttempts
	}
	value, _, err := s.Submit(ctx, job.Submission{
		Type: detail.Job.Type, Payload: detail.Job.Payload, MaxAttempts: maxAttempts,
		RetriedFromJobID: detail.Job.ID,
	})
	return value, err
}

func (s *Store) ListDLQ(ctx context.Context, limit int, cursor string) (job.DLQPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	query := `SELECT d.id,d.job_id,d.job_type,d.payload,d.attempts,COALESCE(d.error_kind,''),d.error,
		d.dead_at,d.replayed_at,COALESCE(d.replayed_as_job_id,'') FROM dlq d
		JOIN jobs j ON j.id=d.job_id WHERE j.internal=FALSE`
	args := []any{}
	if cursor != "" {
		value, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return job.DLQPage{}, job.ErrConflict
		}
		var id int64
		if _, err := fmt.Sscan(string(value), &id); err != nil {
			return job.DLQPage{}, job.ErrConflict
		}
		args = append(args, id)
		query += " AND d.id<$1"
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY d.id DESC LIMIT $%d", len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return job.DLQPage{}, err
	}
	defer rows.Close()
	page := job.DLQPage{Entries: []job.DLQEntry{}}
	for rows.Next() {
		var entry job.DLQEntry
		var payload []byte
		if err := rows.Scan(&entry.ID, &entry.JobID, &entry.JobType, &payload, &entry.Attempts,
			&entry.ErrorKind, &entry.Error, &entry.DeadAt, &entry.ReplayedAt, &entry.ReplayedAsJobID); err != nil {
			return job.DLQPage{}, err
		}
		entry.Payload = payload
		page.Entries = append(page.Entries, entry)
	}
	if len(page.Entries) > limit {
		last := page.Entries[limit-1]
		page.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprint(last.ID)))
		page.Entries = page.Entries[:limit]
	}
	return page, rows.Err()
}

func (s *Store) ReplayDLQ(ctx context.Context, entryID int64, maxAttempts int) (job.Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return job.Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var source job.DLQEntry
	var payload []byte
	err = tx.QueryRow(ctx, `SELECT d.id,d.job_id,d.job_type,d.payload,d.attempts,COALESCE(d.error_kind,''),d.error,
		d.dead_at,d.replayed_at,COALESCE(d.replayed_as_job_id,'') FROM dlq d
		JOIN jobs j ON j.id=d.job_id WHERE d.id=$1 AND j.internal=FALSE FOR UPDATE OF d`, entryID).
		Scan(&source.ID, &source.JobID, &source.JobType, &payload, &source.Attempts, &source.ErrorKind,
			&source.Error, &source.DeadAt, &source.ReplayedAt, &source.ReplayedAsJobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrNotFound
	}
	if err != nil {
		return job.Job{}, err
	}
	if source.ReplayedAsJobID != "" {
		return job.Job{}, job.ErrConflict
	}
	if maxAttempts == 0 {
		maxAttempts = job.DefaultMaxAttempts
	}
	id := job.NewJobID()
	_, err = tx.Exec(ctx, `INSERT INTO jobs
		(id,type,payload,status,attempts,max_attempts,scheduled_at,root_job_id,replayed_from_job_id)
		VALUES ($1,$2,$3::jsonb,$4,0,$5,NOW(),$1,$6)`, id, source.JobType,
		jsonValue(payload), job.StatusPending, maxAttempts, source.JobID)
	if err != nil {
		return job.Job{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE dlq SET replayed_at=NOW(),replayed_as_job_id=$1 WHERE id=$2`, id, entryID)
	if err != nil {
		return job.Job{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO job_events (job_id,event_type,details)
		VALUES ($1,'submitted','{}'),
		       ($1,'replayed',jsonb_build_object('sourceJobId',$2))`, id, source.JobID)
	if err != nil {
		return job.Job{}, err
	}
	value, err := scanServiceJob(tx.QueryRow(ctx, "SELECT "+serviceJobColumns+" FROM jobs WHERE id=$1", id))
	if err != nil {
		return job.Job{}, err
	}
	return value, tx.Commit(ctx)
}

func (s *Store) ReclaimExpired(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT `+serviceJobColumns+` FROM jobs
		WHERE status=$1 AND locked_until<NOW() FOR UPDATE SKIP LOCKED`, job.StatusRunning)
	if err != nil {
		return 0, err
	}
	var expired []job.Job
	for rows.Next() {
		value, err := scanServiceJob(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		expired = append(expired, value)
	}
	rows.Close()
	for _, value := range expired {
		status := job.StatusPending
		if value.CancelRequestedAt != nil {
			status = job.StatusCancelled
		}
		if value.Attempts >= value.MaxAttempts && status != job.StatusCancelled {
			status = job.StatusDeadLettered
		}
		terminal := status == job.StatusCancelled || status == job.StatusDeadLettered
		_, err = tx.Exec(ctx, `UPDATE jobs SET status=$1,locked_by=NULL,lease_token=NULL,
			locked_until=NULL,heartbeat_at=NULL,last_error_kind=$2,last_error='worker lease expired',
			completed_at=CASE WHEN $3 THEN NOW() ELSE NULL END WHERE id=$4`, status,
			job.ErrorTransient, terminal, value.ID)
		if err != nil {
			return 0, err
		}
		_, err = tx.Exec(ctx, `UPDATE job_attempts SET status='abandoned',finished_at=NOW(),
			error_kind=$1,error='worker lease expired' WHERE job_id=$2 AND attempt=$3`,
			job.ErrorTransient, value.ID, value.Attempts)
		if err != nil {
			return 0, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO job_events
			(job_id,event_type,attempt,worker_id,details) VALUES ($1,'lease_expired',$2,$3,'{}')`,
			value.ID, value.Attempts, value.LockedBy)
		if err != nil {
			return 0, err
		}
		if status == job.StatusDeadLettered {
			_, err = tx.Exec(ctx, `INSERT INTO dlq
				(job_id,job_type,payload,attempts,error,error_kind,root_job_id,parent_job_id)
				VALUES ($1,$2,$3::jsonb,$4,'worker lease expired',$5,$6,NULLIF($7,''))`,
				value.ID, value.Type, jsonValue(value.Payload), value.Attempts, job.ErrorTransient,
				value.RootJobID, value.ParentJobID)
			if err != nil {
				return 0, err
			}
		}
		if terminal && value.ID != value.RootJobID {
			if err := finalizeRoot(ctx, tx, value.RootJobID); err != nil {
				return 0, err
			}
		}
	}
	return len(expired), tx.Commit(ctx)
}
