package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// uniqueViolation is the Postgres SQLSTATE for a UNIQUE constraint violation.
const uniqueViolation = "23505"

// Event is the Postgres-backed event log. The events table's UNIQUE
// (job_id, sequence) constraint guarantees a gap-free, conflict-free sequence per job:
// two concurrent Appends racing for the same next sequence collide on the constraint
// (surfaced as ErrSequenceConflict) instead of silently duplicating.
type Event struct {
	pool *pgxpool.Pool
}

var _ repos.EventRepo = (*Event)(nil)

func NewEvent(pool *pgxpool.Pool) *Event {
	return &Event{pool: pool}
}

// Append inserts an event, deriving its sequence as MAX(sequence)+1 for the job in
// the same statement so the UNIQUE (job_id, sequence) constraint can arbitrate
// concurrent writers.
func (s *Event) Append(ctx context.Context, e domain.Event) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	e.ID = id.String()
	e.CreatedAt = time.Now()

	const query = `
		INSERT INTO events (id, job_id, type, sequence, payload, message, created_at, topic, next_attempt_at, schedule_id, priority)
		VALUES (
			@id, @job_id, @type,
			(SELECT COALESCE(MAX(sequence), 0) + 1 FROM events WHERE job_id = @job_id),
			@payload, @message, @created_at, @topic, @next_attempt_at, @schedule_id, @priority
		)
		RETURNING job_id`

	var jobID string
	err = s.pool.QueryRow(ctx, query, pgx.NamedArgs{
		"id":              e.ID,
		"job_id":          e.JobId,
		"type":            e.Type,
		"payload":         e.Payload,
		"message":         e.Message,
		"created_at":      e.CreatedAt,
		"topic":           e.Topic,
		"next_attempt_at": e.NextAttemptAt,
		"schedule_id":     e.ScheduleID,
		"priority":        e.Priority,
	}).Scan(&jobID)
	if err != nil {
		if isUniqueViolation(err) {
			return "", pkg_errors.ErrSequenceConflict
		}
		return "", err
	}
	return jobID, nil
}

// AppendBatch inserts events in order inside one transaction. Each insert derives its
// own sequence as MAX(sequence)+1, so within the transaction the events get consecutive
// sequences; if any collides with a concurrent writer the whole transaction rolls back
// and ErrSequenceConflict is returned for the caller to retry.
func (s *Event) AppendBatch(ctx context.Context, events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op once committed

	const query = `
		INSERT INTO events (id, job_id, type, sequence, payload, message, created_at, topic, next_attempt_at, schedule_id, priority)
		VALUES (
			@id, @job_id, @type,
			(SELECT COALESCE(MAX(sequence), 0) + 1 FROM events WHERE job_id = @job_id),
			@payload, @message, @created_at, @topic, @next_attempt_at, @schedule_id, @priority
		)`

	for _, e := range events {
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, query, pgx.NamedArgs{
			"id":              id.String(),
			"job_id":          e.JobId,
			"type":            e.Type,
			"payload":         e.Payload,
			"message":         e.Message,
			"created_at":      time.Now(),
			"topic":           e.Topic,
			"next_attempt_at": e.NextAttemptAt,
			"schedule_id":     e.ScheduleID,
			"priority":        e.Priority,
		})
		if err != nil {
			if isUniqueViolation(err) {
				return pkg_errors.ErrSequenceConflict
			}
			return err
		}
	}
	return tx.Commit(ctx)
}

// isUniqueViolation reports whether err is a Postgres UNIQUE constraint violation,
// translated into pkg_errors.ErrSequenceConflict so callers never depend on pgx types.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolation
}

// LoadEventsForJob returns every event for a job ordered by sequence.
func (s *Event) LoadEventsForJob(ctx context.Context, jobID string) ([]domain.Event, error) {
	const query = `
		SELECT id, type, job_id, sequence, payload, message, created_at, topic, next_attempt_at, schedule_id, priority
		FROM events
		WHERE job_id = $1
		ORDER BY sequence`

	rows, err := s.pool.Query(ctx, query, jobID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[domain.Event])
}

// ListEventsByTopics returns, for each job, its latest event — but only when that
// latest event is dispatchable (JOB_SUBMITTED for new jobs, or JOB_RETRIED whose
// next_attempt_at has passed). An empty/nil topics slice matches every topic.

// TODO: paginate.
type ListEventOpts struct {
	Page   int32
	Offset int32
}

func (s *Event) ListEventsByTopics(ctx context.Context, topics []string) ([]domain.Event, error) {
	const query = `
		SELECT id, type, job_id, sequence, payload, message, created_at, topic, next_attempt_at, schedule_id, priority
		FROM (
			SELECT DISTINCT ON (job_id)
				id, type, job_id, sequence, payload, message, created_at, topic, next_attempt_at, schedule_id, priority
			FROM events
			ORDER BY job_id, sequence DESC
		) latest
		WHERE (
			type = @submitted
			OR (type = @retried AND next_attempt_at <= now())
		)
		  AND (@topics::text[] IS NULL OR topic = ANY(@topics))
		ORDER BY priority DESC, created_at ASC` // priority first, then FIFO by arrival

	rows, err := s.pool.Query(ctx, query, pgx.NamedArgs{
		"submitted": domain.JobSubmitted,
		"retried":   domain.JobRetried,
		"topics":    topicFilter(topics),
	})
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[domain.Event])
}

// OrphanedRunning returns ids of jobs running (latest event JOB_STARTED) with no
// liveness row, started longer ago than grace — the watchdog's fallback for jobs whose
// best-effort liveness mark never landed. Uses the DB clock for consistency.
func (s *Event) OrphanedRunning(ctx context.Context, grace time.Duration) ([]string, error) {
	const query = `
		SELECT latest.job_id
		FROM (
			SELECT DISTINCT ON (job_id)
				job_id, type, created_at
			FROM events
			ORDER BY job_id, sequence DESC
		) latest
		LEFT JOIN liveness l ON l.job_id = latest.job_id
		WHERE latest.type = @started
		  AND l.job_id IS NULL
		  AND latest.created_at < now() - @grace::interval`

	rows, err := s.pool.Query(ctx, query, pgx.NamedArgs{
		"started": domain.JobStarted,
		"grace":   grace.String(),
	})
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// FiresBySchedule returns the JobSubmitted events tagged with scheduleID (a schedule's fire
// history), newest first.
func (s *Event) FiresBySchedule(ctx context.Context, scheduleID string) ([]domain.Event, error) {
	const query = `
		SELECT id, type, job_id, sequence, payload, message, created_at, topic, next_attempt_at, schedule_id, priority
		FROM events
		WHERE type = $1 AND schedule_id = $2
		ORDER BY created_at DESC`

	rows, err := s.pool.Query(ctx, query, domain.JobSubmitted, scheduleID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[domain.Event])
}

// topicFilter normalizes an empty slice to nil so the SQL @topics::text[] IS NULL
// branch (match every topic) fires instead of ANY('{}') matching nothing.
func topicFilter(topics []string) []string {
	if len(topics) == 0 {
		return nil
	}
	return topics
}
