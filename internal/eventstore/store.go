package eventstore

import (
	"context"
	"errors"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// uniqueViolation is the Postgres SQLSTATE for a UNIQUE constraint violation.
const uniqueViolation = "23505"

type EventStore interface {
	Append(ctx context.Context, event domain.Event) (string, error)
	LoadEventsForJob(ctx context.Context, jobId string) ([]domain.Event, error)
	ListEventsByTopics(ctx context.Context, topics []string) ([]domain.Event, error)
}

// PostgresEventStore is the production EventStore backed by Postgres (pgx).
//
// The events table's UNIQUE (job_id, sequence) constraint is what guarantees a
// gap-free, conflict-free sequence per job: two concurrent Appends racing for the
// same next sequence will collide on the constraint instead of silently
// duplicating (retry handling lives in B3).
type PostgresEventStore struct {
	pool *pgxpool.Pool
}

var _ EventStore = (*PostgresEventStore)(nil)

func NewPostgresEventStore(pool *pgxpool.Pool) *PostgresEventStore {
	return &PostgresEventStore{pool: pool}
}

// Append inserts an event, deriving its sequence as MAX(sequence)+1 for the job in
// the same statement so the UNIQUE (job_id, sequence) constraint can arbitrate
// concurrent writers.
func (s *PostgresEventStore) Append(ctx context.Context, e domain.Event) (string, error) {

	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	e.ID = id.String()
	e.CreatedAt = time.Now()

	const query = `
		INSERT INTO events (id, job_id, type, sequence, payload, message, created_at, topic)
		VALUES (
			@id, @job_id, @type,
			(SELECT COALESCE(MAX(sequence), 0) + 1 FROM events WHERE job_id = @job_id),
			@payload, @message, @created_at, @topic
		)
		RETURNING job_id`

	var jobID string
	err = s.pool.QueryRow(ctx, query, pgx.NamedArgs{
		"id":         e.ID,
		"job_id":     e.JobId,
		"type":       e.Type,
		"payload":    e.Payload,
		"message":    e.Message,
		"created_at": e.CreatedAt,
		"topic":      e.Topic,
	}).Scan(&jobID)
	if err != nil {
		if isUniqueViolation(err) {
			return "", pkg_errors.ErrSequenceConflict
		}
		return "", err
	}
	return jobID, nil
}

// isUniqueViolation reports whether err is a Postgres UNIQUE constraint violation,
// translated by Append into pkg_errors.ErrSequenceConflict so callers never depend
// on pgx error types.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolation
}

// LoadEventsForJob returns every event for a job ordered by sequence.
func (s *PostgresEventStore) LoadEventsForJob(ctx context.Context, jobID string) ([]domain.Event, error) {

	const query = `
		SELECT id, type, job_id, sequence, payload, message, created_at, topic
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
// latest event is JOB_SUBMITTED (i.e. jobs awaiting a worker). An empty/nil topics
// slice matches every topic; otherwise only jobs whose topic is in topics.
// TODO: in the future make this by pagination
func (s *PostgresEventStore) ListEventsByTopics(ctx context.Context, topics []string) ([]domain.Event, error) {

	const query = `
		SELECT id, type, job_id, sequence, payload, message, created_at, topic
		FROM (
			SELECT DISTINCT ON (job_id)
				id, type, job_id, sequence, payload, message, created_at, topic
			FROM events
			ORDER BY job_id, sequence DESC
		) latest
		WHERE type = @event_type
		  AND (@topics::text[] IS NULL OR topic = ANY(@topics))`

	rows, err := s.pool.Query(ctx, query, pgx.NamedArgs{
		"event_type": domain.JobSubmitted,
		"topics":     topicFilter(topics),
	})
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
