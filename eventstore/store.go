package eventstore

import (
	"context"
	"time"

	"github.com/bete7512/pulse/domain"
	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventStore interface {
	Add(ctx context.Context, event domain.Event) (string, error)
	Load(ctx context.Context, jobId string) ([]domain.Event, error)
	ListSubmittedEvents(ctx context.Context) ([]domain.Event, error)
}

// PostgresEventStore is the production EventStore backed by Postgres (pgx).
//
// The events table's UNIQUE (job_id, sequence) constraint is what guarantees a
// gap-free, conflict-free sequence per job: two concurrent Adds racing for the
// same next sequence will collide on the constraint instead of silently
// duplicating (retry handling lives in B3).
type PostgresEventStore struct {
	pool *pgxpool.Pool
}

var _ EventStore = (*PostgresEventStore)(nil)

func NewPostgresEventStore(pool *pgxpool.Pool) *PostgresEventStore {
	return &PostgresEventStore{pool: pool}
}

// Add inserts an event, deriving its sequence as MAX(sequence)+1 for the job in
// the same statement so the UNIQUE (job_id, sequence) constraint can arbitrate
// concurrent writers.
func (s *PostgresEventStore) Add(ctx context.Context, e domain.Event) (string, error) {

	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	e.ID = id.String()
	e.CreatedAt = time.Now()

	const query = `
		INSERT INTO events (id, job_id, type, sequence, payload, message, created_at)
		VALUES (
			$1, $2, $3,
			(SELECT COALESCE(MAX(sequence), 0) + 1 FROM events WHERE job_id = $2),
			$4, $5, $6
		)
		RETURNING job_id`

	var jobID string
	err = s.pool.QueryRow(ctx, query,
		e.ID, e.JobId, e.Type, e.Payload, e.Message, e.CreatedAt,
	).Scan(&jobID)
	if err != nil {
		return "", err
	}
	return jobID, nil
}

// Load returns every event for a job ordered by sequence.
func (s *PostgresEventStore) Load(ctx context.Context, jobID string) ([]domain.Event, error) {

	const query = `
		SELECT id, type, job_id, sequence, payload, message, created_at
		FROM events
		WHERE job_id = $1
		ORDER BY sequence`

	rows, err := s.pool.Query(ctx, query, jobID)
	if err != nil {
		return nil, err
	}
	return scanEvents(rows)
}

// ListSubmittedEvents returns, for each job, its latest event — but only when
// that latest event is JOB_SUBMITTED (i.e. jobs awaiting a worker).
func (s *PostgresEventStore) ListSubmittedEvents(ctx context.Context) ([]domain.Event, error) {

	const query = `
		SELECT id, type, job_id, sequence, payload, message, created_at
		FROM (
			SELECT DISTINCT ON (job_id)
				id, type, job_id, sequence, payload, message, created_at
			FROM events
			ORDER BY job_id, sequence DESC
		) latest
		WHERE type = $1`

	rows, err := s.pool.Query(ctx, query, domain.JobSubmitted)
	if err != nil {
		return nil, err
	}
	return scanEvents(rows)
}

func scanEvents(rows pgx.Rows) ([]domain.Event, error) {
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		var e domain.Event
		if err := rows.Scan(
			&e.ID, &e.Type, &e.JobId, &e.Sequence, &e.Payload, &e.Message, &e.CreatedAt,
		); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
