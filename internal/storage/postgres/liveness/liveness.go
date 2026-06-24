package liveness

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store tracks per-running-job liveness, separate from the event log: one row per
// running job, marked when a worker claims the job, renewed by heartbeats, cleared
// when the job leaves Running. A row whose deadline has passed means the worker
// stopped proving liveness and the job should be recovered.
type Store interface {
	// Mark records (or replaces) the liveness deadline for a freshly claimed job.
	Mark(ctx context.Context, jobID, workerID string, expiresAt time.Time) error
	// Renew pushes the deadline forward, but only for the worker that owns it — a
	// stale/zombie worker's heartbeat (wrong worker_id) is silently ignored.
	Renew(ctx context.Context, jobID, workerID string, expiresAt time.Time) error
	// Clear drops the record once the job is no longer running.
	Clear(ctx context.Context, jobID string) error
	// Expired returns the ids of jobs whose liveness deadline has passed.
	Expired(ctx context.Context) ([]string, error)
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

var _ Store = (*PostgresStore)(nil)

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) Mark(ctx context.Context, jobID, workerID string, expiresAt time.Time) error {
	const query = `
		INSERT INTO liveness (job_id, worker_id, expires_at)
		VALUES (@job_id, @worker_id, @expires_at)
		ON CONFLICT (job_id) DO UPDATE SET
			worker_id = EXCLUDED.worker_id,
			expires_at = EXCLUDED.expires_at`
	_, err := s.pool.Exec(ctx, query, pgx.NamedArgs{
		"job_id":     jobID,
		"worker_id":  workerID,
		"expires_at": expiresAt,
	})
	return err
}

func (s *PostgresStore) Renew(ctx context.Context, jobID, workerID string, expiresAt time.Time) error {
	const query = `
		UPDATE liveness SET expires_at = @expires_at
		WHERE job_id = @job_id AND worker_id = @worker_id`
	_, err := s.pool.Exec(ctx, query, pgx.NamedArgs{
		"job_id":     jobID,
		"worker_id":  workerID,
		"expires_at": expiresAt,
	})
	return err
}

func (s *PostgresStore) Clear(ctx context.Context, jobID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM liveness WHERE job_id = $1`, jobID)
	return err
}

func (s *PostgresStore) Expired(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT job_id FROM liveness WHERE expires_at < now()`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
