package postgres

import (
	"context"
	"time"

	"github.com/bete7512/pulse/internal/repos"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Liveness is the Postgres adapter for per-running-job liveness.
type Liveness struct {
	pool *pgxpool.Pool
}

var _ repos.LivenessRepo = (*Liveness)(nil)

func NewLiveness(pool *pgxpool.Pool) *Liveness {
	return &Liveness{pool: pool}
}

func (s *Liveness) Mark(ctx context.Context, jobID, workerID string, expiresAt time.Time) error {
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

func (s *Liveness) Renew(ctx context.Context, jobID, workerID string, expiresAt time.Time) error {
	// Fencing: the worker_id predicate means only the current owner can renew.
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

func (s *Liveness) Clear(ctx context.Context, jobID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM liveness WHERE job_id = $1`, jobID)
	return err
}

func (s *Liveness) Expired(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT job_id FROM liveness WHERE expires_at < now()`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
