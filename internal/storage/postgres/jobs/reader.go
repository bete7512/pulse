package jobs

import (
	"context"
	"errors"

	"github.com/bete7512/pulse/internal/domain"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// JobReader is the read side of the jobs projection.
type JobReader interface {
	GetJob(ctx context.Context, jobID string) (*domain.Job, error)
	ListJobs(ctx context.Context) ([]domain.Job, error)
}

// Store is the Postgres adapter for the jobs read-model table. It serves queries
// (GetJob/ListJobs) and the projection's writes (LaggingJobs/Upsert) — all the SQL
// touching the jobs table lives here; the projection loop itself lives in internal/projection.
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) GetJob(ctx context.Context, jobID string) (*domain.Job, error) {
	var j domain.Job
	err := s.pool.QueryRow(ctx, `
		SELECT job_id, status, submitted_at, started_at, completed_at, message
		FROM jobs WHERE job_id=$1`, jobID).
		Scan(&j.ID, &j.Status, &j.SubmittedAt, &j.StartedAt, &j.CompletedAt, &j.Message)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, pkg_errors.ErrNotFound
	}
	return &j, err
}

func (s *Store) ListJobs(ctx context.Context) ([]domain.Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT job_id, status, submitted_at, started_at, completed_at, message
		FROM jobs ORDER BY submitted_at DESC`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Job, error) {
		var j domain.Job
		err := row.Scan(&j.ID, &j.Status, &j.SubmittedAt, &j.StartedAt, &j.CompletedAt, &j.Message)
		return j, err
	})
}
