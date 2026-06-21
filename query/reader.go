package query

import (
	"context"
	"errors"

	"github.com/bete7512/pulse/domain"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobReader interface {
	GetJob(ctx context.Context, jobID string) (*domain.Job, error)
	ListJobs(ctx context.Context) ([]domain.Job, error)
}

type PostgresJobReader struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *PostgresJobReader {
	return &PostgresJobReader{
		pool: pool,
	}
}
func (r *PostgresJobReader) GetJob(ctx context.Context, jobID string) (*domain.Job, error) {
	var j domain.Job
	err := r.pool.QueryRow(ctx, `
		SELECT job_id, status, submitted_at, started_at, completed_at, message
		FROM jobs WHERE job_id=$1`, jobID).
		Scan(&j.ID, &j.Status, &j.SubmittedAt, &j.StartedAt, &j.CompletedAt, &j.Message)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, pkg_errors.ErrNotFound
	}
	return &j, err
}

func (r *PostgresJobReader) ListJobs(ctx context.Context) ([]domain.Job, error) {
	rows, err := r.pool.Query(ctx, `
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
