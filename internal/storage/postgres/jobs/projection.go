package jobs

import (
	"context"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/jackc/pgx/v5"
)

// LaggingJobs returns the ids of jobs whose newest event sequence has outrun the
// sequence already reflected in the jobs projection (never-projected jobs included).
func (s *Store) LaggingJobs(ctx context.Context) ([]string, error) {
	const query = `
		SELECT e.job_id
		FROM   (SELECT job_id, MAX(sequence) AS max_seq FROM events GROUP BY job_id) e
		LEFT   JOIN jobs j ON j.job_id = e.job_id
		WHERE  e.max_seq > COALESCE(j.last_sequence, 0)` // events ahead of the projection

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// Upsert writes the folded job into the jobs table, advancing last_sequence only
// when the incoming sequence is newer so concurrent projectors stay monotonic.
func (s *Store) Upsert(ctx context.Context, job domain.Job, seq int64) error {
	const query = `
		INSERT INTO jobs (job_id, status, submitted_at, started_at, completed_at, message, last_sequence)
		VALUES (@job_id, @status, @submitted_at, @started_at, @completed_at, @message, @last_sequence)
		ON CONFLICT (job_id) DO UPDATE SET
			status=EXCLUDED.status, started_at=EXCLUDED.started_at,
			completed_at=EXCLUDED.completed_at, message=EXCLUDED.message,
			last_sequence=EXCLUDED.last_sequence
		WHERE jobs.last_sequence < EXCLUDED.last_sequence` // only move forward

	_, err := s.pool.Exec(ctx, query, pgx.NamedArgs{
		"job_id":        job.ID,
		"status":        string(job.Status),
		"submitted_at":  job.SubmittedAt,
		"started_at":    job.StartedAt,
		"completed_at":  job.CompletedAt,
		"message":       job.Message,
		"last_sequence": seq,
	})
	return err
}
