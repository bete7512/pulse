package postgres

import (
	"context"
	"errors"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job is the Postgres adapter for the jobs read-model table. It serves queries
// (GetJob/ListJobs — the read side) and the projection's writes (LaggingJobs/Upsert),
// so it satisfies both repos.JobRepo and repos.ProjectionRepo.
type Job struct {
	pool *pgxpool.Pool
}

var (
	_ repos.JobRepo        = (*Job)(nil)
	_ repos.ProjectionRepo = (*Job)(nil)
)

func NewJob(pool *pgxpool.Pool) *Job {
	return &Job{pool: pool}
}

func (s *Job) GetJob(ctx context.Context, jobID string) (*domain.Job, error) {
	var j domain.Job
	err := s.pool.QueryRow(ctx, `
		SELECT job_id, status, submitted_at, started_at, completed_at, message, schedule_id
		FROM jobs WHERE job_id=$1`, jobID).
		Scan(&j.ID, &j.Status, &j.SubmittedAt, &j.StartedAt, &j.CompletedAt, &j.Message, &j.ScheduleID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, pkg_errors.ErrNotFound
	}
	return &j, err
}

func (s *Job) ListJobs(ctx context.Context) ([]domain.Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT job_id, status, submitted_at, started_at, completed_at, message, schedule_id
		FROM jobs ORDER BY submitted_at DESC`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Job, error) {
		var j domain.Job
		err := row.Scan(&j.ID, &j.Status, &j.SubmittedAt, &j.StartedAt, &j.CompletedAt, &j.Message, &j.ScheduleID)
		return j, err
	})
}

// JobsBySchedule returns the jobs a schedule spawned (newest first) from the read model.
func (s *Job) JobsBySchedule(ctx context.Context, scheduleID string) ([]domain.Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT job_id, status, submitted_at, started_at, completed_at, message, schedule_id
		FROM jobs WHERE schedule_id = $1 ORDER BY submitted_at DESC`, scheduleID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Job, error) {
		var j domain.Job
		err := row.Scan(&j.ID, &j.Status, &j.SubmittedAt, &j.StartedAt, &j.CompletedAt, &j.Message, &j.ScheduleID)
		return j, err
	})
}

// LaggingJobs returns the ids of jobs whose newest event sequence has outrun the
// sequence already reflected in the jobs projection (never-projected jobs included).
func (s *Job) LaggingJobs(ctx context.Context) ([]string, error) {
	const query = `
		SELECT e.job_id
		FROM   (SELECT job_id, MAX(sequence) AS max_seq FROM events GROUP BY job_id) e
		LEFT   JOIN jobs j ON j.job_id = e.job_id
		WHERE  e.max_seq > COALESCE(j.last_sequence, 0)`

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// Upsert writes the folded job into the jobs table, advancing last_sequence only when
// the incoming sequence is newer so concurrent projectors stay monotonic.
func (s *Job) Upsert(ctx context.Context, job domain.Job, seq int64) error {
	const query = `
		INSERT INTO jobs (job_id, status, submitted_at, started_at, completed_at, message, last_sequence, schedule_id)
		VALUES (@job_id, @status, @submitted_at, @started_at, @completed_at, @message, @last_sequence, @schedule_id)
		ON CONFLICT (job_id) DO UPDATE SET
			status=EXCLUDED.status, started_at=EXCLUDED.started_at,
			completed_at=EXCLUDED.completed_at, message=EXCLUDED.message,
			last_sequence=EXCLUDED.last_sequence, schedule_id=EXCLUDED.schedule_id
		WHERE jobs.last_sequence < EXCLUDED.last_sequence`

	_, err := s.pool.Exec(ctx, query, pgx.NamedArgs{
		"job_id":        job.ID,
		"status":        string(job.Status),
		"submitted_at":  job.SubmittedAt,
		"started_at":    job.StartedAt,
		"completed_at":  job.CompletedAt,
		"message":       job.Message,
		"last_sequence": seq,
		"schedule_id":   job.ScheduleID,
	})
	return err
}
