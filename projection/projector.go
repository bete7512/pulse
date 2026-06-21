package projection

import (
	"context"
	"log/slog"
	"time"

	"github.com/bete7512/pulse/domain"
	"github.com/bete7512/pulse/eventstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Projector struct {
	store    eventstore.EventStore 
	pool     *pgxpool.Pool      
	interval time.Duration
	logger   *slog.Logger
}

func New(store eventstore.EventStore, pool *pgxpool.Pool, interval time.Duration, logger *slog.Logger) *Projector {
	if logger == nil {
		logger = slog.Default()
	}
	return &Projector{
		store:    store,
		pool:     pool,
		interval: interval,
		logger:   logger,
	}
}

func (p *Projector) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.catchUp(ctx); err != nil {
				p.logger.Error("projection catch-up failed", "error", err)
			}
		}
	}
}

func (p *Projector) catchUp(ctx context.Context) error {
	ids, err := p.laggingJobs(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		events, err := p.store.LoadEventsForJob(ctx, id)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			continue
		}
		job := domain.RebuildJob(events) // ← the canonical fold, doing its real read-side job
		if err := p.upsert(ctx, job, maxSeq(events)); err != nil {
			return err
		}
	}
	return nil
}

// laggingJobs returns the ids of jobs whose newest event sequence has outrun the
// sequence already reflected in the jobs projection (never-projected jobs included).
func (p *Projector) laggingJobs(ctx context.Context) ([]string, error) {
	const query = `
		SELECT e.job_id
		FROM   (SELECT job_id, MAX(sequence) AS max_seq FROM events GROUP BY job_id) e
		LEFT   JOIN jobs j ON j.job_id = e.job_id
		WHERE  e.max_seq > COALESCE(j.last_sequence, 0)` // events ahead of the projection

	rows, err := p.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// upsert writes the folded job into the jobs table, advancing last_sequence only
// when the incoming sequence is newer so concurrent projectors stay monotonic.
func (p *Projector) upsert(ctx context.Context, job domain.Job, seq int64) error {
	const query = `
		INSERT INTO jobs (job_id, status, submitted_at, started_at, completed_at, message, last_sequence)
		VALUES (@job_id, @status, @submitted_at, @started_at, @completed_at, @message, @last_sequence)
		ON CONFLICT (job_id) DO UPDATE SET
			status=EXCLUDED.status, started_at=EXCLUDED.started_at,
			completed_at=EXCLUDED.completed_at, message=EXCLUDED.message,
			last_sequence=EXCLUDED.last_sequence
		WHERE jobs.last_sequence < EXCLUDED.last_sequence` // only move forward

	_, err := p.pool.Exec(ctx, query, pgx.NamedArgs{
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

// maxSeq returns the highest sequence in a job's event slice. Load returns events
// ordered by sequence, but scanning defensively keeps this correct regardless.
func maxSeq(events []domain.Event) int64 {
	var max int64
	for _, e := range events {
		if e.Sequence > max {
			max = e.Sequence
		}
	}
	return max
}
