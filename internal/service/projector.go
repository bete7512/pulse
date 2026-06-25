package service

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/projector_mock.go -package=mocks github.com/bete7512/pulse/internal/service ProjectorService

import (
	"context"
	"log/slog"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos"
)

// ProjectorService runs the read-side loop that folds the event log into the jobs read
// model: find jobs whose events have outrun the projection, re-fold each, upsert it. The
// composition root depends on this interface; the loop body is exercised via a test seam.
type ProjectorService interface {
	Run(ctx context.Context)
}

type projectorService struct {
	events   repos.EventRepo
	jobs     repos.ProjectionRepo
	interval time.Duration
	logger   *slog.Logger
}

func NewProjector(events repos.EventRepo, jobs repos.ProjectionRepo, interval time.Duration, logger *slog.Logger) ProjectorService {
	if logger == nil {
		logger = slog.Default()
	}
	return &projectorService{events: events, jobs: jobs, interval: interval, logger: logger}
}

func (p *projectorService) Run(ctx context.Context) {
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

func (p *projectorService) catchUp(ctx context.Context) error {
	ids, err := p.jobs.LaggingJobs(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		events, err := p.events.LoadEventsForJob(ctx, id)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			continue
		}
		job := domain.RebuildJob(events) // the canonical fold, doing its read-side job
		if err := p.jobs.Upsert(ctx, job, maxSeq(events)); err != nil {
			return err
		}
	}
	return nil
}

// maxSeq returns the highest sequence in a job's event slice.
func maxSeq(events []domain.Event) int64 {
	var max int64
	for _, e := range events {
		if e.Sequence > max {
			max = e.Sequence
		}
	}
	return max
}
