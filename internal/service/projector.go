package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/bete7512/pulse/internal/domain"
)

// Projector runs the read-side loop that folds the event log into the jobs read
// model. It drives the Service's stores directly: find jobs whose events have outrun
// the projection, re-fold each, and upsert it.
type Projector struct {
	svc      *Service
	interval time.Duration
	logger   *slog.Logger
}

func NewProjector(svc *Service, interval time.Duration, logger *slog.Logger) *Projector {
	if logger == nil {
		logger = slog.Default()
	}
	return &Projector{svc: svc, interval: interval, logger: logger}
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
	ids, err := p.svc.jobStore.LaggingJobs(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		events, err := p.svc.store.LoadEventsForJob(ctx, id)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			continue
		}
		job := domain.RebuildJob(events) // the canonical fold, doing its real read-side job
		if err := p.svc.jobStore.Upsert(ctx, job, maxSeq(events)); err != nil {
			return err
		}
	}
	return nil
}

// maxSeq returns the highest sequence in a job's event slice. LoadEventsForJob returns
// events ordered by sequence, but scanning defensively keeps this correct regardless.
func maxSeq(events []domain.Event) int64 {
	var max int64
	for _, e := range events {
		if e.Sequence > max {
			max = e.Sequence
		}
	}
	return max
}
