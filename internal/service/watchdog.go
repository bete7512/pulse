package service

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/watchdog_mock.go -package=mocks github.com/bete7512/pulse/internal/service WatchdogService

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos"
)

// Failer fails a stuck job, routing it through the normal retry path. Satisfied by JobService.
type Failer interface {
	FailJob(ctx context.Context, jobID, reason string) error
}

// WatchdogService periodically recovers jobs whose worker appears to have died mid-run:
// expired liveness (stopped heartbeating) or a running job that never got a liveness
// record. It fails such jobs, routing them through the retry path so a crashed worker's job
// is re-dispatched elsewhere. The composition root depends on this interface; the sweep
// body is exercised via a test seam.
//
// Recovery is at-least-once, not exactly-once: a recovered job may in fact still be
// running (e.g. a network-partitioned worker that can't heartbeat), so handlers MUST be
// idempotent (ADR-0004).
type WatchdogService interface {
	Run(ctx context.Context)
}

type watchdogService struct {
	liveness repos.LivenessRepo
	events   repos.EventRepo
	failer   Failer
	grace    time.Duration // fallback window for running jobs that never got a liveness record
	interval time.Duration
	logger   *slog.Logger
}

func NewWatchdog(live repos.LivenessRepo, events repos.EventRepo, failer Failer, grace, interval time.Duration, logger *slog.Logger) WatchdogService {
	if logger == nil {
		logger = slog.Default()
	}
	return &watchdogService{liveness: live, events: events, failer: failer, grace: grace, interval: interval, logger: logger}
}

func (w *watchdogService) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.sweep(ctx); err != nil {
				w.logger.Error("watchdog sweep failed", "error", err)
			}
		}
	}
}

func (w *watchdogService) sweep(ctx context.Context) error {
	expired, err := w.liveness.Expired(ctx)
	if err != nil {
		return err
	}
	orphaned, err := w.events.OrphanedRunning(ctx, w.grace)
	if err != nil {
		return err
	}
	// expired (liveness lapsed) and orphaned (no liveness record) are disjoint sets.
	for _, id := range expired {
		w.recover(ctx, id, "worker liveness expired")
	}
	for _, id := range orphaned {
		w.recover(ctx, id, "no liveness record (worker never heartbeated)")
	}
	return nil
}

func (w *watchdogService) recover(ctx context.Context, id, reason string) {
	err := w.failer.FailJob(ctx, id, reason)
	// A job that finished between the query and now fails the Running invariant — that's
	// the benign race (or a stale liveness record), not an error worth surfacing.
	switch {
	case err == nil:
		w.logger.Info("recovered stuck job", "job_id", id, "reason", reason)
	case errors.Is(err, domain.ErrInvalidTransition):
		// already terminal/retried — ignore
	default:
		w.logger.Warn("recovering stuck job failed", "job_id", id, "error", err)
	}
}
