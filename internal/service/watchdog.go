package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/bete7512/pulse/internal/domain"
)

// Watchdog periodically recovers jobs whose worker appears to have died mid-run:
// expired liveness (worker stopped heartbeating) or a running job that never got a
// liveness record. It fails such jobs via the Service, routing them through the
// normal retry path so a crashed worker's job is re-dispatched elsewhere.
//
// Recovery is at-least-once, not exactly-once: a recovered job may in fact still be
// running (e.g. a network-partitioned worker that can't heartbeat), so handlers MUST
// be idempotent (ADR-0004).
type Watchdog struct {
	svc      *Service
	grace    time.Duration // fallback window for running jobs that never got a liveness record
	interval time.Duration
	logger   *slog.Logger
}

func NewWatchdog(svc *Service, grace, interval time.Duration, logger *slog.Logger) *Watchdog {
	if logger == nil {
		logger = slog.Default()
	}
	return &Watchdog{svc: svc, grace: grace, interval: interval, logger: logger}
}

func (w *Watchdog) Run(ctx context.Context) {
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

func (w *Watchdog) sweep(ctx context.Context) error {
	expired, err := w.svc.liveness.Expired(ctx)
	if err != nil {
		return err
	}
	orphaned, err := w.svc.store.OrphanedRunning(ctx, w.grace)
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

func (w *Watchdog) recover(ctx context.Context, id, reason string) {
	err := w.svc.FailJob(ctx, id, reason)
	// A job that finished between the query and now fails the Running invariant —
	// that's the benign race (or a stale liveness record), not an error worth surfacing.
	switch {
	case err == nil:
		w.logger.Info("recovered stuck job", "job_id", id, "reason", reason)
	case errors.Is(err, domain.ErrInvalidTransition):
		// already terminal/retried — ignore
	default:
		w.logger.Warn("recovering stuck job failed", "job_id", id, "error", err)
	}
}
