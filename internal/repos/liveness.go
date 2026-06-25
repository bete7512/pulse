package repos

import (
	"context"
	"time"
)

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/livenessrepo_mock.go -package=mocks github.com/bete7512/pulse/internal/repos LivenessRepo

// LivenessRepo tracks per-running-job liveness (separate from the event log): one row
// per running job, marked on claim, renewed by heartbeats, cleared when it leaves
// Running. A row past its deadline means the worker stopped proving liveness.
type LivenessRepo interface {
	// Mark records (or replaces) the liveness deadline for a freshly claimed job.
	Mark(ctx context.Context, jobID, workerID string, expiresAt time.Time) error
	// Renew pushes the deadline forward, but only for the worker that owns it — a
	// stale/zombie worker's heartbeat (wrong worker_id) is silently ignored (fencing).
	Renew(ctx context.Context, jobID, workerID string, expiresAt time.Time) error
	// Clear drops the record once the job is no longer running.
	Clear(ctx context.Context, jobID string) error
	// Expired returns ids of jobs whose liveness deadline has passed.
	Expired(ctx context.Context) ([]string, error)
}
