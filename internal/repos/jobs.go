package repos

import (
	"context"

	"github.com/bete7512/pulse/internal/domain"
)

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/jobrepo_mock.go -package=mocks github.com/bete7512/pulse/internal/repos JobRepo,ProjectionRepo

// JobRepo is the read side of the jobs projection (queries).
type JobRepo interface {
	GetJob(ctx context.Context, jobID string) (*domain.Job, error)
	ListJobs(ctx context.Context) ([]domain.Job, error)
}

// ProjectionRepo is the write side of the jobs projection, used by the projector to
// catch the read model up to the event log.
type ProjectionRepo interface {
	// LaggingJobs returns ids of jobs whose newest event sequence has outrun the
	// sequence already reflected in the jobs projection.
	LaggingJobs(ctx context.Context) ([]string, error)
	// Upsert writes a folded job into the read model, advancing last_sequence only forward.
	Upsert(ctx context.Context, job domain.Job, seq int64) error
}
