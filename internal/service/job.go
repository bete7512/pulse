package service

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/job_mock.go -package=mocks github.com/bete7512/pulse/internal/service JobService
//go:generate go run go.uber.org/mock/mockgen -destination=mocks/jobreader_mock.go -package=mocks github.com/bete7512/pulse/internal/service JobReader

import (
	"context"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos"
)

// JobService is the application layer, split along CQRS lines: JobReader (queries, this
// file) and JobWriter (commands, event.go). The gRPC transport depends on the composite;
// a read-only or write-only collaborator can depend on just one half.
type JobService interface {
	JobReader
	JobWriter
}

// JobReader is the read side — queries that never mutate state. GetJob reads the
// eventually-consistent jobs read model; the dispatch queries fold the authoritative event
// log (the write side can't trust the lagging projection for claim decisions — ADR-0004).
type JobReader interface {
	GetJob(ctx context.Context, jobID string) (*domain.Job, error)
	PendingJobs(ctx context.Context) ([]string, error)
	ListPendingJobsByTopics(ctx context.Context, topics []string) ([]domain.Job, error)
}

type jobService struct {
	events      repos.EventRepo
	jobs        repos.JobRepo
	liveness    repos.LivenessRepo
	livenessTTL time.Duration
}

func New(events repos.EventRepo, jobs repos.JobRepo, live repos.LivenessRepo, livenessTTL time.Duration) JobService {
	return &jobService{events: events, jobs: jobs, liveness: live, livenessTTL: livenessTTL}
}

// GetJob reads a job's current status from the jobs read model (cheap, eventually consistent).
func (s *jobService) GetJob(ctx context.Context, jobID string) (*domain.Job, error) {
	return s.jobs.GetJob(ctx, jobID)
}

// PendingJobs returns the ids of every job awaiting a worker.
func (s *jobService) PendingJobs(ctx context.Context) ([]string, error) {
	events, err := s.events.ListEventsByTopics(ctx, nil)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.JobId
	}
	return ids, nil
}

// ListPendingJobsByTopics returns the dispatch-ready jobs whose topic is in topics — the
// gRPC StreamJobs poll query. Each job is folded from its full stream so the dispatcher
// has the original payload/topic and the attempt count (a retried job's head event has neither).
func (s *jobService) ListPendingJobsByTopics(ctx context.Context, topics []string) ([]domain.Job, error) {
	heads, err := s.events.ListEventsByTopics(ctx, topics)
	if err != nil {
		return nil, err
	}
	jobs := make([]domain.Job, 0, len(heads))
	for _, h := range heads {
		events, err := s.events.LoadEventsForJob(ctx, h.JobId)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, domain.RebuildJob(events))
	}
	return jobs, nil
}
