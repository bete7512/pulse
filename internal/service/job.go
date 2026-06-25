package service

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/job_mock.go -package=mocks github.com/bete7512/pulse/internal/service JobService

import (
	"context"
	"errors"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"github.com/gofrs/uuid/v5"
)

// maxTransitionRetries bounds the optimistic-retry loop so a livelock can't spin
// forever; in practice one retry resolves the race (the loser re-folds and either
// fails the invariant or claims the next free sequence).
const maxTransitionRetries = 3

// JobService is the application layer: it turns requests into event-sourced commands and
// queries. The gRPC transport depends on this interface (and mocks it in tests).
type JobService interface {
	Submit(ctx context.Context, topic string, payload []byte) (string, error)
	GetJob(ctx context.Context, jobID string) (*domain.Job, error)
	PendingJobs(ctx context.Context) ([]string, error)
	ListPendingJobsByTopics(ctx context.Context, topics []string) ([]domain.Job, error)
	GetJobForDispatch(ctx context.Context, jobID string) (*domain.Job, error)
	StartJob(ctx context.Context, jobID, workerID string) error
	CompleteJob(ctx context.Context, jobID string) error
	CancelJob(ctx context.Context, jobID string) error
	FailJob(ctx context.Context, jobID, reason string) error
	Heartbeat(ctx context.Context, jobID, workerID string) error
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

func (s *jobService) Submit(ctx context.Context, topic string, payload []byte) (string, error) {
	jobId, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return s.events.Append(ctx, domain.Event{
		JobId:   jobId.String(),
		Topic:   topic,
		Payload: payload,
		Type:    domain.JobSubmitted,
	})
}

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

// GetJobForDispatch folds a single job from its event stream.
func (s *jobService) GetJobForDispatch(ctx context.Context, jobID string) (*domain.Job, error) {
	events, err := s.events.LoadEventsForJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	job := domain.RebuildJob(events)
	return &job, nil
}

// transition is the shared command flow: LoadEventsForJob → RebuildJob → cmd → AppendBatch.
// cmd folds the invariant and returns the event(s) to persist; the batch lands atomically
// so a multi-event outcome (e.g. Failed + Retried) is all-or-nothing.
func (s *jobService) transition(ctx context.Context, jobID string, cmd func(domain.Job) ([]domain.Event, error)) error {
	for attempt := 0; ; attempt++ {
		events, err := s.events.LoadEventsForJob(ctx, jobID)
		if err != nil {
			return err
		}
		job := domain.RebuildJob(events)
		next, err := cmd(job)
		if err != nil {
			return err
		}
		err = s.events.AppendBatch(ctx, next)
		if err == nil {
			return nil
		}
		// Another writer claimed the next sequence first. Re-fold and re-evaluate: if they
		// made our transition, cmd now returns ErrInvalidTransition (we lost the race,
		// correctly); if their event was unrelated, we retry at the freshly advanced sequence.
		if !errors.Is(err, pkg_errors.ErrSequenceConflict) || attempt >= maxTransitionRetries {
			return err
		}
	}
}

// single adapts a single-event command (Start/Complete/Cancel) to the multi-event
// signature transition expects.
func single(cmd func(domain.Job) (domain.Event, error)) func(domain.Job) ([]domain.Event, error) {
	return func(j domain.Job) ([]domain.Event, error) {
		e, err := cmd(j)
		if err != nil {
			return nil, err
		}
		return []domain.Event{e}, nil
	}
}

// StartJob claims a job for workerID, then marks liveness so the watchdog can tell the
// worker is alive. The mark is best-effort (the watchdog's no-liveness fallback covers a
// failed write), so a liveness error never fails the claim.
func (s *jobService) StartJob(ctx context.Context, jobID, workerID string) error {
	if err := s.transition(ctx, jobID, single(domain.Job.Start)); err != nil {
		return err
	}
	_ = s.liveness.Mark(ctx, jobID, workerID, time.Now().Add(s.livenessTTL))
	return nil
}

func (s *jobService) CompleteJob(ctx context.Context, jobID string) error {
	return s.endRun(ctx, jobID, single(domain.Job.Complete))
}

func (s *jobService) CancelJob(ctx context.Context, jobID string) error {
	return s.endRun(ctx, jobID, single(domain.Job.Cancel))
}

// FailJob records a failure; Fail emits Failed + (Retried | DeadLettered), persisted
// atomically by AppendBatch.
func (s *jobService) FailJob(ctx context.Context, jobID, reason string) error {
	now := time.Now()
	return s.endRun(ctx, jobID, func(j domain.Job) ([]domain.Event, error) {
		return j.Fail(reason, now)
	})
}

// endRun runs a transition out of Running, then clears liveness (best-effort: a stale
// record is harmless — the watchdog's FailJob hits the Running invariant and is ignored).
func (s *jobService) endRun(ctx context.Context, jobID string, cmd func(domain.Job) ([]domain.Event, error)) error {
	if err := s.transition(ctx, jobID, cmd); err != nil {
		return err
	}
	_ = s.liveness.Clear(ctx, jobID)
	return nil
}

// Heartbeat renews a running job's liveness. Only the owning worker can renew it (fencing).
func (s *jobService) Heartbeat(ctx context.Context, jobID, workerID string) error {
	return s.liveness.Renew(ctx, jobID, workerID, time.Now().Add(s.livenessTTL))
}
