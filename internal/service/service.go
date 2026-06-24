package service

import (
	"context"
	"errors"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/storage/postgres/eventstore"
	"github.com/bete7512/pulse/internal/storage/postgres/jobs"
	"github.com/bete7512/pulse/internal/storage/postgres/liveness"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"github.com/gofrs/uuid/v5"
)

// maxTransitionRetries bounds the optimistic-retry loop so a livelock can't spin
// forever; in practice one retry resolves the race (the loser re-folds and either
// fails the invariant or claims the next free sequence).
const maxTransitionRetries = 3

type Service struct {
	store       eventstore.EventStore
	jobStore    *jobs.Store
	liveness    liveness.Store
	livenessTTL time.Duration
}

func New(store eventstore.EventStore, jobStore *jobs.Store, live liveness.Store, livenessTTL time.Duration) *Service {
	return &Service{
		store:       store,
		jobStore:    jobStore,
		liveness:    live,
		livenessTTL: livenessTTL,
	}
}

func (s *Service) Submit(ctx context.Context, topic string, payload []byte) (string, error) {
	jobId, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	event := domain.Event{
		JobId:   jobId.String(),
		Topic:   topic,
		Payload: payload,
		Type:    domain.JobSubmitted,
	}
	return s.store.Append(ctx, event)
}

func (s *Service) GetJob(ctx context.Context, jobId string) (*domain.Job, error) {
	return s.jobStore.GetJob(ctx, jobId)
}

// PendingJobs returns the ids of every job awaiting a worker (latest event is JobSubmitted).
func (s *Service) PendingJobs(ctx context.Context) ([]string, error) {
	events, err := s.store.ListEventsByTopics(ctx, nil)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.JobId
	}
	return ids, nil
}

// ListPendingJobsByTopics returns the dispatch-ready jobs whose topic is in topics —
// the gRPC StreamJobs poll query. Each job is folded from its full stream so the
// dispatcher has the original payload/topic and the attempt count (a retried job's
// head event carries neither).
func (s *Service) ListPendingJobsByTopics(ctx context.Context, topics []string) ([]domain.Job, error) {
	heads, err := s.store.ListEventsByTopics(ctx, topics)
	if err != nil {
		return nil, err
	}
	jobs := make([]domain.Job, 0, len(heads))
	for _, h := range heads {
		events, err := s.store.LoadEventsForJob(ctx, h.JobId)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, domain.RebuildJob(events))
	}
	return jobs, nil
}

// GetJobForDispatch folds a single job from its event stream so a worker can be
// handed its type and payload.
func (s *Service) GetJobForDispatch(ctx context.Context, jobId string) (*domain.Job, error) {
	events, err := s.store.LoadEventsForJob(ctx, jobId)
	if err != nil {
		return nil, err
	}
	job := domain.RebuildJob(events)
	return &job, nil
}

// transition is the shared command flow: LoadEventsForJob → RebuildJob → cmd → AppendBatch.
// cmd folds the invariant and returns the event(s) to persist; the batch lands
// atomically so a multi-event outcome (e.g. Failed + Retried) is all-or-nothing.
func (s *Service) transition(ctx context.Context, jobId string, cmd func(domain.Job) ([]domain.Event, error)) error {
	for attempt := 0; ; attempt++ {
		events, err := s.store.LoadEventsForJob(ctx, jobId)
		if err != nil {
			return err
		}
		job := domain.RebuildJob(events)
		next, err := cmd(job)
		if err != nil {
			return err
		}
		err = s.store.AppendBatch(ctx, next)
		if err == nil {
			return nil
		}
		// Another writer claimed the next sequence first. Re-fold and re-evaluate:
		// if they made our transition, cmd now returns ErrInvalidTransition (we lost
		// the race, correctly); if their event was unrelated, we retry our append at
		// the freshly advanced sequence. Any non-conflict error propagates as-is.
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

// StartJob claims a job for workerID. On success it marks liveness so the watchdog
// can tell the worker is alive; the mark is best-effort (the watchdog's no-liveness
// fallback covers a failed write), so a liveness error never fails the claim.
func (s *Service) StartJob(ctx context.Context, jobId, workerID string) error {
	if err := s.transition(ctx, jobId, single(domain.Job.Start)); err != nil {
		return err
	}
	_ = s.liveness.Mark(ctx, jobId, workerID, time.Now().Add(s.livenessTTL))
	return nil
}

func (s *Service) CompleteJob(ctx context.Context, jobId string) error {
	return s.endRun(ctx, jobId, single(domain.Job.Complete))
}

func (s *Service) CancelJob(ctx context.Context, jobId string) error {
	return s.endRun(ctx, jobId, single(domain.Job.Cancel))
}

// FailJob records a job failure (with its reason) through the same transition flow.
// Fail emits Failed + (Retried | DeadLettered), which AppendBatch persists atomically.
func (s *Service) FailJob(ctx context.Context, jobId, reason string) error {
	now := time.Now()
	return s.endRun(ctx, jobId, func(j domain.Job) ([]domain.Event, error) {
		return j.Fail(reason, now)
	})
}

// endRun runs a transition that takes the job out of Running and then clears its
// liveness (best-effort: a stale record is harmless — the watchdog's FailJob hits the
// Running invariant and is ignored, and a re-claim re-marks fresh liveness).
func (s *Service) endRun(ctx context.Context, jobId string, cmd func(domain.Job) ([]domain.Event, error)) error {
	if err := s.transition(ctx, jobId, cmd); err != nil {
		return err
	}
	_ = s.liveness.Clear(ctx, jobId)
	return nil
}

// Heartbeat renews a running job's liveness, proving its worker is still alive. Only
// the worker that owns it can renew it, so a zombie worker's heartbeat is ignored.
func (s *Service) Heartbeat(ctx context.Context, jobId, workerID string) error {
	return s.liveness.Renew(ctx, jobId, workerID, time.Now().Add(s.livenessTTL))
}
