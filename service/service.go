package service

import (
	"context"
	"errors"

	"github.com/bete7512/pulse/domain"
	"github.com/bete7512/pulse/eventstore"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"github.com/bete7512/pulse/query"
	"github.com/gofrs/uuid/v5"
)

// maxTransitionRetries bounds the optimistic-retry loop so a livelock can't spin
// forever; in practice one retry resolves the race (the loser re-folds and either
// fails the invariant or claims the next free sequence).
const maxTransitionRetries = 3

type Service struct {
	store eventstore.EventStore
	query query.JobReader
}

func New(store eventstore.EventStore, query query.JobReader) *Service {
	return &Service{
		store: store,
		query: query,
	}
}

func (s *Service) Submit(ctx context.Context, payload []byte) (string, error) {
	jobId, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	event := domain.Event{
		JobId:   jobId.String(),
		Payload: payload,
		Type:    domain.JobSubmitted,
	}
	return s.store.Append(ctx, event)
}

func (s *Service) GetJob(ctx context.Context, jobId string) (*domain.Job, error) {
	return s.query.GetJob(ctx, jobId)
}

// PendingJobs returns the ids of jobs awaiting a worker (latest event is JobSubmitted).
func (s *Service) PendingJobs(ctx context.Context) ([]string, error) {
	events, err := s.store.ListSubmitted(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.JobId
	}
	return ids, nil
}

// transition is the shared command flow: LoadEventsForJob → RebuildJob → cmd → Append.
// cmd is one of the domain.Job command methods (Start/Complete/Cancel); it folds
// the invariant and returns the next event, which we persist.
func (s *Service) transition(ctx context.Context, jobId string, cmd func(domain.Job) (domain.Event, error)) error {
	for attempt := 0; ; attempt++ {
		events, err := s.store.LoadEventsForJob(ctx, jobId)
		if err != nil {
			return err
		}
		job := domain.RebuildJob(events)
		event, err := cmd(job)
		if err != nil {
			return err
		}
		_, err = s.store.Append(ctx, event)
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

func (s *Service) StartJob(ctx context.Context, jobId string) error {
	return s.transition(ctx, jobId, domain.Job.Start)
}

func (s *Service) CompleteJob(ctx context.Context, jobId string) error {
	return s.transition(ctx, jobId, domain.Job.Complete)
}

func (s *Service) CancelJob(ctx context.Context, jobId string) error {
	return s.transition(ctx, jobId, domain.Job.Cancel)
}
