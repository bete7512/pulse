package service

import (
	"context"

	"github.com/bete7512/pulse/domain"
	"github.com/bete7512/pulse/eventstore"
	"github.com/gofrs/uuid/v5"
)

type Service struct {
	store eventstore.EventStore
}

func New(store eventstore.EventStore) *Service {
	return &Service{store: store}
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
	return s.store.Add(ctx, event)
}

func (s *Service) GetJob(ctx context.Context, jobId string) (*domain.Job, error) {
	events, err := s.store.Load(ctx, jobId)
	if err != nil {
		return nil, err
	}
	job := domain.RebuildJob(events)
	return &job, nil
}
