package worker

import (
	"context"
	"time"

	"github.com/bete7512/pulse/domain"
	"github.com/bete7512/pulse/eventstore"
)

type Worker struct {
	store eventstore.EventStore
}

func New(store eventstore.EventStore) *Worker {
	return &Worker{
		store: store,
	}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events, _ := w.store.ListSubmittedEvents(ctx)
			for _, e := range events {
				w.runJob(ctx, e.JobId)
			}
		}
	}
}

func (w *Worker) runJob(ctx context.Context, jobId string) error {
	event := domain.Event{
		JobId: jobId,
		Type:  domain.JobStarted,
	}
	w.store.Add(ctx, event)
	// TODO: magics here that to be called by developer function over the network
	// via sdk, grpc,..... however whatever is being done here later it will help developer execute their code
	event.Type = domain.JobCompleted
	w.store.Add(ctx, event)
	return nil
}
