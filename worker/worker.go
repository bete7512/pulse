package worker

import (
	"context"
	"time"
)

// JobService is the slice of the service the worker drives jobs through: it finds
// jobs awaiting work and changes their state via domain command methods (and their
// invariants) instead of raw event appends.
type JobService interface {
	PendingJobs(ctx context.Context) ([]string, error)
	StartJob(ctx context.Context, jobId string) error
	CompleteJob(ctx context.Context, jobId string) error
}

type Worker struct {
	svc JobService
}

func New(svc JobService) *Worker {
	return &Worker{svc: svc}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ids, _ := w.svc.PendingJobs(ctx)
			for _, id := range ids {
				w.runJob(ctx, id)
			}
		}
	}
}

func (w *Worker) runJob(ctx context.Context, jobId string) error {
	if err := w.svc.StartJob(ctx, jobId); err != nil {
		return err
	}
	// TODO: magics here that to be called by developer function over the network
	// via sdk, grpc,..... however whatever is being done here later it will help developer execute their code
	return w.svc.CompleteJob(ctx, jobId)
}
