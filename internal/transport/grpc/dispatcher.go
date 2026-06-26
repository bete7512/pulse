package grpc

import (
	"context"
	"time"

	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/service"
)

// dispatchInterval is how often a connected worker's stream polls the store for ready work.
// It bounds dispatch latency (up to one tick) and the poll rate per connected worker.
const dispatchInterval = 500 * time.Millisecond

// assignmentSink delivers one job assignment to a worker. It abstracts stream.Send so the
// dispatch policy is testable without a real gRPC stream.
type assignmentSink func(*pulsev1.JobAssignment) error

// dispatcher owns the streaming poll→claim→send loop: the write-side reader that turns
// pending jobs into assignments. One runs per connected worker (per StreamJobs call).
type dispatcher struct {
	svc      service.JobService
	interval time.Duration
}

func newDispatcher(svc service.JobService, interval time.Duration) *dispatcher {
	return &dispatcher{svc: svc, interval: interval}
}

// run polls every interval until ctx is done (the worker disconnected), dispatching ready
// jobs to sink. It returns ctx.Err() on disconnect, or a sink error if the stream breaks.
func (d *dispatcher) run(ctx context.Context, topics []string, workerID string, sink assignmentSink) error {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.dispatchReady(ctx, topics, workerID, sink); err != nil {
				return err // the stream broke — stop this worker's loop
			}
		}
	}
}

// dispatchReady claims and sends one round of ready jobs. Transient poll errors and lost
// claim races are skipped (retry next tick / another worker won the job); only a sink error
// — the stream itself failing — is returned, which ends the loop.
func (d *dispatcher) dispatchReady(ctx context.Context, topics []string, workerID string, sink assignmentSink) error {
	jobs, err := d.svc.ListPendingJobsByTopics(ctx, topics)
	if err != nil {
		return nil // transient poll failure: don't kill the stream, just retry next tick
	}
	for _, j := range jobs {
		if err := d.svc.StartJob(ctx, j.ID, workerID); err != nil {
			continue // lost the claim race or no longer dispatchable
		}
		if err := sink(assignmentFrom(j)); err != nil {
			return err
		}
	}
	return nil
}

// assignmentFrom maps a folded domain job to its wire assignment. Attempt is attempts+1:
// the job is folded before the claim, and the worker sees the attempt number it will run.
func assignmentFrom(j domain.Job) *pulsev1.JobAssignment {
	return &pulsev1.JobAssignment{
		JobId:    j.ID,
		Topic:    j.Topic,
		Payload:  j.Payload,
		Attempt:  int32(j.Attempts + 1),
		Priority: int32(j.Priority),
	}
}
