package grpc

import (
	"context"

	"github.com/bete7512/pulse/gen/pulsev1"
)

// StreamJobs subscribes a worker to its topics and streams claimed jobs to it. The dispatch
// policy lives in the dispatcher; this handler just adapts the gRPC stream to a sink.
func (s *Server) StreamJobs(req *pulsev1.StreamJobsRequest, stream pulsev1.PulseService_StreamJobsServer) error {
	return s.dispatcher.run(stream.Context(), req.Topics, req.WorkerId, func(a *pulsev1.JobAssignment) error {
		return stream.Send(&pulsev1.StreamJobsResponse{Assignment: a})
	})
}

// ReportResult records a worker's terminal outcome for a job: success completes it, failure
// routes it through the retry/dead-letter path.
func (s *Server) ReportResult(ctx context.Context, r *pulsev1.ReportResultRequest) (*pulsev1.ReportResultResponse, error) {
	if r.Success {
		return &pulsev1.ReportResultResponse{}, s.svc.CompleteJob(ctx, r.JobId)
	}
	return &pulsev1.ReportResultResponse{}, s.svc.FailJob(ctx, r.JobId, r.Error)
}

// Heartbeat renews a running job's liveness — the worker proving it is still alive.
func (s *Server) Heartbeat(ctx context.Context, r *pulsev1.HeartbeatRequest) (*pulsev1.HeartbeatResponse, error) {
	if err := s.svc.Heartbeat(ctx, r.JobId, r.WorkerId); err != nil {
		return nil, err
	}
	return &pulsev1.HeartbeatResponse{}, nil
}
