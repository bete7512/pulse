package grpc

import (
	"context"

	"github.com/bete7512/pulse/gen/pulsev1"
)

// SubmitJob enqueues a job under a topic (producer side).
func (s *Server) SubmitJob(ctx context.Context, r *pulsev1.SubmitJobRequest) (*pulsev1.SubmitJobResponse, error) {
	id, err := s.svc.Submit(ctx, r.Topic, r.Payload)
	if err != nil {
		return nil, err
	}
	return &pulsev1.SubmitJobResponse{JobId: id}, nil
}

// GetJob returns a job's current status, read from the server's read model (producer side).
func (s *Server) GetJob(ctx context.Context, r *pulsev1.GetJobRequest) (*pulsev1.GetJobResponse, error) {
	job, err := s.svc.GetJob(ctx, r.JobId)
	if err != nil {
		return nil, err
	}
	return &pulsev1.GetJobResponse{Job: &pulsev1.JobView{
		JobId:  job.ID,
		Status: string(job.Status),
	}}, nil
}
