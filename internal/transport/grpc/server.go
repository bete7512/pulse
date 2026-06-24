package grpc

import (
	"context"
	"time"

	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/bete7512/pulse/internal/service"
)

type Server struct {
	pulsev1.UnimplementedPulseServiceServer
	svc *service.Service
}

func New(svc *service.Service) *Server {
	return &Server{svc: svc}
}

func (s *Server) SubmitJob(ctx context.Context, r *pulsev1.SubmitJobRequest) (*pulsev1.SubmitJobResponse, error) {
	id, err := s.svc.Submit(ctx, r.Topic, r.Payload)
	if err != nil {
		return nil, err
	}
	return &pulsev1.SubmitJobResponse{JobId: id}, nil
}

func (s *Server) StreamJobs(req *pulsev1.StreamJobsRequest, stream pulsev1.PulseService_StreamJobsServer) error {
	ctx := stream.Context()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			jobs, err := s.svc.ListPendingJobsByTopics(ctx, req.Topics)
			if err != nil {
				continue
			}
			for _, j := range jobs {
				if err := s.svc.StartJob(ctx, j.ID, req.WorkerId); err != nil {
					continue
				}
				if err := stream.Send(&pulsev1.StreamJobsResponse{
					Assignment: &pulsev1.JobAssignment{
						JobId: j.ID, Topic: j.Topic, Payload: j.Payload, Attempt: int32(j.Attempts + 1),
					},
				}); err != nil {
					return err
				}
			}
		}
	}
}

func (s *Server) ReportResult(ctx context.Context, r *pulsev1.ReportResultRequest) (*pulsev1.ReportResultResponse, error) {
	if r.Success {
		return &pulsev1.ReportResultResponse{}, s.svc.CompleteJob(ctx, r.JobId)
	}
	return &pulsev1.ReportResultResponse{}, s.svc.FailJob(ctx, r.JobId, r.Error)
}

func (s *Server) Heartbeat(ctx context.Context, r *pulsev1.HeartbeatRequest) (*pulsev1.HeartbeatResponse, error) {
	if err := s.svc.Heartbeat(ctx, r.JobId, r.WorkerId); err != nil {
		return nil, err
	}
	return &pulsev1.HeartbeatResponse{}, nil
}
