package grpc

import (
	"context"

	"github.com/bete7512/pulse/gen/pulsev1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PauseDispatch stops the server from claiming and sending new jobs to workers. Submits keep
// landing and running jobs keep finishing — only the start of new work is gated.
func (s *Server) PauseDispatch(ctx context.Context, r *pulsev1.PauseDispatchRequest) (*pulsev1.PauseDispatchResponse, error) {
	return &pulsev1.PauseDispatchResponse{}, s.pause.Pause(ctx, r.Reason)
}

// ResumeDispatch re-enables dispatch; the next tick claims the accumulated backlog.
func (s *Server) ResumeDispatch(ctx context.Context, _ *pulsev1.ResumeDispatchRequest) (*pulsev1.ResumeDispatchResponse, error) {
	return &pulsev1.ResumeDispatchResponse{}, s.pause.Resume(ctx)
}

// GetDispatchStatus reports whether dispatch is paused, since when, and why.
func (s *Server) GetDispatchStatus(ctx context.Context, _ *pulsev1.GetDispatchStatusRequest) (*pulsev1.GetDispatchStatusResponse, error) {
	ctrl, err := s.pause.Status(ctx)
	if err != nil {
		return nil, err
	}
	resp := &pulsev1.GetDispatchStatusResponse{Paused: ctrl.Paused, Reason: ctrl.Reason}
	if ctrl.PausedAt != nil {
		resp.PausedAt = timestamppb.New(*ctrl.PausedAt)
	}
	return resp, nil
}
