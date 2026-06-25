package grpc

import (
	"context"

	"github.com/bete7512/pulse/gen/pulsev1"
)

// Test-only seams: drive the dispatcher without a real gRPC stream. The sink stands in for
// stream.Send. Compiled only under `go test`; living in package grpc, they reach the
// unexported dispatcher behind the Server.

func (s *Server) DispatchReady(ctx context.Context, topics []string, workerID string, sink func(*pulsev1.JobAssignment) error) error {
	return s.dispatcher.dispatchReady(ctx, topics, workerID, sink)
}

func (s *Server) RunDispatch(ctx context.Context, topics []string, workerID string, sink func(*pulsev1.JobAssignment) error) error {
	return s.dispatcher.run(ctx, topics, workerID, sink)
}
