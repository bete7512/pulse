package grpc_test

import (
	"context"
	"net"
	"time"

	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/bete7512/pulse/internal/domain"
	grpcserver "github.com/bete7512/pulse/internal/transport/grpc"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// dial stands up a REAL grpc.Server (with the production ServerOptions, so error mapping is
// exercised) on an in-memory bufconn listener, wired to the suite's mock services, and returns
// a connected generated client. Unlike the direct-call handler tests, this drives requests
// across the actual gRPC wire: serialization, interceptors, status codes, and streaming.
//
// interval tunes the dispatcher poll so a StreamJobs round-trip is fast. Cleanup tears the
// server down before the gomock controller's Finish runs (t.Cleanup is LIFO and the controller
// registered its cleanup first in SetupTest), so the dispatch goroutine has stopped — and its
// EXPECT calls are settled — by the time Finish verifies.
func (s *ServerSuite) dial(interval time.Duration) pulsev1.PulseServiceClient {
	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer(grpcserver.ServerOptions()...)
	pulsev1.RegisterPulseServiceServer(gs, grpcserver.New(s.svc, s.schedules,
		grpcserver.WithDispatchInterval(interval), grpcserver.WithPauseControl(s.pause)))
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.Require().NoError(err)
	s.T().Cleanup(func() { conn.Close(); gs.Stop() })
	return pulsev1.NewPulseServiceClient(conn)
}

// TestWire_StreamJobs_DeliversAssignments is the primary win: a worker's StreamJobs call
// receives claimed jobs over a real stream, with the assignment fields mapped correctly —
// the path the direct dispatcher seam can't reach (Send + stream.Context()).
func (s *ServerSuite) TestWire_StreamJobs_DeliversAssignments() {
	topics := []string{"email"}
	pending := []domain.Job{
		{ID: "a", Topic: "email", Payload: []byte(`{}`), Attempts: 0, Priority: 9}, // attempt 1
		{ID: "c", Topic: "email", Attempts: 2},                                     // attempt 3
	}
	// First tick yields the backlog; later ticks find nothing (jobs already claimed).
	s.svc.EXPECT().ListPendingJobsByTopics(gomock.Any(), topics).Return(pending, nil).Times(1)
	s.svc.EXPECT().ListPendingJobsByTopics(gomock.Any(), topics).Return(nil, nil).AnyTimes()
	s.svc.EXPECT().StartJob(gomock.Any(), "a", "w1").Return(nil)
	s.svc.EXPECT().StartJob(gomock.Any(), "c", "w1").Return(nil)

	client := s.dial(5 * time.Millisecond)
	stream, err := client.StreamJobs(ctx(), &pulsev1.StreamJobsRequest{Topics: topics, WorkerId: "w1"})
	s.Require().NoError(err)

	first, err := stream.Recv()
	s.Require().NoError(err)
	s.Equal("a", first.Assignment.JobId)
	s.Equal(int32(1), first.Assignment.Attempt)
	s.Equal(int32(9), first.Assignment.Priority) // priority survives the wire
	s.Equal([]byte(`{}`), first.Assignment.Payload)

	second, err := stream.Recv()
	s.Require().NoError(err)
	s.Equal("c", second.Assignment.JobId)
	s.Equal(int32(3), second.Assignment.Attempt)
}

// TestWire_StreamJobs_ClientCancelStops asserts client-side cancellation propagates to the
// server via stream.Context(): Recv returns codes.Canceled and the dispatch loop exits (the
// gomock controller's Finish would flag a leaked goroutine still calling the mock).
func (s *ServerSuite) TestWire_StreamJobs_ClientCancelStops() {
	s.svc.EXPECT().ListPendingJobsByTopics(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	client := s.dial(5 * time.Millisecond)
	cctx, cancel := context.WithCancel(context.Background())
	stream, err := client.StreamJobs(cctx, &pulsev1.StreamJobsRequest{Topics: []string{"email"}, WorkerId: "w1"})
	s.Require().NoError(err)

	cancel()
	_, err = stream.Recv()
	s.Equal(codes.Canceled, status.Code(err))
}

// TestWire_GetJob_NotFoundMapsToStatus is the error-mapping payoff: a service ErrNotFound
// becomes codes.NotFound on the wire (it serializes to a code, so errors.Is no longer works
// client-side — the interceptor is what gives the client a usable code).
func (s *ServerSuite) TestWire_GetJob_NotFoundMapsToStatus() {
	s.svc.EXPECT().GetJob(gomock.Any(), "missing").Return(nil, pkg_errors.ErrNotFound)

	client := s.dial(time.Second)
	_, err := client.GetJob(ctx(), &pulsev1.GetJobRequest{JobId: "missing"})
	s.Equal(codes.NotFound, status.Code(err))
}

// TestWire_SubmitJob_GenericErrorMapsToInternal: a non-sentinel error maps to codes.Internal
// (the default arm) rather than leaking as codes.Unknown.
func (s *ServerSuite) TestWire_SubmitJob_GenericErrorMapsToInternal() {
	s.svc.EXPECT().Submit(gomock.Any(), "email", gomock.Any(), 0).Return("", errBoom)

	client := s.dial(time.Second)
	_, err := client.SubmitJob(ctx(), &pulsev1.SubmitJobRequest{Topic: "email"})
	s.Equal(codes.Internal, status.Code(err))
}

// TestWire_ReportResult_BothForks smoke-tests the worker-facing unary path over the wire:
// success completes, failure routes to the retry/dead-letter path. Both return OK.
func (s *ServerSuite) TestWire_ReportResult_BothForks() {
	s.svc.EXPECT().CompleteJob(gomock.Any(), "j").Return(nil)
	s.svc.EXPECT().FailJob(gomock.Any(), "j", "boom").Return(nil)

	client := s.dial(time.Second)
	_, err := client.ReportResult(ctx(), &pulsev1.ReportResultRequest{JobId: "j", Success: true})
	s.NoError(err)
	_, err = client.ReportResult(ctx(), &pulsev1.ReportResultRequest{JobId: "j", Success: false, Error: "boom"})
	s.NoError(err)
}

// TestWire_CreateSchedule_MissingSpecStaysInvalidArgument confirms a handler that already
// returns a gRPC status passes through the interceptor unchanged (no double-wrapping).
func (s *ServerSuite) TestWire_CreateSchedule_MissingSpecStaysInvalidArgument() {
	client := s.dial(time.Second)
	_, err := client.CreateSchedule(ctx(), &pulsev1.CreateScheduleRequest{Topic: "email"})
	s.Equal(codes.InvalidArgument, status.Code(err))
}
