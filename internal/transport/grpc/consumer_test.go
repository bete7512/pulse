package grpc_test

import (
	"github.com/bete7512/pulse/gen/pulsev1"
	"go.uber.org/mock/gomock"
)

// TestReportResult tables the success/failure fork: success completes the job, failure
// routes it (with its reason) into FailJob.
func (s *ServerSuite) TestReportResult() {
	cases := []struct {
		name    string
		req     *pulsev1.ReportResultRequest
		arrange func()
	}{
		{
			name:    "success completes the job",
			req:     &pulsev1.ReportResultRequest{JobId: "j", Success: true},
			arrange: func() { s.svc.EXPECT().CompleteJob(gomock.Any(), "j").Return(nil) },
		},
		{
			name:    "failure fails the job with its reason",
			req:     &pulsev1.ReportResultRequest{JobId: "j", Success: false, Error: "boom"},
			arrange: func() { s.svc.EXPECT().FailJob(gomock.Any(), "j", "boom").Return(nil) },
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.SetupTest() // fresh mocks per sub-case
			tc.arrange()
			_, err := s.srv.ReportResult(ctx(), tc.req)
			s.NoError(err)
		})
	}
}

func (s *ServerSuite) TestReportResult_PropagatesError() {
	s.svc.EXPECT().CompleteJob(gomock.Any(), "j").Return(errBoom)

	_, err := s.srv.ReportResult(ctx(), &pulsev1.ReportResultRequest{JobId: "j", Success: true})
	s.ErrorIs(err, errBoom)
}

func (s *ServerSuite) TestHeartbeat() {
	s.svc.EXPECT().Heartbeat(gomock.Any(), "j", "w").Return(nil)

	_, err := s.srv.Heartbeat(ctx(), &pulsev1.HeartbeatRequest{JobId: "j", WorkerId: "w"})
	s.NoError(err)
}

func (s *ServerSuite) TestHeartbeat_PropagatesError() {
	s.svc.EXPECT().Heartbeat(gomock.Any(), "j", "w").Return(errBoom)

	_, err := s.srv.Heartbeat(ctx(), &pulsev1.HeartbeatRequest{JobId: "j", WorkerId: "w"})
	s.ErrorIs(err, errBoom)
}
