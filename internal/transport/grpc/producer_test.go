package grpc_test

import (
	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/bete7512/pulse/internal/domain"
	"go.uber.org/mock/gomock"
)

func (s *ServerSuite) TestSubmitJob() {
	s.svc.EXPECT().Submit(gomock.Any(), "email", []byte(`{}`)).Return("job-1", nil)

	resp, err := s.srv.SubmitJob(ctx(), &pulsev1.SubmitJobRequest{Topic: "email", Payload: []byte(`{}`)})
	s.NoError(err)
	s.Equal("job-1", resp.JobId)
}

func (s *ServerSuite) TestSubmitJob_PropagatesError() {
	s.svc.EXPECT().Submit(gomock.Any(), "email", gomock.AssignableToTypeOf([]byte(nil))).Return("", errBoom)

	_, err := s.srv.SubmitJob(ctx(), &pulsev1.SubmitJobRequest{Topic: "email"})
	s.ErrorIs(err, errBoom)
}

func (s *ServerSuite) TestGetJob_MapsStatusToView() {
	s.svc.EXPECT().GetJob(gomock.Any(), "j").Return(&domain.Job{ID: "j", Status: domain.Running}, nil)

	resp, err := s.srv.GetJob(ctx(), &pulsev1.GetJobRequest{JobId: "j"})
	s.NoError(err)
	s.Equal("j", resp.Job.JobId)
	s.Equal("RUNNING", resp.Job.Status)
}

func (s *ServerSuite) TestGetJob_PropagatesError() {
	s.svc.EXPECT().GetJob(gomock.Any(), "missing").Return(nil, errBoom)

	_, err := s.srv.GetJob(ctx(), &pulsev1.GetJobRequest{JobId: "missing"})
	s.ErrorIs(err, errBoom)
}
