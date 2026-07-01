package grpc_test

import (
	"time"

	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/bete7512/pulse/internal/repos"
	"go.uber.org/mock/gomock"
)

// TestWire_PauseDispatch routes the reason to the service over the wire.
func (s *ServerSuite) TestWire_PauseDispatch() {
	s.pause.EXPECT().Pause(gomock.Any(), "db migration window").Return(nil)

	client := s.dial(time.Second)
	_, err := client.PauseDispatch(ctx(), &pulsev1.PauseDispatchRequest{Reason: "db migration window"})
	s.NoError(err)
}

// TestWire_ResumeDispatch calls the service's Resume.
func (s *ServerSuite) TestWire_ResumeDispatch() {
	s.pause.EXPECT().Resume(gomock.Any()).Return(nil)

	client := s.dial(time.Second)
	_, err := client.ResumeDispatch(ctx(), &pulsev1.ResumeDispatchRequest{})
	s.NoError(err)
}

// TestWire_GetDispatchStatus maps the service DTO (incl. paused_at) to the wire response.
func (s *ServerSuite) TestWire_GetDispatchStatus() {
	pausedAt := time.Now()
	s.pause.EXPECT().Status(gomock.Any()).Return(repos.DispatchControl{
		Paused: true, Reason: "maint", PausedAt: &pausedAt,
	}, nil)

	client := s.dial(time.Second)
	resp, err := client.GetDispatchStatus(ctx(), &pulsev1.GetDispatchStatusRequest{})
	s.Require().NoError(err)
	s.True(resp.Paused)
	s.Equal("maint", resp.Reason)
	s.Require().NotNil(resp.PausedAt)
	s.WithinDuration(pausedAt, resp.PausedAt.AsTime(), time.Second)
}
