package grpc_test

import (
	"time"

	reposmocks "github.com/bete7512/pulse/internal/repos/mocks"
	"github.com/bete7512/pulse/internal/service"
	grpcserver "github.com/bete7512/pulse/internal/transport/grpc"
	"go.uber.org/mock/gomock"
)

// TestDispatchReady_PausedGateClaimsNothing: when the gate is paused, a dispatch tick claims
// and sends nothing — asserted by wiring the JobService mock with NO expectations, so any call
// (ListPendingJobsByTopics/StartJob) fails the test. It pauses the gate the real way (through
// PauseControl), so this also covers pause → gate → dispatcher end-to-end.
func (s *ServerSuite) TestDispatchReady_PausedGateClaimsNothing() {
	gate := service.NewDispatchGate()
	ctrlRepo := reposmocks.NewMockDispatchControlRepo(s.ctrl)
	ctrlRepo.EXPECT().SetPaused(gomock.Any(), true, "").Return(nil)
	pauseCtl := service.NewPauseControl(ctrlRepo, gate, time.Second, nil)
	s.Require().NoError(pauseCtl.Pause(ctx(), ""))

	srv := grpcserver.New(s.svc, s.schedules, grpcserver.WithGate(gate))
	sink := &recordingSink{}
	s.NoError(srv.DispatchReady(ctx(), []string{"email"}, "w1", sink.send))
	s.Empty(sink.got) // nothing dispatched; s.svc had no EXPECT so any claim would have failed
}
