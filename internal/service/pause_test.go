package service_test

import (
	"errors"
	"time"

	"github.com/bete7512/pulse/internal/repos"
	"github.com/bete7512/pulse/internal/service"
	"go.uber.org/mock/gomock"
)

// TestPause_WritesRowAndFlipsGate: Pause persists the switch AND flips the local gate so the
// serving instance reacts immediately.
func (s *ServiceSuite) TestPause_WritesRowAndFlipsGate() {
	gate := service.NewDispatchGate()
	s.dispatch.EXPECT().SetPaused(gomock.Any(), true, "maint").Return(nil)

	p := service.NewPauseControl(s.dispatch, gate, time.Second, nil)
	s.NoError(p.Pause(ctx(), "maint"))
	s.True(gate.Paused())
}

// TestPause_DBErrorLeavesGateUntouched: a failed write must not flip the gate — the caller
// sees the error and dispatch keeps running.
func (s *ServiceSuite) TestPause_DBErrorLeavesGateUntouched() {
	gate := service.NewDispatchGate()
	s.dispatch.EXPECT().SetPaused(gomock.Any(), true, "x").Return(errors.New("db down"))

	p := service.NewPauseControl(s.dispatch, gate, time.Second, nil)
	s.Error(p.Pause(ctx(), "x"))
	s.False(gate.Paused())
}

// TestResume_ClearsGate: Resume writes the switch off and lowers the gate.
func (s *ServiceSuite) TestResume_ClearsGate() {
	gate := service.NewDispatchGate()
	s.dispatch.EXPECT().SetPaused(gomock.Any(), false, "").Return(nil)

	p := service.NewPauseControl(s.dispatch, gate, time.Second, nil)
	s.NoError(p.Resume(ctx()))
	s.False(gate.Paused())
}

// TestPrime_SetsGateFromRow: boot-prime reads the durable row into the gate, so a pause set
// before a restart survives it.
func (s *ServiceSuite) TestPrime_SetsGateFromRow() {
	gate := service.NewDispatchGate()
	s.dispatch.EXPECT().Get(gomock.Any()).Return(repos.DispatchControl{Paused: true}, nil)

	p := service.NewPauseControl(s.dispatch, gate, time.Second, nil)
	s.NoError(p.Prime(ctx()))
	s.True(gate.Paused())
}

// TestRefresh_FailOpenKeepsLastValue: a refresh read error leaves the gate at its last known
// value (fail-open) — a transient DB blip can't silently halt dispatch.
func (s *ServiceSuite) TestRefresh_FailOpenKeepsLastValue() {
	gate := service.NewDispatchGate()
	s.dispatch.EXPECT().Get(gomock.Any()).Return(repos.DispatchControl{Paused: true}, nil)
	p := service.NewPauseControl(s.dispatch, gate, time.Second, nil)
	s.Require().NoError(p.Prime(ctx()))
	s.Require().True(gate.Paused())

	s.dispatch.EXPECT().Get(gomock.Any()).Return(repos.DispatchControl{}, errors.New("db down"))
	service.Refresh(p, ctx())
	s.True(gate.Paused()) // unchanged despite the error
}
