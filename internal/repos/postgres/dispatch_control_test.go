package postgres_test

import (
	"github.com/bete7512/pulse/internal/repos/postgres"
)

// TestDispatchControl_GetAndSetPaused exercises the singleton pause switch end-to-end and pins
// the resume semantics: resuming clears the reason but KEEPS paused_at as "last paused at".
func (s *RepoSuite) TestDispatchControl_GetAndSetPaused() {
	ctrl := postgres.NewDispatchControl(s.pool)

	// The migration seeds one row; TRUNCATE doesn't touch dispatch_control, so reset to a known
	// default for a deterministic start regardless of prior tests.
	_, err := s.pool.Exec(bg(), `UPDATE dispatch_control SET paused=false, reason=NULL, paused_at=NULL WHERE id=1`)
	s.Require().NoError(err)

	got, err := ctrl.Get(bg())
	s.Require().NoError(err)
	s.False(got.Paused)
	s.Empty(got.Reason)
	s.Nil(got.PausedAt)

	// pause: stamps paused_at + records the operator reason.
	s.Require().NoError(ctrl.SetPaused(bg(), true, "db migration window"))
	got, err = ctrl.Get(bg())
	s.Require().NoError(err)
	s.True(got.Paused)
	s.Equal("db migration window", got.Reason)
	s.Require().NotNil(got.PausedAt)
	pausedAt := *got.PausedAt

	// resume: clears reason, keeps paused_at unchanged.
	s.Require().NoError(ctrl.SetPaused(bg(), false, ""))
	got, err = ctrl.Get(bg())
	s.Require().NoError(err)
	s.False(got.Paused)
	s.Empty(got.Reason)
	s.Require().NotNil(got.PausedAt)
	s.Equal(pausedAt, *got.PausedAt)
}
