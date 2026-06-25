package service_test

import (
	"context"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/service"
	"go.uber.org/mock/gomock"
)

// fakeFailer records which job ids the watchdog tried to fail (and can simulate the
// benign already-terminal race via err).
type fakeFailer struct {
	failed []string
	err    error
}

func (f *fakeFailer) FailJob(_ context.Context, id, _ string) error {
	f.failed = append(f.failed, id)
	return f.err
}

// TestWatchdog_Sweep covers the recovery sweep: it fails both the liveness-expired job and
// the orphaned (no-liveness) job, routing each into the retry path; and a job that finished
// between the query and the recover attempt fails the Running invariant, which the watchdog
// swallows (no sweep error).
func (s *ServiceSuite) TestWatchdog_Sweep() {
	anyDuration := gomock.AssignableToTypeOf(time.Duration(0))
	cases := []struct {
		name       string
		expired    []string
		orphaned   []string
		failerErr  error
		wantFailed []string
	}{
		{
			name:       "recovers expired and orphaned jobs",
			expired:    []string{"expired-1"},
			orphaned:   []string{"orphan-1"},
			wantFailed: []string{"expired-1", "orphan-1"},
		},
		{
			name:       "swallows benign already-terminal race",
			expired:    []string{"already-done"},
			orphaned:   nil,
			failerErr:  domain.ErrInvalidTransition,
			wantFailed: []string{"already-done"},
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.SetupTest() // fresh mocks per sub-case
			failer := &fakeFailer{err: tc.failerErr}
			s.live.EXPECT().Expired(gomock.Any()).Return(tc.expired, nil)
			s.events.EXPECT().OrphanedRunning(gomock.Any(), anyDuration).Return(tc.orphaned, nil)

			wd := service.NewWatchdog(s.live, s.events, failer, time.Minute, time.Second, nil)
			s.NoError(service.Sweep(wd, ctx()))
			s.Equal(tc.wantFailed, failer.failed)
		})
	}
}
