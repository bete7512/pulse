package grpc_test

import (
	"context"
	"errors"

	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/bete7512/pulse/internal/domain"
	"go.uber.org/mock/gomock"
)

// recordingSink captures the assignments handed to it and can simulate a broken stream by
// returning err on send.
type recordingSink struct {
	got []*pulsev1.JobAssignment
	err error
}

func (r *recordingSink) send(a *pulsev1.JobAssignment) error {
	r.got = append(r.got, a)
	return r.err
}

// TestDispatchReady_ClaimsAndSendsSkippingLosers: every ready job is claimed and sent, with
// attempt = attempts+1; a job whose claim loses the race (StartJob errors) is skipped, not sent.
func (s *ServerSuite) TestDispatchReady_ClaimsAndSendsSkippingLosers() {
	topics := []string{"email"}
	pending := []domain.Job{
		{ID: "a", Topic: "email", Payload: []byte(`{}`), Attempts: 0}, // attempt 1
		{ID: "b", Topic: "email", Attempts: 1},                        // a retried job — but loses the claim
		{ID: "c", Topic: "email", Attempts: 2},                        // attempt 3
	}
	s.svc.EXPECT().ListPendingJobsByTopics(gomock.Any(), topics).Return(pending, nil)
	s.svc.EXPECT().StartJob(gomock.Any(), "a", "w1").Return(nil)
	s.svc.EXPECT().StartJob(gomock.Any(), "b", "w1").Return(domain.ErrInvalidTransition) // lost the race
	s.svc.EXPECT().StartJob(gomock.Any(), "c", "w1").Return(nil)

	sink := &recordingSink{}
	s.NoError(s.srv.DispatchReady(ctx(), topics, "w1", sink.send))

	s.Require().Len(sink.got, 2) // b skipped
	s.Equal("a", sink.got[0].JobId)
	s.Equal(int32(1), sink.got[0].Attempt)
	s.Equal([]byte(`{}`), sink.got[0].Payload)
	s.Equal("c", sink.got[1].JobId)
	s.Equal(int32(3), sink.got[1].Attempt)
}

// TestDispatchReady_PollErrorIsTransient: a failed poll does not break the stream — the round
// returns nil with nothing claimed or sent, so the loop simply retries next tick.
func (s *ServerSuite) TestDispatchReady_PollErrorIsTransient() {
	s.svc.EXPECT().ListPendingJobsByTopics(gomock.Any(), []string{"x"}).Return(nil, errors.New("db down"))

	sink := &recordingSink{}
	s.NoError(s.srv.DispatchReady(ctx(), []string{"x"}, "w", sink.send))
	s.Empty(sink.got)
}

// TestDispatchReady_SinkErrorStopsRound: a send failure (the stream itself breaking) is
// returned so the worker's loop ends.
func (s *ServerSuite) TestDispatchReady_SinkErrorStopsRound() {
	s.svc.EXPECT().ListPendingJobsByTopics(gomock.Any(), []string{"email"}).
		Return([]domain.Job{{ID: "a", Topic: "email"}}, nil)
	s.svc.EXPECT().StartJob(gomock.Any(), "a", "w").Return(nil)

	sink := &recordingSink{err: errBoom}
	err := s.srv.DispatchReady(ctx(), []string{"email"}, "w", sink.send)
	s.ErrorIs(err, errBoom)
}

// TestRunDispatch_StopsOnContextCancel: when the worker disconnects (ctx cancelled), the run
// loop returns the context error instead of spinning.
func (s *ServerSuite) TestRunDispatch_StopsOnContextCancel() {
	c, cancel := context.WithCancel(ctx())
	cancel() // worker already gone

	sink := &recordingSink{}
	err := s.srv.RunDispatch(c, []string{"x"}, "w", sink.send)
	s.ErrorIs(err, context.Canceled)
	s.Empty(sink.got)
}
