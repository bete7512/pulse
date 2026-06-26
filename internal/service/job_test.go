package service_test

import (
	"context"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/service"
	pkgerr "github.com/bete7512/pulse/pkg/errors"
	"go.uber.org/mock/gomock"
)

// anyEvent / anyBatch / anyTime are the typed argument matchers we use in place of
// gomock.Any() (which is reserved for ctx): they assert the argument's type without
// pinning its value.
var (
	anyEvent = gomock.AssignableToTypeOf(domain.Event{})
	anyBatch = gomock.AssignableToTypeOf([]domain.Event{})
	anyTime  = gomock.AssignableToTypeOf(time.Time{})
)

func (s *ServiceSuite) svc() service.JobService {
	return service.New(s.events, s.jobs, s.live, time.Minute)
}

func (s *ServiceSuite) TestSubmit_AppendsJobSubmitted() {
	s.events.EXPECT().Append(gomock.Any(), anyEvent).DoAndReturn(
		func(_ context.Context, e domain.Event) (string, error) {
			s.Equal(domain.JobSubmitted, e.Type)
			s.Equal("send-email", e.Topic)
			return e.JobId, nil
		})

	id, err := s.svc().Submit(ctx(), "send-email", []byte(`{}`))
	s.NoError(err)
	s.NotEmpty(id)
}

func (s *ServiceSuite) TestGetJob_ReadsReadModel() {
	s.jobs.EXPECT().GetJob(gomock.Any(), "j").Return(&domain.Job{ID: "j", Status: domain.Completed}, nil)

	got, err := s.svc().GetJob(ctx(), "j")
	s.NoError(err)
	s.Equal("j", got.ID)
	s.Equal(domain.Completed, got.Status)
}

func (s *ServiceSuite) TestPendingJobs_ReturnsIDs() {
	s.events.EXPECT().ListEventsByTopics(gomock.Any(), nil).
		Return([]domain.Event{{JobId: "a"}, {JobId: "b"}}, nil)

	ids, err := s.svc().PendingJobs(ctx())
	s.NoError(err)
	s.Equal([]string{"a", "b"}, ids)
}

func (s *ServiceSuite) TestListPendingJobsByTopics_FoldsEachJob() {
	s.events.EXPECT().ListEventsByTopics(gomock.Any(), []string{"t"}).
		Return([]domain.Event{{JobId: "j", Type: domain.JobSubmitted, Topic: "t"}}, nil)
	s.events.EXPECT().LoadEventsForJob(gomock.Any(), "j").
		Return([]domain.Event{{JobId: "j", Type: domain.JobSubmitted, Topic: "t", Payload: []byte(`{}`)}}, nil)

	jobs, err := s.svc().ListPendingJobsByTopics(ctx(), []string{"t"})
	s.NoError(err)
	s.Require().Len(jobs, 1)
	s.Equal("j", jobs[0].ID)
	s.Equal("t", jobs[0].Topic)
	s.Equal(domain.Pending, jobs[0].Status)
}

func (s *ServiceSuite) TestStartJob_RetriesOnConflict() {
	pending := []domain.Event{{JobId: "j", Type: domain.JobSubmitted}}
	s.events.EXPECT().LoadEventsForJob(gomock.Any(), "j").Return(pending, nil).Times(2) // initial + retry
	gomock.InOrder(
		s.events.EXPECT().AppendBatch(gomock.Any(), anyBatch).Return(pkgerr.ErrSequenceConflict),
		s.events.EXPECT().AppendBatch(gomock.Any(), anyBatch).Return(nil),
	)
	s.live.EXPECT().Mark(gomock.Any(), "j", "w1", anyTime).Return(nil)

	s.NoError(s.svc().StartJob(ctx(), "j", "w1"))
}

func (s *ServiceSuite) TestStartJob_RejectsNonDispatchable() {
	// Completed job: Start must be rejected, and neither AppendBatch nor liveness.Mark may run
	// (the suite's mocks have no such expectation, so a call would fail the test).
	completed := []domain.Event{{JobId: "j", Type: domain.JobSubmitted}, {Type: domain.JobStarted}, {Type: domain.JobCompleted}}
	s.events.EXPECT().LoadEventsForJob(gomock.Any(), "j").Return(completed, nil)

	err := s.svc().StartJob(ctx(), "j", "w")
	s.ErrorIs(err, domain.ErrInvalidTransition)
}

// TestEndRun_AppendsTerminalEventAndClearsLiveness covers the endRun commands (Complete,
// Cancel): each loads the job, appends its single terminal event, and clears liveness.
func (s *ServiceSuite) TestEndRun_AppendsTerminalEventAndClearsLiveness() {
	cases := []struct {
		name     string
		stream   []domain.Event
		call     func(service.JobService) error
		wantType domain.EventType
	}{
		{
			name:     "complete a running job",
			stream:   []domain.Event{{JobId: "j", Type: domain.JobSubmitted}, {Type: domain.JobStarted}},
			call:     func(svc service.JobService) error { return svc.CompleteJob(ctx(), "j") },
			wantType: domain.JobCompleted,
		},
		{
			name:     "cancel a pending job",
			stream:   []domain.Event{{JobId: "j", Type: domain.JobSubmitted}},
			call:     func(svc service.JobService) error { return svc.CancelJob(ctx(), "j") },
			wantType: domain.JobCanceled,
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.SetupTest() // fresh mocks per sub-case
			s.events.EXPECT().LoadEventsForJob(gomock.Any(), "j").Return(tc.stream, nil)
			s.events.EXPECT().AppendBatch(gomock.Any(), anyBatch).DoAndReturn(
				func(_ context.Context, evs []domain.Event) error {
					s.Require().Len(evs, 1)
					s.Equal(tc.wantType, evs[0].Type)
					return nil
				})
			s.live.EXPECT().Clear(gomock.Any(), "j").Return(nil)

			s.NoError(tc.call(s.svc()))
		})
	}
}

// TestFailJob_DecidesFate covers Fail's branch: retry while attempts remain, dead-letter at
// the cap. Each case differs only in how many prior attempts the loaded stream encodes and the
// second event it expects appended after JobFailed.
func (s *ServiceSuite) TestFailJob_DecidesFate() {
	started := func(extraAttempts int) []domain.Event {
		evs := []domain.Event{{JobId: "j", Type: domain.JobSubmitted}, {Type: domain.JobStarted}}
		for i := 0; i < extraAttempts; i++ {
			evs = append(evs, domain.Event{Type: domain.JobFailed}, domain.Event{Type: domain.JobRetried}, domain.Event{Type: domain.JobStarted})
		}
		return evs
	}
	cases := []struct {
		name       string
		stream     []domain.Event
		wantSecond domain.EventType
	}{
		{"retry while attempts remain", started(0), domain.JobRetried},
		{"dead-letter at max attempts", started(2), domain.JobDeadLettered}, // 3rd Running
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.SetupTest() // fresh mocks per sub-case
			s.events.EXPECT().LoadEventsForJob(gomock.Any(), "j").Return(tc.stream, nil)
			s.events.EXPECT().AppendBatch(gomock.Any(), anyBatch).DoAndReturn(
				func(_ context.Context, evs []domain.Event) error {
					s.Require().Len(evs, 2)
					s.Equal(domain.JobFailed, evs[0].Type)
					s.Equal(tc.wantSecond, evs[1].Type)
					return nil
				})
			s.live.EXPECT().Clear(gomock.Any(), "j").Return(nil)

			s.NoError(s.svc().FailJob(ctx(), "j", "boom"))
		})
	}
}

func (s *ServiceSuite) TestHeartbeat_RenewsLiveness() {
	s.live.EXPECT().Renew(gomock.Any(), "j", "w", anyTime).Return(nil)

	s.NoError(s.svc().Heartbeat(ctx(), "j", "w"))
}
