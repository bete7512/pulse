package service_test

import (
	"context"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/service"
	"go.uber.org/mock/gomock"
)

func (s *ServiceSuite) admin() service.ScheduleService {
	return service.NewScheduleService(s.schedules, s.jobs, s.events)
}

// TestCreateSchedule_RecurringComputesFirstRun: an interval/cron schedule gets an id and a
// computed NextRunAt before it is persisted.
func (s *ServiceSuite) TestCreateSchedule_RecurringComputesFirstRun() {
	s.schedules.EXPECT().Create(gomock.Any(), gomock.AssignableToTypeOf(domain.Schedule{})).DoAndReturn(
		func(_ context.Context, sc domain.Schedule) error {
			s.NotEmpty(sc.ID)
			s.Equal(domain.ScheduleInterval, sc.Kind)
			s.False(sc.NextRunAt.IsZero(), "interval schedule must get a computed NextRunAt")
			return nil
		})

	id, err := s.admin().CreateSchedule(ctx(), domain.Schedule{Topic: "reconcile", Kind: domain.ScheduleInterval, Interval: time.Minute})
	s.NoError(err)
	s.NotEmpty(id)
}

// TestCreateSchedule_OnceUsesProvidedTime: a one-shot keeps the caller's fire time.
func (s *ServiceSuite) TestCreateSchedule_OnceUsesProvidedTime() {
	at := time.Now().Add(time.Hour)
	s.schedules.EXPECT().Create(gomock.Any(), gomock.AssignableToTypeOf(domain.Schedule{})).DoAndReturn(
		func(_ context.Context, sc domain.Schedule) error {
			s.Equal(domain.ScheduleOnce, sc.Kind)
			s.WithinDuration(at, sc.NextRunAt, time.Second)
			return nil
		})

	_, err := s.admin().CreateSchedule(ctx(), domain.Schedule{Topic: "reminder", Kind: domain.ScheduleOnce, NextRunAt: at})
	s.NoError(err)
}

// TestCreateSchedule_InvalidRejected: a bad schedule never reaches the repo.
func (s *ServiceSuite) TestCreateSchedule_InvalidRejected() {
	// empty topic ⇒ Validate fails; no Create expectation, so a call would fail the test.
	_, err := s.admin().CreateSchedule(ctx(), domain.Schedule{Kind: domain.ScheduleOnce})
	s.ErrorIs(err, domain.ErrInvalidSchedule)
}

func (s *ServiceSuite) TestScheduleAdmin_PauseResumeDelete() {
	s.schedules.EXPECT().SetPaused(gomock.Any(), "s1", true).Return(nil)
	s.NoError(s.admin().PauseSchedule(ctx(), "s1"))

	s.schedules.EXPECT().SetPaused(gomock.Any(), "s1", false).Return(nil)
	s.NoError(s.admin().ResumeSchedule(ctx(), "s1"))

	s.schedules.EXPECT().Delete(gomock.Any(), "s1").Return(nil)
	s.NoError(s.admin().DeleteSchedule(ctx(), "s1"))
}

func (s *ServiceSuite) TestScheduleAdmin_LineageReadsDelegate() {
	s.jobs.EXPECT().JobsBySchedule(gomock.Any(), "s1").Return([]domain.Job{{ID: "j1"}}, nil)
	jobs, err := s.admin().ListScheduleJobs(ctx(), "s1")
	s.NoError(err)
	s.Require().Len(jobs, 1)

	s.events.EXPECT().FiresBySchedule(gomock.Any(), "s1").Return([]domain.Event{{JobId: "j1"}}, nil)
	fires, err := s.admin().ListScheduleFires(ctx(), "s1")
	s.NoError(err)
	s.Require().Len(fires, 1)
}
