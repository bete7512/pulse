package service_test

import (
	"context"
	"errors"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/service"
	pkgerr "github.com/bete7512/pulse/pkg/errors"
	"go.uber.org/mock/gomock"
)

const schedA = "sched-a"

func (s *ServiceSuite) scheduler() service.SchedulerService {
	return service.NewScheduler(s.schedules, s.events, time.Second, nil)
}

// TestNextRun tables the pure next-occurrence computation.
func (s *ServiceSuite) TestNextRun() {
	base := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	cases := []struct {
		name    string
		sched   domain.Schedule
		want    time.Time
		wantErr bool
	}{
		{"interval adds its duration", domain.Schedule{Kind: domain.ScheduleInterval, Interval: 15 * time.Minute}, base.Add(15 * time.Minute), false},
		{"cron advances to next slot", domain.Schedule{Kind: domain.ScheduleCron, CronExpr: "0 * * * *"}, time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC), false},
		{"invalid cron errors", domain.Schedule{Kind: domain.ScheduleCron, CronExpr: "nope"}, time.Time{}, true},
		{"once has no next", domain.Schedule{Kind: domain.ScheduleOnce}, time.Time{}, true},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			got, err := service.NextRun(tc.sched, base)
			if tc.wantErr {
				s.Error(err)
				return
			}
			s.NoError(err)
			s.True(got.Equal(tc.want), "got %v, want %v", got, tc.want)
		})
	}
}

// TestScheduler_Tick_RecurringFiresThenAdvances: a due interval schedule spawns a tagged
// JobSubmitted and the cursor advances (CAS keyed on the fired occurrence).
func (s *ServiceSuite) TestScheduler_Tick_RecurringFiresThenAdvances() {
	occ := time.Now().Add(-time.Minute).Truncate(time.Second)
	sched := domain.Schedule{ID: schedA, Topic: "rollup", Payload: []byte(`{}`), Kind: domain.ScheduleInterval, Interval: 5 * time.Minute, NextRunAt: occ}

	s.schedules.EXPECT().Due(gomock.Any(), anyTime, gomock.AssignableToTypeOf(0)).Return([]domain.Schedule{sched}, nil)
	s.events.EXPECT().Append(gomock.Any(), anyEvent).DoAndReturn(
		func(_ context.Context, e domain.Event) (string, error) {
			s.Equal(domain.JobSubmitted, e.Type)
			s.Equal("rollup", e.Topic)
			s.Require().NotNil(e.ScheduleID)
			s.Equal(schedA, *e.ScheduleID)
			return e.JobId, nil
		})
	s.schedules.EXPECT().Advance(gomock.Any(), schedA, occ, anyTime, anyTime).Return(nil)

	s.NoError(service.Tick(s.scheduler(), ctx()))
}

// TestScheduler_Tick_OnceFiresThenDeletes: a one-shot fires and is removed (not advanced).
func (s *ServiceSuite) TestScheduler_Tick_OnceFiresThenDeletes() {
	sched := domain.Schedule{ID: schedA, Topic: "reminder", Kind: domain.ScheduleOnce, NextRunAt: time.Now()}

	s.schedules.EXPECT().Due(gomock.Any(), anyTime, gomock.AssignableToTypeOf(0)).Return([]domain.Schedule{sched}, nil)
	s.events.EXPECT().Append(gomock.Any(), anyEvent).Return("job-1", nil)
	s.schedules.EXPECT().Delete(gomock.Any(), schedA).Return(nil)

	s.NoError(service.Tick(s.scheduler(), ctx()))
}

// TestScheduler_Tick_AlreadyFiredStillAdvances: a re-fire of the same occurrence hits the
// event store's UNIQUE constraint (ErrSequenceConflict) — treated as already-fired, so the
// cursor still advances (no stuck schedule).
func (s *ServiceSuite) TestScheduler_Tick_AlreadyFiredStillAdvances() {
	occ := time.Now().Truncate(time.Second)
	sched := domain.Schedule{ID: schedA, Topic: "rollup", Kind: domain.ScheduleInterval, Interval: time.Minute, NextRunAt: occ}

	s.schedules.EXPECT().Due(gomock.Any(), anyTime, gomock.AssignableToTypeOf(0)).Return([]domain.Schedule{sched}, nil)
	s.events.EXPECT().Append(gomock.Any(), anyEvent).Return("", pkgerr.ErrSequenceConflict)
	s.schedules.EXPECT().Advance(gomock.Any(), schedA, occ, anyTime, anyTime).Return(nil)

	s.NoError(service.Tick(s.scheduler(), ctx()))
}

// TestScheduler_Tick_AppendErrorLeavesCursor: a real append failure must NOT advance or delete
// — the cursor is left so the next tick retries. (No Advance/Delete expectation ⇒ a call fails.)
func (s *ServiceSuite) TestScheduler_Tick_AppendErrorLeavesCursor() {
	sched := domain.Schedule{ID: schedA, Topic: "rollup", Kind: domain.ScheduleInterval, Interval: time.Minute, NextRunAt: time.Now()}

	s.schedules.EXPECT().Due(gomock.Any(), anyTime, gomock.AssignableToTypeOf(0)).Return([]domain.Schedule{sched}, nil)
	s.events.EXPECT().Append(gomock.Any(), anyEvent).Return("", errors.New("db down"))

	// tick swallows the per-schedule error (logs it) and returns nil.
	s.NoError(service.Tick(s.scheduler(), ctx()))
}
