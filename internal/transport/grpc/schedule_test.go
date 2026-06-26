package grpc_test

import (
	"time"

	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/bete7512/pulse/internal/domain"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestCreateSchedule maps each spec variant of the oneof to the right domain kind.
func (s *ServerSuite) TestCreateSchedule() {
	cases := []struct {
		name    string
		req     *pulsev1.CreateScheduleRequest
		expect  func(sc domain.Schedule)
	}{
		{
			name: "cron",
			req:  &pulsev1.CreateScheduleRequest{Topic: "rollup", Spec: &pulsev1.CreateScheduleRequest_Cron{Cron: "0 * * * *"}},
			expect: func(sc domain.Schedule) {
				s.Equal(domain.ScheduleCron, sc.Kind)
				s.Equal("0 * * * *", sc.CronExpr)
			},
		},
		{
			name: "every",
			req:  &pulsev1.CreateScheduleRequest{Topic: "reconcile", Spec: &pulsev1.CreateScheduleRequest_Every{Every: durationpb.New(5 * time.Minute)}},
			expect: func(sc domain.Schedule) {
				s.Equal(domain.ScheduleInterval, sc.Kind)
				s.Equal(5*time.Minute, sc.Interval)
			},
		},
		{
			name: "at",
			req:  &pulsev1.CreateScheduleRequest{Topic: "reminder", Spec: &pulsev1.CreateScheduleRequest_At{At: timestamppb.New(time.Now().Add(time.Hour))}},
			expect: func(sc domain.Schedule) {
				s.Equal(domain.ScheduleOnce, sc.Kind)
			},
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.SetupTest()
			s.schedules.EXPECT().CreateSchedule(gomock.Any(), gomock.AssignableToTypeOf(domain.Schedule{})).DoAndReturn(
				func(_ interface{}, sc domain.Schedule) (string, error) {
					s.Equal(tc.req.Topic, sc.Topic)
					tc.expect(sc)
					return "sched-1", nil
				})

			resp, err := s.srv.CreateSchedule(ctx(), tc.req)
			s.NoError(err)
			s.Equal("sched-1", resp.ScheduleId)
		})
	}
}

func (s *ServerSuite) TestCreateSchedule_MissingSpecIsInvalidArgument() {
	_, err := s.srv.CreateSchedule(ctx(), &pulsev1.CreateScheduleRequest{Topic: "x"})
	s.Equal(codes.InvalidArgument, status.Code(err))
}

func (s *ServerSuite) TestPauseResumeDelete() {
	s.schedules.EXPECT().PauseSchedule(gomock.Any(), "s1").Return(nil)
	_, err := s.srv.PauseSchedule(ctx(), &pulsev1.PauseScheduleRequest{ScheduleId: "s1"})
	s.NoError(err)

	s.schedules.EXPECT().ResumeSchedule(gomock.Any(), "s1").Return(nil)
	_, err = s.srv.ResumeSchedule(ctx(), &pulsev1.ResumeScheduleRequest{ScheduleId: "s1"})
	s.NoError(err)

	s.schedules.EXPECT().DeleteSchedule(gomock.Any(), "s1").Return(nil)
	_, err = s.srv.DeleteSchedule(ctx(), &pulsev1.DeleteScheduleRequest{ScheduleId: "s1"})
	s.NoError(err)
}

func (s *ServerSuite) TestListSchedules_MapsView() {
	last := time.Now()
	s.schedules.EXPECT().ListSchedules(gomock.Any()).Return([]domain.Schedule{
		{ID: "s1", Topic: "rollup", Kind: domain.ScheduleInterval, Interval: time.Minute, NextRunAt: time.Now(), LastRunAt: &last, Paused: true},
	}, nil)

	resp, err := s.srv.ListSchedules(ctx(), &pulsev1.ListSchedulesRequest{})
	s.NoError(err)
	s.Require().Len(resp.Schedules, 1)
	v := resp.Schedules[0]
	s.Equal("s1", v.ScheduleId)
	s.Equal("interval", v.Kind)
	s.Equal(time.Minute, v.Every.AsDuration())
	s.True(v.Paused)
	s.NotNil(v.LastRunAt)
}

func (s *ServerSuite) TestListScheduleJobsAndFires() {
	s.schedules.EXPECT().ListScheduleJobs(gomock.Any(), "s1").
		Return([]domain.Job{{ID: "j1", Status: domain.Completed}}, nil)
	jobsResp, err := s.srv.ListScheduleJobs(ctx(), &pulsev1.ListScheduleJobsRequest{ScheduleId: "s1"})
	s.NoError(err)
	s.Require().Len(jobsResp.Jobs, 1)
	s.Equal("j1", jobsResp.Jobs[0].JobId)
	s.Equal("COMPLETED", jobsResp.Jobs[0].Status)

	firedAt := time.Now()
	s.schedules.EXPECT().ListScheduleFires(gomock.Any(), "s1").
		Return([]domain.Event{{JobId: "j1", Type: domain.JobSubmitted, CreatedAt: firedAt}}, nil)
	firesResp, err := s.srv.ListScheduleFires(ctx(), &pulsev1.ListScheduleFiresRequest{ScheduleId: "s1"})
	s.NoError(err)
	s.Require().Len(firesResp.Fires, 1)
	s.Equal("j1", firesResp.Fires[0].JobId)
	s.NotNil(firesResp.Fires[0].FiredAt)
}
