package grpc

import (
	"context"

	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/bete7512/pulse/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CreateSchedule registers a schedule (once / interval / cron) and returns its id.
func (s *Server) CreateSchedule(ctx context.Context, r *pulsev1.CreateScheduleRequest) (*pulsev1.CreateScheduleResponse, error) {
	sc := domain.Schedule{Topic: r.Topic, Payload: r.Payload}
	switch spec := r.Spec.(type) {
	case *pulsev1.CreateScheduleRequest_At:
		sc.Kind = domain.ScheduleOnce
		sc.NextRunAt = spec.At.AsTime()
	case *pulsev1.CreateScheduleRequest_Every:
		sc.Kind = domain.ScheduleInterval
		sc.Interval = spec.Every.AsDuration()
	case *pulsev1.CreateScheduleRequest_Cron:
		sc.Kind = domain.ScheduleCron
		sc.CronExpr = spec.Cron
	default:
		return nil, status.Error(codes.InvalidArgument, "schedule spec required (at, every, or cron)")
	}

	id, err := s.schedules.CreateSchedule(ctx, sc)
	if err != nil {
		return nil, err
	}
	return &pulsev1.CreateScheduleResponse{ScheduleId: id}, nil
}

func (s *Server) PauseSchedule(ctx context.Context, r *pulsev1.PauseScheduleRequest) (*pulsev1.PauseScheduleResponse, error) {
	return &pulsev1.PauseScheduleResponse{}, s.schedules.PauseSchedule(ctx, r.ScheduleId)
}

func (s *Server) ResumeSchedule(ctx context.Context, r *pulsev1.ResumeScheduleRequest) (*pulsev1.ResumeScheduleResponse, error) {
	return &pulsev1.ResumeScheduleResponse{}, s.schedules.ResumeSchedule(ctx, r.ScheduleId)
}

func (s *Server) DeleteSchedule(ctx context.Context, r *pulsev1.DeleteScheduleRequest) (*pulsev1.DeleteScheduleResponse, error) {
	return &pulsev1.DeleteScheduleResponse{}, s.schedules.DeleteSchedule(ctx, r.ScheduleId)
}

func (s *Server) ListSchedules(ctx context.Context, _ *pulsev1.ListSchedulesRequest) (*pulsev1.ListSchedulesResponse, error) {
	scheds, err := s.schedules.ListSchedules(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*pulsev1.ScheduleView, 0, len(scheds))
	for _, sc := range scheds {
		out = append(out, scheduleView(sc))
	}
	return &pulsev1.ListSchedulesResponse{Schedules: out}, nil
}

func (s *Server) ListScheduleJobs(ctx context.Context, r *pulsev1.ListScheduleJobsRequest) (*pulsev1.ListScheduleJobsResponse, error) {
	jobs, err := s.schedules.ListScheduleJobs(ctx, r.ScheduleId)
	if err != nil {
		return nil, err
	}
	out := make([]*pulsev1.JobView, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, &pulsev1.JobView{JobId: j.ID, Status: string(j.Status)})
	}
	return &pulsev1.ListScheduleJobsResponse{Jobs: out}, nil
}

func (s *Server) ListScheduleFires(ctx context.Context, r *pulsev1.ListScheduleFiresRequest) (*pulsev1.ListScheduleFiresResponse, error) {
	fires, err := s.schedules.ListScheduleFires(ctx, r.ScheduleId)
	if err != nil {
		return nil, err
	}
	out := make([]*pulsev1.ScheduleFire, 0, len(fires))
	for _, e := range fires {
		out = append(out, &pulsev1.ScheduleFire{JobId: e.JobId, FiredAt: timestamppb.New(e.CreatedAt)})
	}
	return &pulsev1.ListScheduleFiresResponse{Fires: out}, nil
}

// scheduleView maps a domain schedule to its wire view.
func scheduleView(sc domain.Schedule) *pulsev1.ScheduleView {
	v := &pulsev1.ScheduleView{
		ScheduleId: sc.ID,
		Topic:      sc.Topic,
		Kind:       string(sc.Kind),
		Cron:       sc.CronExpr,
		NextRunAt:  timestamppb.New(sc.NextRunAt),
		Paused:     sc.Paused,
	}
	if sc.Interval > 0 {
		v.Every = durationpb.New(sc.Interval)
	}
	if sc.LastRunAt != nil {
		v.LastRunAt = timestamppb.New(*sc.LastRunAt)
	}
	return v
}
