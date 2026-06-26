package service

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/scheduleadmin_mock.go -package=mocks github.com/bete7512/pulse/internal/service ScheduleService

import (
	"context"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos"
	"github.com/gofrs/uuid/v5"
)

// ScheduleService is the request-driven side of scheduling: create/manage schedule
// definitions and read their lineage (the jobs they spawned, when they fired). The firing
// loop itself is SchedulerService. The gRPC transport depends on this interface.
type ScheduleService interface {
	// CreateSchedule validates sc, assigns it an id, computes its first NextRunAt (for
	// interval/cron; ScheduleOnce uses the caller-supplied NextRunAt), and persists it.
	CreateSchedule(ctx context.Context, sc domain.Schedule) (string, error)
	PauseSchedule(ctx context.Context, id string) error
	ResumeSchedule(ctx context.Context, id string) error
	DeleteSchedule(ctx context.Context, id string) error
	ListSchedules(ctx context.Context) ([]domain.Schedule, error)
	// ListScheduleJobs returns the jobs a schedule spawned (read model).
	ListScheduleJobs(ctx context.Context, id string) ([]domain.Job, error)
	// ListScheduleFires returns a schedule's fire history (the tagged JobSubmitted events).
	ListScheduleFires(ctx context.Context, id string) ([]domain.Event, error)
}

type scheduleService struct {
	schedules repos.ScheduleRepo
	jobs      repos.JobRepo
	events    repos.EventRepo
}

func NewScheduleService(schedules repos.ScheduleRepo, jobs repos.JobRepo, events repos.EventRepo) ScheduleService {
	return &scheduleService{schedules: schedules, jobs: jobs, events: events}
}

func (m *scheduleService) CreateSchedule(ctx context.Context, sc domain.Schedule) (string, error) {
	if err := sc.Validate(); err != nil {
		return "", err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	sc.ID = id.String()
	sc.CreatedAt = time.Now()

	// A one-shot fires at the caller's NextRunAt; recurring schedules compute their first slot.
	if sc.Kind != domain.ScheduleOnce {
		next, err := nextRun(sc, time.Now())
		if err != nil {
			return "", err
		}
		sc.NextRunAt = next
	}
	if err := m.schedules.Create(ctx, sc); err != nil {
		return "", err
	}
	return sc.ID, nil
}

func (m *scheduleService) PauseSchedule(ctx context.Context, id string) error {
	return m.schedules.SetPaused(ctx, id, true)
}

func (m *scheduleService) ResumeSchedule(ctx context.Context, id string) error {
	return m.schedules.SetPaused(ctx, id, false)
}

func (m *scheduleService) DeleteSchedule(ctx context.Context, id string) error {
	return m.schedules.Delete(ctx, id)
}

func (m *scheduleService) ListSchedules(ctx context.Context) ([]domain.Schedule, error) {
	return m.schedules.List(ctx)
}

func (m *scheduleService) ListScheduleJobs(ctx context.Context, id string) ([]domain.Job, error) {
	return m.jobs.JobsBySchedule(ctx, id)
}

func (m *scheduleService) ListScheduleFires(ctx context.Context, id string) ([]domain.Event, error) {
	return m.events.FiresBySchedule(ctx, id)
}
