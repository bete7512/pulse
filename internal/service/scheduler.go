package service

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/scheduler_mock.go -package=mocks github.com/bete7512/pulse/internal/service SchedulerService

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"github.com/gofrs/uuid/v5"
	"github.com/robfig/cron/v3"
)

// scheduleBatch bounds how many due schedules one tick fires.
const scheduleBatch = 100

// scheduleNamespace seeds the deterministic (uuidv5) job id for a schedule occurrence, so a
// crashed or re-run fire re-derives the SAME job id and is deduped by UNIQUE(job_id, sequence).
var scheduleNamespace = uuid.Must(uuid.FromString("6ba7b814-9dad-11d1-80b4-00c04fd430c8"))

// SchedulerService fires due schedules. Each tick it reads schedules whose next_run_at has
// passed, appends an ordinary (lineage-tagged) JobSubmitted for each, then advances the
// schedule (or deletes a one-shot). Exactly-once per occurrence comes from a deterministic job
// id plus a CAS advance — there is no new dispatch gate, and the retry-only next_attempt_at is
// never touched.
type SchedulerService interface {
	Run(ctx context.Context)
}

type schedulerService struct {
	schedules repos.ScheduleRepo
	events    repos.EventRepo
	interval  time.Duration
	logger    *slog.Logger
}

func NewScheduler(schedules repos.ScheduleRepo, events repos.EventRepo, interval time.Duration, logger *slog.Logger) SchedulerService {
	if logger == nil {
		logger = slog.Default()
	}
	return &schedulerService{schedules: schedules, events: events, interval: interval, logger: logger}
}

func (sc *schedulerService) Run(ctx context.Context) {
	ticker := time.NewTicker(sc.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sc.tick(ctx); err != nil {
				sc.logger.Error("scheduler tick failed", "error", err)
			}
		}
	}
}

// tick fires every schedule that is due now. One schedule's failure is logged and skipped so
// it can't stall the rest.
func (sc *schedulerService) tick(ctx context.Context) error {
	due, err := sc.schedules.Due(ctx, time.Now(), scheduleBatch)
	if err != nil {
		return err
	}
	for _, s := range due {
		if err := sc.fire(ctx, s); err != nil {
			sc.logger.Error("schedule fire failed", "schedule_id", s.ID, "error", err)
		}
	}
	return nil
}

// fire submits the schedule's job (deterministic id, tagged with the schedule), then advances
// the cursor. Submit-before-advance plus the deterministic id make a crash between the two
// steps safe: the next tick re-derives the same id and the re-append is a no-op (sequence
// conflict). A real append failure leaves the cursor so the next tick retries.
func (sc *schedulerService) fire(ctx context.Context, s domain.Schedule) error {
	occurrence := s.NextRunAt
	jobID := deterministicJobID(s.ID, occurrence)
	scheduleID := s.ID

	_, err := sc.events.Append(ctx, domain.Event{
		JobId:      jobID,
		Type:       domain.JobSubmitted,
		Topic:      s.Topic,
		Payload:    s.Payload,
		ScheduleID: &scheduleID,
	})
	if err != nil && !errors.Is(err, pkg_errors.ErrSequenceConflict) {
		return err
	}
	// err is nil (fired) or ErrSequenceConflict (already fired this occurrence) — advance.

	now := time.Now()
	if s.Kind == domain.ScheduleOnce {
		return sc.schedules.Delete(ctx, s.ID)
	}
	next, err := nextRun(s, now)
	if err != nil {
		return err
	}
	return sc.schedules.Advance(ctx, s.ID, occurrence, next, now)
}

// nextRun computes the next fire time after now for a recurring schedule. Catch-up policy is
// "fire once, resync forward": next is computed from now, not the stale occurrence, so missed
// occurrences collapse into a single fire.
func nextRun(s domain.Schedule, now time.Time) (time.Time, error) {
	switch s.Kind {
	case domain.ScheduleInterval:
		return now.Add(s.Interval), nil
	case domain.ScheduleCron:
		sched, err := cron.ParseStandard(s.CronExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse cron %q: %w", s.CronExpr, err)
		}
		return sched.Next(now), nil
	default:
		return time.Time{}, fmt.Errorf("%w: cannot advance kind %q", domain.ErrInvalidSchedule, s.Kind)
	}
}

// deterministicJobID derives a stable job id from (schedule, occurrence): re-firing the same
// occurrence yields the same id, so the duplicate JobSubmitted is deduped by the event store.
func deterministicJobID(scheduleID string, occurrence time.Time) string {
	name := scheduleID + "|" + occurrence.UTC().Format(time.RFC3339Nano)
	return uuid.NewV5(scheduleNamespace, name).String()
}
