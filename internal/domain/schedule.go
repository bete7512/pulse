package domain

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidSchedule is returned by Schedule.Validate when a schedule is internally
// inconsistent for its kind.
var ErrInvalidSchedule = errors.New("invalid schedule")

// ScheduleKind is how a schedule recurs.
type ScheduleKind string

const (
	ScheduleOnce     ScheduleKind = "once"     // fire once at NextRunAt, then delete
	ScheduleInterval ScheduleKind = "interval" // fire every Interval
	ScheduleCron     ScheduleKind = "cron"     // fire on CronExpr
)

// Schedule is a mutable definition that spawns jobs over time. It is plain config — NOT
// event-sourced — and lives in the schedules table; the scheduling time lives only in
// NextRunAt (never on an event). When a schedule comes due the scheduler appends an ordinary
// JobSubmitted (tagged with the schedule id for lineage), then advances NextRunAt or, for a
// one-shot, deletes the schedule. The jobs it spawns are ordinary, event-sourced jobs.
type Schedule struct {
	ID        string
	Topic     string          // topic of the jobs it spawns
	Payload   []byte          // payload handed to each spawned job
	Kind      ScheduleKind
	CronExpr  string          // set when Kind == ScheduleCron
	Interval  time.Duration   // set when Kind == ScheduleInterval
	NextRunAt time.Time       // when it next fires
	LastRunAt *time.Time      // when it last fired (nil if never)
	Paused    bool
	CreatedAt time.Time
}

// Validate checks the schedule is internally consistent for its kind. It does not parse the
// cron expression (that needs a parser, which lives in the scheduler service) — only that the
// kind-specific fields are present.
func (s Schedule) Validate() error {
	if s.Topic == "" {
		return fmt.Errorf("%w: empty topic", ErrInvalidSchedule)
	}
	switch s.Kind {
	case ScheduleOnce:
		// NextRunAt is the fire time; nothing else required.
	case ScheduleInterval:
		if s.Interval <= 0 {
			return fmt.Errorf("%w: interval must be > 0", ErrInvalidSchedule)
		}
	case ScheduleCron:
		if s.CronExpr == "" {
			return fmt.Errorf("%w: empty cron expression", ErrInvalidSchedule)
		}
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidSchedule, s.Kind)
	}
	return nil
}
