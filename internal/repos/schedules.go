package repos

import (
	"context"
	"time"

	"github.com/bete7512/pulse/internal/domain"
)

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/schedulerepo_mock.go -package=mocks github.com/bete7512/pulse/internal/repos ScheduleRepo

// ScheduleRepo persists schedule definitions (mutable config — not the event log). The
// scheduler loop reads Due schedules, fires each by appending a job, and Advances (or
// Deletes a one-shot). Advance is a compare-and-swap on next_run_at, so a stale writer in a
// multi-server deployment can't move a schedule backwards.
type ScheduleRepo interface {
	Create(ctx context.Context, s domain.Schedule) error
	// Due returns up to limit schedules whose next_run_at has passed and that aren't paused.
	Due(ctx context.Context, now time.Time, limit int) ([]domain.Schedule, error)
	// Advance re-arms a schedule from occurrence to nextRunAt (CAS on next_run_at = occurrence),
	// stamping last_run_at. A no-op if another writer already advanced it.
	Advance(ctx context.Context, id string, occurrence, nextRunAt, lastRunAt time.Time) error
	Delete(ctx context.Context, id string) error
	SetPaused(ctx context.Context, id string, paused bool) error
	Get(ctx context.Context, id string) (*domain.Schedule, error)
	List(ctx context.Context) ([]domain.Schedule, error)
}
