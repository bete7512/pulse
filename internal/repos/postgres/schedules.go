package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schedule is the Postgres adapter for the schedules table — mutable schedule definitions.
type Schedule struct {
	pool *pgxpool.Pool
}

var _ repos.ScheduleRepo = (*Schedule)(nil)

func NewSchedule(pool *pgxpool.Pool) *Schedule {
	return &Schedule{pool: pool}
}

// scheduleColumns is the canonical column list / scan order for a schedule row.
const scheduleColumns = `id, topic, payload, kind, cron_expr, interval_ms, next_run_at, last_run_at, paused, created_at`

func (s *Schedule) Create(ctx context.Context, sc domain.Schedule) error {
	const query = `
		INSERT INTO schedules (id, topic, payload, kind, cron_expr, interval_ms, next_run_at, paused, created_at)
		VALUES (@id, @topic, @payload, @kind, @cron_expr, @interval_ms, @next_run_at, @paused, @created_at)`

	_, err := s.pool.Exec(ctx, query, pgx.NamedArgs{
		"id":          sc.ID,
		"topic":       sc.Topic,
		"payload":     sc.Payload,
		"kind":        string(sc.Kind),
		"cron_expr":   cronArg(sc),
		"interval_ms": intervalArg(sc),
		"next_run_at": sc.NextRunAt,
		"paused":      sc.Paused,
		"created_at":  sc.CreatedAt,
	})
	return err
}

func (s *Schedule) Due(ctx context.Context, now time.Time, limit int) ([]domain.Schedule, error) {
	const query = `
		SELECT ` + scheduleColumns + `
		FROM schedules
		WHERE next_run_at <= @now AND NOT paused
		ORDER BY next_run_at
		LIMIT @limit`

	rows, err := s.pool.Query(ctx, query, pgx.NamedArgs{"now": now, "limit": limit})
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanSchedule)
}

func (s *Schedule) Advance(ctx context.Context, id string, occurrence, nextRunAt, lastRunAt time.Time) error {
	// CAS on next_run_at = occurrence: only advance if the schedule is still at the occurrence
	// we processed, so a stale writer can't move it backwards or re-fire a past slot.
	const query = `
		UPDATE schedules
		SET next_run_at = @next, last_run_at = @last
		WHERE id = @id AND next_run_at = @occurrence`

	_, err := s.pool.Exec(ctx, query, pgx.NamedArgs{
		"id":         id,
		"occurrence": occurrence,
		"next":       nextRunAt,
		"last":       lastRunAt,
	})
	return err
}

func (s *Schedule) Delete(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM schedules WHERE id = $1`, id)
	return err
}

func (s *Schedule) SetPaused(ctx context.Context, id string, paused bool) error {
	_, err := s.pool.Exec(ctx, `UPDATE schedules SET paused = $2 WHERE id = $1`, id, paused)
	return err
}

func (s *Schedule) Get(ctx context.Context, id string) (*domain.Schedule, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+scheduleColumns+` FROM schedules WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	sc, err := pgx.CollectExactlyOneRow(rows, scanSchedule)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, pkg_errors.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &sc, nil
}

func (s *Schedule) List(ctx context.Context) ([]domain.Schedule, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+scheduleColumns+` FROM schedules ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanSchedule)
}

// scanSchedule maps a schedule row, translating the nullable cron_expr/interval_ms columns
// into the domain's CronExpr string / Interval duration.
func scanSchedule(row pgx.CollectableRow) (domain.Schedule, error) {
	var (
		sc         domain.Schedule
		cronExpr   *string
		intervalMs *int64
	)
	err := row.Scan(&sc.ID, &sc.Topic, &sc.Payload, &sc.Kind, &cronExpr, &intervalMs,
		&sc.NextRunAt, &sc.LastRunAt, &sc.Paused, &sc.CreatedAt)
	if err != nil {
		return sc, err
	}
	if cronExpr != nil {
		sc.CronExpr = *cronExpr
	}
	if intervalMs != nil {
		sc.Interval = time.Duration(*intervalMs) * time.Millisecond
	}
	return sc, nil
}

// cronArg / intervalArg store only the field relevant to the kind, NULL otherwise.
func cronArg(sc domain.Schedule) *string {
	if sc.Kind == domain.ScheduleCron {
		return &sc.CronExpr
	}
	return nil
}

func intervalArg(sc domain.Schedule) *int64 {
	if sc.Kind == domain.ScheduleInterval {
		ms := sc.Interval.Milliseconds()
		return &ms
	}
	return nil
}
