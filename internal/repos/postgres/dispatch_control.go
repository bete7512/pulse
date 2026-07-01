package postgres

import (
	"context"

	"github.com/bete7512/pulse/internal/repos"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DispatchControl is the Postgres adapter for the dispatch_control singleton table — the
// durable truth behind the in-memory pause gate.
type DispatchControl struct {
	pool *pgxpool.Pool
}

var _ repos.DispatchControlRepo = (*DispatchControl)(nil)

func NewDispatchControl(pool *pgxpool.Pool) *DispatchControl {
	return &DispatchControl{pool: pool}
}

func (s *DispatchControl) Get(ctx context.Context) (repos.DispatchControl, error) {
	var c repos.DispatchControl
	// COALESCE(reason,'') so a NULL reason scans into the string field instead of failing.
	err := s.pool.QueryRow(ctx,
		`SELECT paused, COALESCE(reason, ''), paused_at FROM dispatch_control WHERE id = 1`).
		Scan(&c.Paused, &c.Reason, &c.PausedAt)
	return c, err
}

func (s *DispatchControl) SetPaused(ctx context.Context, paused bool, reason string) error {
	// paused_at is stamped when pausing and left untouched when resuming, so Status can always
	// report "last paused at". reason is overwritten either way (cleared to '' on resume).
	_, err := s.pool.Exec(ctx, `
		UPDATE dispatch_control SET
			paused     = @paused,
			reason     = @reason,
			paused_at  = CASE WHEN @paused THEN now() ELSE paused_at END,
			updated_at = now()
		WHERE id = 1`,
		pgx.NamedArgs{"paused": paused, "reason": reason})
	return err
}
