package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func (s *migrationService) registerMigration_0007AddPriority() {
	goose.AddMigrationContext(s.up_0007AddPriority, s.down_0007AddPriority)
}

func (s *migrationService) up_0007AddPriority(ctx context.Context, tx *sql.Tx) error {
	// priority is a submit-time fact set on JOB_SUBMITTED (like topic): higher = dispatched
	// sooner, default 0 so every existing job and un-annotated Submit is unchanged. The
	// projector copies it onto the read model. The partial index covers the dispatch
	// head-scan (filter by topic, order by priority then arrival) so the planner needn't
	// sort the whole pending set per poll.
	_, err := tx.Exec(`
		ALTER TABLE events ADD COLUMN priority int NOT NULL DEFAULT 0;
		ALTER TABLE jobs   ADD COLUMN priority int NOT NULL DEFAULT 0;
		CREATE INDEX idx_events_dispatch ON events (topic, priority DESC, created_at ASC)
			WHERE type = 'JOB_SUBMITTED';
	`)
	return err
}

func (s *migrationService) down_0007AddPriority(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		DROP INDEX IF EXISTS idx_events_dispatch;
		ALTER TABLE events DROP COLUMN IF EXISTS priority;
		ALTER TABLE jobs   DROP COLUMN IF EXISTS priority;
	`)
	return err
}
