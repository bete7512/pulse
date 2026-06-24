package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func (s *migrationService) registerMigration_0004AddEventNextAttemptAt() {
	goose.AddMigrationContext(s.up_0004AddEventNextAttemptAt, s.down_0004AddEventNextAttemptAt)
}

func (s *migrationService) up_0004AddEventNextAttemptAt(ctx context.Context, tx *sql.Tx) error {
	// next_attempt_at is set only on JOB_RETRIED events; the partial index keeps the
	// "which retries are due now" lookup small and cheap.
	_, err := tx.Exec(`
		ALTER TABLE events ADD COLUMN next_attempt_at timestamptz;
		CREATE INDEX idx_events_next_attempt_at ON events (next_attempt_at) WHERE type = 'JOB_RETRIED';
	`)
	if err != nil {
		return err
	}
	return nil
}

func (s *migrationService) down_0004AddEventNextAttemptAt(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		DROP INDEX IF EXISTS idx_events_next_attempt_at;
		ALTER TABLE events DROP COLUMN next_attempt_at;
	`)
	if err != nil {
		return err
	}
	return nil
}
