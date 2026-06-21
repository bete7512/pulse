package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func (s *migrationService) registerMigration_0003AddEventTopic() {
	goose.AddMigrationContext(s.up_0003AddEventTopic, s.down_0003AddEventTopic)
}

func (s *migrationService) up_0003AddEventTopic(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		ALTER TABLE events ADD COLUMN topic text NOT NULL DEFAULT '';
		CREATE INDEX idx_events_topic ON events (topic);
	`)
	if err != nil {
		return err
	}
	return nil
}

func (s *migrationService) down_0003AddEventTopic(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		DROP INDEX IF EXISTS idx_events_topic;
		ALTER TABLE events DROP COLUMN topic;
	`)
	if err != nil {
		return err
	}
	return nil
}
