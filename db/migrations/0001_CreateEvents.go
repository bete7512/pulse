package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func (s *migrationService) registerMigration_0001CreateEvents() {
	goose.AddMigrationContext(s.up_0001CreateEvents, s.down_0001CreateEvents)

}

func (s *migrationService) up_0001CreateEvents(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE events (
		    id          uuid PRIMARY KEY,
		    job_id      uuid NOT NULL,
		    type        text NOT NULL,
		    sequence    bigint NOT NULL,
		    payload     jsonb,
		    message     text,
		    created_at  timestamptz NOT NULL DEFAULT now(),
		    UNIQUE (job_id, sequence)       
		);
		CREATE INDEX idx_events_job_id ON events (job_id);

	`)
	if err != nil {
		return err
	}
	return nil
}

func (s *migrationService) down_0001CreateEvents(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		DROP table events;
	`)
	if err != nil {
		return err
	}
	return nil
}
