package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func (s *migrationService) registerMigration_0008AddDispatchControl() {
	goose.AddMigrationContext(s.up_0008AddDispatchControl, s.down_0008AddDispatchControl)
}

func (s *migrationService) up_0008AddDispatchControl(ctx context.Context, tx *sql.Tx) error {
	// Pause is mutable operational config, not a job fact — so it lives in a plain singleton
	// table, never on the append-only event log (exactly like schedules.paused). The id=1 check
	// constraint makes the state unambiguous: there is one row and nothing to reconcile. The row
	// is seeded paused=false so every existing deployment behaves exactly as today.
	_, err := tx.Exec(`
		CREATE TABLE dispatch_control (
			id         int         PRIMARY KEY DEFAULT 1,
			paused     boolean     NOT NULL DEFAULT false,
			reason     text,
			paused_at  timestamptz,
			updated_at timestamptz NOT NULL DEFAULT now(),
			CONSTRAINT dispatch_control_singleton CHECK (id = 1)
		);
		INSERT INTO dispatch_control (id, paused) VALUES (1, false);
	`)
	return err
}

func (s *migrationService) down_0008AddDispatchControl(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE IF EXISTS dispatch_control;`)
	return err
}
