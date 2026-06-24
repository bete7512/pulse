package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func (s *migrationService) registerMigration_0005CreateLiveness() {
	goose.AddMigrationContext(s.up_0005CreateLiveness, s.down_0005CreateLiveness)
}

func (s *migrationService) up_0005CreateLiveness(ctx context.Context, tx *sql.Tx) error {
	// liveness is mutable, operational state (not the event log): one row per running
	// job, renewed by worker heartbeats. A row whose expires_at has passed marks a
	// job whose worker stopped heartbeating — presumed dead, to be recovered.
	_, err := tx.Exec(`
		CREATE TABLE liveness (
			job_id     uuid PRIMARY KEY,
			worker_id  text NOT NULL,
			expires_at timestamptz NOT NULL
		);
		CREATE INDEX idx_liveness_expires_at ON liveness (expires_at);
	`)
	if err != nil {
		return err
	}
	return nil
}

func (s *migrationService) down_0005CreateLiveness(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE liveness;`)
	if err != nil {
		return err
	}
	return nil
}
