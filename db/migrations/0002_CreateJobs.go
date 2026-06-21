package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func (s *migrationService) registerMigration_0002CreatJobs() {
	goose.AddMigrationContext(s.up_0002CreatJobs, s.down_0002CreatJobs)

}

func (s *migrationService) up_0002CreatJobs(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
				CREATE TABLE jobs (
				    job_id        uuid PRIMARY KEY,
				    status        text NOT NULL,
				    submitted_at  timestamptz,
				    started_at    timestamptz,
				    completed_at  timestamptz,
				    message       text,
				    last_sequence bigint NOT NULL DEFAULT 0
				);
	`)
	if err != nil {
		return err
	}
	return nil
}

func (s *migrationService) down_0002CreatJobs(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		DROP table jobs;
	`)
	if err != nil {
		return err
	}
	return nil
}
