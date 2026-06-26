package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func (s *migrationService) registerMigration_0006AddSchedules() {
	goose.AddMigrationContext(s.up_0006AddSchedules, s.down_0006AddSchedules)
}

func (s *migrationService) up_0006AddSchedules(ctx context.Context, tx *sql.Tx) error {
	// schedules is mutable config — NOT the event log. A future run time is not a fact that
	// happened, so the "when" lives here. When a schedule fires, the scheduler appends an
	// ordinary JOB_SUBMITTED tagged with schedule_id (a past fact, hence allowed on the log).
	_, err := tx.Exec(`
		CREATE TABLE schedules (
			id           uuid        PRIMARY KEY,
			topic        text        NOT NULL,           -- topic of the jobs it spawns
			payload      bytea,                           -- payload handed to each spawned job
			kind         text        NOT NULL,            -- 'once' | 'interval' | 'cron'
			cron_expr    text,                            -- when kind = 'cron'
			interval_ms  bigint,                          -- when kind = 'interval'
			next_run_at  timestamptz NOT NULL,            -- the ONLY place scheduling-time lives
			last_run_at  timestamptz,
			paused       boolean     NOT NULL DEFAULT false,
			created_at   timestamptz NOT NULL DEFAULT now()
		);
		-- the loop's hot query: due & not paused
		CREATE INDEX idx_schedules_due ON schedules (next_run_at) WHERE NOT paused;

		-- lineage: schedule_id is a past fact ("this job was submitted by schedule X").
		ALTER TABLE events ADD COLUMN schedule_id uuid;   -- set on JOB_SUBMITTED by the scheduler; else NULL
		ALTER TABLE jobs   ADD COLUMN schedule_id uuid;   -- projector copies it from JOB_SUBMITTED
		CREATE INDEX idx_events_schedule_id ON events (schedule_id) WHERE schedule_id IS NOT NULL;
		CREATE INDEX idx_jobs_schedule_id   ON jobs   (schedule_id) WHERE schedule_id IS NOT NULL;
	`)
	return err
}

func (s *migrationService) down_0006AddSchedules(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		DROP INDEX IF EXISTS idx_events_schedule_id;
		DROP INDEX IF EXISTS idx_jobs_schedule_id;
		ALTER TABLE events DROP COLUMN IF EXISTS schedule_id;
		ALTER TABLE jobs   DROP COLUMN IF EXISTS schedule_id;
		DROP TABLE IF EXISTS schedules;
	`)
	return err
}
