package migrations

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
	"github.com/pressly/goose/v3"
)

// MigrationService represents the MigrationService interface
type MigrationService interface {
	Run(ctx context.Context, dbPool *pgxpool.Pool, args []string) error
}

type Config struct {}
type migrationService struct {
	config *Config
}

func NewMigrationsService(config *Config) *migrationService {
	s := &migrationService{
		config: config,
	}

	s.registerMigrations()

	return s
}

// Run runs migrate command
func (s *migrationService) Run(ctx context.Context, dbPool *pgxpool.Pool, args []string) error {
	err := goose.SetDialect(string(goose.DialectPostgres))
	if err != nil {
		return err
	}

	db := stdlib.OpenDBFromPool(dbPool)
	defer db.Close()

	if len(args) == 0 {
		return errors.New("no command specified")
	}

	command := args[0]

	arguments := []string{}
	if len(args) > 1 {
		arguments = append(arguments, args[1:]...)
	}

	err = goose.RunContext(ctx, command, db, ".", arguments...)
	if err != nil {
		return errors.Wrapf(err, "error running migrations")
	}

	return nil
}

func (s *migrationService) registerMigrations() {
	s.registerMigration_0001CreateEvents()
	s.registerMigration_0002CreatJobs()
	s.registerMigration_0003AddEventTopic()
}
