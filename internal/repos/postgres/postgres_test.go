package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
)

// RepoSuite is the shared harness for the Postgres repo integration tests. It connects to
// TEST_DB_URL once (SetupSuite) and truncates the tables these tests touch before each case
// (SetupTest), so every test starts clean. If TEST_DB_URL is unset the whole suite skips, so a
// plain `go test ./...` without a database still passes. Point it at a throwaway DB:
//
//	TEST_DB_URL=postgres://user:pass@localhost:5432/pulse_test go test ./internal/repos/postgres/
type RepoSuite struct {
	suite.Suite
	pool *pgxpool.Pool
}

func TestRepoSuite(t *testing.T) { suite.Run(t, new(RepoSuite)) }

func (s *RepoSuite) SetupSuite() {
	dsn := os.Getenv("TEST_DB_URL")
	if dsn == "" {
		return // pool stays nil; SetupTest skips each test
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	s.Require().NoError(err, "connect TEST_DB_URL")
	s.pool = pool
}

func (s *RepoSuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *RepoSuite) SetupTest() {
	if s.pool == nil {
		s.T().Skip("TEST_DB_URL not set — skipping Postgres integration tests")
	}
	_, err := s.pool.Exec(bg(), `TRUNCATE events, jobs, liveness`)
	s.Require().NoError(err, "truncate (did you run migrations on the test DB?)")
}

func bg() context.Context { return context.Background() }
