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
	_, err := s.pool.Exec(bg(), `TRUNCATE events, jobs, liveness, schedules`)
	s.Require().NoError(err, "truncate (did you run migrations on the test DB?)")
}

func bg() context.Context { return context.Background() }

// Fixture job ids. The events/jobs job_id columns are typed uuid (real ids are uuidv7),
// so fixtures must be syntactically valid UUIDs — not "j"/"a". Stable values keep the
// assertions readable.
const (
	jobA       = "aaaaaaaa-0000-4000-8000-000000000001"
	jobB       = "bbbbbbbb-0000-4000-8000-000000000002"
	jobC       = "cccccccc-0000-4000-8000-000000000003"
	jobJ       = "11111111-0000-4000-8000-000000000001"
	jobStuck   = "22222222-0000-4000-8000-000000000002"
	jobMissing = "33333333-0000-4000-8000-000000000003"
)
