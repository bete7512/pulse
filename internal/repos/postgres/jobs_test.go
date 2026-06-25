package postgres_test

import (
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos/postgres"
	pkgerr "github.com/bete7512/pulse/pkg/errors"
)

func (s *RepoSuite) TestJob_GetJob_NotFound() {
	jobs := postgres.NewJob(s.pool)
	_, err := jobs.GetJob(bg(), jobMissing)
	s.ErrorIs(err, pkgerr.ErrNotFound)
}

func (s *RepoSuite) TestJob_UpsertAndGet() {
	jobs := postgres.NewJob(s.pool)
	err := jobs.Upsert(bg(), domain.Job{ID: jobJ, Status: domain.Completed, SubmittedAt: time.Now()}, 3)
	s.Require().NoError(err)
	got, err := jobs.GetJob(bg(), jobJ)
	s.Require().NoError(err)
	s.Equal(domain.Completed, got.Status)

	// Upsert only moves forward: a stale (lower last_sequence) write is ignored.
	err = jobs.Upsert(bg(), domain.Job{ID: jobJ, Status: domain.Running, SubmittedAt: time.Now()}, 2)
	s.Require().NoError(err)
	got, err = jobs.GetJob(bg(), jobJ)
	s.Require().NoError(err)
	s.Equal(domain.Completed, got.Status, "stale upsert must not apply")
}

func (s *RepoSuite) TestJob_LaggingJobs() {
	events := postgres.NewEvent(s.pool)
	jobs := postgres.NewJob(s.pool)

	// events exist for jobJ, but no jobs projection row → it lags.
	_, err := events.Append(bg(), domain.Event{JobId: jobJ, Type: domain.JobSubmitted})
	s.Require().NoError(err)

	lagging, err := jobs.LaggingJobs(bg())
	s.Require().NoError(err)
	s.Equal([]string{jobJ}, lagging)
}
