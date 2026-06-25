package postgres_test

import (
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos/postgres"
	pkgerr "github.com/bete7512/pulse/pkg/errors"
)

func (s *RepoSuite) TestJob_GetJob_NotFound() {
	jobs := postgres.NewJob(s.pool)
	_, err := jobs.GetJob(bg(), "missing")
	s.ErrorIs(err, pkgerr.ErrNotFound)
}

func (s *RepoSuite) TestJob_UpsertAndGet() {
	jobs := postgres.NewJob(s.pool)
	err := jobs.Upsert(bg(), domain.Job{ID: "j", Status: domain.Completed, SubmittedAt: time.Now()}, 3)
	s.Require().NoError(err)
	got, err := jobs.GetJob(bg(), "j")
	s.Require().NoError(err)
	s.Equal(domain.Completed, got.Status)

	// Upsert only moves forward: a stale (lower last_sequence) write is ignored.
	err = jobs.Upsert(bg(), domain.Job{ID: "j", Status: domain.Running, SubmittedAt: time.Now()}, 2)
	s.Require().NoError(err)
	got, err = jobs.GetJob(bg(), "j")
	s.Require().NoError(err)
	s.Equal(domain.Completed, got.Status, "stale upsert must not apply")
}

func (s *RepoSuite) TestJob_LaggingJobs() {
	events := postgres.NewEvent(s.pool)
	jobs := postgres.NewJob(s.pool)

	// events exist for j, but no jobs projection row → j lags.
	events.Append(bg(), domain.Event{JobId: "j", Type: domain.JobSubmitted})

	lagging, err := jobs.LaggingJobs(bg())
	s.Require().NoError(err)
	s.Equal([]string{"j"}, lagging)
}
