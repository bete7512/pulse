package postgres_test

import (
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos/postgres"
	pkgerr "github.com/bete7512/pulse/pkg/errors"
)

// schedule ids (valid UUIDs — the column is typed uuid).
const (
	schedA = "5c000000-0000-4000-8000-000000000001"
	schedB = "5c000000-0000-4000-8000-000000000002"
)

func (s *RepoSuite) newSchedule(id string, kind domain.ScheduleKind, nextRun time.Time) domain.Schedule {
	sc := domain.Schedule{ID: id, Topic: "rollup", Payload: []byte(`{}`), Kind: kind, NextRunAt: nextRun, CreatedAt: time.Now()}
	switch kind {
	case domain.ScheduleInterval:
		sc.Interval = 5 * time.Minute
	case domain.ScheduleCron:
		sc.CronExpr = "0 * * * *"
	}
	return sc
}

func (s *RepoSuite) TestSchedule_CreateGetList() {
	repo := postgres.NewSchedule(s.pool)
	sc := s.newSchedule(schedA, domain.ScheduleInterval, time.Now())
	s.Require().NoError(repo.Create(bg(), sc))

	got, err := repo.Get(bg(), schedA)
	s.Require().NoError(err)
	s.Equal("rollup", got.Topic)
	s.Equal(domain.ScheduleInterval, got.Kind)
	s.Equal(5*time.Minute, got.Interval) // round-trips via interval_ms

	all, err := repo.List(bg())
	s.Require().NoError(err)
	s.Require().Len(all, 1)
	s.Equal(schedA, all[0].ID)
}

func (s *RepoSuite) TestSchedule_Get_NotFound() {
	repo := postgres.NewSchedule(s.pool)
	_, err := repo.Get(bg(), schedB)
	s.ErrorIs(err, pkgerr.ErrNotFound)
}

func (s *RepoSuite) TestSchedule_Due_RespectsPausedAndTime() {
	repo := postgres.NewSchedule(s.pool)
	now := time.Now()
	s.Require().NoError(repo.Create(bg(), s.newSchedule(schedA, domain.ScheduleCron, now.Add(-time.Minute)))) // due
	s.Require().NoError(repo.Create(bg(), s.newSchedule(schedB, domain.ScheduleCron, now.Add(time.Hour))))    // not due

	due, err := repo.Due(bg(), now, 100)
	s.Require().NoError(err)
	s.Require().Len(due, 1)
	s.Equal(schedA, due[0].ID)

	// paused schedules are excluded even when due.
	s.Require().NoError(repo.SetPaused(bg(), schedA, true))
	due, err = repo.Due(bg(), now, 100)
	s.Require().NoError(err)
	s.Empty(due)
}

func (s *RepoSuite) TestSchedule_Advance_IsCASOnOccurrence() {
	repo := postgres.NewSchedule(s.pool)
	occ := time.Now().Truncate(time.Second)
	s.Require().NoError(repo.Create(bg(), s.newSchedule(schedA, domain.ScheduleInterval, occ)))

	next := occ.Add(5 * time.Minute)
	// advancing from the real occurrence moves the cursor forward.
	s.Require().NoError(repo.Advance(bg(), schedA, occ, next, occ))
	got, _ := repo.Get(bg(), schedA)
	s.WithinDuration(next, got.NextRunAt, time.Second)

	// a stale writer (wrong occurrence) must NOT move it backwards — CAS no-op.
	s.Require().NoError(repo.Advance(bg(), schedA, occ, occ.Add(-time.Hour), occ))
	got, _ = repo.Get(bg(), schedA)
	s.WithinDuration(next, got.NextRunAt, time.Second) // unchanged
}

func (s *RepoSuite) TestSchedule_Delete() {
	repo := postgres.NewSchedule(s.pool)
	s.Require().NoError(repo.Create(bg(), s.newSchedule(schedA, domain.ScheduleOnce, time.Now())))
	s.Require().NoError(repo.Delete(bg(), schedA))
	_, err := repo.Get(bg(), schedA)
	s.ErrorIs(err, pkgerr.ErrNotFound)
}

// TestEvent_CarriesScheduleID: a JobSubmitted tagged with a schedule id round-trips through
// the event log and the fold (lineage).
func (s *RepoSuite) TestEvent_CarriesScheduleID() {
	event := postgres.NewEvent(s.pool)
	sid := schedA
	_, err := event.Append(bg(), domain.Event{JobId: jobJ, Type: domain.JobSubmitted, Topic: "t", ScheduleID: &sid})
	s.Require().NoError(err)

	events, err := event.LoadEventsForJob(bg(), jobJ)
	s.Require().NoError(err)
	s.Require().Len(events, 1)
	s.Require().NotNil(events[0].ScheduleID)
	s.Equal(schedA, *events[0].ScheduleID)

	job := domain.RebuildJob(events)
	s.Require().NotNil(job.ScheduleID)
	s.Equal(schedA, *job.ScheduleID)
}

// TestJob_UpsertCarriesScheduleID: the projection persists and returns the lineage tag.
func (s *RepoSuite) TestJob_UpsertCarriesScheduleID() {
	jobs := postgres.NewJob(s.pool)
	sid := schedA
	s.Require().NoError(jobs.Upsert(bg(), domain.Job{ID: jobJ, Status: domain.Pending, SubmittedAt: time.Now(), ScheduleID: &sid}, 1))

	got, err := jobs.GetJob(bg(), jobJ)
	s.Require().NoError(err)
	s.Require().NotNil(got.ScheduleID)
	s.Equal(schedA, *got.ScheduleID)
}
