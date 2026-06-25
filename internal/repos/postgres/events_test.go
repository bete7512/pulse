package postgres_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/repos/postgres"
	pkgerr "github.com/bete7512/pulse/pkg/errors"
)

func (s *RepoSuite) TestEvent_AppendAssignsSequence() {
	event := postgres.NewEvent(s.pool)
	for _, typ := range []domain.EventType{domain.JobSubmitted, domain.JobStarted, domain.JobCompleted} {
		_, err := event.Append(bg(), domain.Event{JobId: "j", Type: typ, Topic: "t"})
		s.Require().NoError(err, "Append %s", typ)
	}
	events, err := event.LoadEventsForJob(bg(), "j")
	s.Require().NoError(err)
	s.Require().Len(events, 3)
	s.Equal(int64(1), events[0].Sequence)
	s.Equal(int64(2), events[1].Sequence)
	s.Equal(int64(3), events[2].Sequence)
}

// TestEvent_ConcurrentAppend_OneWinner is the claim race against real Postgres: many writers
// race for the same next sequence; the UNIQUE(job_id, sequence) constraint lets exactly one
// win and surfaces the rest as ErrSequenceConflict.
func (s *RepoSuite) TestEvent_ConcurrentAppend_OneWinner() {
	event := postgres.NewEvent(s.pool)
	_, err := event.Append(bg(), domain.Event{JobId: "j", Type: domain.JobSubmitted})
	s.Require().NoError(err) // seq 1

	const racers = 50
	var winners, conflicts int32
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := event.Append(bg(), domain.Event{JobId: "j", Type: domain.JobStarted}) // all want seq 2
			switch {
			case err == nil:
				atomic.AddInt32(&winners, 1)
			case errors.Is(err, pkgerr.ErrSequenceConflict):
				atomic.AddInt32(&conflicts, 1)
			default:
				s.Failf("unexpected error", "%v", err)
			}
		}()
	}
	wg.Wait()

	s.Equal(int32(1), winners, "exactly one winner")
	s.Equal(int32(racers-1), conflicts)
}

func (s *RepoSuite) TestEvent_AppendBatchAtomic() {
	event := postgres.NewEvent(s.pool)
	_, err := event.Append(bg(), domain.Event{JobId: "j", Type: domain.JobSubmitted})
	s.Require().NoError(err)
	err = event.AppendBatch(bg(), []domain.Event{
		{JobId: "j", Type: domain.JobFailed, Message: "boom"},
		{JobId: "j", Type: domain.JobRetried},
	})
	s.Require().NoError(err)

	events, err := event.LoadEventsForJob(bg(), "j")
	s.Require().NoError(err)
	s.Require().Len(events, 3)
	s.Equal(domain.JobFailed, events[1].Type)
	s.Equal(domain.JobRetried, events[2].Type)
}

func (s *RepoSuite) TestEvent_ListEventsByTopics() {
	event := postgres.NewEvent(s.pool)
	event.Append(bg(), domain.Event{JobId: "a", Type: domain.JobSubmitted, Topic: "email"})
	event.Append(bg(), domain.Event{JobId: "b", Type: domain.JobSubmitted, Topic: "sms"})

	got, err := event.ListEventsByTopics(bg(), []string{"email"})
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("a", got[0].JobId)
}

func (s *RepoSuite) TestEvent_OrphanedRunning() {
	event := postgres.NewEvent(s.pool)
	// a Running job (submitted + started) with NO liveness row.
	event.Append(bg(), domain.Event{JobId: "stuck", Type: domain.JobSubmitted})
	event.Append(bg(), domain.Event{JobId: "stuck", Type: domain.JobStarted})

	// negative grace ⇒ "older than now()+1h" ⇒ any running job qualifies (avoids clock flakiness).
	ids, err := event.OrphanedRunning(bg(), -time.Hour)
	s.Require().NoError(err)
	s.Equal([]string{"stuck"}, ids)
}
