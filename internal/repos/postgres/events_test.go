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
		_, err := event.Append(bg(), domain.Event{JobId: jobJ, Type: typ, Topic: "t"})
		s.Require().NoError(err, "Append %s", typ)
	}
	events, err := event.LoadEventsForJob(bg(), jobJ)
	s.Require().NoError(err)
	s.Require().Len(events, 3)
	s.Equal(int64(1), events[0].Sequence)
	s.Equal(int64(2), events[1].Sequence)
	s.Equal(int64(3), events[2].Sequence)
}

// TestEvent_ConcurrentAppend_NoDuplicateSequences hammers Append concurrently against real
// Postgres and asserts the guarantee the UNIQUE(job_id, sequence) constraint actually gives:
// concurrent appends never duplicate a sequence. Racers that read the same MAX collide
// (ErrSequenceConflict); racers that run after a commit see the new MAX and succeed at the
// next sequence — so several win, at distinct, gap-free sequences. (The "one claim wins"
// semantics live a layer up, in service.StartJob's folded invariant, not in raw Append.)
func (s *RepoSuite) TestEvent_ConcurrentAppend_NoDuplicateSequences() {
	event := postgres.NewEvent(s.pool)
	_, err := event.Append(bg(), domain.Event{JobId: jobJ, Type: domain.JobSubmitted})
	s.Require().NoError(err) // seq 1

	const racers = 50
	var winners, conflicts int32
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := event.Append(bg(), domain.Event{JobId: jobJ, Type: domain.JobStarted})
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

	// Every racer either won or lost to the constraint — none errored otherwise, and at
	// least one made progress.
	s.Equal(int32(racers), winners+conflicts)
	s.GreaterOrEqual(winners, int32(1))

	// The decisive invariant: no sequence was handed out twice. The stored stream is a
	// gap-free 1..N (N = the initial event + the winners).
	events, err := event.LoadEventsForJob(bg(), jobJ)
	s.Require().NoError(err)
	s.Require().Len(events, int(1+winners))
	for i, e := range events {
		s.Equal(int64(i+1), e.Sequence, "sequences must be unique and gap-free")
	}
}

func (s *RepoSuite) TestEvent_AppendBatchAtomic() {
	event := postgres.NewEvent(s.pool)
	_, err := event.Append(bg(), domain.Event{JobId: jobJ, Type: domain.JobSubmitted})
	s.Require().NoError(err)
	err = event.AppendBatch(bg(), []domain.Event{
		{JobId: jobJ, Type: domain.JobFailed, Message: "boom"},
		{JobId: jobJ, Type: domain.JobRetried},
	})
	s.Require().NoError(err)

	events, err := event.LoadEventsForJob(bg(), jobJ)
	s.Require().NoError(err)
	s.Require().Len(events, 3)
	s.Equal(domain.JobFailed, events[1].Type)
	s.Equal(domain.JobRetried, events[2].Type)
}

func (s *RepoSuite) TestEvent_ListEventsByTopics() {
	event := postgres.NewEvent(s.pool)
	_, err := event.Append(bg(), domain.Event{JobId: jobA, Type: domain.JobSubmitted, Topic: "email"})
	s.Require().NoError(err)
	_, err = event.Append(bg(), domain.Event{JobId: jobB, Type: domain.JobSubmitted, Topic: "sms"})
	s.Require().NoError(err)

	got, err := event.ListEventsByTopics(bg(), []string{"email"})
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal(jobA, got[0].JobId)
}

// TestEvent_ListEventsByTopics_OrdersByPriorityThenFIFO proves the dispatch contract:
// the head query returns higher priority first, and ties break by sequence (arrival/FIFO).
// jobB is appended first but at priority 0; jobA and jobC share priority 5 — so the order
// must be A, C (both priority 5, A arrived before C), then B.
func (s *RepoSuite) TestEvent_ListEventsByTopics_OrdersByPriorityThenFIFO() {
	event := postgres.NewEvent(s.pool)
	_, err := event.Append(bg(), domain.Event{JobId: jobB, Type: domain.JobSubmitted, Topic: "email", Priority: 0})
	s.Require().NoError(err)
	_, err = event.Append(bg(), domain.Event{JobId: jobA, Type: domain.JobSubmitted, Topic: "email", Priority: 5})
	s.Require().NoError(err)
	_, err = event.Append(bg(), domain.Event{JobId: jobC, Type: domain.JobSubmitted, Topic: "email", Priority: 5})
	s.Require().NoError(err)

	got, err := event.ListEventsByTopics(bg(), []string{"email"})
	s.Require().NoError(err)
	s.Require().Len(got, 3)
	s.Equal([]string{jobA, jobC, jobB}, []string{got[0].JobId, got[1].JobId, got[2].JobId})
	s.Equal(5, got[0].Priority) // priority round-trips through the store
}

func (s *RepoSuite) TestEvent_OrphanedRunning() {
	event := postgres.NewEvent(s.pool)
	// a Running job (submitted + started) with NO liveness row.
	_, err := event.Append(bg(), domain.Event{JobId: jobStuck, Type: domain.JobSubmitted})
	s.Require().NoError(err)
	_, err = event.Append(bg(), domain.Event{JobId: jobStuck, Type: domain.JobStarted})
	s.Require().NoError(err)

	// negative grace ⇒ "older than now()+1h" ⇒ any running job qualifies (avoids clock flakiness).
	ids, err := event.OrphanedRunning(bg(), -time.Hour)
	s.Require().NoError(err)
	s.Equal([]string{jobStuck}, ids)
}
