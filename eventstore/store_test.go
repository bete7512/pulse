package eventstore_test

import (
	"testing"

	"github.com/bete7512/pulse/domain"
	"github.com/bete7512/pulse/eventstore"
	"github.com/stretchr/testify/suite"
)

// EventStoreTestSuite exercises the real InMemoryStore implementation.
type EventStoreTestSuite struct {
	suite.Suite
	store *eventstore.InMemoryStore
}

// SetupTest runs before every Test* method, giving each test a fresh store.
func (s *EventStoreTestSuite) SetupTest() {
	s.store = eventstore.NewInMemoryStore()
}

// TestEventStoreTestSuite is the entry point go test discovers; it runs the suite.
func TestEventStoreTestSuite(t *testing.T) {
	suite.Run(t, new(EventStoreTestSuite))
}

func (s *EventStoreTestSuite) TestEventStoreAdd() {
	jobID, err := s.store.Add(domain.Event{JobId: "job-1", Type: domain.JobSubmitted})
	s.Require().NoError(err)
	s.Equal("job-1", jobID)

	loaded, err := s.store.Load("job-1")
	s.Require().NoError(err)
	s.Require().Len(loaded, 1)

	// Add populates ID, CreatedAt, and assigns Sequence starting at 1.
	s.NotEmpty(loaded[0].ID)
	s.False(loaded[0].CreatedAt.IsZero())
	s.Equal(int64(1), loaded[0].Sequence)
}

func (s *EventStoreTestSuite) TestEventStoreLoad() {
	// Sequences should increment per job and Load returns them sorted.
	_, err := s.store.Add(domain.Event{JobId: "job-1", Type: domain.JobSubmitted})
	s.Require().NoError(err)
	_, err = s.store.Add(domain.Event{JobId: "job-1", Type: domain.JobStarted})
	s.Require().NoError(err)
	_, err = s.store.Add(domain.Event{JobId: "job-2", Type: domain.JobSubmitted})
	s.Require().NoError(err)

	loaded, err := s.store.Load("job-1")
	s.Require().NoError(err)
	s.Require().Len(loaded, 2)
	s.Equal(int64(1), loaded[0].Sequence)
	s.Equal(int64(2), loaded[1].Sequence)

	// Unknown job returns no events and no error.
	none, err := s.store.Load("missing")
	s.Require().NoError(err)
	s.Empty(none)
}

func (s *EventStoreTestSuite) TestEventStoreListSubmittedEvents() {
	// job-1's latest event is JobStarted -> not reported as submitted.
	_, err := s.store.Add(domain.Event{JobId: "job-1", Type: domain.JobSubmitted})
	s.Require().NoError(err)
	_, err = s.store.Add(domain.Event{JobId: "job-1", Type: domain.JobStarted})
	s.Require().NoError(err)
	// job-2's latest event is JobSubmitted -> included.
	_, err = s.store.Add(domain.Event{JobId: "job-2", Type: domain.JobSubmitted})
	s.Require().NoError(err)

	submitted, err := s.store.ListSubmittedEvents()
	s.Require().NoError(err)
	s.Require().Len(submitted, 1)
	s.Equal("job-2", submitted[0].JobId)
}
