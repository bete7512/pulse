package eventstore

import (
	"slices"
	"sync"
	"time"

	"github.com/bete7512/pulse/domain"
	"github.com/gofrs/uuid/v5"
)

type InMemoryStore struct {
	mu     sync.RWMutex
	events []domain.Event
}

type EventStore interface {
	Add(event domain.Event) (string, error)
	Load(jobId string) ([]domain.Event, error)
	ListSubmittedEvents() ([]domain.Event, error)
}

var _ EventStore = (*InMemoryStore)(nil)

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{events: []domain.Event{}}
}

func (i *InMemoryStore) Add(e domain.Event) (string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	e.CreatedAt = time.Now()
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	e.ID = id.String()
	// get maximum sequence for this coming job id to generate next sequence
	maxSequence := int64(0)
	for _, existing := range i.events {
		if existing.JobId == e.JobId && existing.Sequence > maxSequence {
			maxSequence = existing.Sequence
		}
	}

	e.Sequence = maxSequence + 1

	i.events = append(i.events, e)
	return e.JobId, nil
}

func (i *InMemoryStore) Load(jobId string) ([]domain.Event, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	var filtered []domain.Event

	for _, job := range i.events {
		if job.JobId == jobId {
			filtered = append(filtered, job)
		}
	}

	slices.SortFunc(filtered, func(a, b domain.Event) int {
		if a.Sequence < b.Sequence {
			return -1
		}
		if a.Sequence > b.Sequence {
			return 1
		}
		return 0
	})
	return filtered, nil
}

func (i *InMemoryStore) ListSubmittedEvents() ([]domain.Event, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	submittedJobs := []domain.Event{}
	jobs := map[string]domain.Event{}
	for _, event := range i.events {
		if _, ok := jobs[event.JobId]; ok {
			if jobs[event.JobId].Sequence < event.Sequence {
				jobs[event.JobId] = event
			}
		} else {
			jobs[event.JobId] = event
		}
	}

	for _, v := range jobs {
		if v.Type == domain.JobSubmitted {
			submittedJobs = append(submittedJobs, v)
		}
	}
	return submittedJobs, nil
}
