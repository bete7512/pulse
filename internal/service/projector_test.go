package service_test

import (
	"context"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/bete7512/pulse/internal/service"
	"go.uber.org/mock/gomock"
)

// TestProjector_CatchUp_UpsertsLaggingJobs: a lagging job is re-folded from its events and
// upserted into the read model at its latest sequence.
func (s *ServiceSuite) TestProjector_CatchUp_UpsertsLaggingJobs() {
	s.proj.EXPECT().LaggingJobs(gomock.Any()).Return([]string{"j"}, nil)
	s.events.EXPECT().LoadEventsForJob(gomock.Any(), "j").Return([]domain.Event{
		{JobId: "j", Type: domain.JobSubmitted, Sequence: 1},
		{JobId: "j", Type: domain.JobStarted, Sequence: 2},
	}, nil)
	s.proj.EXPECT().Upsert(gomock.Any(), gomock.AssignableToTypeOf(domain.Job{}), int64(2)).DoAndReturn(
		func(_ context.Context, job domain.Job, _ int64) error {
			s.Equal("j", job.ID)
			s.Equal(domain.Running, job.Status)
			return nil
		})

	p := service.NewProjector(s.events, s.proj, time.Second, nil)
	s.NoError(service.CatchUp(p, ctx()))
}

// TestProjector_CatchUp_NoLaggingJobs: nothing lagging → no reads or writes.
func (s *ServiceSuite) TestProjector_CatchUp_NoLaggingJobs() {
	s.proj.EXPECT().LaggingJobs(gomock.Any()).Return(nil, nil) // no LoadEventsForJob / Upsert expected

	p := service.NewProjector(s.events, s.proj, time.Second, nil)
	s.NoError(service.CatchUp(p, ctx()))
}
