package pulse

import (
	"context"
	"encoding/json"
	"time"

	"github.com/bete7512/pulse/gen/pulsev1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ScheduleSpec describes when a schedule fires. Build one with At, After, Every, or Cron.
type ScheduleSpec struct {
	apply func(*pulsev1.CreateScheduleRequest)
}

// At fires the job once at time t.
func At(t time.Time) ScheduleSpec {
	return ScheduleSpec{apply: func(r *pulsev1.CreateScheduleRequest) {
		r.Spec = &pulsev1.CreateScheduleRequest_At{At: timestamppb.New(t)}
	}}
}

// After fires the job once, d from now.
func After(d time.Duration) ScheduleSpec { return At(time.Now().Add(d)) }

// Every fires the job repeatedly on the given interval.
func Every(d time.Duration) ScheduleSpec {
	return ScheduleSpec{apply: func(r *pulsev1.CreateScheduleRequest) {
		r.Spec = &pulsev1.CreateScheduleRequest_Every{Every: durationpb.New(d)}
	}}
}

// Cron fires the job on a standard 5-field cron expression (e.g. "0 * * * *").
func Cron(expr string) ScheduleSpec {
	return ScheduleSpec{apply: func(r *pulsev1.CreateScheduleRequest) {
		r.Spec = &pulsev1.CreateScheduleRequest_Cron{Cron: expr}
	}}
}

// ScheduleInfo is the SDK-facing view of a schedule definition.
type ScheduleInfo struct {
	ID        string
	Topic     string
	Kind      string // once | interval | cron
	Cron      string
	Every     time.Duration
	NextRunAt time.Time
	LastRunAt time.Time
	Paused    bool
}

// ScheduleFire records one firing of a schedule: the spawned job and when it ran.
type ScheduleFire struct {
	JobID   string
	FiredAt time.Time
}

// ScheduleJob is the typed mirror of Schedule: it JSON-encodes args as the payload each
// spawned job receives (mirror of Enqueue).
//
//	pulse.ScheduleJob(ctx, p, "report", ReportArgs{...}, pulse.Cron("0 * * * *"))
func ScheduleJob[T any](ctx context.Context, c *Client, topic string, args T, spec ScheduleSpec) (string, error) {
	b, err := json.Marshal(args)
	if err != nil {
		return "", err
	}
	return c.Schedule(ctx, topic, b, spec)
}

// Schedule registers a schedule that spawns jobs on topic with payload, per spec, and
// returns its id. Use At/After for once, Every for an interval, Cron for a cron expression.
func (c *Client) Schedule(ctx context.Context, topic string, payload []byte, spec ScheduleSpec) (string, error) {
	req := &pulsev1.CreateScheduleRequest{Topic: topic, Payload: payload}
	spec.apply(req)
	r, err := c.api.CreateSchedule(ctx, req)
	if err != nil {
		return "", err
	}
	return r.GetScheduleId(), nil
}

// PauseSchedule stops a schedule from firing until ResumeSchedule.
func (c *Client) PauseSchedule(ctx context.Context, id string) error {
	_, err := c.api.PauseSchedule(ctx, &pulsev1.PauseScheduleRequest{ScheduleId: id})
	return err
}

// ResumeSchedule re-enables a paused schedule.
func (c *Client) ResumeSchedule(ctx context.Context, id string) error {
	_, err := c.api.ResumeSchedule(ctx, &pulsev1.ResumeScheduleRequest{ScheduleId: id})
	return err
}

// DeleteSchedule removes a schedule.
func (c *Client) DeleteSchedule(ctx context.Context, id string) error {
	_, err := c.api.DeleteSchedule(ctx, &pulsev1.DeleteScheduleRequest{ScheduleId: id})
	return err
}

// ListSchedules returns all schedule definitions.
func (c *Client) ListSchedules(ctx context.Context) ([]ScheduleInfo, error) {
	r, err := c.api.ListSchedules(ctx, &pulsev1.ListSchedulesRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]ScheduleInfo, 0, len(r.GetSchedules()))
	for _, v := range r.GetSchedules() {
		info := ScheduleInfo{
			ID:        v.GetScheduleId(),
			Topic:     v.GetTopic(),
			Kind:      v.GetKind(),
			Cron:      v.GetCron(),
			Paused:    v.GetPaused(),
			NextRunAt: v.GetNextRunAt().AsTime(),
		}
		if v.GetEvery() != nil {
			info.Every = v.GetEvery().AsDuration()
		}
		if v.GetLastRunAt() != nil {
			info.LastRunAt = v.GetLastRunAt().AsTime()
		}
		out = append(out, info)
	}
	return out, nil
}

// ListScheduleJobs returns the jobs a schedule has spawned, with their current status.
func (c *Client) ListScheduleJobs(ctx context.Context, id string) ([]JobInfo, error) {
	r, err := c.api.ListScheduleJobs(ctx, &pulsev1.ListScheduleJobsRequest{ScheduleId: id})
	if err != nil {
		return nil, err
	}
	out := make([]JobInfo, 0, len(r.GetJobs()))
	for _, j := range r.GetJobs() {
		out = append(out, JobInfo{ID: j.GetJobId(), Status: j.GetStatus()})
	}
	return out, nil
}

// ListScheduleFires returns a schedule's fire history (when it ran → which job).
func (c *Client) ListScheduleFires(ctx context.Context, id string) ([]ScheduleFire, error) {
	r, err := c.api.ListScheduleFires(ctx, &pulsev1.ListScheduleFiresRequest{ScheduleId: id})
	if err != nil {
		return nil, err
	}
	out := make([]ScheduleFire, 0, len(r.GetFires()))
	for _, f := range r.GetFires() {
		out = append(out, ScheduleFire{JobID: f.GetJobId(), FiredAt: f.GetFiredAt().AsTime()})
	}
	return out, nil
}
