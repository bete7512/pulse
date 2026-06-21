// Package pulse is the client SDK for the pulse job server. A single *Client plays
// both roles — producer (Submit) and consumer (Handle + Start/Run) — over one shared
// gRPC connection. Developers only ever import this package and know a URL.
package pulse

import (
	"context"

	"github.com/bete7512/pulse/gen/pulsev1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the SDK handle. Use it to Submit jobs (producer) and/or to register
// handlers and Run/Start a worker (consumer). It is safe for concurrent use.
type Client struct {
	cc          *grpc.ClientConn
	api         pulsev1.PulseServiceClient
	handlers    map[string]func(Job) error // topic -> raw handler (JobType[T].Handle wraps the typed fn)
	concurrency int
}

// Option configures a Client.
type Option func(*Client)

// WithConcurrency caps how many handlers run at once on the consumer side (default 10).
func WithConcurrency(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// New dials addr and returns a Client. Insecure transport by default (local dev);
// the same connection serves both the producer and consumer roles.
func New(addr string, opts ...Option) (*Client, error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := &Client{
		cc:          cc,
		api:         pulsev1.NewPulseServiceClient(cc),
		handlers:    make(map[string]func(Job) error),
		concurrency: 10,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// Close releases the gRPC connection.
func (c *Client) Close() error { return c.cc.Close() }

// Submit enqueues a job with a raw JSON payload under topic. Prefer the typed
// JobType[T].Submit — this is the escape hatch for dynamically-typed topics.
func (c *Client) Submit(ctx context.Context, topic string, payload []byte) (string, error) {
	r, err := c.api.SubmitJob(ctx, &pulsev1.SubmitJobRequest{Topic: topic, Payload: payload})
	if err != nil {
		return "", err
	}
	return r.GetJobId(), nil
}

// JobInfo is the SDK-facing view of a job's current state.
type JobInfo struct {
	ID     string
	Status string
}

// GetJob returns a job's current status, read from the server's read model.
func (c *Client) GetJob(ctx context.Context, jobID string) (JobInfo, error) {
	r, err := c.api.GetJob(ctx, &pulsev1.GetJobRequest{JobId: jobID})
	if err != nil {
		return JobInfo{}, err
	}
	j := r.GetJob()
	return JobInfo{ID: j.GetJobId(), Status: j.GetStatus()}, nil
}

// register stores a raw handler under a topic. JobType[T].Handle calls this.
func (c *Client) register(topic string, h func(Job) error) {
	c.handlers[topic] = h
}

// Start opens the job stream and begins processing in the background, bounded by the
// configured concurrency. It returns as soon as the stream is open; processing runs
// until ctx is cancelled or the stream fails.
func (c *Client) Start(ctx context.Context) error {
	stream, err := c.api.StreamJobs(ctx, &pulsev1.StreamJobsRequest{Topics: c.topics()})
	if err != nil {
		return err
	}
	sem := make(chan struct{}, c.concurrency) // bounded worker pool
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				return // ctx cancelled or stream closed. TODO(phase5): reconnect with backoff.
			}
			a := resp.GetAssignment()
			if a == nil {
				continue
			}
			sem <- struct{}{} // blocks here when all slots are busy = natural backpressure
			go func(a *pulsev1.JobAssignment) {
				defer func() { <-sem }()
				c.handle(ctx, a)
			}(a)
		}
	}()
	return nil
}

// Run starts processing and blocks until ctx is cancelled. Convenience for a
// worker-only binary.
func (c *Client) Run(ctx context.Context) error {
	if err := c.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// handle dispatches one assignment to its registered handler and reports the result.
func (c *Client) handle(ctx context.Context, a *pulsev1.JobAssignment) {
	h := c.handlers[a.Topic]
	if h == nil {
		c.report(ctx, a.JobId, false, "no handler registered for topic "+a.Topic)
		return
	}
	err := h(Job{
		ID:      a.JobId,
		Name:    a.Topic,
		Attempt: int(a.Attempt),
		Payload: a.Payload,
		ctx:     ctx,
	})
	c.report(ctx, a.JobId, err == nil, errString(err))
}

// report is best-effort: a failed report means the job may be redelivered
// (at-least-once), which is why handlers should be idempotent.
func (c *Client) report(ctx context.Context, jobID string, ok bool, errMsg string) {
	_, _ = c.api.ReportResult(ctx, &pulsev1.ReportResultRequest{
		JobId: jobID, Success: ok, Error: errMsg,
	})
}

func (c *Client) topics() []string {
	out := make([]string, 0, len(c.handlers))
	for t := range c.handlers {
		out = append(out, t)
	}
	return out
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
