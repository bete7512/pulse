// Package pulse is the client SDK for the pulse job server. A single *Client plays
// both roles — producer (Submit) and consumer (Handle + Start/Run) — over one shared
// gRPC connection. Developers only ever import this package and know a URL.
package pulse

import (
	"context"
	"time"

	"crypto/tls"

	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/gofrs/uuid/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// heartbeatInterval is how often a running handler renews its liveness. It must be
// comfortably below the server's liveness TTL (≈3×) so a couple of dropped beats
// don't trip a false recovery.
const heartbeatInterval = 10 * time.Second

// Client is the SDK handle. Use it to Submit jobs (producer) and/or to register
// handlers and Run/Start a worker (consumer). It is safe for concurrent use.
type Client struct {
	cc          *grpc.ClientConn
	api         pulsev1.PulseServiceClient
	handlers    map[string]func(Job) error // topic -> raw handler (JobType[T].Handle wraps the typed fn)
	concurrency int
	workerID    string                           // identifies this worker for liveness (heartbeats + fencing)
	creds       credentials.TransportCredentials // transport security; defaults to insecure (local dev)
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

// WithTLS secures the connection using TLS. Pass nil to use the host's default
// settings and system root CAs (the common case for a server with a public cert);
// pass a *tls.Config to customise roots, client certs (mTLS), or the server name.
func WithTLS(cfg *tls.Config) Option {
	return func(c *Client) {
		c.creds = credentials.NewTLS(cfg)
	}
}

// WithTransportCredentials sets the gRPC transport credentials directly, for callers
// who need full control over transport security. Overrides the insecure default.
func WithTransportCredentials(creds credentials.TransportCredentials) Option {
	return func(c *Client) {
		if creds != nil {
			c.creds = creds
		}
	}
}

// New dials addr and returns a Client. Insecure transport by default (local dev);
// pass WithTLS (or WithTransportCredentials) to secure the connection. The same
// connection serves both the producer and consumer roles.
func New(addr string, opts ...Option) (*Client, error) {
	workerID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	c := &Client{
		handlers:    make(map[string]func(Job) error),
		concurrency: 10,
		workerID:    workerID.String(),
		creds:       insecure.NewCredentials(),
	}
	for _, o := range opts {
		o(c)
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(c.creds))
	if err != nil {
		return nil, err
	}
	c.cc = cc
	c.api = pulsev1.NewPulseServiceClient(cc)
	return c, nil
}

// Close releases the gRPC connection.
func (c *Client) Close() error { return c.cc.Close() }

// Submit enqueues a job with a raw JSON payload under topic. Prefer the typed
// JobType[T].Submit — this is the escape hatch for dynamically-typed topics.
// Pass WithPriority to dispatch ahead of lower-priority work on the same topic.
func (c *Client) Submit(ctx context.Context, topic string, payload []byte, opts ...SubmitOption) (string, error) {
	req := &pulsev1.SubmitJobRequest{Topic: topic, Payload: payload}
	for _, opt := range opts {
		opt(req)
	}
	r, err := c.api.SubmitJob(ctx, req)
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
	stream, err := c.api.StreamJobs(ctx, &pulsev1.StreamJobsRequest{Topics: c.topics(), WorkerId: c.workerID})
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
// While the handler runs, a background ticker heartbeats the job's liveness so the
// watchdog doesn't reclaim a job that's simply long-running; the beat stops the moment
// the handler returns.
func (c *Client) handle(ctx context.Context, a *pulsev1.JobAssignment) {
	h := c.handlers[a.Topic]
	if h == nil {
		c.report(ctx, a.JobId, false, "no handler registered for topic "+a.Topic)
		return
	}

	hbCtx, stopBeat := context.WithCancel(ctx)
	defer stopBeat()
	go c.heartbeat(hbCtx, a.JobId)

	err := h(Job{
		ID:      a.JobId,
		Name:    a.Topic,
		Attempt: int(a.Attempt),
		Payload: a.Payload,
		ctx:     ctx,
	})
	stopBeat() // stop renewing liveness before we report the terminal result
	c.report(ctx, a.JobId, err == nil, errString(err))
}

// heartbeat renews the job's liveness every heartbeatInterval until ctx is cancelled
// (handler finished or worker shutting down). Best-effort: a missed beat just risks
// an early reap, which the at-least-once + idempotency contract already tolerates.
func (c *Client) heartbeat(ctx context.Context, jobID string) {
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = c.api.Heartbeat(ctx, &pulsev1.HeartbeatRequest{JobId: jobID, WorkerId: c.workerID})
		}
	}
}

// report is best-effort: a failed report means the job may be redelivered
// (at-least-once), which is why handlers should be idempotent.
func (c *Client) report(ctx context.Context, jobID string, ok bool, errMsg string) {
	_, _ = c.api.ReportResult(ctx, &pulsev1.ReportResultRequest{
		JobId:   jobID,
		Success: ok,
		Error:   errMsg,
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
