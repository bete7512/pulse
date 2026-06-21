package pulse

import (
	"context"
	"encoding/json"
)

// Job is the raw view a low-level handler receives. Most code never touches it —
// use Register, which hands you a decoded, typed payload. It's here for the dynamic
// escape hatch and for the ID/Attempt fields (e.g. an idempotency key).
type Job struct {
	ID      string
	Name    string
	Attempt int
	Payload []byte
	ctx     context.Context
}

// Context returns the per-job context (cancelled when the worker stops).
func (j Job) Context() context.Context { return j.ctx }

// Unmarshal decodes the JSON payload into v.
func (j Job) Unmarshal(v any) error { return json.Unmarshal(j.Payload, v) }

// Register binds a handler to a job name. The handler is a plain typed function —
// the SDK decodes the payload into T for you. Call it once at startup, before Run.
//
//	pulse.Register(p, "send-email", func(ctx context.Context, a EmailArgs) error {
//		return sendEmail(a.To)
//	})
func Register[T any](c *Client, name string, fn func(context.Context, T) error) {
	c.register(name, func(j Job) error {
		var args T
		if err := j.Unmarshal(&args); err != nil {
			return err
		}
		return fn(j.Context(), args)
	})
}

// Enqueue submits a job under name, JSON-encoding args. Mirror of Register.
//
//	pulse.Enqueue(ctx, p, "send-email", EmailArgs{To: "a@b.com"})
func Enqueue[T any](ctx context.Context, c *Client, name string, args T) (string, error) {
	b, err := json.Marshal(args)
	if err != nil {
		return "", err
	}
	return c.Submit(ctx, name, b)
}
