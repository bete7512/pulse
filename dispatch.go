package pulse

import (
	"context"
	"time"

	"github.com/bete7512/pulse/gen/pulsev1"
)

// DispatchStatus is the SDK-facing view of the dispatch pause switch.
type DispatchStatus struct {
	Paused   bool
	Reason   string
	PausedAt time.Time // zero if never paused
}

// PauseDispatch stops the server from handing new jobs to workers, server-wide. Submits keep
// working and running jobs keep finishing — only the start of new work is gated. reason is an
// optional operator note surfaced by DispatchStatus.
func (c *Client) PauseDispatch(ctx context.Context, reason string) error {
	_, err := c.api.PauseDispatch(ctx, &pulsev1.PauseDispatchRequest{Reason: reason})
	return err
}

// ResumeDispatch re-enables dispatch; the accumulated backlog flows again, priority-ordered.
func (c *Client) ResumeDispatch(ctx context.Context) error {
	_, err := c.api.ResumeDispatch(ctx, &pulsev1.ResumeDispatchRequest{})
	return err
}

// DispatchStatus reports whether dispatch is paused, since when, and why.
func (c *Client) DispatchStatus(ctx context.Context) (DispatchStatus, error) {
	r, err := c.api.GetDispatchStatus(ctx, &pulsev1.GetDispatchStatusRequest{})
	if err != nil {
		return DispatchStatus{}, err
	}
	st := DispatchStatus{Paused: r.Paused, Reason: r.Reason}
	if r.PausedAt != nil {
		st.PausedAt = r.PausedAt.AsTime()
	}
	return st, nil
}
