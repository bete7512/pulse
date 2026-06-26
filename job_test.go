package pulse

import (
	"testing"

	"github.com/bete7512/pulse/gen/pulsev1"
)

// TestWithPriority verifies the submit option sets the request's priority field, and that
// it is genuinely opt-in: a request built without it keeps the zero-value default.
func TestWithPriority(t *testing.T) {
	var req pulsev1.SubmitJobRequest
	if req.GetPriority() != 0 {
		t.Fatalf("default priority = %d, want 0", req.GetPriority())
	}

	WithPriority(10)(&req)
	if req.GetPriority() != 10 {
		t.Errorf("priority = %d, want 10", req.GetPriority())
	}

	WithPriority(-3)(&req) // negative de-prioritizes
	if req.GetPriority() != -3 {
		t.Errorf("priority = %d, want -3", req.GetPriority())
	}
}
