package pkg_errors

import "errors"

var (
	ErrNotFound = errors.New("not found")

	// ErrSequenceConflict is returned when an event append loses the race for a
	// job's next sequence (the UNIQUE(job_id, sequence) constraint rejected it).
	// The service layer retries on this without knowing it came from Postgres.
	ErrSequenceConflict = errors.New("event sequence conflict")
)
