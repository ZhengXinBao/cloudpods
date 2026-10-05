package syncqueue

import "time"

// ClaimTiming is an in-memory snapshot from the existing claim transaction.
// It needs no schema migration and is absent from job-list API responses.
type ClaimTiming struct {
	PreviousState string
	EligibleAt    time.Time
	RetryBackoff  time.Duration
}

func claimTiming(job *Job) *ClaimTiming {
	timing := &ClaimTiming{PreviousState: job.State}
	switch job.State {
	case Waiting, Retry:
		timing.EligibleAt = job.AvailableAt
		if job.State == Retry && !job.UpdatedAt.IsZero() && !job.AvailableAt.IsZero() {
			timing.RetryBackoff = max(0, job.AvailableAt.Sub(job.UpdatedAt))
		}
	case Running:
		if job.LeaseUntil.Valid {
			timing.EligibleAt = job.LeaseUntil.Time
		}
	}
	return timing
}
