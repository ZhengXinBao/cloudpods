package syncqueue

import (
	"database/sql"
	"testing"
	"time"
)

func TestClaimTimingSeparatesRetryBackoffAndLeaseRecovery(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		state    string
		eligible time.Time
		backoff  time.Duration
	}{
		{Waiting, now.Add(time.Minute), 0},
		{Retry, now.Add(time.Minute), time.Minute},
		{Running, now.Add(2 * time.Minute), 0},
	} {
		t.Run(tc.state, func(t *testing.T) {
			j := &Job{State: tc.state, UpdatedAt: now, AvailableAt: now.Add(time.Minute), LeaseUntil: sql.NullTime{Valid: true, Time: now.Add(2 * time.Minute)}}
			timing := claimTiming(j)
			if timing.PreviousState != tc.state || !timing.EligibleAt.Equal(tc.eligible) || timing.RetryBackoff != tc.backoff {
				t.Fatalf("invalid timing: %+v", timing)
			}
		})
	}
}

func TestClaimTimingMissingLeaseDoesNotInventQueueWait(t *testing.T) {
	timing := claimTiming(&Job{State: Running, AvailableAt: time.Now()})
	if !timing.EligibleAt.IsZero() || timing.RetryBackoff != 0 {
		t.Fatalf("missing lease treated as waiting job: %+v", timing)
	}
}
