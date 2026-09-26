package models

import (
	"testing"
	"time"
)

func TestLastSyncCost(t *testing.T) {
	start := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, status string
		start, end   time.Time
		want         string
	}{
		{"completed", "idle", start, start.Add(8*time.Minute + 7*time.Second), "8m7s"},
		{"missing start", "idle", time.Time{}, start, ""},
		{"missing end", "syncing", start, time.Time{}, ""},
		{"old end", "idle", start, start.Add(-time.Hour), ""},
		{"running stale pair", "syncing", start, start.Add(time.Hour), ""},
		{"queued stale pair", "queued", start, start.Add(time.Hour), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := SSyncableBaseResource{SyncStatus: tc.status, LastSync: tc.start, LastSyncEndAt: tc.end}
			if got := r.GetLastSyncCost(); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
