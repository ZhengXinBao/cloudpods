package models

import (
	"testing"

	api "yunion.io/x/onecloud/pkg/apis/compute"
)

func TestIndependentSyncProviderStatusWaitsForQueue(t *testing.T) {
	if got := syncStatusWithQueue(api.CLOUD_PROVIDER_SYNC_STATUS_IDLE, 1); got != api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING {
		t.Fatalf("active queue must keep provider syncing, got %q", got)
	}
	if got := syncStatusWithQueue(api.CLOUD_PROVIDER_SYNC_STATUS_IDLE, 0); got != api.CLOUD_PROVIDER_SYNC_STATUS_IDLE {
		t.Fatalf("empty queue must allow provider idle, got %q", got)
	}
}
