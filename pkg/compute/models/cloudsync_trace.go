package models

import (
	"context"

	"yunion.io/x/onecloud/pkg/compute/synctrace"
)

// Closures retain the original SDK/lock context; ctx is only for diagnostics.
func traceCloudSyncStage(ctx context.Context, phase string, work func() error) error {
	if !synctrace.Enabled(ctx) {
		return work()
	}
	return synctrace.Measure(ctx, phase, work)
}

// Result-set and void routines report their own failures, so observe only cost.
func observeCloudSyncStage(ctx context.Context, phase string, work func()) {
	synctrace.Observe(ctx, phase, work)
}

// Reuse existing timers without changing synchronization or adding locks to the
// result map. Call after the synchronous executor returns, including on errors.
// Coverage is partial: "reconcile" includes application work, not just SQL time.
func traceSyncResultCosts(ctx context.Context, results SSyncResultSet) {
	if !synctrace.Enabled(ctx) {
		return
	}
	for resource, result := range results {
		if result == nil {
			continue
		}
		traceCtx := synctrace.WithIdentity(ctx, synctrace.Identity{Resource: resource})
		if result.rc > 0 {
			synctrace.Cost(traceCtx, "resource_request", result.rc)
		}
		if result.sc > 0 {
			synctrace.Cost(traceCtx, "resource_reconcile", result.sc)
		}
	}
}

// A timed-out probe can still own driver connections. Instrument that wait, but
// never use ctx.Done() to bypass it or free the provider's scheduling slot.
func traceCloudProbeDrain(ctx context.Context, done <-chan struct{}) {
	select {
	case <-done:
		return
	default:
	}
	_ = synctrace.Measure(ctx, "probe_drain", func() error {
		<-done
		return nil
	})
}
