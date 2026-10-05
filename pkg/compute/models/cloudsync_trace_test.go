package models

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"yunion.io/x/onecloud/pkg/compute/synctrace"
	"yunion.io/x/pkg/tristate"
)

type syncTraceCapture struct {
	mu     sync.Mutex
	events []synctrace.Event
}

func (c *syncTraceCapture) context() context.Context {
	ctx := synctrace.WithIdentity(context.Background(), synctrace.Identity{AccountID: "account", RunID: "run"})
	return synctrace.WithSink(ctx, func(e synctrace.Event) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.events = append(c.events, e)
	})
}

func (c *syncTraceCapture) ends(phase string) []synctrace.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	var events []synctrace.Event
	for _, e := range c.events {
		if e.Phase == phase && e.Kind != "start" {
			events = append(events, e)
		}
	}
	return events
}

func TestTraceProviderPreparation(t *testing.T) {
	capture := &syncTraceCapture{}
	ctx := capture.context()
	providers := make([]SCloudprovider, 3)
	for i, id := range []string{"one", "two", "disabled"} {
		providers[i].Id = id
		providers[i].CloudaccountId = "account"
		providers[i].Enabled = tristate.False
		if i != 2 {
			providers[i].Enabled = tristate.True
		}
	}
	want := errors.New("timeout")
	err := prepareAccountProviderRegions(ctx, providers, 2, func(ctx context.Context, p *SCloudprovider) error {
		if p.Id == "two" {
			return want
		}
		return nil
	})
	if !errors.Is(err, want) {
		t.Fatalf("work result changed: %v", err)
	}
	events := capture.ends("provider_prepare")
	if len(events) != 2 {
		t.Fatalf("missing provider events: %+v", capture.events)
	}
	for _, e := range events {
		if e.AccountID != "account" || e.RunID != "run" || e.ProviderID == "disabled" || (e.Outcome == "error") != (e.ProviderID == "two") {
			t.Fatalf("invalid provider event: %+v", e)
		}
	}
}

func TestTracePlanningPreservesCallbackContextAndFailure(t *testing.T) {
	capture := &syncTraceCapture{}
	ctx := capture.context()
	var discovered context.Context
	want := errors.New("access denied")
	err := planIndependentSyncProviders(ctx, SSyncRange{}, "run",
		func(ctx context.Context, ready func(context.Context, *SCloudprovider) error) error {
			discovered = ctx
			p := &SCloudprovider{}
			p.Id = "provider"
			p.CloudaccountId = "account"
			return ready(ctx, p)
		},
		func(ctx context.Context, p *SCloudprovider, sr SSyncRange, run string) error {
			if ctx != discovered {
				t.Error("tracing changed provider lock owner")
			}
			if run != "run" {
				t.Errorf("run changed: %s", run)
			}
			return want
		})
	if !errors.Is(err, want) {
		t.Fatalf("failure lost: %v", err)
	}
	for _, phase := range []string{"discover_plan", "provider_plan"} {
		events := capture.ends(phase)
		if len(events) != 1 || events[0].Outcome != "error" || events[0].RunID != "run" {
			t.Fatalf("missing %s failure: %+v", phase, capture.events)
		}
	}
}

func TestTraceResourceCostsDoNotClaimSuccessfulInventory(t *testing.T) {
	capture := &syncTraceCapture{}
	ctx := synctrace.WithIdentity(capture.context(), synctrace.Identity{JobID: "job", ResourceGroup: "core", Attempt: 2})
	traceSyncResultCosts(ctx, SSyncResultSet{
		"servers": {rc: 2 * time.Second, sc: 3 * time.Second},
		"empty":   {},
		"nil":     nil,
	})
	events := capture.ends("resource_request")
	if len(events) != 1 || events[0].DurationMS != 2000 || events[0].Resource != "servers" || events[0].JobID != "job" || events[0].Outcome != "" {
		t.Fatalf("incorrect request costs: %+v", capture.events)
	}
	events = capture.ends("resource_reconcile")
	if len(events) != 1 || events[0].DurationMS != 3000 || events[0].Outcome != "" {
		t.Fatalf("incorrect reconciliation costs: %+v", capture.events)
	}
}

func TestTraceProbeDrainDoesNotAbandonCanceledSDK(t *testing.T) {
	events := make(chan synctrace.Event, 4)
	ctx, cancel := context.WithCancel(synctrace.WithSink(context.Background(), func(e synctrace.Event) { events <- e }))
	defer cancel()
	drained := make(chan struct{})
	done := make(chan struct{})
	go func() {
		traceCloudProbeDrain(ctx, drained)
		close(done)
	}()
	select {
	case e := <-events:
		if e.Phase != "probe_drain" || e.Kind != "start" {
			t.Errorf("missing drain start: %+v", e)
		}
	case <-time.After(time.Second):
		close(drained)
		t.Fatal("missing drain start")
	}
	cancel()
	select {
	case <-done:
		t.Error("canceled diagnostic scope abandoned SDK")
	case <-time.After(10 * time.Millisecond):
	}
	close(drained)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drain did not return")
	}
	e := <-events
	if e.Phase != "probe_drain" || e.Kind != "end" {
		t.Fatalf("missing drain completion: %+v", e)
	}
}

func TestTraceCompletedProbeDoesNotAddDrainEvents(t *testing.T) {
	capture := &syncTraceCapture{}
	done := make(chan struct{})
	close(done)
	traceCloudProbeDrain(capture.context(), done)
	if len(capture.events) != 0 {
		t.Fatalf("already drained probe generated events: %+v", capture.events)
	}
}
