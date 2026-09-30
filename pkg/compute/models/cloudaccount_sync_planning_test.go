package models

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	api "yunion.io/x/onecloud/pkg/apis/compute"
)

func TestPlanIndependentProvidersStreamsAndPreservesRunScope(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	ready := make(chan string, 3)
	result := make(chan error, 1)
	sr := SSyncRange{SyncRangeInput: api.SyncRangeInput{Region: []string{"region-1"}, FullSync: true}}
	want := AccountFanoutSyncRange(sr)
	failure := errors.New("enqueue failed")
	go func() {
		result <- planIndependentSyncProviders(context.Background(), sr, "original-run",
			func(ctx context.Context, onReady func(context.Context, *SCloudprovider) error) error {
				return runCloudProviderWork(ctx, []cloudProviderWork{
					{providerID: "slow", run: func(context.Context) error { <-release; return nil }},
					{providerID: "fast"},
				}, 2, func(ctx context.Context, id string) error { p := &SCloudprovider{}; p.Id = id; return onReady(ctx, p) })
			}, func(ctx context.Context, p *SCloudprovider, got SSyncRange, runID string) error {
				if !reflect.DeepEqual(got, want) || runID != "original-run" {
					t.Errorf("scope/run changed: %+v %s", got, runID)
				}
				ready <- p.Id
				if p.Id == "fast" {
					return failure
				}
				return nil
			})
	}()
	if id := awaitProviderWork(t, ready); id != "fast" {
		t.Fatalf("ready=%s", id)
	}
	select {
	case err := <-result:
		t.Fatalf("sealed while slow discovery pending: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	if id := awaitProviderWork(t, ready); id != "slow" {
		t.Fatalf("ready=%s", id)
	}
	if err := <-result; !errors.Is(err, failure) {
		t.Fatalf("lost enqueue error: %v", err)
	}
}

func TestPlanIndependentProvidersCollectsDiscoveryErrorsWithoutSkippingReady(t *testing.T) {
	called := false
	err := planIndependentSyncProviders(context.Background(), SSyncRange{}, "run",
		func(ctx context.Context, onReady func(context.Context, *SCloudprovider) error) error {
			cloudSyncError(ctx, "discovery failed")
			return onReady(ctx, &SCloudprovider{})
		}, func(context.Context, *SCloudprovider, SSyncRange, string) error { called = true; return nil })
	if err == nil || !called {
		t.Fatalf("err=%v ready=%v", err, called)
	}
}
