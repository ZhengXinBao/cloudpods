// Copyright 2019 Yunion
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package models

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"yunion.io/x/onecloud/pkg/cloudcommon/db"
	"yunion.io/x/pkg/tristate"
	"yunion.io/x/sqlchemy"

	"yunion.io/x/cloudmux/pkg/cloudprovider"

	api "yunion.io/x/onecloud/pkg/apis/compute"
	pkgerrors "yunion.io/x/pkg/errors"
)

type fakeCloudRegion struct {
	vms      []cloudprovider.ICloudVM
	vmErr    error
	eips     []cloudprovider.ICloudEIP
	eipErr   error
	rds      []cloudprovider.ICloudDBInstance
	rdsErr   error
	lbs      []cloudprovider.ICloudLoadbalancer
	lbErr    error
	caches   []cloudprovider.ICloudElasticcache
	cacheErr error
	slow     time.Duration

	vmCalls  int
	eipCalls int
}

func (f *fakeCloudRegion) GetIVMs() ([]cloudprovider.ICloudVM, error) {
	if f.slow > 0 {
		time.Sleep(f.slow)
	}
	f.vmCalls++
	return f.vms, f.vmErr
}
func (f *fakeCloudRegion) GetIEips() ([]cloudprovider.ICloudEIP, error) {
	f.eipCalls++
	return f.eips, f.eipErr
}
func (f *fakeCloudRegion) GetIDBInstances() ([]cloudprovider.ICloudDBInstance, error) {
	return f.rds, f.rdsErr
}
func (f *fakeCloudRegion) GetILoadBalancers() ([]cloudprovider.ICloudLoadbalancer, error) {
	return f.lbs, f.lbErr
}
func (f *fakeCloudRegion) GetIElasticcaches() ([]cloudprovider.ICloudElasticcache, error) {
	return f.caches, f.cacheErr
}

func TestShouldDiscoverProviderRegion(t *testing.T) {
	now := time.Date(2026, 9, 16, 6, 0, 0, 0, time.UTC)
	hour := time.Hour
	tests := []struct {
		name string
		in   providerRegionDiscoverInput
		want bool
	}{
		{name: "disabled idle never probed", in: providerRegionDiscoverInput{}, want: true},
		{name: "enabled already synced skipped", in: providerRegionDiscoverInput{Enabled: true}, want: false},
		{name: "enabled never-synced is scanned", in: providerRegionDiscoverInput{Enabled: true, NeverSynced: true}, want: true},
		{
			name: "busy skipped",
			in:   providerRegionDiscoverInput{SyncStatus: api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING},
			want: false,
		},
		{
			name: "recent miss skipped",
			in:   providerRegionDiscoverInput{LastDiscoverAt: now.Add(-10 * time.Minute)},
			want: false,
		},
		{
			name: "stale miss probed again",
			in:   providerRegionDiscoverInput{LastDiscoverAt: now.Add(-2 * time.Hour)},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldDiscoverProviderRegion(tt.in, now, hour); got != tt.want {
				t.Fatalf("shouldDiscoverProviderRegion(%+v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestSelectDiscoverCandidateIndexes(t *testing.T) {
	now := time.Date(2026, 9, 16, 6, 0, 0, 0, time.UTC)
	items := []providerRegionDiscoverInput{
		{Enabled: true},
		{LastDiscoverAt: now.Add(-3 * time.Hour)},
		{},
		{SyncStatus: api.CLOUD_PROVIDER_SYNC_STATUS_QUEUED},
		{LastDiscoverAt: now.Add(-10 * time.Minute)},
		{LastDiscoverAt: now.Add(-90 * time.Minute)},
		{Enabled: true, NeverSynced: true},
	}
	got := selectDiscoverCandidateIndexes(items, now, time.Hour, 2)
	if len(got) != 2 || got[0] != 6 || got[1] != 2 {
		t.Fatalf("selectDiscoverCandidateIndexes = %v, want [6 2]", got)
	}
}

func TestSelectDiscoverCandidateIndexesBatchBoundsFirstProbe(t *testing.T) {
	now := time.Date(2026, 9, 16, 6, 0, 0, 0, time.UTC)
	items := []providerRegionDiscoverInput{
		{NeverSynced: true, ProviderId: "p-a"},
		{NeverSynced: true, ProviderId: "p-b"},
		{NeverSynced: true, ProviderId: "p-c"},
		{NeverSynced: true, ProviderId: "p-d"},
	}
	got := selectDiscoverCandidateIndexes(items, now, 0, 2)
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("first discover batch = %v, want [0 1]", got)
	}
}

func TestSelectDiscoverCandidateIndexesRoundRobinProviders(t *testing.T) {
	now := time.Date(2026, 9, 16, 6, 0, 0, 0, time.UTC)
	items := []providerRegionDiscoverInput{
		{NeverSynced: true, ProviderId: "p-a"},
		{NeverSynced: true, ProviderId: "p-a"},
		{NeverSynced: true, ProviderId: "p-b"},
		{NeverSynced: true, ProviderId: "p-b"},
		{LastDiscoverAt: now.Add(-3 * time.Hour), ProviderId: "p-a"},
		{LastDiscoverAt: now.Add(-3 * time.Hour), ProviderId: "p-b"},
	}
	got := selectDiscoverCandidateIndexes(items, now, time.Hour, 30)
	want := []int{0, 2, 1, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("selectDiscoverCandidateIndexes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selectDiscoverCandidateIndexes = %v, want %v", got, want)
		}
	}
}

func TestSelectDiscoverCandidateIndexesBatchAfterRoundRobin(t *testing.T) {
	now := time.Date(2026, 9, 16, 6, 0, 0, 0, time.UTC)
	stale := now.Add(-3 * time.Hour)
	items := []providerRegionDiscoverInput{
		{LastDiscoverAt: stale, ProviderId: "p-a"},
		{LastDiscoverAt: stale, ProviderId: "p-a"},
		{LastDiscoverAt: stale, ProviderId: "p-b"},
		{LastDiscoverAt: stale, ProviderId: "p-b"},
		{LastDiscoverAt: stale, ProviderId: "p-c"},
		{LastDiscoverAt: stale, ProviderId: "p-c"},
	}
	got := selectDiscoverCandidateIndexes(items, now, time.Hour, 3)
	want := []int{0, 2, 4}
	if len(got) != len(want) {
		t.Fatalf("selectDiscoverCandidateIndexes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selectDiscoverCandidateIndexes = %v, want %v", got, want)
		}
	}
}

func TestProviderDriverCacheDoesNotHoldMapLockDuringLoad(t *testing.T) {
	cache := providerDriverCache{drivers: make(map[string]cloudprovider.ICloudProvider)}
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		_, _ = cache.get("provider-a", func() (cloudprovider.ICloudProvider, error) {
			close(started)
			<-release
			return nil, errors.New("provider-a unavailable")
		})
		close(firstDone)
	}()
	<-started

	secondDone := make(chan struct{})
	go func() {
		_, _ = cache.get("provider-b", func() (cloudprovider.ICloudProvider, error) {
			return nil, errors.New("provider-b unavailable")
		})
		close(secondDone)
	}()
	select {
	case <-secondDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("provider cache serialized an unrelated provider load")
	}
	close(release)
	<-firstDone
}

func TestFirstInventorySyncRange(t *testing.T) {
	rng := firstInventorySyncRange()
	if rng.FullSync || rng.DeepSync {
		t.Fatal("first inventory must not full-sync or deep-sync buckets and images")
	}
	if !rng.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_COMPUTE) ||
		!rng.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_NETWORK) ||
		!rng.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_RDS) ||
		!rng.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_LOADBALANCER) {
		t.Fatal("first inventory should include the compute-first set")
	}
	if rng.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_OBJECTSTORE) || rng.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_IMAGE) {
		t.Fatal("first inventory should skip objectstore and image")
	}
	if rng.IsNotSkipSyncResource(CachedimageManager) || rng.IsNotSkipSyncResource(BucketManager) {
		t.Fatal("first inventory should skip cached images and buckets")
	}
}

func TestShouldFullSyncProviderRegion(t *testing.T) {
	empty := SCloudproviderregion{Enabled: true}
	if empty.shouldFullSync() {
		t.Fatal("enabled never-synced without discover hit must not full sync")
	}
	hit := time.Date(2026, 9, 16, 5, 0, 0, 0, time.UTC)
	discovered := SCloudproviderregion{Enabled: true, LastDiscoverHitAt: hit}
	if !discovered.shouldFullSync() {
		t.Fatal("recent discover hit should full sync")
	}
	imported := SCloudproviderregion{Enabled: true}
	imported.LastSyncEndAt = hit
	if !imported.shouldFullSync() {
		t.Fatal("already imported enabled region should refresh")
	}
}

func TestIsRecentlyDiscoveredProviderRegion(t *testing.T) {
	hit := time.Date(2026, 9, 16, 5, 0, 0, 0, time.UTC)
	sync := hit.Add(-time.Hour)
	if !isRecentlyDiscoveredProviderRegion(SCloudproviderregion{
		SSyncableBaseResource: SSyncableBaseResource{LastSync: sync},
		LastDiscoverHitAt:     hit,
	}) {
		t.Fatal("hit after last sync should keep region on")
	}
	if isRecentlyDiscoveredProviderRegion(SCloudproviderregion{
		SSyncableBaseResource: SSyncableBaseResource{LastSync: hit.Add(time.Hour)},
		LastDiscoverHitAt:     hit,
	}) {
		t.Fatal("completed sync after discovery should fall back to local inventory")
	}
	if isRecentlyDiscoveredProviderRegion(SCloudproviderregion{}) {
		t.Fatal("never discovered should not keep region on")
	}
}

func TestCloudRegionHasResources(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		region := &fakeCloudRegion{}
		got, err := cloudRegionHasResources(region)
		if err != nil || got {
			t.Fatalf("empty region = %v, %v", got, err)
		}
		if region.vmCalls != 1 || region.eipCalls != 1 {
			t.Fatalf("expected all cheap checks, vm=%d eip=%d", region.vmCalls, region.eipCalls)
		}
	})
	t.Run("vm hit short circuits", func(t *testing.T) {
		region := &fakeCloudRegion{vms: []cloudprovider.ICloudVM{nil}}
		got, err := cloudRegionHasResources(region)
		if err != nil || !got {
			t.Fatalf("vm hit = %v, %v", got, err)
		}
		if region.eipCalls != 0 {
			t.Fatal("should not probe eips after vm hit")
		}
	})
	t.Run("unsupported vm still checks eip", func(t *testing.T) {
		region := &fakeCloudRegion{
			vmErr: pkgerrors.Wrap(cloudprovider.ErrNotSupported, "vm"),
			eips:  []cloudprovider.ICloudEIP{nil},
		}
		got, err := cloudRegionHasResources(region)
		if err != nil || !got {
			t.Fatalf("eip after unsupported vm = %v, %v", got, err)
		}
	})
	t.Run("hard error fails open", func(t *testing.T) {
		region := &fakeCloudRegion{vmErr: errors.New("throttled")}
		got, err := cloudRegionHasResources(region)
		if err == nil || got {
			t.Fatalf("hard error = %v, %v", got, err)
		}
	})
	t.Run("auth failure is returned", func(t *testing.T) {
		region := &fakeCloudRegion{vmErr: errors.New("GetInstances: InvalidClientTokenId: The security token included in the request is invalid.")}
		got, err := cloudRegionHasResources(region)
		if err == nil || got {
			t.Fatalf("unavailable region = %v, %v", got, err)
		}
		if region.eipCalls != 0 {
			t.Fatal("should not probe eips after region unavailable")
		}
	})
	t.Run("timeout wrapper keeps hit", func(t *testing.T) {
		region := &fakeCloudRegion{vms: []cloudprovider.ICloudVM{nil}}
		got, err := cloudRegionHasResourcesWithTimeout(region, time.Second)
		if err != nil || !got {
			t.Fatalf("timeout wrapper = %v, %v", got, err)
		}
	})
	t.Run("timeout returns without waiting for probe", func(t *testing.T) {
		region := &fakeCloudRegion{slow: 200 * time.Millisecond, vms: []cloudprovider.ICloudVM{nil}}
		start := time.Now()
		got, err := cloudRegionHasResourcesWithTimeout(region, 20*time.Millisecond)
		if time.Since(start) > 80*time.Millisecond {
			t.Fatal("timeout still blocked on the probe")
		}
		if err == nil || got {
			t.Fatalf("timeout must fail-open, got %v, %v", got, err)
		}
	})
	t.Run("timeout exposes probe completion", func(t *testing.T) {
		region := &fakeCloudRegion{slow: 40 * time.Millisecond, vms: []cloudprovider.ICloudVM{nil}}
		_, err, done := cloudRegionHasResourcesWithTimeoutDone(region, 5*time.Millisecond)
		if err == nil {
			t.Fatal("slow probe should time out")
		}
		select {
		case <-done:
			t.Fatal("probe must still be running after timeout")
		default:
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("probe did not report completion")
		}
	})
}

func TestExhaustiveDiscoveryCoversAllCandidates(t *testing.T) {
	now := time.Now()
	items := make([]providerRegionDiscoverInput, 45)
	for i := range items {
		items[i] = providerRegionDiscoverInput{Enabled: true, NeverSynced: true, LastDiscoverAt: now.Add(-time.Minute)}
	}
	items = append(items, providerRegionDiscoverInput{NeverSynced: true, SyncStatus: api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING})
	for _, exhaustive := range []bool{false, true} {
		interval, batch := cloudRegionDiscoverLimits(3600, 30, exhaustive)
		got := selectDiscoverCandidateIndexes(items, now, interval, batch)
		want := 0
		if exhaustive {
			want = 45
		}
		if len(got) != want {
			t.Errorf("exhaustive=%v: selected %d candidates, want %d", exhaustive, len(got), want)
		}
	}
}

func TestDiscoveryReturnsDatabaseEnumerationFailure(t *testing.T) {
	// Queries are constructed normally; the injected fetch supplies database failures.
	if sqlchemy.GetDefaultDB() == nil {
		sqlchemy.SetDefaultDB(nil)
	}
	original := CloudproviderRegionManager
	if original == nil {
		CloudproviderRegionManager = &SCloudproviderregionManager{SJointResourceBaseManager: db.NewJointResourceBaseManager(SCloudproviderregion{}, "cloud_provider_regions_tbl", "cloudproviderregion", "cloudproviderregions", CloudproviderManager, CloudregionManager)}
		CloudproviderRegionManager.SetVirtualObject(CloudproviderRegionManager)
		t.Cleanup(func() { CloudproviderRegionManager = original })
	}
	for _, failAt := range []int{1, 2} {
		calls := 0
		failure := errors.New("database connection lost")
		account := &SCloudaccount{}
		account.Id = "account-id"
		err := account.discoverCloudproviderRegions(context.Background(), nil, false, true, func(manager db.IModelManager, query *sqlchemy.SQuery, targets interface{}) error {
			calls++
			if calls == failAt {
				return failure
			}
			provider := SCloudprovider{}
			provider.Id = "provider-id"
			provider.Enabled = tristate.True
			*targets.(*[]SCloudprovider) = []SCloudprovider{provider}
			rows := *targets.(*[]SCloudprovider)
			rows[0].SetModelManager(CloudproviderManager, &rows[0])
			return nil
		})
		if pkgerrors.Cause(err) != failure || !strings.Contains(err.Error(), "discover") {
			t.Errorf("failure at query %d was lost or lacks context: %v", failAt, err)
		}
		if calls != failAt {
			t.Errorf("queries=%d, want stop at failing query %d", calls, failAt)
		}
	}
}
