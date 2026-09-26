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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/log"
	"yunion.io/x/pkg/errors"
	"yunion.io/x/pkg/util/timeutils"
	"yunion.io/x/sqlchemy"

	api "yunion.io/x/onecloud/pkg/apis/compute"
	"yunion.io/x/onecloud/pkg/cloudcommon/db"
	"yunion.io/x/onecloud/pkg/compute/options"
	"yunion.io/x/onecloud/pkg/mcclient"
)

const cloudRegionDiscoverTimeout = 20 * time.Second

type providerRegionDiscoverInput struct {
	Enabled        bool
	NeverSynced    bool
	SyncStatus     string
	LastDiscoverAt time.Time
	ProviderId     string
}

type cloudRegionInventory interface {
	GetIVMs() ([]cloudprovider.ICloudVM, error)
	GetIEips() ([]cloudprovider.ICloudEIP, error)
	GetIDBInstances() ([]cloudprovider.ICloudDBInstance, error)
	GetILoadBalancers() ([]cloudprovider.ICloudLoadbalancer, error)
	GetIElasticcaches() ([]cloudprovider.ICloudElasticcache, error)
}

type providerDriverCache struct {
	mu      sync.Mutex
	drivers map[string]cloudprovider.ICloudProvider
	errs    map[string]error
}

func (c *providerDriverCache) get(id string, load func() (cloudprovider.ICloudProvider, error)) (cloudprovider.ICloudProvider, error) {
	c.mu.Lock()
	if driver, ok := c.drivers[id]; ok {
		c.mu.Unlock()
		return driver, nil
	}
	if err, ok := c.errs[id]; ok {
		c.mu.Unlock()
		return nil, err
	}
	c.mu.Unlock()

	driver, err := load()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		if c.errs == nil {
			c.errs = make(map[string]error)
		}
		c.errs[id] = err
		return nil, err
	}
	if c.drivers == nil {
		c.drivers = make(map[string]cloudprovider.ICloudProvider)
	}
	c.drivers[id] = driver
	return driver, nil
}

func shouldDiscoverProviderRegion(in providerRegionDiscoverInput, now time.Time, interval time.Duration) bool {
	if isProviderRegionBusy(in.SyncStatus) {
		return false
	}
	if in.NeverSynced {
		if interval > 0 && !in.LastDiscoverAt.IsZero() && now.Sub(in.LastDiscoverAt) < interval {
			return false
		}
		return true
	}
	if in.Enabled {
		return false
	}
	if interval > 0 && !in.LastDiscoverAt.IsZero() && now.Sub(in.LastDiscoverAt) < interval {
		return false
	}
	return true
}

type discoverCand struct {
	index       int
	neverSynced bool
	at          time.Time
	providerId  string
}

func selectDiscoverCandidateIndexes(items []providerRegionDiscoverInput, now time.Time, interval time.Duration, batch int) []int {
	cands := make([]discoverCand, 0, len(items))
	for i, in := range items {
		if !shouldDiscoverProviderRegion(in, now, interval) {
			continue
		}
		cands = append(cands, discoverCand{index: i, neverSynced: in.NeverSynced, at: in.LastDiscoverAt, providerId: in.ProviderId})
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].neverSynced != cands[j].neverSynced {
			return cands[i].neverSynced
		}
		if cands[i].at.IsZero() != cands[j].at.IsZero() {
			return cands[i].at.IsZero()
		}
		return cands[i].at.Before(cands[j].at)
	})
	neverSynced := make([]discoverCand, 0, len(cands))
	reprobes := make([]discoverCand, 0)
	for _, c := range cands {
		if c.neverSynced {
			neverSynced = append(neverSynced, c)
		} else {
			reprobes = append(reprobes, c)
		}
	}
	reprobes = interleaveDiscoverCandsByProvider(reprobes)
	cands = append(interleaveDiscoverCandsByProvider(neverSynced), reprobes...)
	if batch > 0 && len(cands) > batch {
		cands = cands[:batch]
	}
	out := make([]int, len(cands))
	for i := range cands {
		out[i] = cands[i].index
	}
	return out
}

func interleaveDiscoverCandsByProvider(cands []discoverCand) []discoverCand {
	if len(cands) < 2 {
		return cands
	}
	groups := make([][]discoverCand, 0)
	indexOf := map[string]int{}
	for _, c := range cands {
		key := c.providerId
		if key == "" {
			key = fmt.Sprintf("#%d", c.index)
		}
		gi, ok := indexOf[key]
		if !ok {
			gi = len(groups)
			indexOf[key] = gi
			groups = append(groups, nil)
		}
		groups[gi] = append(groups[gi], c)
	}
	out := make([]discoverCand, 0, len(cands))
	for added := 0; added < len(cands); {
		progress := false
		for i := range groups {
			if len(groups[i]) == 0 {
				continue
			}
			out = append(out, groups[i][0])
			groups[i] = groups[i][1:]
			added++
			progress = true
		}
		if !progress {
			break
		}
	}
	return out
}

func isUnsupportedCloudErr(err error) bool {
	if err == nil {
		return false
	}
	cause := errors.Cause(err)
	return cause == cloudprovider.ErrNotSupported || cause == cloudprovider.ErrNotImplemented
}

func countCloudItems[T any](fn func() ([]T, error)) (int, error) {
	items, err := fn()
	if err != nil {
		if isUnsupportedCloudErr(err) {
			return 0, nil
		}
		return 0, err
	}
	return len(items), nil
}

func cloudRegionHasResources(region cloudRegionInventory) (bool, error) {
	// Region opt-in is account-specific. Only authoritative disabled metadata
	// permits skipping; authentication errors or missing metadata must still fail.
	if optIn, ok := region.(interface{ GetRegionOptInStatus() string }); ok && optIn.GetRegionOptInStatus() == "not-opted-in" {
		log.Infof("Skip cloud region discovery: account has not opted in to region")
		return false, nil
	}

	for _, fn := range []func() (int, error){
		func() (int, error) { return countCloudItems(region.GetIVMs) },
		func() (int, error) { return countCloudItems(region.GetIEips) },
		func() (int, error) { return countCloudItems(region.GetIDBInstances) },
		func() (int, error) { return countCloudItems(region.GetILoadBalancers) },
		func() (int, error) { return countCloudItems(region.GetIElasticcaches) },
	} {
		cnt, err := fn()
		if err != nil {
			return false, err
		}
		if cnt > 0 {
			return true, nil
		}
	}
	return false, nil
}

func firstInventorySyncRange() SSyncRange {
	return SSyncRange{
		SyncRangeInput: api.SyncRangeInput{
			SkipSyncResources: []string{
				CachedimageManager.Keyword(),
				SnapshotManager.Keyword(),
				SnapshotPolicyManager.Keyword(),
				BucketManager.Keyword(),
			},
		},
	}
}

func cloudRegionHasResourcesWithTimeoutDone(region cloudRegionInventory, timeout time.Duration) (bool, error, <-chan struct{}) {
	done := make(chan struct{})
	if timeout <= 0 {
		has, err := cloudRegionHasResources(region)
		close(done)
		return has, err, done
	}
	type probeResult struct {
		has bool
		err error
	}
	ch := make(chan probeResult, 1)
	go func() {
		defer close(done)
		has, err := cloudRegionHasResources(region)
		ch <- probeResult{has: has, err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.has, r.err, done
	case <-timer.C:
		return false, errors.Wrap(context.DeadlineExceeded, "cloud region discover timeout"), done
	}
}

func cloudRegionHasResourcesWithTimeout(region cloudRegionInventory, timeout time.Duration) (bool, error) {
	has, err, _ := cloudRegionHasResourcesWithTimeoutDone(region, timeout)
	return has, err
}

func (acnt *SCloudaccount) DiscoverDisabledCloudproviderRegions(ctx context.Context, userCred mcclient.TokenCredential, submitSync bool) error {
	return acnt.DiscoverCloudproviderRegions(ctx, userCred, submitSync, false)
}

func cloudRegionDiscoverLimits(intervalSeconds, batchSize int, exhaustive bool) (time.Duration, int) {
	interval := time.Duration(intervalSeconds) * time.Second
	batch := batchSize
	if batch <= 0 {
		batch = 30
	}
	if exhaustive {
		interval = 0
		batch = 0 // The selector treats zero as unlimited; concurrency remains bounded.
	}

	return interval, batch
}

func (acnt *SCloudaccount) DiscoverCloudproviderRegions(ctx context.Context, userCred mcclient.TokenCredential, submitSync bool, exhaustive bool) error {
	return acnt.discoverCloudproviderRegions(ctx, userCred, submitSync, exhaustive, db.FetchModelObjects)
}

func (acnt *SCloudaccount) discoverCloudproviderRegions(ctx context.Context, userCred mcclient.TokenCredential, submitSync bool, exhaustive bool, fetch func(db.IModelManager, *sqlchemy.SQuery, interface{}) error) error {
	if acnt.IsOnPremise {
		return nil
	}
	interval, batch := cloudRegionDiscoverLimits(options.Options.CloudRegionDiscoverIntervalSeconds, options.Options.CloudRegionDiscoverBatchSize, exhaustive)

	type workItem struct {
		provider *SCloudprovider
		cpr      *SCloudproviderregion
	}
	inputs := make([]providerRegionDiscoverInput, 0)
	items := make([]workItem, 0)
	providers := []SCloudprovider{}
	if err := fetch(CloudproviderManager, CloudproviderManager.Query().Equals("cloudaccount_id", acnt.Id), &providers); err != nil {
		return errors.Wrapf(err, "discover providers for account %s", acnt.Id)
	}
	for i := range providers {
		if !providers[i].GetEnabled() {
			continue
		}
		cprs := []SCloudproviderregion{}
		if err := fetch(CloudproviderRegionManager, CloudproviderRegionManager.Query().Equals("cloudprovider_id", providers[i].Id), &cprs); err != nil {
			return errors.Wrapf(err, "discover regions for provider %s", providers[i].Id)
		}
		for j := range cprs {
			inputs = append(inputs, providerRegionDiscoverInput{
				Enabled:        cprs[j].Enabled,
				NeverSynced:    cprs[j].isNeverSynced(),
				SyncStatus:     cprs[j].SyncStatus,
				LastDiscoverAt: cprs[j].LastDiscoverAt,
				ProviderId:     providers[i].Id,
			})
			items = append(items, workItem{provider: &providers[i], cpr: &cprs[j]})
		}
	}

	idxs := selectDiscoverCandidateIndexes(inputs, time.Now(), interval, batch)
	if len(idxs) == 0 {
		return nil
	}

	conc := options.Options.CloudRegionDiscoverConcurrency
	if conc <= 0 {
		conc = 10
	}

	remaining := map[string]*int32{}
	for _, idx := range idxs {
		pid := items[idx].provider.Id
		if remaining[pid] == nil {
			v := int32(0)
			remaining[pid] = &v
		}
		*remaining[pid]++
	}
	var hitMu sync.Mutex
	hitsByProvider := map[string][]*SCloudproviderregion{}
	recordHit := func(providerId string, cpr *SCloudproviderregion) {
		hitMu.Lock()
		hitsByProvider[providerId] = append(hitsByProvider[providerId], cpr)
		hitMu.Unlock()
	}
	flushHits := func(providerId string) {
		if !submitSync {
			return
		}
		hitMu.Lock()
		cprs := append([]*SCloudproviderregion{}, hitsByProvider[providerId]...)
		hitMu.Unlock()
		rng := firstInventorySyncRange()
		for i := range cprs {
			log.Infof("submit first-inventory sync %s / %s", cprs[i].CloudproviderId, cprs[i].CloudregionId)
			if err := cprs[i].submitSyncTask(ctx, userCred, rng); err != nil {
				cloudSyncError(ctx, "submit discovered region: %v", err)
			}
		}
	}

	var driverMu sync.Mutex
	drivers := providerDriverCache{drivers: make(map[string]cloudprovider.ICloudProvider)}
	providerMus := map[string]*sync.Mutex{}
	getDriver := func(provider *SCloudprovider) (cloudprovider.ICloudProvider, error) {
		return drivers.get(provider.Id, func() (cloudprovider.ICloudProvider, error) {
			return provider.GetProvider(ctx)
		})
	}
	lockProvider := func(id string) func() {
		driverMu.Lock()
		mu, ok := providerMus[id]
		if !ok {
			mu = &sync.Mutex{}
			providerMus[id] = mu
		}
		driverMu.Unlock()
		mu.Lock()
		return mu.Unlock
	}

	now := timeutils.UtcNow()
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	for _, idx := range idxs {
		if err := ctx.Err(); err != nil {
			wg.Wait()
			return err
		}
		item := items[idx]
		wg.Add(1)
		sem <- struct{}{}
		go func(provider *SCloudprovider, cpr *SCloudproviderregion) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if n := remaining[provider.Id]; n != nil && atomic.AddInt32(n, -1) == 0 {
					flushHits(provider.Id)
				}
			}()
			if err := ctx.Err(); err != nil {
				return
			}
			region, err := cpr.GetRegion()
			if err != nil {
				cloudSyncError(ctx, "discover GetRegion %s/%s: %v", provider.Id, cpr.CloudregionId, err)
				return
			}
			unlock := lockProvider(provider.Id)
			defer unlock()
			driver, err := getDriver(provider)
			if err != nil {
				cloudSyncError(ctx, "discover GetProvider %s: %v", provider.Id, err)
				return
			}
			iregion, err := driver.GetIRegionById(region.ExternalId)
			if err != nil {
				cloudSyncError(ctx, "discover GetIRegionById %s/%s: %v", provider.Id, region.ExternalId, err)
				return
			}
			hasResources, err, probeDone := cloudRegionHasResourcesWithTimeoutDone(iregion, cloudRegionDiscoverTimeout)
			// Keep the per-provider mutex until a timed-out probe exits. The
			// provider driver may still be using a connection after timeout.
			defer func() { <-probeDone }()
			if err != nil {
				cloudSyncError(ctx, "discover cloud resources %s/%s: %v", provider.Name, region.Name, err)
				return
			}

			_, err = db.Update(cpr, func() error {
				cpr.LastDiscoverAt = now
				if hasResources {
					cpr.Enabled = true
					cpr.LastDiscoverHitAt = now
				}
				return nil
			})
			if err != nil {
				cloudSyncError(ctx, "update discovered cloudproviderregion %s/%s: %v", provider.Id, region.Id, err)
				return
			}
			if !hasResources {
				return
			}
			log.Infof("enable cloudproviderregion %s(%s) / %s(%s) after discovering cloud resources", provider.Name, provider.Id, region.Name, region.Id)
			recordHit(provider.Id, cpr)
		}(item.provider, item.cpr)
	}
	wg.Wait()
	return nil
}
