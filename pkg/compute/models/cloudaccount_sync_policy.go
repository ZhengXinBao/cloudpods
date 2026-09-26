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
	"strings"
	"time"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/log"
	"yunion.io/x/pkg/errors"
	"yunion.io/x/pkg/utils"
	"yunion.io/x/sqlchemy"

	api "yunion.io/x/onecloud/pkg/apis/compute"
	"yunion.io/x/onecloud/pkg/cloudcommon/db"
	"yunion.io/x/onecloud/pkg/cloudcommon/db/lockman"
	"yunion.io/x/onecloud/pkg/mcclient"
)

type providerRegionEnableInput struct {
	EnabledInterruptedSync bool
	OnPremise              bool
	Allowlisted            bool
	HasLocalResources      bool
	RecentlyDiscovered     bool
	NeverSynced            bool
	DiscoverMissed         bool
}

func shouldEnableProviderRegion(in providerRegionEnableInput) bool {
	if in.OnPremise || in.Allowlisted || in.HasLocalResources || in.RecentlyDiscovered || in.EnabledInterruptedSync {
		return true
	}
	// Keep untried regions on so cheap cloud discover can find resources.
	// Do not disable them just because a sibling region already finished empty.
	if in.NeverSynced && !in.DiscoverMissed {
		return true
	}
	return false
}

func isDiscoverMissedProviderRegion(cpr SCloudproviderregion) bool {
	if cpr.LastDiscoverAt.IsZero() {
		return false
	}
	if cpr.LastDiscoverHitAt.IsZero() {
		return true
	}
	return cpr.LastDiscoverAt.After(cpr.LastDiscoverHitAt)
}

func (cpr SCloudproviderregion) isNeverSynced() bool {
	return cpr.LastSync.IsZero() && cpr.LastSyncEndAt.IsZero()
}

// A first import that started but never recorded its end must remain eligible
// for recovery. Submission still checks in-flight state and the queue owns leases.
func (cpr SCloudproviderregion) isInterruptedFirstSync() bool {
	return !cpr.LastSync.IsZero() && cpr.LastSyncEndAt.IsZero()
}

func (cpr SCloudproviderregion) shouldFullSync() bool {
	if !cpr.Enabled {
		return false
	}
	if isRecentlyDiscoveredProviderRegion(cpr) || cpr.isInterruptedFirstSync() {
		return true
	}
	// Already imported: refresh on later account/provider syncs.
	return !cpr.LastSyncEndAt.IsZero()
}

func isProviderRegionInFlight(status string) bool {
	return status == api.CLOUD_PROVIDER_SYNC_STATUS_QUEUED ||
		status == api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING
}

func isProviderRegionBusy(status string) bool {
	return isProviderRegionInFlight(status) ||
		status == api.CLOUD_PROVIDER_SYNC_STATUS_QUEUING
}

func (cpr SCloudproviderregion) recentlyCompletedSync(now time.Time, minInterval time.Duration) bool {
	if minInterval <= 0 || cpr.LastSyncEndAt.IsZero() {
		return false
	}
	return now.Sub(cpr.LastSyncEndAt) < minInterval
}

func (cpr SCloudproviderregion) shouldSubmitResourceSync(explicit bool, force bool, now time.Time, minInterval time.Duration) bool {
	if !cpr.Enabled {
		return false
	}
	if isProviderRegionInFlight(cpr.SyncStatus) {
		return false
	}
	if explicit {
		return true
	}
	if !cpr.shouldFullSync() {
		return false
	}
	if !force && cpr.recentlyCompletedSync(now, minInterval) && !isRecentlyDiscoveredProviderRegion(cpr) {
		return false
	}
	return true
}

func accountListSyncStatus(probeStatus string, resourceSyncCount int) (listStatus string, resourceStatus string) {
	listStatus = probeStatus
	resourceStatus = api.CLOUD_PROVIDER_SYNC_STATUS_IDLE
	if resourceSyncCount > 0 {
		listStatus = api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING
		resourceStatus = api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING
	}
	return listStatus, resourceStatus
}

func syncStatusWithQueue(status string, activeJobs int) string {
	if activeJobs > 0 {
		return api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING
	}
	return status
}

func AccountFanoutSyncRange(in SSyncRange) SSyncRange {
	out := in
	targeted := len(in.Resources) > 0 || len(in.Region) > 0 || len(in.Zone) > 0 || len(in.Host) > 0
	out.DeepSync = in.DeepSync && !targeted && !in.FullSync
	if !targeted {
		out.Force = false
	}
	return out
}

type independentSyncPlan struct {
	ResourceGroup string
	ScopeType     string
	Range         SSyncRange
}

func isProviderScopedSyncResource(resource string) bool {
	switch resource {
	case cloudprovider.CLOUD_CAPABILITY_PROJECT,
		cloudprovider.CLOUD_CAPABILITY_CDN,
		cloudprovider.CLOUD_CAPABILITY_DNSZONE,
		cloudprovider.CLOUD_CAPABILITY_INTERVPCNETWORK,
		cloudprovider.CLOUD_CAPABILITY_CERT,
		cloudprovider.CLOUD_CAPABILITY_AI_GATEWAY:
		return true
	default:
		return false
	}
}

// independentSyncPlans keeps the legacy core path together and only splits
// capability groups that do not depend on its topology/compute writes.
func independentSyncPlans(in SSyncRange) []independentSyncPlan {
	if len(in.Resources) > 0 {
		regionResources := make([]string, 0, len(in.Resources))
		providerResources := make([]string, 0, len(in.Resources))
		for _, resource := range in.Resources {
			if isProviderScopedSyncResource(resource) {
				providerResources = append(providerResources, resource)
			} else {
				regionResources = append(regionResources, resource)
			}
			if resource == cloudprovider.CLOUD_CAPABILITY_NETWORK {
				providerResources = append(providerResources, resource)
			}
		}
		plans := make([]independentSyncPlan, 0, 2)
		if len(providerResources) > 0 {
			providerRange := in
			providerRange.Resources = providerResources
			plans = append(plans, independentSyncPlan{
				ResourceGroup: "provider",
				ScopeType:     "provider",
				Range:         providerRange,
			})
		}
		if len(regionResources) > 0 {
			regionRange := in
			regionRange.Resources = regionResources
			plans = append(plans, independentSyncPlan{
				ResourceGroup: "requested",
				ScopeType:     "region",
				Range:         regionRange,
			})
		}
		return plans
	}

	core := []string{
		cloudprovider.CLOUD_CAPABILITY_COMPUTE,
		cloudprovider.CLOUD_CAPABILITY_NETWORK,
		cloudprovider.CLOUD_CAPABILITY_EIP,
		cloudprovider.CLOUD_CAPABILITY_NAT,
		cloudprovider.CLOUD_CAPABILITY_SNAPSHOT_POLICY,
	}
	deep := in.FullSync || in.DeepSync
	if deep {
		core = append(core, cloudprovider.CLOUD_CAPABILITY_QUOTA, cloudprovider.CLOUD_CAPABILITY_IMAGE)
	}
	providerResources := []string{cloudprovider.CLOUD_CAPABILITY_NETWORK}
	if deep {
		providerResources = append(providerResources,
			cloudprovider.CLOUD_CAPABILITY_PROJECT,
			cloudprovider.CLOUD_CAPABILITY_CDN,
			cloudprovider.CLOUD_CAPABILITY_DNSZONE,
			cloudprovider.CLOUD_CAPABILITY_INTERVPCNETWORK,
			cloudprovider.CLOUD_CAPABILITY_CERT,
			cloudprovider.CLOUD_CAPABILITY_AI_GATEWAY,
		)
	}
	groups := [][]string{
		providerResources,
		core,
		{
			cloudprovider.CLOUD_CAPABILITY_LOADBALANCER,
			cloudprovider.CLOUD_CAPABILITY_RDS,
			cloudprovider.CLOUD_CAPABILITY_CACHE,
		},
	}
	names := []string{"core", "services"}
	if deep {
		groups = append(groups,
			[]string{
				cloudprovider.CLOUD_CAPABILITY_OBJECTSTORE,
				cloudprovider.CLOUD_CAPABILITY_NAS,
			},
			[]string{
				cloudprovider.CLOUD_CAPABILITY_WAF,
				cloudprovider.CLOUD_CAPABILITY_MONGO_DB,
				cloudprovider.CLOUD_CAPABILITY_ES,
				cloudprovider.CLOUD_CAPABILITY_KAFKA,
				cloudprovider.CLOUD_CAPABILITY_APP,
				cloudprovider.CLOUD_CAPABILITY_CONTAINER,
				cloudprovider.CLOUD_CAPABILITY_TABLESTORE,
				cloudprovider.CLOUD_CAPABILITY_MODELARTES,
				cloudprovider.CLOUD_CAPABILITY_MISC,
			},
		)
		names = append(names, "storage", "extended")
	}
	names = append([]string{"provider"}, names...)
	plans := make([]independentSyncPlan, 0, len(groups))
	for i, resources := range groups {
		rng := in
		rng.Resources = append([]string(nil), resources...)
		scopeType := "region"
		if names[i] == "provider" {
			scopeType = "provider"
		}
		plans = append(plans, independentSyncPlan{
			ResourceGroup: names[i],
			ScopeType:     scopeType,
			Range:         rng,
		})
	}
	return plans
}

func isRecentlyDiscoveredProviderRegion(cpr SCloudproviderregion) bool {
	if cpr.LastDiscoverHitAt.IsZero() {
		return false
	}
	return cpr.LastSync.IsZero() || cpr.LastDiscoverHitAt.After(cpr.LastSync)
}

func (acnt *SCloudaccount) getSubAccountRegionExternalIds() []string {
	if acnt.SubAccounts == nil {
		return nil
	}
	regionIds := make([]string, 0, len(acnt.SubAccounts.Cloudregions))
	for _, region := range acnt.SubAccounts.Cloudregions {
		if len(region.Id) > 0 && !strings.HasSuffix(region.Id, "/") {
			regionIds = append(regionIds, region.Id)
		}
	}
	return regionIds
}

func (acnt *SCloudaccount) RefreshEnabledRegionsAfterDiscover(ctx context.Context, userCred mcclient.TokenCredential, extraRegionIds []string) error {
	return acnt.RefreshEnabledCloudproviderRegions(ctx, userCred, acnt.getSubAccountRegionExternalIds(), extraRegionIds)
}

func (acnt *SCloudaccount) RefreshEnabledCloudproviderRegions(ctx context.Context, userCred mcclient.TokenCredential, keepRegionExtIds []string, extraRegionIds []string) error {
	lockman.LockObject(ctx, acnt)
	defer lockman.ReleaseObject(ctx, acnt)
	// Queued regions are not yet marked syncing. Background account probes
	// must preserve their requested scope until the durable run has drained.
	if IndependentSyncAccount(acnt.Id) {
		counts, err := CloudSyncQueue().Stats(ctx, acnt.Id)
		if err != nil {
			return errors.Wrap(err, "check active independent sync before refreshing regions")
		}
		if preserveIndependentSyncRegions(counts) {
			return nil
		}
	}
	if userCred != nil {
		log.Debugf("refresh enabled cloudprovider regions for %s by %s", acnt.Id, userCred.GetUserId())
	}

	providers := []SCloudprovider{}
	if err := db.FetchModelObjects(CloudproviderManager, CloudproviderManager.Query().Equals("cloudaccount_id", acnt.Id), &providers); err != nil {
		return errors.Wrapf(err, "refresh enabled regions: fetch providers for account %s", acnt.Id)
	}
	onPremise := acnt.IsOnPremise
	for i := range providers {
		if !providers[i].GetEnabled() {
			continue
		}
		cprs := []SCloudproviderregion{}
		if err := db.FetchModelObjects(CloudproviderRegionManager, CloudproviderRegionManager.Query().Equals("cloudprovider_id", providers[i].Id), &cprs); err != nil {
			return errors.Wrapf(err, "refresh enabled regions: fetch regions for provider %s", providers[i].Id)
		}
		_, _, localResources, countErrs := providerSyncInventory(cprs, providers[i].Id)
		for j := range cprs {
			if isProviderRegionBusy(cprs[j].SyncStatus) {
				continue
			}
			region, err := cprs[j].GetRegion()
			if err != nil {
				return errors.Wrapf(err, "refresh enabled regions: GetRegion %s/%s", providers[i].Id, cprs[j].CloudregionId)
			}
			if err, ok := countErrs[cprs[j].CloudregionId]; ok {
				return errors.Wrapf(err, "refresh enabled regions: count local resources %s/%s", providers[i].Id, region.Id)
			}
			hasResources := localResources[cprs[j].CloudregionId]
			allowlisted := utils.IsInStringArray(region.ExternalId, keepRegionExtIds) ||
				utils.IsInStringArray(region.Id, extraRegionIds) ||
				utils.IsInStringArray(region.ExternalId, extraRegionIds)
			enable := shouldEnableProviderRegion(providerRegionEnableInput{
				// Preserve recovery eligibility without re-enabling a disabled region.
				EnabledInterruptedSync: cprs[j].Enabled && cprs[j].isInterruptedFirstSync(),
				OnPremise:              onPremise,
				Allowlisted:            allowlisted,
				HasLocalResources:      hasResources,
				RecentlyDiscovered:     isRecentlyDiscoveredProviderRegion(cprs[j]),
				NeverSynced:            cprs[j].isNeverSynced(),
				DiscoverMissed:         isDiscoverMissedProviderRegion(cprs[j]),
			})
			if cprs[j].Enabled == enable {
				continue
			}
			_, err = db.Update(&cprs[j], func() error {
				cprs[j].Enabled = enable
				if !enable && !isProviderRegionBusy(cprs[j].SyncStatus) {
					cprs[j].SyncStatus = api.CLOUD_PROVIDER_SYNC_STATUS_IDLE
				}
				return nil
			})
			if err != nil {
				return errors.Wrapf(err, "update cloudproviderregion %s/%s", providers[i].Id, region.Id)
			}
			if !enable {
				log.Infof("disable empty cloudproviderregion %s(%s) / %s(%s)", providers[i].Name, providers[i].Id, region.Name, region.Id)
			}
		}
	}
	return nil
}

func preserveIndependentSyncRegions(counts map[string]int) bool {
	return counts["waiting"]+counts["running"]+counts["retry"] > 0
}

func providerSyncInventory(cprs []SCloudproviderregion, providerId string) (hasResources bool, hasCompleted bool, localResources map[string]bool, countErrs map[string]error) {
	localResources = make(map[string]bool, len(cprs))
	countErrs = make(map[string]error)
	for i := range cprs {
		if !cprs[i].LastSyncEndAt.IsZero() {
			hasCompleted = true
		}
		found, err := providerRegionHasLocalResources(providerId, cprs[i].CloudregionId)
		if err != nil {
			countErrs[cprs[i].CloudregionId] = err
			hasResources = true
			continue
		}
		localResources[cprs[i].CloudregionId] = found
		if found {
			hasResources = true
		}
	}
	return hasResources, hasCompleted, localResources, countErrs
}

func providerRegionHasLocalResources(providerId, regionId string) (bool, error) {
	for _, fn := range []func(string, string) (int, error){
		func(pid, rid string) (int, error) { return countByManagerAndRegion(ElasticipManager, pid, rid) },
		func(pid, rid string) (int, error) { return countByManagerAndRegion(DBInstanceManager, pid, rid) },
		func(pid, rid string) (int, error) { return countByManagerAndRegion(LoadbalancerManager, pid, rid) },
		countGuestsInProviderRegion,
		countElasticcachesInProviderRegion,
	} {
		cnt, err := fn(providerId, regionId)
		if err != nil {
			return false, err
		}
		if cnt > 0 {
			return true, nil
		}
	}
	return false, nil
}

func countByManagerAndRegion(manager db.IModelManager, providerId, regionId string) (int, error) {
	q := manager.Query().Equals("manager_id", providerId).Equals("cloudregion_id", regionId)
	cnt, err := q.CountWithError()
	if err != nil {
		return 0, errors.Wrapf(err, "countByManagerAndRegion %s %s/%s", manager.Keyword(), providerId, regionId)
	}
	return cnt, nil
}

func countGuestsInProviderRegion(providerId, regionId string) (int, error) {
	hosts := HostManager.Query().SubQuery()
	zones := ZoneManager.Query().SubQuery()
	q := GuestManager.Query()
	q = q.Join(hosts, sqlchemy.Equals(q.Field("host_id"), hosts.Field("id")))
	q = q.Join(zones, sqlchemy.Equals(hosts.Field("zone_id"), zones.Field("id")))
	q = q.Filter(sqlchemy.Equals(hosts.Field("manager_id"), providerId))
	q = q.Filter(sqlchemy.Equals(zones.Field("cloudregion_id"), regionId))
	cnt, err := q.CountWithError()
	if err != nil {
		return 0, errors.Wrapf(err, "countGuestsInProviderRegion %s/%s", providerId, regionId)
	}
	return cnt, nil
}

func countElasticcachesInProviderRegion(providerId, regionId string) (int, error) {
	q := ElasticcacheManager.Query().Equals("manager_id", providerId).Equals("cloudregion_id", regionId)
	cnt, err := q.CountWithError()
	if err != nil {
		return 0, errors.Wrapf(err, "countElasticcachesInProviderRegion %s/%s", providerId, regionId)
	}
	return cnt, nil
}

func (acnt *SCloudaccount) EnableCloudproviderRegions(regionIds []string) error {
	if len(regionIds) == 0 {
		return nil
	}
	providers := acnt.GetCloudproviders()
	for i := range providers {
		cprs := providers[i].GetCloudproviderRegions()
		for j := range cprs {
			region, err := cprs[j].GetRegion()
			if err != nil {
				continue
			}
			if !utils.IsInStringArray(region.Id, regionIds) && !utils.IsInStringArray(region.ExternalId, regionIds) {
				continue
			}
			if cprs[j].Enabled {
				continue
			}
			_, err = db.Update(&cprs[j], func() error {
				cprs[j].Enabled = true
				return nil
			})
			if err != nil {
				return errors.Wrapf(err, "enable cloudproviderregion %s/%s", providers[i].Id, region.Id)
			}
		}
	}
	return nil
}

func (cprvd *SCloudprovider) HasEnabledCloudproviderRegion() bool {
	q := CloudproviderRegionManager.Query().Equals("cloudprovider_id", cprvd.Id).IsTrue("enabled")
	cnt, err := q.CountWithError()
	if err != nil {
		log.Errorf("hasEnabledCloudproviderRegion %s: %v", cprvd.Id, err)
		return true
	}
	return cnt > 0
}
