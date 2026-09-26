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
	"testing"
	"time"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/pkg/utils"

	api "yunion.io/x/onecloud/pkg/apis/compute"
	"yunion.io/x/onecloud/pkg/cloudcommon/db"
)

func TestShouldEnableProviderRegion(t *testing.T) {
	tests := []struct {
		name string
		in   providerRegionEnableInput
		want bool
	}{
		{name: "empty public region stays off", in: providerRegionEnableInput{}, want: false},
		{name: "on-premise stays on", in: providerRegionEnableInput{OnPremise: true}, want: true},
		{name: "user allowlist stays on", in: providerRegionEnableInput{Allowlisted: true}, want: true},
		{name: "local resources stay on", in: providerRegionEnableInput{HasLocalResources: true}, want: true},
		{
			name: "allowlist wins over empty resources",
			in:   providerRegionEnableInput{Allowlisted: true, HasLocalResources: false},
			want: true,
		},
		{
			name: "first import keeps never-synced empty region",
			in:   providerRegionEnableInput{NeverSynced: true},
			want: true,
		},
		{
			name: "never-synced sibling stays on until cheap discover misses",
			in: providerRegionEnableInput{
				NeverSynced: true,
			},
			want: true,
		},
		{
			name: "cheap discover miss turns never-synced region off",
			in: providerRegionEnableInput{
				NeverSynced:    true,
				DiscoverMissed: true,
			},
			want: false,
		},
		{
			name: "confirmed empty after a completed sync stays off",
			in:   providerRegionEnableInput{NeverSynced: false},
			want: false,
		},
		{
			name: "recent cloud discovery keeps region on until local sync",
			in:   providerRegionEnableInput{RecentlyDiscovered: true},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldEnableProviderRegion(tt.in); got != tt.want {
				t.Fatalf("shouldEnableProviderRegion(%+v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestCloudaccountCanSyncIgnoresRegionQueue(t *testing.T) {
	acnt := SCloudaccount{}
	acnt.SyncStatus = api.CLOUD_PROVIDER_SYNC_STATUS_IDLE
	if !acnt.CanSync() {
		t.Fatal("idle account should be able to sync")
	}

	acnt.SyncStatus = api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING
	acnt.LastSync = time.Now()
	if acnt.CanSync() {
		t.Fatal("account still probing should not start another probe")
	}

	acnt.LastSync = time.Now().Add(-31 * time.Minute)
	if !acnt.CanSync() {
		t.Fatal("stale account probe should be allowed to retry")
	}
}

func TestGetSubAccountRegionExternalIds(t *testing.T) {
	acnt := SCloudaccount{}
	if got := acnt.getSubAccountRegionExternalIds(); len(got) != 0 {
		t.Fatalf("nil SubAccounts should return empty, got %v", got)
	}

	acnt.SubAccounts = &cloudprovider.SubAccounts{
		Cloudregions: []struct {
			Id     string
			Name   string
			Status string
		}{
			{Id: "AWS/us-east-1"},
			{Id: "AWS/"},
			{Id: ""},
			{Id: "AWS/ap-southeast-1"},
		},
	}
	got := acnt.getSubAccountRegionExternalIds()
	if len(got) != 2 || got[0] != "AWS/us-east-1" || got[1] != "AWS/ap-southeast-1" {
		t.Fatalf("unexpected region ids: %v", got)
	}
}

func TestAccountListSyncStatus(t *testing.T) {
	list, resource := accountListSyncStatus(api.CLOUD_PROVIDER_SYNC_STATUS_IDLE, 0)
	if list != api.CLOUD_PROVIDER_SYNC_STATUS_IDLE || resource != api.CLOUD_PROVIDER_SYNC_STATUS_IDLE {
		t.Fatalf("idle account with no region jobs = %s/%s", list, resource)
	}
	list, resource = accountListSyncStatus(api.CLOUD_PROVIDER_SYNC_STATUS_IDLE, 4)
	if list != api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING || resource != api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING {
		t.Fatalf("idle probe with running regions should list as syncing, got %s/%s", list, resource)
	}
	list, resource = accountListSyncStatus(api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING, 0)
	if list != api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING {
		t.Fatalf("probe still running should stay syncing, got %s", list)
	}
}

func TestNeedSyncResource(t *testing.T) {
	fast := SSyncRange{}
	if !fast.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_COMPUTE) {
		t.Fatal("default range should sync compute")
	}
	if fast.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_OBJECTSTORE) || fast.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_IMAGE) {
		t.Fatal("default range must skip objectstore and image")
	}

	deep := SSyncRange{SyncRangeInput: api.SyncRangeInput{DeepSync: true}}
	if !deep.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_OBJECTSTORE) {
		t.Fatal("deep sync should include objectstore")
	}

	full := SSyncRange{SyncRangeInput: api.SyncRangeInput{FullSync: true}}
	for _, resource := range []string{
		cloudprovider.CLOUD_CAPABILITY_OBJECTSTORE,
		cloudprovider.CLOUD_CAPABILITY_IMAGE,
		cloudprovider.CLOUD_CAPABILITY_PROJECT,
	} {
		if !full.NeedSyncResource(resource) {
			t.Fatalf("full sync should include %s", resource)
		}
	}

	explicit := SSyncRange{SyncRangeInput: api.SyncRangeInput{
		FullSync:  true,
		DeepSync:  true,
		Resources: []string{cloudprovider.CLOUD_CAPABILITY_RDS},
	}}
	if !explicit.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_RDS) {
		t.Fatal("explicit resources should be honored")
	}
	if explicit.NeedSyncResource(cloudprovider.CLOUD_CAPABILITY_COMPUTE) {
		t.Fatal("explicit resources should ignore other types even when DeepSync")
	}
}

func TestEmptySyncRangeNeedsSyncInfo(t *testing.T) {
	sr := SSyncRange{}
	if !sr.NeedSyncInfo() {
		t.Fatal("empty account sync range should use the default resource sync")
	}
}

func TestIsProviderRegionInFlight(t *testing.T) {
	if isProviderRegionInFlight(api.CLOUD_PROVIDER_SYNC_STATUS_QUEUING) {
		t.Fatal("queuing is leftover mark, not in-flight")
	}
	if !isProviderRegionInFlight(api.CLOUD_PROVIDER_SYNC_STATUS_QUEUED) || !isProviderRegionInFlight(api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING) {
		t.Fatal("queued/syncing must be in-flight")
	}
	if !isProviderRegionBusy(api.CLOUD_PROVIDER_SYNC_STATUS_QUEUING) {
		t.Fatal("queuing is still busy for enable/disable")
	}
}

func TestShouldSubmitResourceSync(t *testing.T) {
	now := time.Date(2026, 9, 16, 6, 0, 0, 0, time.UTC)
	minInterval := 15 * time.Minute

	disabled := SCloudproviderregion{Enabled: false}
	if disabled.shouldSubmitResourceSync(false, false, now, minInterval) {
		t.Fatal("disabled region must not submit")
	}

	queued := SCloudproviderregion{Enabled: true, SSyncableBaseResource: SSyncableBaseResource{SyncStatus: api.CLOUD_PROVIDER_SYNC_STATUS_QUEUED}}
	if queued.shouldSubmitResourceSync(true, true, now, minInterval) {
		t.Fatal("queued region must not be submitted again")
	}

	queuing := SCloudproviderregion{Enabled: true, SSyncableBaseResource: SSyncableBaseResource{
		SyncStatus:    api.CLOUD_PROVIDER_SYNC_STATUS_QUEUING,
		LastSyncEndAt: now.Add(-time.Hour),
	}}
	if !queuing.shouldSubmitResourceSync(false, false, now, minInterval) {
		t.Fatal("leftover queuing should be submitted")
	}

	neverSynced := SCloudproviderregion{Enabled: true}
	if neverSynced.shouldSubmitResourceSync(false, false, now, minInterval) {
		t.Fatal("never-synced without discover hit must wait")
	}

	recent := SCloudproviderregion{Enabled: true, SSyncableBaseResource: SSyncableBaseResource{
		LastSyncEndAt: now.Add(-5 * time.Minute),
	}}
	if recent.shouldSubmitResourceSync(false, false, now, minInterval) {
		t.Fatal("recently completed region should skip")
	}
	if !recent.shouldSubmitResourceSync(false, true, now, minInterval) {
		t.Fatal("force should resubmit recently completed region")
	}
	if !recent.shouldSubmitResourceSync(true, false, now, minInterval) {
		t.Fatal("explicit region list should resubmit")
	}

	leftoverRecent := SCloudproviderregion{Enabled: true, SSyncableBaseResource: SSyncableBaseResource{
		SyncStatus:    api.CLOUD_PROVIDER_SYNC_STATUS_QUEUING,
		LastSyncEndAt: now.Add(-5 * time.Minute),
	}}
	if leftoverRecent.shouldSubmitResourceSync(false, false, now, minInterval) {
		t.Fatal("leftover queuing on a recently completed region should be cancelled, not submitted")
	}
}

func TestAccountFanoutSyncRange(t *testing.T) {
	generic := AccountFanoutSyncRange(SSyncRange{SyncRangeInput: api.SyncRangeInput{
		Force:    true,
		FullSync: true,
		DeepSync: true,
	}})
	if generic.Force || generic.DeepSync {
		t.Fatalf("generic account fanout must drop force/deep, got force=%v deep=%v", generic.Force, generic.DeepSync)
	}
	if !generic.FullSync {
		t.Fatal("generic account fanout should keep full_sync")
	}

	deepOnly := AccountFanoutSyncRange(SSyncRange{SyncRangeInput: api.SyncRangeInput{
		DeepSync: true,
	}})
	if !deepOnly.DeepSync {
		t.Fatal("deep-only account fanout should keep deep_sync")
	}

	targeted := AccountFanoutSyncRange(SSyncRange{SyncRangeInput: api.SyncRangeInput{
		Force:     true,
		FullSync:  true,
		DeepSync:  true,
		Resources: []string{cloudprovider.CLOUD_CAPABILITY_PROJECT},
	}})
	if !targeted.Force || targeted.DeepSync {
		t.Fatalf("targeted account fanout should keep force and drop deep, got force=%v deep=%v", targeted.Force, targeted.DeepSync)
	}
	if len(targeted.Resources) != 1 || targeted.Resources[0] != cloudprovider.CLOUD_CAPABILITY_PROJECT {
		t.Fatalf("targeted account fanout should keep resources: %+v", targeted)
	}
}

func TestIndependentSyncPlans(t *testing.T) {
	plans := independentSyncPlans(SSyncRange{SyncRangeInput: api.SyncRangeInput{}})
	if len(plans) != 3 {
		t.Fatalf("default range plans=%d want 3 (including global networks): %+v", len(plans), plans)
	}
	if plans[0].ResourceGroup != "provider" || plans[1].ResourceGroup != "core" || plans[2].ResourceGroup != "services" {
		t.Fatalf("unexpected default plan groups: %+v", plans)
	}
	if plans[0].ScopeType != "provider" || !utils.IsInStringArray(cloudprovider.CLOUD_CAPABILITY_NETWORK, plans[0].Range.Resources) {
		t.Fatalf("global networks must be enqueued before regional resources: %+v", plans)
	}
	if len(plans[1].Range.Resources) == 0 || len(plans[2].Range.Resources) == 0 {
		t.Fatalf("default plan must carry explicit resources: %+v", plans)
	}
	for _, resource := range []string{
		cloudprovider.CLOUD_CAPABILITY_COMPUTE,
		cloudprovider.CLOUD_CAPABILITY_SNAPSHOT_POLICY,
	} {
		if !utils.IsInStringArray(resource, plans[1].Range.Resources) {
			t.Fatalf("default core plan must include %s: %+v", resource, plans[1])
		}
	}

	deep := independentSyncPlans(SSyncRange{SyncRangeInput: api.SyncRangeInput{DeepSync: true}})
	if len(deep) != 5 {
		t.Fatalf("deep range should be split into safe groups: %+v", deep)
	}
	for _, plan := range deep {
		if !plan.Range.DeepSync {
			t.Fatalf("deep sync plan %s silently dropped deep_sync: %+v", plan.ResourceGroup, plan)
		}
	}
	if deep[0].ScopeType != "provider" || !utils.IsInStringArray(cloudprovider.CLOUD_CAPABILITY_SNAPSHOT_POLICY, deep[1].Range.Resources) {
		t.Fatalf("deep sync must enqueue provider before core: %+v", deep)
	}

	full := independentSyncPlans(SSyncRange{SyncRangeInput: api.SyncRangeInput{FullSync: true}})
	if len(full) != 5 {
		t.Fatalf("full range should include every resource group: %+v", full)
	}
	if full[0].ScopeType != "provider" || !utils.IsInStringArray(cloudprovider.CLOUD_CAPABILITY_SNAPSHOT_POLICY, full[1].Range.Resources) {
		t.Fatalf("full sync must enqueue provider before core: %+v", full)
	}
	for _, plan := range full {
		if plan.ResourceGroup == "provider" {
			if plan.ScopeType != "provider" {
				t.Fatalf("provider resources must use provider scope: %+v", plan)
			}
			for _, resource := range plan.Range.Resources {
				if !isProviderScopedSyncResource(resource) && resource != cloudprovider.CLOUD_CAPABILITY_NETWORK {
					t.Fatalf("region resource %s leaked into provider plan: %+v", resource, plan)
				}
			}
		} else {
			if plan.ScopeType == "provider" {
				t.Fatalf("region resource group %s uses provider scope: %+v", plan.ResourceGroup, plan)
			}
			for _, resource := range plan.Range.Resources {
				if isProviderScopedSyncResource(resource) {
					t.Fatalf("provider resource %s leaked into region plan: %+v", resource, plan)
				}
			}
		}
	}

	explicit := SSyncRange{SyncRangeInput: api.SyncRangeInput{
		Resources: []string{cloudprovider.CLOUD_CAPABILITY_RDS},
	}}
	explicitPlans := independentSyncPlans(explicit)
	if len(explicitPlans) != 1 || explicitPlans[0].ResourceGroup != "requested" {
		t.Fatalf("explicit ranges must remain one job: %+v", explicitPlans)
	}

	providerOnly := SSyncRange{SyncRangeInput: api.SyncRangeInput{
		Resources: []string{
			cloudprovider.CLOUD_CAPABILITY_PROJECT,
			cloudprovider.CLOUD_CAPABILITY_DNSZONE,
		},
	}}
	providerPlans := independentSyncPlans(providerOnly)
	if len(providerPlans) != 1 || providerPlans[0].ScopeType != "provider" {
		t.Fatalf("provider-only resources must use one provider job: %+v", providerPlans)
	}

	mixed := SSyncRange{SyncRangeInput: api.SyncRangeInput{
		Resources: []string{
			cloudprovider.CLOUD_CAPABILITY_PROJECT,
			cloudprovider.CLOUD_CAPABILITY_COMPUTE,
		},
	}}
	mixedPlans := independentSyncPlans(mixed)
	if len(mixedPlans) != 2 || mixedPlans[0].ScopeType != "provider" || mixedPlans[1].ScopeType != "region" {
		t.Fatalf("mixed provider and region resources need two jobs: %+v", mixedPlans)
	}

	network := independentSyncPlans(SSyncRange{SyncRangeInput: api.SyncRangeInput{
		Resources: []string{cloudprovider.CLOUD_CAPABILITY_NETWORK},
	}})
	if len(network) != 2 || network[0].ScopeType != "provider" || network[1].ScopeType != "region" {
		t.Fatalf("network must enqueue global VPCs before regional VPCs: %+v", network)
	}
}

func TestDeepSyncSkusStayWithinResourceGroup(t *testing.T) {
	for _, plan := range independentSyncPlans(SSyncRange{SyncRangeInput: api.SyncRangeInput{DeepSync: true}}) {
		for _, sku := range []struct {
			capability string
			manager    db.IModelManager
			group      string
		}{
			{cloudprovider.CLOUD_CAPABILITY_COMPUTE, ServerSkuManager, "core"},
			{cloudprovider.CLOUD_CAPABILITY_NAT, NatSkuManager, "core"},
			{cloudprovider.CLOUD_CAPABILITY_RDS, DBInstanceSkuManager, "services"},
			{cloudprovider.CLOUD_CAPABILITY_CACHE, ElasticcacheSkuManager, "services"},
			{cloudprovider.CLOUD_CAPABILITY_NAS, NasSkuManager, "storage"},
		} {
			want := plan.ResourceGroup == sku.group
			if got := shouldSyncResourceSkus(&plan.Range, sku.capability, sku.manager); got != want {
				t.Fatalf("group=%s SKU=%s got=%v want=%v", plan.ResourceGroup, sku.capability, got, want)
			}
			skipped := plan.Range
			skipped.SkipSyncResources = []string{sku.manager.Keyword()}
			if shouldSyncResourceSkus(&skipped, sku.capability, sku.manager) {
				t.Fatalf("skip list ignored for %s", sku.capability)
			}
		}
	}
	if shouldSyncResourceSkus(&SSyncRange{}, cloudprovider.CLOUD_CAPABILITY_COMPUTE, ServerSkuManager) {
		t.Fatal("non-deep range must not sync SKUs")
	}
}

func TestDeepOnlyRangeNeedsSyncInfo(t *testing.T) {
	sr := SSyncRange{SyncRangeInput: api.SyncRangeInput{DeepSync: true}}
	if !sr.NeedSyncInfo() {
		t.Fatal("deep-only account sync must reach the resource planner")
	}
}

func TestSyncRangeShouldSyncZonesOnlyForTopologyGroups(t *testing.T) {
	tests := []struct {
		name      string
		resources []string
		want      bool
	}{
		{
			name: "core",
			resources: []string{
				cloudprovider.CLOUD_CAPABILITY_COMPUTE,
				cloudprovider.CLOUD_CAPABILITY_NETWORK,
			},
			want: true,
		},
		{
			name: "services",
			resources: []string{
				cloudprovider.CLOUD_CAPABILITY_LOADBALANCER,
				cloudprovider.CLOUD_CAPABILITY_RDS,
				cloudprovider.CLOUD_CAPABILITY_CACHE,
			},
			want: false,
		},
		{
			name: "storage",
			resources: []string{
				cloudprovider.CLOUD_CAPABILITY_OBJECTSTORE,
				cloudprovider.CLOUD_CAPABILITY_NAS,
			},
			want: false,
		},
		{
			name: "extended",
			resources: []string{
				cloudprovider.CLOUD_CAPABILITY_DNSZONE,
				cloudprovider.CLOUD_CAPABILITY_CDN,
			},
			want: false,
		},
		{
			name:      "image-targeted",
			resources: []string{cloudprovider.CLOUD_CAPABILITY_IMAGE},
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rng := SSyncRange{SyncRangeInput: api.SyncRangeInput{Resources: tt.resources}}
			if got := shouldSyncRegionZones(&rng); got != tt.want {
				t.Fatalf("shouldSyncRegionZones(%v) = %v, want %v", tt.resources, got, tt.want)
			}
		})
	}
}

func TestSyncRangeVPCDependencies(t *testing.T) {
	tests := []struct {
		name     string
		resource string
		vpc      bool
		network  bool
		secgroup bool
		nat      bool
		peer     bool
		ipv6     bool
	}{
		{
			name:     "network keeps full topology",
			resource: cloudprovider.CLOUD_CAPABILITY_NETWORK,
			vpc:      true,
			network:  true,
			secgroup: true,
			nat:      true,
			peer:     true,
			ipv6:     true,
		},
		{
			name:     "security group needs vpc and security groups only",
			resource: cloudprovider.CLOUD_CAPABILITY_SECURITY_GROUP,
			vpc:      true,
			secgroup: true,
		},
		{
			name:     "nat needs vpc and nat only",
			resource: cloudprovider.CLOUD_CAPABILITY_NAT,
			vpc:      true,
			nat:      true,
		},
		{
			name:     "vpc peer needs vpc and peer connections only",
			resource: cloudprovider.CLOUD_CAPABILITY_VPC_PEER,
			vpc:      true,
			peer:     true,
		},
		{
			name:     "ipv6 gateway needs vpc and ipv6 gateways only",
			resource: cloudprovider.CLOUD_CAPABILITY_IPV6_GATEWAY,
			vpc:      true,
			ipv6:     true,
		},
		{
			name:     "eip needs vpc lookup but no topology children",
			resource: cloudprovider.CLOUD_CAPABILITY_EIP,
			vpc:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rng := SSyncRange{SyncRangeInput: api.SyncRangeInput{Resources: []string{tt.resource}}}
			if got := shouldSyncRegionVPCs(&rng); got != tt.vpc {
				t.Fatalf("shouldSyncRegionVPCs(%v) = %v, want %v", tt.resource, got, tt.vpc)
			}
			if got := shouldSyncVPCNetwork(&rng); got != tt.network {
				t.Fatalf("shouldSyncVPCNetwork(%v) = %v, want %v", tt.resource, got, tt.network)
			}
			if got := shouldSyncVPCSecurityGroups(&rng); got != tt.secgroup {
				t.Fatalf("shouldSyncVPCSecurityGroups(%v) = %v, want %v", tt.resource, got, tt.secgroup)
			}
			if got := shouldSyncVPCNatGateways(&rng); got != tt.nat {
				t.Fatalf("shouldSyncVPCNatGateways(%v) = %v, want %v", tt.resource, got, tt.nat)
			}
			if got := shouldSyncVPCPeerings(&rng); got != tt.peer {
				t.Fatalf("shouldSyncVPCPeerings(%v) = %v, want %v", tt.resource, got, tt.peer)
			}
			if got := shouldSyncVPCIPv6Gateways(&rng); got != tt.ipv6 {
				t.Fatalf("shouldSyncVPCIPv6Gateways(%v) = %v, want %v", tt.resource, got, tt.ipv6)
			}
		})
	}
}

func TestShouldSyncSnapshotPolicies(t *testing.T) {
	tests := []struct {
		name string
		in   SSyncRange
		want bool
	}{
		{
			name: "default compute path keeps legacy snapshot policy sync",
			in:   SSyncRange{},
			want: true,
		},
		{
			name: "explicit snapshot policy is independent",
			in: SSyncRange{SyncRangeInput: api.SyncRangeInput{
				Resources: []string{cloudprovider.CLOUD_CAPABILITY_SNAPSHOT_POLICY},
			}},
			want: true,
		},
		{
			name: "explicit compute does not pull unrelated snapshot policies",
			in: SSyncRange{SyncRangeInput: api.SyncRangeInput{
				Resources: []string{cloudprovider.CLOUD_CAPABILITY_COMPUTE},
			}},
			want: false,
		},
		{
			name: "skipped snapshot policy stays skipped",
			in: SSyncRange{SyncRangeInput: api.SyncRangeInput{
				SkipSyncResources: []string{SnapshotPolicyManager.Keyword()},
			}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldSyncSnapshotPolicies(&tt.in); got != tt.want {
				t.Fatalf("shouldSyncSnapshotPolicies(%+v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestPreserveRegionsWhileIndependentJobsAreActive(t *testing.T) {
	for _, state := range []string{"waiting", "running", "retry"} {
		if !preserveIndependentSyncRegions(map[string]int{state: 1}) {
			t.Errorf("automatic discovery must not disable regions with %s jobs", state)
		}
	}
	if preserveIndependentSyncRegions(map[string]int{"succeeded": 10, "failed": 2}) {
		t.Fatal("completed runs must allow automatic region policy again")
	}
}

func TestInterruptedProviderRegionRecovery(t *testing.T) {
	now := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	started := now.Add(-5 * 24 * time.Hour)
	for _, status := range []string{api.CLOUD_PROVIDER_SYNC_STATUS_IDLE, api.CLOUD_PROVIDER_SYNC_STATUS_QUEUING, api.CLOUD_PROVIDER_SYNC_STATUS_QUEUED, api.CLOUD_PROVIDER_SYNC_STATUS_SYNCING} {
		for _, enabled := range []bool{false, true} {
			cpr := SCloudproviderregion{Enabled: enabled, LastDiscoverHitAt: started.Add(-time.Hour)}
			cpr.LastSync = started
			cpr.SyncStatus = status
			want := enabled && (status == api.CLOUD_PROVIDER_SYNC_STATUS_IDLE || status == api.CLOUD_PROVIDER_SYNC_STATUS_QUEUING)
			if got := cpr.shouldSubmitResourceSync(false, false, now, time.Hour); got != want {
				t.Errorf("interrupted region enabled=%v status=%s: submission=%v, want %v", enabled, status, got, want)
			}
		}
	}
}

func TestEnabledInterruptedRegionSurvivesRefresh(t *testing.T) {
	if !shouldEnableProviderRegion(providerRegionEnableInput{EnabledInterruptedSync: true, DiscoverMissed: true}) {
		t.Fatal("enabled interrupted sync must survive refresh even when no local inventory was committed")
	}
}
