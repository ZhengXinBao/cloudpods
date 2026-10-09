package models

import (
	"context"
	"errors"
	"fmt"
	"time"

	"yunion.io/x/jsonutils"
	"yunion.io/x/sqlchemy"

	"yunion.io/x/onecloud/pkg/cloudcommon/db"
	"yunion.io/x/onecloud/pkg/cloudcommon/db/lockman"
	"yunion.io/x/onecloud/pkg/compute/options"
	"yunion.io/x/onecloud/pkg/compute/syncqueue"
	"yunion.io/x/onecloud/pkg/compute/synctrace"
	"yunion.io/x/onecloud/pkg/compute/syncworker"
	"yunion.io/x/onecloud/pkg/httperrors"
	"yunion.io/x/onecloud/pkg/mcclient"
)

func IndependentSyncAccount(accountID string) bool {
	return syncworker.RoutesAccount(options.Options.IndependentCloudSync, options.Options.IndependentCloudSyncAccounts, accountID)
}

// ensureLegacySyncAllowed blocks in-process sync for an account that left the
// allowlist while its routed jobs still run: legacy serialization is per-process
// and cannot see worker-held region locks. Keep IndependentCloudSync on until
// the queue drains; with it off the queue is assumed absent.
func ensureLegacySyncAllowed(ctx context.Context, accountID string) error {
	if !options.Options.IndependentCloudSync || IndependentSyncAccount(accountID) {
		return nil
	}
	active, err := CloudSyncQueue().ActiveAccountJobs(ctx, accountID)
	if err != nil {
		return fmt.Errorf("check queued sync jobs: %w", err)
	}
	if active > 0 {
		return httperrors.NewConflictError("account %s still has %d queued independent sync jobs; legacy sync resumes after they drain", accountID, active)
	}
	return nil
}

func CloudSyncQueue() *syncqueue.Queue {
	queue := syncqueue.New(sqlchemy.GetDB())
	queue.GlobalLimit = options.Options.SyncWorkerGlobalLimit
	queue.AccountLimit = options.Options.SyncWorkerAccountLimit
	queue.ProviderLimit = options.Options.SyncWorkerProviderLimit
	return queue
}

func (self *SCloudproviderregion) enqueueResourceSync(ctx context.Context, cred mcclient.TokenCredential, sr SSyncRange, resourceGroup, runID string) error {
	provider, err := self.GetProvider()
	if err != nil {
		return err
	}
	request := syncqueue.Request{
		RunID:         runID,
		AccountID:     provider.CloudaccountId,
		ProviderID:    provider.Id,
		RegionID:      self.CloudregionId,
		ScopeType:     "region",
		ResourceGroup: resourceGroup,
		RangeJSON:     jsonutils.Marshal(sr).String(),
	}
	if cred != nil {
		request.RequestedBy = cred.GetUserId()
		request.ProjectID = cred.GetProjectId()
		request.DomainID = cred.GetProjectDomainId()
	}
	start := time.Now()
	jobID, err := CloudSyncQueue().Enqueue(ctx, request)
	traceCtx := synctrace.WithIdentity(ctx, synctrace.Identity{RunID: runID, AccountID: provider.CloudaccountId, ProviderID: provider.Id, RegionID: self.CloudregionId, ResourceGroup: resourceGroup, JobID: jobID})
	synctrace.Duration(traceCtx, "enqueue", time.Since(start), err)
	if err != nil {
		return fmt.Errorf("enqueue region sync: %w", err)
	}

	return nil
}

func (provider *SCloudprovider) enqueueProviderSync(ctx context.Context, cred mcclient.TokenCredential, sr SSyncRange, resourceGroup, runID string) error {
	request := syncqueue.Request{
		RunID:         runID,
		AccountID:     provider.CloudaccountId,
		ProviderID:    provider.Id,
		ScopeType:     "provider",
		ResourceGroup: resourceGroup,
		RangeJSON:     jsonutils.Marshal(sr).String(),
	}
	if cred != nil {
		request.RequestedBy = cred.GetUserId()
		request.ProjectID = cred.GetProjectId()
		request.DomainID = cred.GetProjectDomainId()
	}
	start := time.Now()
	jobID, err := CloudSyncQueue().Enqueue(ctx, request)
	traceCtx := synctrace.WithIdentity(ctx, synctrace.Identity{RunID: runID, AccountID: provider.CloudaccountId, ProviderID: provider.Id, ResourceGroup: resourceGroup, JobID: jobID})
	synctrace.Duration(traceCtx, "enqueue", time.Since(start), err)
	if err != nil {
		return fmt.Errorf("enqueue provider sync: %w", err)
	}
	return nil
}

// EnqueueIndependentAccountRegions streams each discovered provider into the
// same durable run. The caller must seal only after this producer returns.
func (account *SCloudaccount) EnqueueIndependentAccountRegions(ctx context.Context, cred mcclient.TokenCredential, sr SSyncRange, runID string) error {
	// Establish producer identity once, before creating provider lock contexts.
	ctx = synctrace.WithIdentity(ctx, synctrace.Identity{RunID: runID, AccountID: account.Id})
	if err := sr.Normalize(ctx); err != nil {
		return err
	}
	return planIndependentSyncProviders(ctx, sr, runID,
		func(ctx context.Context, ready func(context.Context, *SCloudprovider) error) error {
			// Never submit anonymous first-inventory jobs outside this run.
			return account.discoverCloudproviderRegionsReady(ctx, cred, false, true, db.FetchModelObjects, ready)
		}, func(ctx context.Context, provider *SCloudprovider, sr SSyncRange, runID string) error {
			// Make refresh+enqueue atomic with respect to background account
			// refreshes. No cloud SDK calls occur under this account lock.
			lockman.LockObject(ctx, account)
			defer lockman.ReleaseObject(ctx, account)
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := account.refreshEnabledCloudproviderRegions(ctx, cred, account.getSubAccountRegionExternalIds(), sr.Region, provider.Id); err != nil {
				return err
			}
			return provider.EnqueueIndependentRegions(ctx, cred, sr, runID)
		})
}

func planIndependentSyncProviders(ctx context.Context, sr SSyncRange, runID string, discover func(context.Context, func(context.Context, *SCloudprovider) error) error, ready func(context.Context, *SCloudprovider, SSyncRange, string) error) error {
	traceCtx := synctrace.WithIdentity(ctx, synctrace.Identity{RunID: runID})
	return synctrace.Measure(traceCtx, "discover_plan", func() error {
		discoverCtx, failures := WithCloudSyncErrors(ctx)
		sr = AccountFanoutSyncRange(sr)
		err := discover(discoverCtx, func(providerCtx context.Context, provider *SCloudprovider) error {
			if err := providerCtx.Err(); err != nil {
				return err
			}
			providerTrace := synctrace.WithIdentity(traceCtx, synctrace.Identity{ProviderID: provider.Id, AccountID: provider.CloudaccountId})
			return synctrace.Measure(providerTrace, "provider_plan", func() error {
				return ready(providerCtx, provider, sr, runID)
			})
		})
		return errors.Join(err, failures.Error())
	})
}

// EnqueueIndependentRegions plans all eligible regions synchronously. The caller
// seals its task stage only after this returns, so an empty queue cannot finish
// a run while its producer is still adding jobs.
func (provider *SCloudprovider) EnqueueIndependentRegions(ctx context.Context, cred mcclient.TokenCredential, sr SSyncRange, runID string) error {
	if err := sr.Normalize(ctx); err != nil {
		return err
	}
	regionIDs, err := sr.GetRegionIds()
	if err != nil {
		return err
	}
	if len(sr.Region)+len(sr.Zone)+len(sr.Host) > 0 && len(regionIDs) == 0 {
		return fmt.Errorf("requested sync scope matches no region")
	}

	var regions []SCloudproviderregion
	if err := db.FetchModelObjects(CloudproviderRegionManager, CloudproviderRegionManager.Query().Equals("cloudprovider_id", provider.Id), &regions); err != nil {
		return err
	}
	plans := independentSyncPlans(sr)
	for _, plan := range plans {
		if plan.ScopeType == "region" {
			regions, err = independentRegionCandidates(regions, regionIDs)
			if err != nil {
				return fmt.Errorf("plan provider %s: %w", provider.Id, err)
			}
			break
		}
	}
	hasRegionPlan := false
	for _, plan := range plans {
		if plan.ScopeType == "provider" {
			if err := provider.enqueueProviderSync(ctx, cred, plan.Range, plan.ResourceGroup, runID); err != nil {
				return err
			}
			continue
		}
		hasRegionPlan = true
	}
	if !hasRegionPlan {
		return nil
	}
	return enqueueRegionPlans(ctx, cred, regions, sr, runID, time.Now())
}

// enqueueRegionPlans plans each region with its own periodic deep sync
// decision, then enqueues group by group so every region's core job is queued
// before any of its dependent groups.
func enqueueRegionPlans(ctx context.Context, cred mcclient.TokenCredential, regions []SCloudproviderregion, sr SSyncRange, runID string, now time.Time) error {
	regionPlans := make([][]independentSyncPlan, len(regions))
	maxPlans := 0
	for i := range regions {
		for _, plan := range independentSyncPlans(regionSyncRange(sr, regions[i].LastDeepSyncAt, now)) {
			if plan.ScopeType == "region" {
				regionPlans[i] = append(regionPlans[i], plan)
			}
		}
		if len(regionPlans[i]) > maxPlans {
			maxPlans = len(regionPlans[i])
		}
	}
	for idx := 0; idx < maxPlans; idx++ {
		for i := range regions {
			if idx >= len(regionPlans[i]) {
				continue
			}
			plan := regionPlans[i][idx]
			if err := regions[i].enqueueResourceSync(ctx, cred, plan.Range, plan.ResourceGroup, runID); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetDetailsSyncQueue uses the existing cloudaccount authorization boundary.
func (account *SCloudaccount) GetDetailsSyncQueue(ctx context.Context, cred mcclient.TokenCredential, query jsonutils.JSONObject) (jsonutils.JSONObject, error) {
	stats, err := CloudSyncQueue().Stats(ctx, account.Id)
	if err != nil {
		return nil, err
	}
	jobs, err := CloudSyncQueue().List(ctx, account.Id, 100)
	if err != nil {
		return nil, err
	}
	result := jsonutils.NewDict()
	result.Set("counts", jsonutils.Marshal(stats))
	result.Set("jobs", jsonutils.Marshal(jobs))
	return result, nil
}

// ExecuteCloudSyncJob resolves fresh models, never serializes account secrets,
// and validates that the original resource still belongs to the same account.
func ExecuteCloudSyncJob(ctx context.Context, cred mcclient.TokenCredential, job *syncqueue.Job) error {
	obj, err := CloudproviderManager.FetchById(job.ProviderID)
	if err != nil {
		return err
	}
	provider := obj.(*SCloudprovider)
	if provider.CloudaccountId != job.AccountID {
		return fmt.Errorf("sync job account binding changed")
	}
	if !provider.GetEnabled() {
		return fmt.Errorf("sync provider disabled")
	}
	account, err := provider.GetCloudaccount()
	if err != nil {
		return err
	}
	if !account.GetEnabled() {
		return fmt.Errorf("sync account disabled")
	}
	data, err := jsonutils.ParseString(job.RangeJSON)
	if err != nil {
		return err
	}
	sr := SSyncRange{}
	if err = data.Unmarshal(&sr); err != nil {
		return err
	}
	ctx, failures := WithCloudSyncErrors(ctx)
	scope := job.ScopeType
	if scope == "" {
		scope = "region"
	}
	if scope == "provider" {
		err = SyncCloudproviderResources(ctx, cred, provider, &sr)
		err = failures.Merge(err)
		if endErr := provider.markEndSyncWithLock(ctx, cred, false); err == nil {
			err = endErr
		}
		return err
	}
	if scope != "region" {
		return fmt.Errorf("unsupported sync scope %q", job.ScopeType)
	}
	var region SCloudproviderregion
	err = CloudproviderRegionManager.Query().Equals("cloudprovider_id", job.ProviderID).Equals("cloudregion_id", job.RegionID).First(&region)
	if err != nil {
		return err
	}
	region.SetModelManager(CloudproviderRegionManager, &region)
	if !region.Enabled {
		return fmt.Errorf("sync region disabled")
	}
	manageRegionStatus := job.ResourceGroup == "" || job.ResourceGroup == "core"
	if !manageRegionStatus {
		ctx = context.WithValue(ctx, skipCloudSyncStatusKey{}, true)
	}
	err = region.DoSync(ctx, cred, sr)
	return failures.Merge(err)
}

// ResetIndependentSyncFailure clears legacy display flags while the caller still
// owns the execution mutex. It does not claim a successful resource scan.
func ResetIndependentSyncFailure(ctx context.Context, cred mcclient.TokenCredential, job *syncqueue.Job) error {
	if job.ScopeType == "provider" {
		providerObj, err := CloudproviderManager.FetchById(job.ProviderID)
		if err != nil {
			return err
		}
		provider := providerObj.(*SCloudprovider)
		return provider.markEndSyncWithLock(ctx, cred, false)
	}
	var region SCloudproviderregion
	if err := CloudproviderRegionManager.Query().Equals("cloudprovider_id", job.ProviderID).Equals("cloudregion_id", job.RegionID).First(&region); err != nil {
		return err
	}
	region.SetModelManager(CloudproviderRegionManager, &region)
	if _, err := db.Update(&region, func() error { region.SyncStatus = "idle"; return nil }); err != nil {
		return err
	}
	provider, err := region.GetProvider()
	if err != nil {
		return err
	}
	return provider.markEndSyncWithLock(ctx, cred, false)
}
