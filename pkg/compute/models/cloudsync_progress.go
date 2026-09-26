package models

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"yunion.io/x/jsonutils"
	"yunion.io/x/onecloud/pkg/cloudcommon/db"

	"yunion.io/x/onecloud/pkg/cloudcommon/db/taskman"
	"yunion.io/x/onecloud/pkg/compute/syncqueue"
	"yunion.io/x/onecloud/pkg/httperrors"
	"yunion.io/x/onecloud/pkg/mcclient"
)

type syncProgressRun struct {
	ID              string     `json:"id"`
	State           string     `json:"state"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	DurationSeconds int64      `json:"duration_seconds"`
	RetryOf         string     `json:"retry_of"`
	Error           string     `json:"error"`
}

func validateSyncRun(task *taskman.STask, accountID string) error {
	if task.ObjId != accountID || task.TaskName != "CloudAccountSyncInfoTask" {
		return httperrors.NewNotFoundError("sync run not found")
	}
	return nil
}
func fetchSyncRun(accountID, runID string) (*taskman.STask, error) {
	var task taskman.STask
	err := taskman.TaskManager.Query().Equals("id", runID).Equals("obj_id", accountID).Equals("task_name", "CloudAccountSyncInfoTask").First(&task)
	if err == sql.ErrNoRows {
		return nil, httperrors.NewNotFoundError("sync run not found")
	}
	if err != nil {
		return nil, err
	}
	if err = validateSyncRun(&task, accountID); err != nil {
		return nil, err
	}
	task.SetModelManager(taskman.TaskManager, &task)
	return &task, nil
}
func syncRunView(task *taskman.STask) syncProgressRun {
	r := syncProgressRun{ID: task.Id, State: "running", StartedAt: task.CreatedAt}
	if !task.StartAt.IsZero() {
		r.StartedAt = task.StartAt
	}
	end := time.Now()
	switch task.Stage {
	case taskman.TASK_STAGE_FAILED:
		r.State = "failed"
		r.Error = "Synchronization failed"
		end = task.UpdatedAt
		r.FinishedAt = &end
	case taskman.TASK_STAGE_COMPLETE:
		r.State = "succeeded"
		end = task.UpdatedAt
		r.FinishedAt = &end
	case "OnInit", "on_init", "":
		r.State = "waiting"
	}
	if r.FinishedAt != nil && !task.EndAt.IsZero() {
		end = task.EndAt
		r.FinishedAt = &end
	}
	r.DurationSeconds = int64(end.Sub(r.StartedAt).Seconds())
	if r.DurationSeconds < 0 {
		r.DurationSeconds = 0
	}
	if task.Params != nil {
		r.RetryOf, _ = task.Params.GetString("retry_of")
		if task.Params.Contains("independent_sync_plan_error") {
			r.Error = "Synchronization planning failed"
		}
	}
	return r
}
func syncRetryReason(task *taskman.STask, failed int) string {
	if task.Stage != taskman.TASK_STAGE_FAILED && task.Stage != taskman.TASK_STAGE_COMPLETE {
		return "run_active"
	}
	if failed == 0 {
		if task.Params != nil && task.Params.Contains("independent_sync_plan_error") {
			return "planning_failed_without_jobs"
		}
		return "no_failed_jobs"
	}
	return ""
}

func (account *SCloudaccount) GetDetailsSyncProgress(ctx context.Context, cred mcclient.TokenCredential, query jsonutils.JSONObject) (jsonutils.JSONObject, error) {
	runID, _ := query.GetString("run_id")
	o, _ := query.Int("offset")
	l, _ := query.Int("limit")
	offset, limit := syncqueue.ProgressPagination(int(o), int(l))
	var tasks []taskman.STask
	if err := db.FetchModelObjects(taskman.TaskManager, taskman.TaskManager.Query().Equals("obj_id", account.Id).Equals("task_name", "CloudAccountSyncInfoTask").Desc("created_at").Desc("id").Limit(20), &tasks); err != nil {
		return nil, err
	}
	runs := []syncProgressRun{}
	for i := range tasks {
		runs = append(runs, syncRunView(&tasks[i]))
	}
	result := jsonutils.NewDict()
	result.Set("runs", jsonutils.Marshal(runs))
	result.Set("selected_run", jsonutils.JSONNull)
	result.Set("jobs", jsonutils.NewArray())
	result.Set("counts", jsonutils.Marshal(syncqueue.EmptyProgressCounts()))
	result.Set("region_counts", jsonutils.Marshal(syncqueue.EmptyProgressCounts()))
	result.Set("total", jsonutils.NewInt(0))
	result.Set("offset", jsonutils.NewInt(int64(offset)))
	result.Set("limit", jsonutils.NewInt(int64(limit)))
	result.Set("independent_sync", jsonutils.NewBool(IndependentSyncAccount(account.Id)))
	result.Set("can_retry", jsonutils.JSONFalse)
	result.Set("retry_disabled_reason", jsonutils.NewString("no_failed_jobs"))
	if runID == "" && len(tasks) > 0 {
		runID = tasks[0].Id
	}
	if runID == "" {
		return result, nil
	}
	task, err := fetchSyncRun(account.Id, runID)
	if err != nil {
		return nil, err
	}
	result.Set("selected_run", jsonutils.Marshal(syncRunView(task)))
	counts, regions, err := CloudSyncQueue().RunProgressCounts(ctx, account.Id, runID)
	if err != nil {
		return nil, httperrors.NewInternalServerError("sync progress unavailable")
	}
	jobs, err := CloudSyncQueue().RunJobs(ctx, account.Id, runID, offset, limit, false)
	if err != nil {
		return nil, httperrors.NewInternalServerError("sync progress unavailable")
	}
	safeJobs := jsonutils.NewArray()
	providerNames := map[string]string{}
	regionNames := map[string]string{}
	for _, j := range jobs {
		if _, ok := providerNames[j.ProviderID]; !ok {
			providerNames[j.ProviderID] = j.ProviderID
			if obj, e := CloudproviderManager.FetchById(j.ProviderID); e == nil {
				p := obj.(*SCloudprovider)
				if p.CloudaccountId == account.Id {
					providerNames[j.ProviderID] = p.Name
				}
			}
		}
		if _, ok := regionNames[j.RegionID]; !ok {
			regionNames[j.RegionID] = j.RegionID
			if j.RegionID != "" {
				if obj, e := CloudregionManager.FetchById(j.RegionID); e == nil {
					regionNames[j.RegionID] = obj.GetName()
				}
			}
		}
		scope := j.ScopeType
		if scope == "" {
			scope = "region"
		}
		safeJobs.Add(jsonutils.Marshal(map[string]interface{}{"id": j.ID, "provider_id": j.ProviderID, "provider_name": providerNames[j.ProviderID], "region_id": j.RegionID, "region_name": regionNames[j.RegionID], "scope_type": scope, "resource_group": j.ResourceGroup, "state": j.State, "attempts": j.Attempts, "created_at": j.CreatedAt, "updated_at": j.UpdatedAt, "last_error": syncqueue.PublicError(j.LastError), "error_class": j.ErrorClass}))
	}
	reason := syncRetryReason(task, counts["failed"])
	if !IndependentSyncAccount(account.Id) {
		reason = "independent_sync_disabled"
	}
	if !account.GetEnabled() {
		reason = "account_disabled"
	}
	result.Set("counts", jsonutils.Marshal(counts))
	result.Set("region_counts", jsonutils.Marshal(regions))
	result.Set("jobs", safeJobs)
	result.Set("total", jsonutils.NewInt(int64(counts["total"])))
	result.Set("can_retry", jsonutils.NewBool(reason == ""))
	result.Set("retry_disabled_reason", jsonutils.NewString(reason))
	return result, nil
}

func (account *SCloudaccount) RetryFailedSyncRun(ctx context.Context, cred mcclient.TokenCredential, sourceID string) (jsonutils.JSONObject, error) {
	unlock, lockErr := CloudSyncQueue().AcquireRetryLock(ctx, "account:"+account.Id)
	if lockErr != nil {
		return nil, lockErr
	}
	defer unlock()
	if !IndependentSyncAccount(account.Id) {
		return nil, httperrors.NewInvalidStatusError("independent_sync_disabled")
	}
	source, err := fetchSyncRun(account.Id, sourceID)
	if err != nil {
		return nil, err
	}
	var children []taskman.STask
	if err = db.FetchModelObjects(taskman.TaskManager, taskman.TaskManager.Query().Equals("obj_id", account.Id).Equals("task_name", "CloudAccountSyncInfoTask").Contains("params", sourceID), &children); err != nil {
		return nil, err
	}
	for i := range children {
		if children[i].Params == nil {
			continue
		}
		retryOf, _ := children[i].Params.GetString("retry_of")
		if retryOf == sourceID {
			if children[i].Stage == taskman.TASK_INIT_STAGE || children[i].Stage == "OnRetrySyncPrepare" {
				if err := children[i].ScheduleRunAtStage(nil); err != nil {
					return nil, err
				}
			}
			return jsonutils.Marshal(map[string]interface{}{"run_id": children[i].Id, "retry_of": sourceID, "reused": true}), nil
		}
	}
	jobs, err := CloudSyncQueue().RunJobs(ctx, account.Id, sourceID, 0, 0, true)
	if err != nil {
		return nil, err
	}
	if reason := syncRetryReason(source, len(jobs)); reason != "" {
		return nil, httperrors.NewInvalidStatusError("%s", reason)
	}
	if err = account.validateRetryProviders(jobs); err != nil {
		return nil, err
	}
	params := jsonutils.NewDict()
	params.Set("retry_of", jsonutils.NewString(sourceID))
	params.Set("sync_range", jsonutils.Marshal(SSyncRange{}))
	task, err := taskman.TaskManager.NewTask(ctx, "CloudAccountSyncInfoTask", account, cred, params, "", "", nil)
	if err != nil {
		return nil, err
	}
	if err = task.ScheduleRunAtStage(nil); err != nil {
		return nil, err
	}
	return jsonutils.Marshal(map[string]interface{}{"run_id": task.Id, "retry_of": sourceID, "reused": false}), nil
}
func (account *SCloudaccount) validateRetryProviders(jobs []syncqueue.Job) error {
	seen := map[string]bool{}
	for _, j := range jobs {
		if j.AccountID != account.Id {
			return httperrors.NewInvalidStatusError("provider_unavailable")
		}
		if seen[j.ProviderID] {
			continue
		}
		seen[j.ProviderID] = true
		obj, err := CloudproviderManager.FetchById(j.ProviderID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return httperrors.NewInvalidStatusError("provider_unavailable")
			}
			return err
		}
		p := obj.(*SCloudprovider)
		if p.CloudaccountId != account.Id || !p.GetEnabled() {
			return httperrors.NewInvalidStatusError("provider_unavailable")
		}
	}
	return nil
}

// EnqueueFailedSyncRun never normalizes or expands the original server-held range.
func (account *SCloudaccount) EnqueueFailedSyncRun(ctx context.Context, cred mcclient.TokenCredential, sourceID, runID string) error {
	if !account.GetEnabled() || !IndependentSyncAccount(account.Id) {
		return httperrors.NewInvalidStatusError("retry routing unavailable")
	}
	source, err := fetchSyncRun(account.Id, sourceID)
	if err != nil {
		return err
	}
	jobs, err := CloudSyncQueue().RunJobs(ctx, account.Id, sourceID, 0, 0, true)
	if err != nil {
		return err
	}
	if reason := syncRetryReason(source, len(jobs)); reason != "" {
		return httperrors.NewInvalidStatusError("%s", reason)
	}
	if err = account.validateRetryProviders(jobs); err != nil {
		return err
	}
	existing, err := CloudSyncQueue().RunJobs(ctx, account.Id, runID, 0, 0, false)
	if err != nil {
		return err
	}
	copied := map[string]bool{}
	for _, j := range existing {
		key, err := syncqueue.RetryRequestIdentity(j.Request)
		if err != nil {
			return err
		}
		copied[key] = true
	}
	for _, j := range jobs {
		r, err := syncqueue.RetryRequest(j, account.Id, runID)
		if err != nil {
			return err
		}
		key, err := syncqueue.RetryRequestIdentity(r)
		if err != nil {
			return err
		}
		if copied[key] {
			continue
		}
		r.RequestedBy = cred.GetUserId()
		r.ProjectID = cred.GetProjectId()
		r.DomainID = cred.GetProjectDomainId()
		if _, err = CloudSyncQueue().EnqueueRetryRequest(ctx, r); err != nil {
			return err
		}
		copied[key] = true
	}
	return nil
}
