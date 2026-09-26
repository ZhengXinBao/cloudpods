package cloudaccount

import (
	"context"
	"errors"
	"fmt"
	"time"

	"yunion.io/x/jsonutils"
	"yunion.io/x/log"
	"yunion.io/x/onecloud/pkg/cloudcommon/db"
	"yunion.io/x/onecloud/pkg/cloudcommon/db/taskman"
	"yunion.io/x/onecloud/pkg/compute/models"
	"yunion.io/x/onecloud/pkg/compute/options"
	"yunion.io/x/onecloud/pkg/mcclient"
	"yunion.io/x/pkg/util/httputils"
)

const independentSyncWaitStage = "OnIndependentSyncComplete"
const retrySyncPrepareStage = "OnRetrySyncPrepare"

var independentSyncTaskNames = []string{"CloudAccountSyncInfoTask", "CloudProviderSyncInfoTask"}

func init() {
	taskman.RegisterRestartPreservedStage("CloudAccountSyncInfoTask", retrySyncPrepareStage)
	taskman.RegisterRestartPreservedPredicate("CloudAccountSyncInfoTask", func(task *taskman.STask) bool {
		if task.Stage != taskman.TASK_INIT_STAGE || task.Params == nil {
			return false
		}
		source, _ := task.Params.GetString("retry_of")
		return source != ""
	})
	taskman.RegisterRestartPreservedStage("CloudAccountSyncInfoTask", independentSyncWaitStage)
	taskman.RegisterRestartPreservedStage("CloudProviderSyncInfoTask", independentSyncWaitStage)
}

func (self *CloudAccountSyncInfoTask) prepareIndependentSync(ctx context.Context, account *models.SCloudaccount, sr models.SSyncRange) {
	if err := self.SetStage("OnIndependentSyncPrepared", nil); err != nil {
		self.SetStageFailed(ctx, jsonutils.NewString(err.Error()))
		return
	}
	taskman.LocalTaskRun(self, func() (jsonutils.JSONObject, error) {
		if err := sr.Normalize(ctx); err != nil {
			return nil, err
		}
		var planningErrors []error
		// Discovery must not submit anonymous jobs that escape this run's progress.
		discoverCtx, discoverFailures := models.WithCloudSyncErrors(ctx)
		if err := account.DiscoverCloudproviderRegions(discoverCtx, self.UserCred, false, true); err != nil {
			planningErrors = append(planningErrors, err)
		}
		if err := discoverFailures.Error(); err != nil {
			planningErrors = append(planningErrors, err)
		}
		if err := account.RefreshEnabledRegionsAfterDiscover(ctx, self.UserCred, sr.Region); err != nil {
			return nil, err
		}
		sr = models.AccountFanoutSyncRange(sr)
		var providers []models.SCloudprovider
		if err := db.FetchModelObjects(models.CloudproviderManager, models.CloudproviderManager.Query().Equals("cloudaccount_id", account.Id).IsTrue("enabled"), &providers); err != nil {
			return nil, err
		}
		for i := range providers {
			if err := providers[i].EnqueueIndependentRegions(ctx, self.UserCred, sr, self.GetTaskId()); err != nil {
				planningErrors = append(planningErrors, err)
			}
		}
		return nil, errors.Join(planningErrors...)
	})
}

func (self *CloudProviderSyncInfoTask) prepareIndependentSync(ctx context.Context, provider *models.SCloudprovider, sr models.SSyncRange) {
	if err := self.SetStage("OnIndependentSyncPrepared", nil); err != nil {
		self.SetStageFailed(ctx, jsonutils.NewString(err.Error()))
		return
	}
	taskman.LocalTaskRun(self, func() (jsonutils.JSONObject, error) {
		return nil, provider.EnqueueIndependentRegions(ctx, self.UserCred, sr, self.GetTaskId())
	})
}

func sealIndependentRun(ctx context.Context, task *taskman.STask, data jsonutils.JSONObject) {
	params := jsonutils.NewDict()
	if data != nil {
		params.Set("independent_sync_plan_error", data)
	}
	if err := task.SetStage(independentSyncWaitStage, params); err != nil {
		log.Errorf("seal independent sync run %s: %v", task.Id, err)
	}
}

func (self *CloudAccountSyncInfoTask) OnIndependentSyncPrepared(ctx context.Context, obj db.IStandaloneModel, data jsonutils.JSONObject) {
	sealIndependentRun(ctx, &self.STask, nil)
}
func (self *CloudAccountSyncInfoTask) OnIndependentSyncPreparedFailed(ctx context.Context, obj db.IStandaloneModel, data jsonutils.JSONObject) {
	sealIndependentRun(ctx, &self.STask, data)
}
func (self *CloudProviderSyncInfoTask) OnIndependentSyncPrepared(ctx context.Context, obj db.IStandaloneModel, data jsonutils.JSONObject) {
	sealIndependentRun(ctx, &self.STask, nil)
}
func (self *CloudProviderSyncInfoTask) OnIndependentSyncPreparedFailed(ctx context.Context, obj db.IStandaloneModel, data jsonutils.JSONObject) {
	sealIndependentRun(ctx, &self.STask, data)
}

// Notifications are deliberately revalidated: duplicate recovery/cron messages
// may arrive after a stage transition, and task notifications carry only an ID.
func independentCompletion(ctx context.Context, task *taskman.STask) (bool, bool, jsonutils.JSONObject) {
	taskID := task.Id
	var current taskman.STask
	if err := taskman.TaskManager.Query().Equals("id", taskID).First(&current); err != nil {
		log.Errorf("read sync run %s: %v", taskID, err)
		return false, false, nil
	}
	task.Stage = current.Stage
	task.Params = current.Params
	if current.Stage != independentSyncWaitStage {
		return false, false, nil
	}
	stats, err := models.CloudSyncQueue().StatsRun(ctx, taskID)
	if err != nil {
		log.Errorf("read sync progress %s: %v", taskID, err)
		return false, false, nil
	}
	done, result := independentRunResult(stats, current.Params)
	failed := stats["failed"] > 0 || (current.Params != nil && current.Params.Contains("independent_sync_plan_error"))
	return done, failed, result
}
func (self *CloudAccountSyncInfoTask) OnIndependentSyncComplete(ctx context.Context, obj db.IStandaloneModel, data jsonutils.JSONObject) {
	unlock, err := models.CloudSyncQueue().AcquireRetryLock(ctx, "run:"+self.GetTaskId())
	if err != nil {
		log.Errorf("lock sync completion %s: %v", self.GetTaskId(), err)
		return
	}
	defer unlock()
	done, failed, result := independentCompletion(ctx, &self.STask)
	if !done {
		return
	}
	if failed {
		self.OnCloudaccountSyncCompleteFailed(ctx, obj, result)
	} else {
		self.OnCloudaccountSyncComplete(ctx, obj, result)
	}
}
func (self *CloudAccountSyncInfoTask) OnIndependentSyncCompleteFailed(ctx context.Context, obj db.IStandaloneModel, data jsonutils.JSONObject) {
	self.OnIndependentSyncComplete(ctx, obj, data)
}
func (self *CloudProviderSyncInfoTask) OnIndependentSyncComplete(ctx context.Context, obj db.IStandaloneModel, data jsonutils.JSONObject) {
	unlock, err := models.CloudSyncQueue().AcquireRetryLock(ctx, "run:"+self.GetTaskId())
	if err != nil {
		log.Errorf("lock sync completion %s: %v", self.GetTaskId(), err)
		return
	}
	defer unlock()
	done, failed, result := independentCompletion(ctx, &self.STask)
	if !done {
		return
	}
	if failed {
		self.OnSyncCloudProviderInfoCompleteFailed(ctx, obj, result)
	} else {
		self.OnSyncCloudProviderInfoComplete(ctx, obj, result)
	}
}
func (self *CloudProviderSyncInfoTask) OnIndependentSyncCompleteFailed(ctx context.Context, obj db.IStandaloneModel, data jsonutils.JSONObject) {
	self.OnIndependentSyncComplete(ctx, obj, data)
}

// ReconcileIndependentSyncRuns is a leader cron, not a goroutine parked for
// every run. The durable waiting stage survives coordinator restarts.
func ReconcileIndependentSyncRuns(ctx context.Context, cred mcclient.TokenCredential, start bool) {
	var tasks []taskman.STask
	q := taskman.TaskManager.Query().In("task_name", independentSyncTaskNames).In("stage", []string{independentSyncWaitStage, retrySyncPrepareStage, taskman.TASK_INIT_STAGE})
	if err := db.FetchModelObjects(taskman.TaskManager, q, &tasks); err != nil {
		log.Errorf("list sync runs: %v", err)
		return
	}
	for i := range tasks {
		task := &tasks[i]
		if task.Stage != independentSyncWaitStage {
			if task.TaskName != "CloudAccountSyncInfoTask" || task.Params == nil {
				continue
			}
			source, _ := task.Params.GetString("retry_of")
			if source == "" {
				continue
			}
			if err := task.ScheduleRunAtStage(nil); err != nil {
				log.Errorf("resume retry sync run %s: %v", task.Id, err)
			}
			continue
		}
		stats, err := models.CloudSyncQueue().StatsRun(ctx, task.Id)
		if err != nil {
			log.Errorf("sync run progress %s: %v", task.Id, err)
			continue
		}
		done, data := independentRunResult(stats, task.Params)
		if !done {
			continue
		}
		if err := task.ScheduleRunAtStage(data); err != nil {
			log.Errorf("finish sync run %s: %v", task.Id, err)
		}
	}
}

func independentRunResult(stats map[string]int, params *jsonutils.JSONDict) (bool, jsonutils.JSONObject) {
	if stats["waiting"]+stats["running"]+stats["retry"] > 0 {
		return false, nil
	}
	if params != nil {
		if planErr, err := params.Get("independent_sync_plan_error"); err == nil {
			return true, taskman.Error2TaskData(fmt.Errorf("sync planning failed: %s", planErr.String()))
		}
	}
	if stats["failed"] > 0 {
		return true, taskman.Error2TaskData(fmt.Errorf("%d resource sync jobs failed", stats["failed"]))
	}
	return true, jsonutils.Marshal(stats)
}

const syncQueuePurgeBatch = 1000
const syncQueuePurgeMaxBatches = 50

// PurgeFinishedSyncJobs bounds queue growth. Unfinished runs and the sources of
// in-flight retries are protected so their progress never reads as empty.
func PurgeFinishedSyncJobs(ctx context.Context, cred mcclient.TokenCredential, start bool) {
	days := options.Options.SyncQueueRetentionDays
	if days <= 0 {
		return
	}
	protected, err := protectedSyncRuns()
	if err != nil {
		log.Errorf("list protected sync runs: %v", err)
		return
	}
	retention := time.Duration(days) * 24 * time.Hour
	total := 0
	for i := 0; i < syncQueuePurgeMaxBatches; i++ {
		n, err := models.CloudSyncQueue().PurgeFinished(ctx, retention, syncQueuePurgeBatch, protected)
		if err != nil {
			log.Errorf("purge finished sync jobs: %v", err)
			break
		}
		total += n
		if n < syncQueuePurgeBatch {
			break
		}
	}
	if total > 0 {
		log.Infof("purged %d finished sync jobs older than %d days", total, days)
	}
}

func protectedSyncRuns() ([]string, error) {
	var tasks []taskman.STask
	q := taskman.TaskManager.Query().In("task_name", independentSyncTaskNames).NotIn("stage", []string{taskman.TASK_STAGE_COMPLETE, taskman.TASK_STAGE_FAILED})
	if err := db.FetchModelObjects(taskman.TaskManager, q, &tasks); err != nil {
		return nil, err
	}
	runs := make([]string, 0, len(tasks))
	for i := range tasks {
		runs = append(runs, tasks[i].Id)
		if tasks[i].Params == nil {
			continue
		}
		if source, _ := tasks[i].Params.GetString("retry_of"); source != "" {
			runs = append(runs, source)
		}
	}
	return runs, nil
}

func (self *CloudAccountSyncInfoTask) prepareRetrySync(ctx context.Context, account *models.SCloudaccount, sourceID string) {
	self.OnRetrySyncPrepare(ctx, account, nil)
}
func (self *CloudAccountSyncInfoTask) OnRetrySyncPrepare(ctx context.Context, obj db.IStandaloneModel, data jsonutils.JSONObject) {
	// Include stage validation, enqueue and seal in the same cross-replica lock.
	unlock, err := models.CloudSyncQueue().AcquireRetryLock(ctx, "run:"+self.GetTaskId())
	if err != nil {
		log.Errorf("lock retry planning %s: %v", self.GetTaskId(), err)
		return
	}
	defer unlock()
	var current taskman.STask
	if err := taskman.TaskManager.Query().Equals("id", self.GetTaskId()).First(&current); err != nil {
		log.Errorf("read retry planning %s: %v", self.GetTaskId(), err)
		return
	}
	self.Stage = current.Stage
	self.Params = current.Params
	if current.Stage != taskman.TASK_INIT_STAGE && current.Stage != retrySyncPrepareStage {
		return
	}
	if self.Stage == taskman.TASK_INIT_STAGE {
		if err := self.SetStage(retrySyncPrepareStage, nil); err != nil {
			log.Errorf("prepare retry planning %s: %v", self.GetTaskId(), err)
			return
		}
	}
	sourceID, _ := self.Params.GetString("retry_of")
	if err := obj.(*models.SCloudaccount).EnqueueFailedSyncRun(ctx, self.UserCred, sourceID, self.GetTaskId()); err != nil {
		if failure, ok := err.(*httputils.JSONClientError); ok && failure.Code < 500 {
			sealIndependentRun(ctx, &self.STask, taskman.Error2TaskData(err))
		} else {
			// Leave the durable prepare stage intact on database/transport failures.
			log.Errorf("retry sync run %s planning will resume: %v", self.GetTaskId(), err)
		}
		return
	}
	sealIndependentRun(ctx, &self.STask, nil)
}
