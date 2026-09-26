package models

import (
	"testing"
	"yunion.io/x/jsonutils"
	"yunion.io/x/onecloud/pkg/cloudcommon/db/taskman"
)

func TestSyncProgressOwnership(t *testing.T) {
	task := &taskman.STask{}
	task.ObjId = "other"
	task.TaskName = "CloudAccountSyncInfoTask"
	if validateSyncRun(task, "account") == nil {
		t.Fatal("cross-account run accepted")
	}
	task.ObjId = "account"
	task.TaskName = "OtherTask"
	if validateSyncRun(task, "account") == nil {
		t.Fatal("unrelated task accepted")
	}
	task.TaskName = "CloudAccountSyncInfoTask"
	if err := validateSyncRun(task, "account"); err != nil {
		t.Fatal(err)
	}
}
func TestSyncRetryPlanningError(t *testing.T) {
	task := &taskman.STask{}
	task.Stage = taskman.TASK_STAGE_FAILED
	task.Params = jsonutils.NewDict()
	task.Params.Set("independent_sync_plan_error", jsonutils.NewString("secret"))
	if got := syncRetryReason(task, 0); got != "planning_failed_without_jobs" {
		t.Fatal(got)
	}
	if got := syncRetryReason(task, 2); got != "" {
		t.Fatal(got)
	}
	task.Stage = "OnIndependentSyncComplete"
	if got := syncRetryReason(task, 2); got != "run_active" {
		t.Fatal(got)
	}
}
