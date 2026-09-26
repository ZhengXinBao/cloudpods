package taskman

import (
	"testing"
	"yunion.io/x/jsonutils"
)

func TestRestartPreservedStages(t *testing.T) {
	RegisterRestartPreservedStage("test-sync-task", "waiting-external")
	if !preserveTaskOnRestart("test-sync-task", "waiting-external") {
		t.Fatal("durable wait lost on restart")
	}
	if preserveTaskOnRestart("other-task", "waiting-external") {
		t.Fatal("unrelated task must retain recovery behavior")
	}
	if preserveTaskOnRestart("test-sync-task", "planning") {
		t.Fatal("unsealed producer must not be preserved")
	}
}

func TestRestartPreservedPredicateOnlyRetryInit(t *testing.T) {
	RegisterRestartPreservedPredicate("retry-test", func(task *STask) bool {
		return task.Stage == TASK_INIT_STAGE && task.Params != nil && task.Params.Contains("retry_of")
	})
	task := &STask{}
	task.TaskName = "retry-test"
	task.Stage = TASK_INIT_STAGE
	if preserveTaskInstanceOnRestart(task) {
		t.Fatal("ordinary init preserved")
	}
	task.Params = jsonutils.NewDict()
	task.Params.Set("retry_of", jsonutils.NewString("source"))
	if !preserveTaskInstanceOnRestart(task) {
		t.Fatal("durable retry init lost")
	}
	task.Stage = "ordinary-stage"
	if preserveTaskInstanceOnRestart(task) {
		t.Fatal("unrelated stage preserved")
	}
}
