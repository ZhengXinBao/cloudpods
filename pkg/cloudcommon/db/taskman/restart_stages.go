package taskman

import "sync"

var restartPreservedStages sync.Map

// RegisterRestartPreservedStage registers a durable external wait. Its owner
// must reconcile completion after startup; ordinary stages still fail/recover.
func RegisterRestartPreservedStage(taskName, stage string) {
	restartPreservedStages.Store([2]string{taskName, stage}, true)
}
func preserveTaskOnRestart(taskName, stage string) bool {
	_, ok := restartPreservedStages.Load([2]string{taskName, stage})
	return ok
}

var restartPreservedPredicates sync.Map

// RegisterRestartPreservedPredicate supports durable task subtypes whose init
// stage must survive a crash before the first scheduling notification.
func RegisterRestartPreservedPredicate(taskName string, predicate func(*STask) bool) {
	restartPreservedPredicates.Store(taskName, predicate)
}
func preserveTaskInstanceOnRestart(task *STask) bool {
	if preserveTaskOnRestart(task.TaskName, task.Stage) {
		return true
	}
	if predicate, ok := restartPreservedPredicates.Load(task.TaskName); ok {
		return predicate.(func(*STask) bool)(task)
	}
	return false
}
