# Region Sync Preparation Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement locally,
> task-by-task. No delegated agents or remote operations.

**Goal:** Remove avoidable subscription-level head-of-line blocking without
changing synchronization ownership, completeness or final result semantics.

**Architecture:** A provider-keyed bounded dispatcher serves preparation and
discovery. A provider-local completion callback refreshes and enqueues its regions;
the account task seals only when the entire producer has drained.

**Tech Stack:** Go 1.24, existing model/task framework, durable MySQL sync queue.

## Tasks

- [x] Establish baseline:
  `go test ./pkg/compute/models ./pkg/compute/tasks/cloudaccount ./pkg/compute/syncqueue ./pkg/compute/syncworker`
- [x] Add regression tests in `pkg/compute/models/cloudaccount_provider_work_test.go`
  for slow-provider isolation, bounded parallelism, same-provider exclusion,
  exactly-once ready callbacks, aggregated errors and cancel/drain. First run
  must fail before `runCloudProviderWork` exists; then implement the scheduler
  in `cloudaccount_provider_work.go` and rerun.
- [x] Add preparation tests, then wire the bounded dispatcher into
  `cloudaccounts.go`; add the default-4 option in `options/options.go`.
  Retain failed-provider status and carry errors through account planning.
- [x] Add discovery orchestration tests, then replace semaphore/mutex scheduling
  in `cloudaccount_region_discover.go`. Retain the SDK completion wait.
- [x] Add streaming-planning tests, then update `cloudsync_queue.go`,
  `cloudaccount_sync_policy.go`, and account task entry points. Provider-local
  refresh must precede enqueue, preserve that provider's active jobs, and use the
  original run ID and normalized range. No callback for canceled pending work.
- [x] Verify existing task sealing and queue deduplication regressions; add
  coverage for preparation errors surviving through planning.
- [x] Run gofmt, targeted tests, race tests, package regressions, and
  `git diff --check`. Review all changed call sites for lock ordering, late
  callbacks, missing providers and changed legacy behavior.
- [x] Transfer only this patch to the original feature workspace after verifying
  original source files have not changed. Record actual verification results;
  leave unrelated untracked files untouched.

## Verification notes

- Baseline model, account task, queue and worker package tests passed.
- Regression RED runs confirmed the missing dispatcher, preparation/discovery
  completion hooks, planner and provider-wide queue query APIs.
- A further RED test exposed shared etcd lock ownership. Providers now use
  distinct child contexts while preserving one context per provider lifetime.
- Package regressions and `go test -race` passed for models, account tasks,
  syncqueue and syncworker. The race run used a disposable local MySQL database.
- All syncqueue MySQL integration tests passed, including duplicate enqueue/run
  membership, provider dependencies, active region scope, retry and retention.
  MySQL was initialized in a fresh `/tmp` directory, with networking disabled;
  no application database or cloud API was used.
- Taskman and compute service package tests passed. Both `cmd/region` and
  `cmd/sync-worker` built successfully to temporary files outside the repository.
- Reviewed refresh-before-enqueue ordering and account lock coverage; scoped
  refresh checks every active job for its provider, not only provider-scope jobs.
- Existing timeout drain and full discovery selection remain unchanged. No
  production throughput measurements or deployment were performed.

Operational note: `cloud_account_region_prepare_concurrency` defaults to 4 per
account; setting it to 1 restores serial preparation. Independent streaming still
uses the existing account-routing switch and discovery concurrency setting.

Final workspace verification: the 16-file patch was copied to
`/opt/tcl/cmp/cloudpods` after confirming its HEAD matched and tracked files were
unchanged. All six relevant packages passed `go test -race -count=1` there,
including MySQL queue integration tests; scheduler/planner regressions passed 20
repetitions. `git diff --check` passed. The disposable MySQL server was stopped
and its temporary data removed. Changes remain uncommitted; no deployment.
