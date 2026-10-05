# Cloud Sync Phase Timing Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans inline, task-by-task.
> No delegated agents are needed for this tightly coupled first phase.

**Goal:** Make preparation, execution and retry long tails measurable without changing synchronization behavior.

**Architecture:** A small context-scoped JSON event helper is shared by producers
and workers. Keep IDs on logs rather than add high-cardinality metric labels or
database migrations. Preserve existing queue and etcd ownership.

**Tech Stack:** Go 1.24, existing logging, MySQL durable jobs, etcd leases.

## Paths

All paths below are relative to the isolated worktree
`/Users/janetyoun/.config/superpowers/worktrees/cloudpods/region-sync-observability`.

- Create `pkg/compute/synctrace/trace.go` and `trace_test.go`: safe event contract,
  context-local sinks/identity, start/end timing, ages and resource durations.
- Modify `pkg/compute/syncworker/runner.go`, add `trace_test.go`: claim,
  attempt and completion timing without changing heartbeat/drain logic.
- Modify `pkg/compute/service/sync_worker.go`: lock, lease check and execution.
- Modify `pkg/compute/models/cloudaccounts.go`, `cloudaccount_provider_work.go`,
  `cloudaccount_region_discover.go`, `cloudsync_queue.go`: producer phases.
- Modify `pkg/compute/models/cloudproviderregions.go`, add
  `cloudsync_trace.go`/`cloudsync_trace_test.go`: existing resource timers.
- Modify `pkg/compute/tasks/cloudaccount/cloud_account_sync_task.go`: connect
  task/run identity to account preparation.
- Add `docs/cloud-sync-phase-timing.md`: querying and interpretation.

## Tasks

- [ ] Baseline: `go test ./pkg/compute/syncqueue ./pkg/compute/syncworker ./pkg/compute/models ./pkg/compute/tasks/cloudaccount ./pkg/compute/service`.
- [ ] TDD safe tracing helper:
  ```go
  ctx := synctrace.WithIdentity(context.Background(), synctrace.Identity{RunID: "run", AccountID: "account"})
  ctx = synctrace.WithSink(ctx, func(event synctrace.Event) { events = append(events, event) })
  finish := synctrace.Start(ctx, "prepare")
  finish(errors.New("access denied: SECRET"))
  // Assert exactly start/end; class permission; JSON never contains SECRET.
  ```
  Run `go test ./pkg/compute/synctrace` first RED, then implement the helper and
  verify GREEN. Use internal deterministic timestamp helpers for negative/missing
  age and duration tests. A failing diagnostic sink must not fail synchronization.
- [ ] TDD runner integration: use existing fakeStore to assert attempt 2/version 3,
  execution failure and completion failure are separate events; empty polling
  emits nothing; panic and fatal lease failure do not emit false success.
  Run `go test ./pkg/compute/syncworker -run Trace -count=1` RED, add hooks,
  then GREEN. Use `synctrace.WithJob` only at the per-job execution boundary.
  Measure claim with `time.Now()`/`time.Since()`, not job timestamps.
- [ ] TDD producer and resource integration: capture events from
  `prepareAccountProviderRegions` and `planIndependentSyncProviders` using their
  existing fake callbacks. Assert original callback contexts are preserved.
  Check per-resource request/reconcile durations and error outcomes from a
  synthetic `SSyncResultSet`. Run targeted tests RED, then add hooks and GREEN.
  ```
  traceCtx := synctrace.WithIdentity(ctx, identity)
  finish := synctrace.Start(traceCtx, phase)
  defer func() { finish(resultErr) }()
  // Existing work still receives ctx, never traceCtx merely for logging.
  ```
  Add admission/pool wait and account probe/import/prepare phase timing.
  Enqueue logs use the ID returned by queue.Enqueue plus the originating run ID.
  Per-region discovery finishing must remain after the existing SDK drain.
- [ ] Document semantics: claim latency vs job age, overlapping phases, per-attempt
  failures, missing end events, partial request/SQL timer coverage and run-job joins.
- [ ] Verify: `gofmt`, targeted repeated tests, all five baseline packages plus
  synctrace, `go test -race`, disposable MySQL integration if available,
  `git diff --check`, build `cmd/region` and `cmd/sync-worker`.
- [ ] Review hooks for error shadowing, altered context/lock identity, early
  release and leaked payloads. Commit only phase-1 files. Integrate by fast-forward
  only if the original branch is unchanged.
- [ ] Check release route read-only. Build a checksummed Linux test release if
  possible. Do not restart remotely without an authorized transfer and rollout
  path. Report local verification and deployment status separately.

## Verification record

Results are recorded here after commands run; a skipped integration test does not
count as an integration pass.
