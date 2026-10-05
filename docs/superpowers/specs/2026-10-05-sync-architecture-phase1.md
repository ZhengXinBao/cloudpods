# Cloud synchronization architecture: phase 1

## Approved direction

Preserve MySQL queue + etcd ownership, all-resource completion semantics, and
the current resource scope. Improve incrementally rather than add workers or
replace the queue. The user approved execution on 2026-10-05.

The first independently releasable change is phase timing on top of 58ad936cb9
(bounded provider preparation and streaming discovery/planning). That commit
has not yet been verified in the deployed v19 image.

## Boundaries

- No queue schema migration, scheduling changes, concurrency changes, retry
  policy changes, new execution timeout, or deletion/reconciliation changes.
- Do not drop optional/slow resources or reinterpret a failed run as success.
- Do not release ownership on timeout while an SDK operation is still running.
- Do not change SDK/lock contexts simply to attach diagnostic metadata.
- Do not log credentials, request bodies, raw errors, resource inventories,
  sync range JSON, or entire job objects in the new diagnostic events.
- Do not alter unrelated untracked files in the original worktree.

## Diagnostic contract

Emit `cloud_sync_trace` JSON log events with schema version 1, UTC timestamp,
phase, event kind, IDs, attempt/version, duration in milliseconds and error class.
Start/end pairs expose unfinished operations. Duration uses a monotonic clock.
Negative or absent wall-clock ages must not be reported as real queue latency.

Record account admission and worker-pool wait separately from account preparation;
record per-provider preparation, per-region discovery (including SDK drain),
per-provider planning and per-job enqueue. Enqueue events link run IDs to job IDs;
a deduplicated job may belong to multiple runs, so workers must not invent one
authoritative run ID.

Worker records claim transaction latency for successful claims/errors, actual
waiting/retry eligibility-to-claim time, configured retry backoff, expired-lease
recovery wait, job age at claim, attempt duration, lock acquisition, ownership verification, resource
execution and completion persistence. Empty polling must remain quiet.
Job age includes past attempts/backoff and is NOT queue wait or execution time.
Use an in-memory pre-claim timing snapshot with existing DB timestamps; do not
add persisted fields or confuse a reclaimed running job with an ordinary wait.

Reuse existing per-resource request/reconciliation timers. They cover only
instrumented calls; reconciliation time is not pure database-server time.
Do not sum overlapping parent/child phases as wall-clock duration.
An attempt's successful execution is not proof of successful durable completion.

## Validation

Test log safety, context propagation, independent sibling identities, nonnegative
durations, failed/canceled/panicking operations, retry-attempt correlation,
completion failure, empty-poll silence, provider preparation and planner hooks,
resource timing conversion and ownership/drain regressions.
Run package tests, race tests, build region and sync-worker, and check the diff.
Use only a disposable local database for destructive queue integration tests.

## Release and subsequent phases

Resolve an authorized test release channel before uploading or restarting.
Keep the current image/config as rollback inputs, drain workers using their
existing ownership protocol, and verify deployed digests, service health,
diagnostic events and unchanged resource completeness before expanding.
If no release channel is available, deliver tested local commits/artifacts and
report the blocker explicitly; never call local builds deployed.

After measuring the baseline, separately implement/review:
1. Fully pipelined provider prepare → discover → enqueue.
2. True prerequisite dependencies, separating independent global resources.
3. Measured slow resource-group splits and completed-scope checkpoints.
4. Structured error causes, safe retry budgets and cancellation.
5. Capacity/quota-aware account admission replacing fixed two-minute staggering.

These later stages are not included in the phase-1 completion claim.
