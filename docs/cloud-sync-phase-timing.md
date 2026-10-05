# Cloud sync phase timing (schema 1)

## Scope and safety

This first-stage architecture change adds `cloud_sync_trace` JSON lines to the
existing logs. It builds on the provider preparation/streaming patch 58ad936cb9.
It does **not** change resource selection, success criteria, concurrency, retry
policy, deletion reconciliation, leases or the nightly schedule.

No database migration is needed. The pre-claim timing snapshot is in memory only;
the job-list JSON API is unchanged. New/old worker versions can read the same
queue schema. Compatibility does not justify interrupting a running worker:
use its existing stop-admission/drain protocol when rolling out.

The new events contain only internal IDs, phases, durations and error classes.
They never serialize a job, sync range, request body, credential or raw error.
This statement applies to the new events, not to all pre-existing service logs.
Per-account/resource identifiers are log fields, not Prometheus labels.

## Event fields

- `schema`: 1.
- `at`: UTC emission time.
- `kind`: `start`, `end`, `duration`, `age` or `cost`.
- `phase`: stable phase name from the table below.
- `duration_ms`: monotonic elapsed time for phase execution; database timestamp
  differences for queue/job ages. Start events have duration 0.
- `account_id`, `provider_id`, `region_id`, `resource_group`, `resource`:
  present where that scope is known.
- `run_id`: account task ID on producer events.
- `job_id`, `worker_id`, `attempt`, `version`: worker correlation identity.
- `outcome`: `ok`, `error`, `canceled`; omitted for start and cost observations.
- `error_class`: existing queue classification, without raw error text.

An `ok` phase means that operation returned without an error, **not** that an
account or inventory run succeeded. In particular, account preparation can
collect provider errors while allowing healthy providers to continue.

## Phases and interpretation

| Phase | Meaning |
| --- | --- |
| `account_admission` | Submission to admission decision; may represent coalesced background work, not a new admitted run |
| `account_pool_wait` | Admitted account waiting for the account preparation worker pool |
| `account_prepare` | Full account status/region preparation method, excluding admission/pool wait |
| `account_probe` | Cloud account credential/subscription probe |
| `subscription_import` | Import subscription models; method-return timing, not proof all rows succeeded |
| `provider_prepare` | One enabled provider's preparation callback |
| `discover_plan` | Account discovery and streaming planning, including provider callbacks |
| `region_discover` | One region discovery step, including driver lookup, reconciliation and SDK drain |
| `probe_drain` | A timed-out or unfinished SDK probe still draining before its slot can be released |
| `provider_plan` | Provider-local refresh and enqueue callback |
| `enqueue` | One queue enqueue transaction; records resulting job ID, including deduplicated jobs |
| `claim` | One successful/error claim operation; includes candidate queries and transaction lock wait; empty polls are silent |
| `queue_wait` | Claim timestamp minus pre-claim `available_at` for waiting/retry jobs; includes dependency/capacity waits, excludes configured retry backoff |
| `retry_backoff` | Prior completion to `available_at` on a retry claim; configured delay, not execution duration |
| `lease_recovery_wait` | Expired lease deadline to reclaim; never includes the abandoned execution as queue wait |
| `job_age` | Original job creation to this claim; includes previous attempts/backoff/waits |
| `attempt` | One worker execution callback, including lock, ownership check, execution, failure-display reset and unlock |
| `lock` | Distributed execution-lock acquisition |
| `ownership_check` | Lease validation after obtaining the execution lock |
| `resource_execute` | Model validation and actual provider/region synchronization |
| `resource_request` | Existing accumulated request timer for one resource; `cost`, not a success assertion |
| `resource_reconcile` | Existing accumulated application reconciliation timer; includes more than SQL server time |
| `complete` | Persisting the attempt result/retry decision |
| `heartbeat_failure` | Failed lease renewal only; successful periodic renewals remain quiet |

Resource timer coverage is **partial**. A missing timer does not mean zero API or
SQL work, and these timers cannot diagnose all cloud SDK calls yet. Use measured
slow groups to choose the next operations to instrument/split.

Parent/child phases overlap. Do not sum `attempt + lock + resource_execute`, or
`discover_plan + provider_plan`, and call that wall-clock time. Parallel
providers likewise must not be added together as account duration.

An execution error followed by `complete: ok` means the **failure/retry result was
persisted successfully**. An execution `ok` followed by completion failure is not
a durably successful job. Queue state and run memberships remain authoritative.

Each `(job_id, version, attempt)` distinguishes retries/recovery. Worker events
intentionally have no inferred run ID: a deduplicated job can belong to several
runs. Join producer `enqueue` events or durable `sync_queue_run_memberships`.

Missing end events can mean still-running work, process termination, incomplete
log collection or log rotation. They are not alone proof of a hung SDK.
Unknown/future queue timestamps are omitted rather than shown as negative waits.

## Read a bounded log sample

Use the existing approved log collector, or read a bounded time range from region
and every worker. Do not trigger a second full sync merely to obtain metrics.
After saving that sample locally as `/tmp/cloud-sync-sample.log`:

```sh
sed -n 's/^.*cloud_sync_trace //p' /tmp/cloud-sync-sample.log \
  | jq -c 'select(.schema == 1)' > /tmp/cloud-sync-events.jsonl

# Longest measured operations, not accumulated job age.
jq -s '
  map(select(.kind == "end"))
  | sort_by(.duration_ms) | reverse | .[:30]
  | map({phase, account_id, provider_id, region_id, resource_group,
         job_id, attempt, version, duration_ms, outcome, error_class})
' /tmp/cloud-sync-events.jsonl

# Dependency/capacity wait versus retry delay versus lock wait.
jq -s '
  map(select((.phase == "queue_wait" or .phase == "retry_backoff"
              or .phase == "lease_recovery_wait" or .phase == "lock")
             and .kind != "start"))
  | group_by([.phase, .account_id, .resource_group])
  | map({phase: .[0].phase, account: .[0].account_id,
         group: .[0].resource_group, count: length,
         total_ms: (map(.duration_ms) | add),
         max_ms: (map(.duration_ms) | max)})
' /tmp/cloud-sync-events.jsonl
```

These are sample statistics, not automatically one nightly run. Filter by the
account task's run/job memberships and collect the full time window before
comparing whole rounds. Keep the same 55 accounts, resource scope and success
criteria. Include failed/blocked scopes alongside makespan and p95 attempt times.

## Rollout acceptance

1. Identify the authorized test release route, exact artifacts and current image
   digests/configuration; obtain the existing operational confirmation.
2. Retain rollback artifacts. Use graceful draining and verify old ownership
   ended before introducing replacement executors; do not bypass with SIGKILL.
3. Verify deployed commit/build IDs on region and workers, queue schema unchanged,
   service health and start/end events. A local build is not a deployment.
4. Observe the next ordinary scheduled run; measure preparation, queue wait,
   execution and retry separately, including inventory errors.
5. Only then select the next change: real dependency edges, measured slow-group
   splits or capacity-aware admission. Do not claim an unmeasured speedup.

Current release-channel finding on 2026-10-05: CDP application resolution for
`cloudpods` and `cmp` returned no match; the installed JumpServer CLI has command
execution but no file-upload command. No deployment was performed by this patch.
