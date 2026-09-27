# Independent sync worker implementation plan

**Goal:** Deploy provider-region resource synchronization independently, with a durable MySQL queue, account canary routing, bounded retries and inspectable progress.

**Architecture:** Region remains the authorized producer and account discovery coordinator. Sync jobs carry IDs and serialized sync ranges, never cloud credentials. Workers reuse DoSync. Durable claim/version/heartbeat controls job ownership; an etcd execution mutex and fail-closed worker termination protect non-cancellable legacy drivers. First release supports MySQL and requires etcd locking. Legacy routing is the default.

**Tech stack:** Go, database/sql, existing MySQL and etcd clients, compute models.

## Tasks
- [x] Implement isolated queue package with schema migration, idempotent enqueue, conditional claims, lease heartbeat, retries, completion and progress queries. Test contention, stale completion, retries and isolation using an opt-in disposable MySQL database.
- [x] Add worker engine and tests for claim capacity, heartbeat loss, graceful stop and execution errors. Scope execution locks to provider-region and provider concurrency slots.
- [x] Add producer routing and account progress integration; preserve legacy route by default and fail closed on enqueue errors. Add tests for account selection and scope serialization.
- [x] Add cmd/sync-worker and compute service bootstrap that omits master cron jobs and API serving. Add build target and deployment/runbook documentation.
- [x] Run package tests, race tests and compile checks; review failure/recovery and rollback paths. Record external integration prerequisites honestly.

## Validation
Run `go test ./pkg/compute/syncqueue ./pkg/compute/syncworker`, then relevant model tests and compile `./cmd/sync-worker ./cmd/region`. MySQL tests use an explicitly supplied disposable test DSN; never infer production credentials. Exercise two consumers, duplicate submit, stale heartbeat/completion, retry exhaustion, and progress aggregation. Verify SIGTERM drains and failed renewal terminates before resource work can continue.

## Rollout
Create additive queue tables via an explicit migrate command. Deploy idle workers and enable a small account allowlist on region. Drain before rollback; do not automatically fall back to local execution when queue insertion fails. Existing uncommitted changes are preserved. No live deployment is part of local implementation.

## Delivered validation

- Full tests passed for taskman, compute models, cloudaccount tasks; service and both command packages compiled.
- Real disposable MySQL 8.0.46 queue and two-runner integration tests passed with the race detector.
- Real disposable etcd exclusion, cancellation, release and revocation tests passed with the race detector.
- Linux amd64 static sync-worker binary built at `_output/bin/sync-worker`.
- Independent review found a provider-global error propagation gap; it was fixed and tests rerun.
- No production deployment or real cloud-account performance measurement was performed. Strict resource-write fencing under prolonged process suspension is not implemented; the rollout guide explicitly describes this boundary. Queue history retention is manual.

Existing task IDs serve as persisted SyncRun identifiers rather than adding a duplicate run model. The queue membership table records per-run progress. See `docs/independent-sync-worker.md` for migration, configuration, systemd, inspection and rollback.
