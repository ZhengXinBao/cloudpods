# Durable sync queue

`New(db)` never creates tables. Run `Migrate(ctx)` as an explicit operator action,
then use `CheckSchema(ctx)` in producers and workers. The MySQL connection needs
`parseTime=true&loc=UTC`. All scheduling and lease comparisons use database UTC.
The schema requires InnoDB and DATETIME(6); integration validation uses MySQL 8.0.

`Enqueue` deduplicates active requests by provider, region and canonical JSON
range. Object key order and whitespace are ignored; numeric spelling and array
order remain significant. Different ranges become durable followups. Successful
or exhausted jobs release their dedup keys so a later run can request the same
range. Optional `Request.RunID` links each request to its account task, including
coalesced requests. It is stored in memberships, not the canonical job payload.
`StatsRun` reports these memberships; account-wide history uses `Stats`/`List`.
Callers must authorize account and admin queries.

Claims serialize through a short transaction gate and a provider-region row.
Only one live lease per provider-region is permitted. `GlobalLimit`,
`AccountLimit`, and `ProviderLimit` count live leases across replicas, defaulting
to 16, 8 and 2 when nonpositive. Set the same values on every worker before use.
Candidate selection skips accounts and providers already at capacity. At most
32 candidate resources are examined per call; workers should poll with jitter.
The gate is a deliberate throughput tradeoff for a low-frequency sync queue.

Every claim, including an expired lease recovery, increments `Attempts` and
`Version`. The worker must check its configured attempt cap before invoking the
cloud driver; it can claim an exhausted job and call `Complete` with an error to
mark it failed. `Heartbeat` and `Complete` reject expired or replaced owners.
Lease fencing protects queue state, not non-cancellable external cloud/database
writes: the worker's execution mutex and fail-closed process termination remain
required. Progress states describe whole jobs, not individual resources.

There is no automatic retention cleanup of jobs, membership rows, or resource
lock rows. Retention tooling is a future operator concern. DDL is additive and
not transactional in MySQL; migration writes its version after creating tables
and can be rerun after interruption. A future incompatible version is rejected.

Run the pure tests with `go test -race ./pkg/compute/syncqueue`. Full tests require
an explicitly disposable database and drop/delete queue tables:

```
SYNCQUEUE_TEST_MYSQL_DSN='root@tcp(127.0.0.1:33367)/syncqueue_test?parseTime=true&loc=UTC' go test -race -v ./pkg/compute/syncqueue
```

Coverage includes active deduplication, durable followups, coalesced run progress,
concurrent ownership, expired-lease replacement, stale heartbeat/completion,
retry exhaustion/delay, successful completion, account isolation, schema version
rejection, and shared replica concurrency limits.
