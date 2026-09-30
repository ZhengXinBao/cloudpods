# Region sync preparation throughput

Approved scope: bounded subscription preparation, enqueue each subscription after
its discovery completes, and prevent same-subscription lock waiters from consuming
all discovery slots. Local implementation and tests only; no deployment or cloud sync.

## Design

- Add a provider-keyed bounded dispatcher. At most one operation per provider is
  running; ready providers are scheduled round-robin. Completion callbacks are
  provider-local, run once after all that provider's operations, and use the same
  concurrency budget. Providers with no discovery candidates still complete.
  Each provider gets a distinct child context, reused through its probes and
  completion callback: etcd uses context identity as its reentrant lock owner.
  All child contexts retain cancellation and error collectors from the parent.
- Parallelize subscription region preparation with a new per-account option,
  `cloud_account_region_prepare_concurrency` (default 4). Keep the existing
  region reconciliation locks. Aggregate preparation errors without preventing
  healthy providers from reaching discovery.
- Replace discovery's semaphore-before-provider-mutex arrangement with the
  dispatcher, retaining candidate selection, full discovery scope and serial SDK
  access within each provider. A timed-out SDK probe must still drain before the
  provider can be reused. Cancellation stops new work but drains admitted work.
- Independent account planning refreshes and enqueues one provider when its
  discovery finishes. Refresh only that provider, and preserve its active queue
  scope; another provider's jobs must not suppress its refresh. Account locking
  covers refresh plus enqueue to avoid background refresh races.
- Keep the parent in the preparation stage until all discovery callbacks finish.
  Preserve run membership, queue deduplication, provider dependencies and error
  aggregation; neither an empty queue nor fast completed jobs seals planning.
- Existing legacy discovery callers keep their first-inventory submission policy.
  Provider-scoped requests and queue/worker formats remain unchanged.

## Validation

Use deterministic blocking callbacks to test overlap, concurrency bounds, one
operation per provider, round-robin progress, one completion per provider, empty
providers, cancellation/drain, failed discovery with healthy sibling progress,
refresh-before-enqueue, callback failure collection and run sealing.
Run existing model/task/queue/worker regressions and targeted race tests.
Database-dependent tests require an explicitly disposable database; never use
runtime credentials or a production database.

## Non-goals

No SDK cancellation retrofit, early unlocking, resource-group split, worker
expansion, queue-schema change or claim of measured production speedup.
