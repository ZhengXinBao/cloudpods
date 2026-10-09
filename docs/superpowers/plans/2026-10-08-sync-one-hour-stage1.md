# One-hour full sync, stage 1 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Follow test-first steps and keep independent file ownership.

**Goal:** Deliver core-stage diagnostics, one-snapshot Bucket reconciliation, and structured retry classification without claiming the 60-minute objective is achieved.

**Architecture:** Preserve the existing MySQL queue, etcd ownership, provider-region collection locks and resource scope. Keep remote property acquisition separate from object updates. Classify real causes before bounded error formatting, without reducing retry limits.

**Tech Stack:** Go 1.24, vendored cloudmux, MySQL, etcd, existing synctrace.

**Workspace:** `/Users/janetyoun/.config/superpowers/worktrees/cloudpods/region-sync-observability` (reused clean worktree, branch `feat/sync-one-hour-stage1`, base `e6c53871eb`). The desktop worktree tool cannot create a worktree for `/opt/tcl/cmp` because that parent is not a Git repository; no new checkout was created.

**Approved spec:** `/opt/tcl/cmp/cloudpods/docs/superpowers/specs/2026-10-08-sync-one-hour-stage1-design.md`. User approved the written specification in this turn. The initial implementation was local-only. On 2026-10-09 the user approved the dependency root fix and requested deployment to the existing JumpServer-managed test target, 10.20.30.43. Deployment still requires fresh verification and an idle/drain gate; a new full sync is not implied.

## Execution status — 2026-10-09

- A accepted and locally committed: `f79c6f1933`.
- B1 accepted and locally committed: `e0d2dc2e3c`.
- C root-fix implementation passed independent specification and quality reviews, including real AWS SDK 503/AllError/Add/collector paths. The implementation is ready for the explicit release gate.
- Historical root cause (now fixed): production `SSyncResultSet.Add` invokes `res.Error(result.AllError())`, losing types and error boundaries before the final `syncResultSetError`, which formats again. Direct-model tests did not cover this actual Add path.
- Approved scope adjustment: `forks/pkg` now adds a mutex-protected defensive-copy `Errors()` accessor. The model Add and final result paths consume actual causes and retain bounded class summaries before formatting. The one changed dependency source is mirrored in vendor and pinned to published fork commit `fe0b3d3bb1c997677806dd25ed72b9200cec6232`, module `v1.10.4-0.20261009033653-fe0b3d3bb1c9`, with verified go.sum entries. No broad vendor refresh.
- Final pinned six-package race run passed: 223 top-level tests, 444 passing events including subtests, zero failures; five isolated MySQL integration tests passed. Two etcd tests were skipped. Release builds run after the cloudpods commit and are recorded in the release evidence.
- The dedicated Unix-socket MySQL server was shut down after the final root-fix verification; its pid file was confirmed absent. Existing source, logs and compiler artifacts are preserved; deployment progress is recorded separately.
- Evidence: `build/sync-one-hour-stage1/VERIFICATION.md`, `repro-aws-classification.go`, `repro-aws-classification.log`.

## Work sequence and ownership

- [x] A: Instrument core stage boundaries; implementer owns only trace/model stage files.
- [x] B1: Single-object Bucket snapshot and honest unknown statistics; controller owns only Bucket files.
- [x] C: Structured classification and bounded cause summaries; separate implementer owns error files.
- [x] Independent spec review, then code quality review, with repairs before completion.
- [ ] Fresh package/race tests and region/sync-worker builds, record skipped external integration tests.
- [ ] Commit exact file lists; preserve existing untracked build/docs in the primary checkout.

B2 (cross-Bucket concurrency), remote canary measurements, Tencent hotspot-specific splits, Azure discovery and durable checkpoints remain separate patches. B1 must be validated before enabling more parallel requests. No config concurrency changes in this patch.

## A. Core execution spans

**Files (relative to the absolute workspace above):**
- Modify `pkg/compute/models/cloudproviderregions.go`, `cloudsync.go`, `cloudsync_trace.go`.
- Test `pkg/compute/models/cloudsync_trace_test.go`.
- If a payload-free observation primitive is needed, modify `pkg/compute/synctrace/trace.go` and test `trace_test.go`.

- [x] Write failure tests for a new disabled-aware model trace wrapper that executes the closure once, preserves the original context captured by the closure, preserves the exact error and panic, and emits no new events when tracing is disabled. Start with:

```go
func TestCoreStagePreservesError(t *testing.T) {
    capture := &syncTraceCapture{}
    ctx := capture.context()
    want := errors.New("private payload must not appear in trace")
    calls := 0
    got := traceCloudSyncStage(ctx, "core_driver", func() error {
        calls++
        return want
    })
    if got != want || calls != 1 { t.Fatalf("got=%v calls=%d", got, calls) }
    events := capture.ends("core_driver")
    if len(events) != 1 || events[0].Outcome != "error" { t.Fatalf("%+v", events) }
}
```

- [x] Run `go test -mod=vendor ./pkg/compute/models -run 'TestCoreStage' -count=1`; verify failure is the absent wrapper/behavior, not environment setup.
- [x] Implement the error-returning wrapper with the existing trace primitive:

```go
func traceCloudSyncStage(ctx context.Context, phase string, work func() error) error {
    if !synctrace.Enabled(ctx) { return work() }
    return synctrace.Measure(ctx, phase, work)
}
```

- [x] For work which reports errors through result sets/logs rather than return values, add a neutral start/cost observation primitive. It must NOT emit `outcome=ok`. Test success-as-observation, panic propagation, disabled tracing and exactly-one execution before implementation. Use monotonic duration and preserve the original panic; reuse the existing payload-free emitter.
- [x] Wrap the real driver/region lookups and zone/SKU/storage/host/snapshot/image call sites in closures. Capture the original ctx, outputs and errors; do not substitute a trace-only context into SDK/locks. Error-returning driver/region calls use `traceCloudSyncStage`; result-set/void routines use the neutral observation wrapper. Example integration shape:

```go
var driver cloudprovider.ICloudProvider
err = traceCloudSyncStage(ctx, "core_driver", func() error {
    var fetchErr error
    driver, fetchErr = provider.GetProvider(ctx)
    return fetchErr
})
```

- [x] Add a source-wiring regression test covering all selected boundaries, supplementing behavioral helper tests, so wrappers cannot be implemented without actually instrumenting DoSync. No per-object unrestricted logging.
- [x] Run `go test -mod=vendor -race ./pkg/compute/synctrace ./pkg/compute/models -count=1`; review behavior and exact changed-file set.

## B1. Bucket snapshot

**Files:**
- Modify `pkg/compute/models/buckets.go`.
- Create `pkg/compute/models/cloudsync_bucket_snapshot.go` and `cloudsync_bucket_snapshot_test.go`.

- [x] Add a fake cloud bucket embedding `cloudprovider.ICloudBucket`; implement only the required property getters and count calls. Snapshot helper contract is `captureCloudBucketSnapshot(ext cloudprovider.ICloudBucket, statsOnly bool) cloudBucketSnapshot`; its `apply(*SBucket)` mutates only known fields, its `statsError() error` reports unavailable statistics without raw payload. Tests cover one stats call, known zero, independently unknown size/count, created time, statsOnly and no remote getters during apply.

```go
func TestBucketSnapshotKeepsUnknownStats(t *testing.T) {
    remote := &snapshotTestBucket{stats: cloudprovider.SBucketStats{SizeBytes:-1, ObjectCount:0}}
    snapshot := captureCloudBucketSnapshot(remote, true)
    local := SBucket{SizeBytes:91, ObjectCnt:7}
    snapshot.apply(&local)
    if local.SizeBytes != 91 || local.ObjectCnt != 0 { t.Fatalf("%+v", local) }
    if snapshot.statsError() == nil || remote.statsCalls != 1 { t.Fatal("unknown stats lost or queried twice") }
}
```

- [x] Run `go test -mod=vendor ./pkg/compute/models -run TestBucketSnapshot -count=1` and record RED.
- [x] Implement a value snapshot with stats, createdAt, statsOnly, limit/support, ACL, location, storage class and access URLs. Copy URL slices when capturing. `apply` updates a numeric statistic only when it is nonnegative, preserving known old values otherwise. Preserve `statsOnly` behavior (statistics + nonzero created time only). `statsError` returns a bounded generic error when either statistic is negative. No global cache and no extra SDK queries.
- [x] In private `syncWithCloudBucket`, capture before `db.UpdateWithLock` and apply the snapshot inside the callback. After a successful update, use this update's `size_bytes`/`object_cnt` diff keys for statistics events, not values refreshed from the database. Preserve metadata/project/share/notify operations and return the standalone incomplete-statistics sentinel. `syncBuckets` calls this strict path; public `SyncWithCloudBucket` ignores only that standalone sentinel to preserve otherwise successful create/ACL/status operations. Wrapped, joined and database errors must not be suppressed.
- [x] In `newFromCloudBucket`, apply the snapshot to the new object (zero defaults are retained for unknown numeric fields); preserve identity/name/insert/metadata/project/share/event operations. Return the inserted object together with the stats error after insertion and notifications. Do not silently discard its identity or count the object as completely synchronized.
- [x] Add source-level regression checks for both integration paths: exactly one snapshot capture, none of the remote getters inside UpdateWithLock, no second GetStats in update/event comparison. Unit-test unknown new-object defaults and no fabricated stats change for an unchanged object.
- [x] Run the complete models tests and race tests. No collection-lock removal, deletion diff changes, configuration switches or vendor refresh in B1.

## C. Retry classification and bounded structured cause retention

**Files:**
- Modify `pkg/compute/syncqueue/errors.go`, test `errors_sdk_test.go` (additional focused error test file allowed).
- Modify `pkg/compute/models/cloudsync_errors.go`, test `cloudsync_errors_test.go`.
- Preserve `pkg/compute/syncqueue/queue.go` retry count/delay and ownership predicate.

- [x] Tests first: context deadline/net timeout versus JSON `timeout: 30` data; wrapped SDK error-code interfaces versus misleading body; wrapped `Cause()`/`Unwrap()`/aggregate children; multiple errors with stable precedence; classification beyond the eighth stored error; large-message truncation preserving class; missing relation and duplicate text remain retryable unknown.

```go
func TestTimeoutParameterIsNotTimeout(t *testing.T) {
    err := errors.New(`decode field: {"timeout":30,"name":"broken"}`)
    if got := ClassifyError(err); got != ErrorClassUnknown { t.Fatalf("got %s", got) }
}
```

- [x] Run queue/model focused tests RED. Classifier must inspect real causes/code interfaces before textual compatibility fallback. The text fallback must not scan JSON payload keys to infer permanent failures or timeout. Maintain existing supported serialized error tokens through bounded exact token/phrase matching.
- [x] Add a bounded class-summary error wrapper shared via an interface rather than retained raw provider objects. Summary retains counts/classes, the existing bounded error text, and deterministic classification. Capture error arguments before `fmt.Sprintf`; string-only errors use conservative fallback. Do not increase raw log payloads.
- [x] `syncResultSetError` must collect all failing resource results in deterministic resource-name order rather than return a map's first error. Preserve non-nil failure if error counters exist without error objects. Class summaries must include every class even when message collection is truncated.
- [x] Mixed failures remain failures. Deterministic precedence keeps authentication/permission/region-disabled/unsupported visible, then throttle/timeout/temporary/unknown. Do not turn unknown into permanent. Do not mark unsupported as N/A. Preserve the existing remote maxAttempts=8, source default=3, and default delay=60s; these are distinct settings, none changed by this patch.
- [x] Run `go test -mod=vendor -race ./pkg/compute/syncqueue ./pkg/compute/synctrace ./pkg/compute/syncworker ./pkg/compute/models -count=1` without production DSNs; add regression tests for existing queue Retryable policy.

## Final integration checks

- [x] Spec review all implemented acceptance conditions; explicitly exclude B2 and remote performance validation from completion claims.
- [x] Quality review error propagation, stale stats, event correctness, SDK getter placement, context/lock identity, panic/drain, class precedence and truncated error paths.
- [ ] Run fresh tests with `env -u SYNCQUEUE_TEST_MYSQL_DSN -u SYNCWORKER_TEST_ETCD_ENDPOINTS go test -mod=vendor -race -p 2 -count=1 -json ./pkg/compute/syncqueue ./pkg/compute/synctrace ./pkg/compute/syncworker ./pkg/compute/models ./pkg/compute/tasks/cloudaccount ./pkg/compute/service` and save evidence. Explicitly enumerate MySQL/etcd skips.
- [ ] Build `go build -mod=vendor -o build/sync-one-hour-stage1/region ./cmd/region` and equivalent `./cmd/sync-worker`; target linux/amd64 separately if the local toolchain permits, reporting exact results.
- [ ] `git diff --check`, inspect the exact diff, commit only plan/spec and changed Go files. Initial no-publish scope is superseded only by the explicit 2026-10-09 dependency-publication and test-host deployment approval; do not trigger a new full cloud synchronization.
- [ ] Deliver tested local changes and limitations; the one-hour objective requires subsequent work and live full-scope measurements.

## Review-driven corrections

- B1: Reproduced nil/empty URL normalization (`null` versus `[]`); use `slices.Clone` to preserve representation.
- B1: Keep full inventory reconciliation strict, but do not turn optional-statistics refresh into create/ACL/status failure. A standalone sentinel and explicit private/public entry points preserve that boundary.
- B1: SQLAlchemy reloads after writing but does not fetch before diffing. Events therefore use this write's statistics diff keys, not comparisons against refreshed fields.
- C: A malformed JSON fragment in vendor-flattened errors can hide another message depending on map order. Treat ambiguous malformed unstructured text consistently as unknown, without guessing original boundaries or losing structured collector classifications.
- C: Preserve compatibility for all serialized 500–599 status codes, not only common 500/502/503/504 examples.
- C: Test actual Tencent and Alibaba SDK error types and serialized formats, including Alibaba `OriginError()` and `HttpStatus()`; match only exact supported labels/full codes rather than exempting arbitrary brackets.
- C: SDK `Message`, `Recommend`, `RequestId`, and nested diagnostic labels must not override the code or convert a transient error into a permanent one after `AllError`/`Merge`. Read the recognized SDK code only; keep unescaped-semicolon SDK mixtures unknown because their boundaries are irrecoverable. Separate resource results, collectors, and real `Join` causes retain all independently known classifications. An SDK status lost during serialization is not inferred from body text.

## Approved root-cause correction and release gate

- Add `util/compare/syncresults_errors_test.go` in `/opt/tcl/cmp/forks/pkg`: typed identity, defensive list copy, append isolation and concurrent snapshot race tests. RED preceded the eight-line accessor implementation.
- Add `pkg/compute/models/cloudsync_causes_test.go`: actual `compare.SyncResult → SSyncResultSet.Add → syncResultSetError → Merge`, AWS v1/v2/HTTP status/OrigErr(s), Alibaba/Tencent, mixed classes beyond stored message #8 and bounded display. These tests reproduced the old failure before the implementation.
- Capture class summaries in Add and consume retained causes again at the final result boundary. Neither classification step may use `AllError()`.
- Prefer AWS `OrigErrs()` over its synthesized `OrigErr()` batch wrapper; retain bounded traversal and count originals once.
- Keep legacy serialized diagnostic tests separately from typed cause tests. Do not use historical text limitations to excuse loss in the new typed path.
- Publish only the minimal dependency fork commit, pin its module version/checksum, verify vendor equality, and run scoped race/MySQL tests plus exact Linux/amd64 release builds.
- Recheck current target image, quiescence and scheduler; stage immutable artifacts via JumpServer native SFTP, verify hashes and images-only compose diff, then canary one worker before remaining workers and region. No hard kill or queue cleanup to force the gate.

### Complete producer-boundary repair

`AllError()` now keeps its historical display/dedup/empty-error behavior while returning a private aggregate with the same defensive `Errors()` accessor. It intentionally does NOT implement `Unwrap` or `Cause`, so existing optional-skip and `errors.Is` behavior does not change. This protects legacy reconciliation-return-to-collector paths throughout the models. Add/final summaries remain bounded and do not retain SDK objects.

The 60 preformatted collector sites in `cloudsync.go` and `cloudsyncelasticcache.go` now pass the original format and error arguments directly. Existing optional warning/skip branches are unchanged. Actual failing Bucket fetch, `AllError → collector → Merge`, and a source guard cover these producer boundaries.
