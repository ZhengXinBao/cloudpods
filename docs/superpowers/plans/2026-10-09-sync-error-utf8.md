# Sync Error UTF-8 Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans for this tightly coupled fix; request independent review before committing application changes. Steps use checkbox syntax for tracking.

**Goal:** Prevent malformed diagnostic UTF-8 from making queue completion fail with MySQL 1366, without changing execution outcomes or ownership protection.

**Architecture:** Sanitize display strings with strings.ToValidUTF8, then keep complete rune boundaries within byte budgets. A shared syncqueue helper supports collector/summary head-tail retention; a private prefix helper guards Complete. Error classification continues to inspect the original error.

**Tech Stack:** Go 1.24, standard library, existing go-sql-driver/mysql, disposable local MySQL via Unix socket only.

**Workspace:** `/Users/janetyoun/.config/superpowers/worktrees/cloudpods/region-sync-observability` (existing isolated branch `feat/sync-one-hour-stage1`). All commands run here. No remote commands or push.

---

### Task 1: Reproduce the three real boundaries (RED)

Files to create:
- `pkg/compute/syncqueue/errors_utf8_test.go`: NewErrorSummary and helper text properties.
- `pkg/compute/syncqueue/completion_utf8_test.go`: real MySQL Complete regression, isolated DSN opt-in only.
- `pkg/compute/models/cloudsync_utf8_test.go`: bounded collector and actual compare.SyncResult → SSyncResultSet.Add → aggregation/Merge.

- [x] Add table tests for ASCII, Chinese, 2-/4-byte runes, exact limits, long input, invalid short/exact-limit strings, invalid byte runs and incomplete trailing characters. Assert valid UTF-8, byte budgets, unchanged valid short strings, preserved head/tail, stable counts and idempotence. The minimal production regression is:

```go
message := strings.Repeat("错误", 1500)
err := NewErrorSummary(message, map[ErrorClass]int{ErrorClassPermission: 2})
if !utf8.ValidString(err.Error()) || len(err.Error()) > 8192 {
    t.Fatalf("invalid/big summary: bytes=%d valid=%v", len(err.Error()), utf8.ValidString(err.Error()))
}
```

- [x] Initialize a new `/tmp/cmp-sync-utf8-mysql.XXXXXX` datadir with `mysqld --no-defaults --initialize-insecure`. Start that instance with `--skip-networking --mysqlx=0 --socket=<new-dir>/mysql.sock`; create dedicated `syncqueue_utf8_test` database with utf8mb4. Use only this socket's DSN, never a business database. Record directory under `build/sync-error-utf8/mysql-test-root`.
- [x] MySQL tests call real Enqueue/Claim/Complete/Get. Cases cover long valid text, short invalid bytes, invalid bytes at the exact 8192 limit, expanding replacements, typed permanent/unknown/transient errors, aggregated errors, and nil success. Check LastError encoding and exact text, state, ErrorClass, Attempts, Version, cleared WorkerID/LeaseUntil and active_key retention only for Retry. Retry must eventually fail at maxAttempts; stale owner/expired lease must return ErrLeaseLost without updates; closed DB must return a real SQL error.
- [x] Run `go test -mod=vendor -count=1 ./pkg/compute/syncqueue -run 'Test(NewErrorSummaryUTF8|MySQLCompleteUTF8)' -v` with the isolated DSN. Expect malformed UTF-8 and/or MySQL 1366 failures.
- [x] Run `go test -mod=vendor -count=1 ./pkg/compute/models -run 'Test(BoundedCloudSyncMessageUTF8|CloudSyncUTF8)' -v`. Expect encoding/byte-limit failures, not compile failures. Save RED evidence locally.

### Task 2: Minimal shared boundary fix (GREEN)

Files:
- Create `pkg/compute/syncqueue/error_message.go` (display-only helpers).
- Modify `pkg/compute/models/cloudsync_errors.go`, `pkg/compute/syncqueue/errors.go`, `pkg/compute/syncqueue/queue.go` (only the existing truncation blocks).

- [x] Implement the following shape (prefix/suffix operations receive sanitized strings):

```go
func BoundedErrorMessage(message string, maxBytes int) string {
    message = strings.ToValidUTF8(message, "\uFFFD")
    if maxBytes <= 0 { return "" }
    if len(message) <= maxBytes { return strings.Clone(message) }
    if maxBytes < 3 { return strings.Clone(utf8Prefix(message, maxBytes)) }
    budget := maxBytes - 3
    head := budget / 2
    return utf8Prefix(message, head) + "..." + utf8Suffix(message, budget-head)
}
func boundedErrorPrefix(message string, maxBytes int) string {
    message = strings.ToValidUTF8(message, "\uFFFD")
    if maxBytes <= 0 { return "" }
    return strings.Clone(utf8Prefix(message, maxBytes))
}
func utf8Prefix(message string, maxBytes int) string {
    if len(message) <= maxBytes { return message }
    end := maxBytes
    for end > 0 && !utf8.RuneStart(message[end]) { end-- }
    return message[:end]
}
func utf8Suffix(message string, maxBytes int) string {
    if len(message) <= maxBytes { return message }
    start := len(message) - maxBytes
    for start < len(message) && !utf8.RuneStart(message[start]) { start++ }
    return message[start:]
}
```

- [x] Replace collector implementation with `return syncqueue.BoundedErrorMessage(message, 512)` and NewErrorSummary initialization with `message = BoundedErrorMessage(message, 8192)`.
- [x] Replace Complete's rendering/truncation with `message = boundedErrorPrefix(executionErr.Error(), 8192)`; retain `ClassifyError(executionErr)` and all state/owner/SQL logic unchanged.
- [x] Add focused helper boundary/fuzz property coverage, run gofmt, rerun RED commands expecting PASS. Re-run the original `build/release-sync-hour1-28cb487189/full-sync-20261009-manual/utf8_diagnostic.go` expecting valid summary and <=8192 bytes.

### Task 3: Verify, review and commit

- [x] Run with isolated MySQL DSN: `go test -mod=vendor -race -count=1 -json ./pkg/compute/syncqueue ./pkg/compute/synctrace ./pkg/compute/syncworker ./pkg/compute/models ./pkg/compute/tasks/cloudaccount ./pkg/compute/service`, saving JSON output. Confirm all six packages pass, record skip reasons (etcd when no isolated endpoint is configured).
- [x] Build `go build -mod=vendor -o build/sync-error-utf8/region ./cmd/region` and `go build -mod=vendor -o build/sync-error-utf8/sync-worker ./cmd/sync-worker`.
- [x] Independent review of spec compliance and code correctness (no remote operations); address actionable issues and rerun relevant tests. Keep helper changes local to diagnostic text. Preparation task parameters via Error2TaskData remain explicitly outside scope.
- [x] Shut down only the newly created MySQL socket with mysqladmin; verify it is stopped. Preserve evidence, do not remove unrelated artifacts.
- [x] Update this checklist with actual results. `git diff --check`; precisely stage the three production files, helper, three tests and plan. Commit as `fix(compute): preserve UTF-8 in sync error diagnostics`. Do not push origin, modify vendor, deploy, or start another full sync.

## Review correction

Independent review found that strings.ToValidUTF8 may retain an input-sized buffer after a long invalid run collapses to one replacement character. A 16 MiB input regression retained 16,798,112 heap bytes before the correction. Clone the short sanitized result as well as prefixes; this preserves text while bounding retained storage. The new GC-based regression allows 8 MiB of runtime noise and retains only the returned summary. Re-run race and builds after this correction before commit.

## Final verification (2026-10-09)

- The final six-package race run passed 525 test cases (including subtests), with no failures. Two etcd integration tests were explicitly skipped because no isolated etcd endpoint was configured.
- The real MySQL UTF-8 regression passed all 14 subcases, including failed/retry/success, raw/merged errors, exact byte limits, stale owners, expired leases and visible SQL errors. Existing queue/worker integration tests also passed.
- Final fuzz validation passed 17,934 executions; the memory-retention regression passed 10 consecutive race-enabled runs.
- region and sync-worker both built successfully for the local darwin/arm64 host. No release image was built or deployed.
- Original 9,000-byte Chinese reproduction now returns valid UTF-8 of 8,190 bytes.
- Independent spec and quality review passed after the retained-buffer correction.
- The new temporary MySQL instance was shut down through its exact Unix socket and shutdown was verified. Evidence is retained under `build/sync-error-utf8/` (not staged).
- This is a local code fix and commit only: no push, remote deployment, cloud configuration changes, or full-sync retrigger. One-hour sync performance remains unverified.
