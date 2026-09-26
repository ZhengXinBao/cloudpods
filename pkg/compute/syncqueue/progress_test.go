package syncqueue

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestProgressPagination(t *testing.T) {
	for _, tc := range []struct{ offset, limit, wantOffset, wantLimit int }{{-1, 0, 0, 50}, {7, 200, 7, 100}, {3, 20, 3, 20}} {
		o, l := ProgressPagination(tc.offset, tc.limit)
		if o != tc.wantOffset || l != tc.wantLimit {
			t.Fatalf("pagination %d %d", o, l)
		}
	}
}
func TestProgressErrorDoesNotExposeCredentials(t *testing.T) {
	for _, raw := range []string{"password=secret123", "Authorization: Bearer secret123", `request failed {"access_key":"secret123"}`, "https://u:secret123@host/path?token=secret123"} {
		if strings.Contains(PublicError(raw), "secret123") {
			t.Fatal("credential exposed")
		}
	}
	if PublicError("") != "" {
		t.Fatal("empty error")
	}
}
func TestRetryCopiesExactScopeAndRejectsOwnership(t *testing.T) {
	j := Job{Request: Request{AccountID: "a", ProviderID: "p", RegionID: "r", ScopeType: "region", ResourceGroup: "network", RangeJSON: `{"zone":["z"]}`}, State: Failed}
	r, err := RetryRequest(j, "a", "new")
	if err != nil || r.RangeJSON != j.RangeJSON || r.ResourceGroup != j.ResourceGroup || r.RunID != "new" {
		t.Fatal(r, err)
	}
	if _, err := RetryRequest(j, "other", "new"); err == nil {
		t.Fatal("cross-account retry")
	}
	j.State = Succeeded
	if _, err := RetryRequest(j, "a", "new"); err == nil {
		t.Fatal("successful job retry")
	}
}

func TestMySQLRunProgressIsolationAndRetry(t *testing.T) {
	dsn := os.Getenv("SYNCQUEUE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("disposable MySQL DSN required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := New(db)
	ctx := context.Background()
	if err = q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("progress-%d", time.Now().UnixNano())
	run, account := prefix+"run", prefix+"account"
	defer db.Exec("DELETE m FROM sync_queue_run_memberships m JOIN sync_queue_jobs j ON j.id=m.job_id WHERE j.account_id=?", account)
	defer func() {
		db.Exec("DELETE m FROM sync_queue_run_memberships m JOIN sync_queue_jobs j ON j.id=m.job_id WHERE j.account_id=?", account)
		db.Exec("DELETE FROM sync_queue_jobs WHERE account_id=?", account)
	}()
	for i, state := range []string{Waiting, Running, Retry, Succeeded, Failed} {
		r := Request{RunID: run, AccountID: account, ProviderID: prefix + "provider", RegionID: "r", ScopeType: "region", ResourceGroup: fmt.Sprint(i), RangeJSON: `{"region":["r"]}`}
		if i == 0 {
			r.ScopeType = "provider"
			r.RegionID = ""
		}
		id, e := q.Enqueue(ctx, r)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.Exec("UPDATE sync_queue_jobs SET state=?,active_key=NULL WHERE id=?", state, id); e != nil {
			t.Fatal(e)
		}
	}
	_, err = q.Enqueue(ctx, Request{RunID: prefix + "old", AccountID: account, ProviderID: prefix + "other", RegionID: "r", RangeJSON: `{}`})
	if err != nil {
		t.Fatal(err)
	}
	counts, regions, err := q.RunProgressCounts(ctx, account, run)
	if err != nil {
		t.Fatal(err)
	}
	if counts["total"] != 5 || regions["total"] != 1 || regions[Failed] != 1 {
		t.Fatal(counts, regions)
	}
	rows, err := q.RunJobs(ctx, account, run, 2, 2, false)
	if err != nil || len(rows) != 2 {
		t.Fatal(rows, err)
	}
	counts, _, err = q.RunProgressCounts(ctx, "another", run)
	if err != nil || counts["total"] != 0 {
		t.Fatal("account leakage", counts, err)
	}
	failed, err := q.RunJobs(ctx, account, run, 0, 0, true)
	if err != nil || len(failed) != 1 {
		t.Fatal(failed, err)
	}
	req, err := RetryRequest(failed[0], account, prefix+"retry")
	if err != nil {
		t.Fatal(err)
	}
	first, err := q.EnqueueRetryRequest(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.EnqueueRetryRequest(ctx, req)
	if err != nil || first != second || first == failed[0].ID {
		t.Fatal("retry deduplication failed", err)
	}
	if _, err = db.Exec("UPDATE sync_queue_jobs SET state='succeeded',active_key=NULL WHERE id=?", first); err != nil {
		t.Fatal(err)
	}
	replay, err := q.EnqueueRetryRequest(ctx, req)
	if err != nil || replay != first {
		t.Fatal("completed retry copied again after resume", err)
	}
	childCounts, _, err := q.RunProgressCounts(ctx, account, req.RunID)
	if err != nil || childCounts["total"] != 1 {
		t.Fatal("resume duplicated child memberships", childCounts, err)
	}
	original, err := q.Get(ctx, failed[0].ID)
	if err != nil || original.State != Failed {
		t.Fatal("original failure changed", err)
	}
	child, err := q.Get(ctx, first)
	if err != nil || child.RangeJSON != failed[0].RangeJSON || child.ScopeType != failed[0].ScopeType || child.ResourceGroup != failed[0].ResourceGroup {
		t.Fatal("retry scope changed", err)
	}
}

func TestPublicInventoryErrorSummary(t *testing.T) {
	raw := `SDK auth token=secret123: cloud inventory incomplete: missing=12 sample=["i-0123456789abcdef0", "vol-123abc", "lb-javo0c4j", "secret123", "AKIASECRET"] headers Authorization=secret123`
	got := PublicError(raw)
	if !strings.Contains(got, "missing=12") || !strings.Contains(got, "i-0123456789abcdef0") || !strings.Contains(got, "lb-javo0c4j") || strings.Contains(got, "secret123") || strings.Contains(got, "AKIA") || strings.Contains(got, "headers") {
		t.Fatal(got)
	}
	if got := PublicError(`cloud inventory incomplete: missing=2 sample=["https://user:password@host/?token=secret"]`); strings.Contains(got, "password") || strings.Contains(got, "token") {
		t.Fatal(got)
	}
}
