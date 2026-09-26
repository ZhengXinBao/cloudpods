package syncqueue

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

func TestPurgeFinishedAndActiveAccountJobs(t *testing.T) {
	dsn := os.Getenv("SYNCQUEUE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("disposable MySQL DSN required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	q := New(db)
	for _, table := range []string{"sync_queue_run_memberships", "sync_queue_jobs", "sync_queue_resources", "sync_queue_schema"} {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	enqueue := func(run, account, region string) string {
		id, err := q.Enqueue(ctx, Request{RunID: run, AccountID: account, ProviderID: "p-" + account, RegionID: region, RangeJSON: `{}`})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	finish := func(id string, age time.Duration) {
		_, err := db.Exec("UPDATE sync_queue_jobs SET state='succeeded',active_key=NULL,available_at=TIMESTAMPADD(MICROSECOND,-?,UTC_TIMESTAMP(6)) WHERE id=?", age.Microseconds(), id)
		if err != nil {
			t.Fatal(err)
		}
	}
	old := 10 * 24 * time.Hour
	protectedJob := enqueue("keep-run", "a", "r1")
	purgeableJob := enqueue("old-run", "a", "r2")
	recentJob := enqueue("recent-run", "a", "r3")
	waitingJob := enqueue("live-run", "b", "r1")
	finish(protectedJob, old)
	finish(purgeableJob, old)
	finish(recentJob, time.Hour)

	if n, err := q.ActiveAccountJobs(ctx, "a"); err != nil || n != 0 {
		t.Fatalf("account with only finished jobs: n=%d err=%v", n, err)
	}
	if n, err := q.ActiveAccountJobs(ctx, "b"); err != nil || n != 1 {
		t.Fatalf("account with waiting job: n=%d err=%v", n, err)
	}
	if _, err := q.ActiveAccountJobs(ctx, ""); err == nil {
		t.Fatal("empty account accepted")
	}

	n, err := q.PurgeFinished(ctx, 7*24*time.Hour, 1000, []string{"keep-run"})
	if err != nil || n != 1 {
		t.Fatalf("purge: n=%d err=%v", n, err)
	}
	exists := func(id string) bool {
		var c int
		if err := db.QueryRow("SELECT COUNT(*) FROM sync_queue_jobs WHERE id=?", id).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c == 1
	}
	if exists(purgeableJob) {
		t.Fatal("old unprotected job survived")
	}
	for name, id := range map[string]string{"protected": protectedJob, "recent": recentJob, "waiting": waitingJob} {
		if !exists(id) {
			t.Fatalf("%s job purged", name)
		}
	}
	var orphans int
	if err := db.QueryRow("SELECT COUNT(*) FROM sync_queue_run_memberships WHERE job_id=?", purgeableJob).Scan(&orphans); err != nil || orphans != 0 {
		t.Fatalf("membership of purged job left: %d err=%v", orphans, err)
	}
	stats, err := q.StatsRun(ctx, "keep-run")
	if err != nil || stats[Succeeded] != 1 {
		t.Fatalf("protected run lost progress: %v err=%v", stats, err)
	}

	if n, err := q.PurgeFinished(ctx, 7*24*time.Hour, 1000, nil); err != nil || n != 1 {
		t.Fatalf("unprotected second pass: n=%d err=%v", n, err)
	}
	if _, err := q.PurgeFinished(ctx, 0, 1000, nil); err == nil {
		t.Fatal("zero retention accepted")
	}
}
