package syncqueue

import (
	"context"
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRequestIdentity(t *testing.T) {
	a := Request{AccountID: "a", ProviderID: "p", RegionID: "r", RangeJSON: `{"b":2,"a":1}`}
	b := a
	b.RangeJSON = `{ "a": 1, "b": 2 }`
	ka, da, err := identity(a)
	if err != nil {
		t.Fatal(err)
	}
	kb, db, err := identity(b)
	if err != nil || ka != kb || da != db {
		t.Fatal("equivalent requests must coalesce")
	}
	b.RangeJSON = `{"a":2}`
	kc, dc, _ := identity(b)
	if kc != ka || dc == da {
		t.Fatal("different ranges must share execution lock, not dedup key")
	}
	b.RegionID = "other"
	kd, _, _ := identity(b)
	if kd == ka {
		t.Fatal("regions must be isolated")
	}

	provider := Request{
		AccountID: "a", ProviderID: "p", ScopeType: "provider",
		ResourceGroup: "extended", RangeJSON: `{}`,
	}
	providerKey, providerActive, err := identity(provider)
	if err != nil {
		t.Fatal(err)
	}
	provider.RegionID = "must-be-ignored"
	otherProviderKey, otherProviderActive, err := identity(provider)
	if err != nil || providerKey != otherProviderKey || providerActive != otherProviderActive {
		t.Fatal("provider-scope requests must not depend on a region")
	}

	a.RangeJSON = `no`
	if _, _, err := identity(a); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	a.RangeJSON = `{}`
	a.ProviderID = ""
	if _, _, err := identity(a); err == nil {
		t.Fatal("empty provider accepted")
	}
	a.ProviderID = "p"
	a.RegionID = ""
	if _, _, err := identity(a); err == nil {
		t.Fatal("region scope requires a region")
	}
	a.ScopeType = "unknown"
	if _, _, err := identity(a); err == nil {
		t.Fatal("unknown scope accepted")
	}
}

func TestRequestIdentitySeparatesResourceGroups(t *testing.T) {
	base := Request{AccountID: "a", ProviderID: "p", RegionID: "r", RangeJSON: `{}`}
	core := base
	core.ResourceGroup = "core"
	data := base
	data.ResourceGroup = "database"

	coreResource, coreActive, err := identity(core)
	if err != nil {
		t.Fatal(err)
	}
	dataResource, dataActive, err := identity(data)
	if err != nil {
		t.Fatal(err)
	}
	if coreResource == dataResource || coreActive == dataActive {
		t.Fatal("resource groups must have independent queue identities")
	}
}

func TestErrorClassification(t *testing.T) {
	tests := []struct {
		name      string
		err       string
		class     ErrorClass
		retryable bool
	}{
		{"invalid token", "InvalidClientTokenId: token is invalid", ErrorClassAuth, false},
		{"permission", "AccessDenied: not authorized", ErrorClassPermission, false},
		{"disabled region", "region is not enabled for this account", ErrorClassRegionDisabled, false},
		{"unsupported", "operation not implemented", ErrorClassUnsupported, false},
		{"throttle", "RequestLimitExceeded", ErrorClassThrottle, true},
		{"timeout", "dial tcp: i/o timeout", ErrorClassTimeout, true},
		{"temporary server", "service unavailable", ErrorClassTemporary, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := ClassifyError(errors.New(tt.err))
			if class != tt.class {
				t.Fatalf("class=%q want %q", class, tt.class)
			}
			if Retryable(class) != tt.retryable {
				t.Fatalf("retryable(%q)=%v want %v", class, Retryable(class), tt.retryable)
			}
		})
	}
}

func TestProviderDependencyConditionBlocksActiveProviderJobs(t *testing.T) {
	condition := providerDependencyCondition("candidate")
	for _, fragment := range []string{
		"candidate.scope_type IN ('','region')",
		"dependency.scope_type='provider'",
		"dependency.state IN ('waiting','running','retry')",
		"sync_queue_run_memberships",
	} {
		if !strings.Contains(condition, fragment) {
			t.Fatalf("provider dependency condition missing %q: %s", fragment, condition)
		}
	}
}

// This DSN must designate a disposable database; this test drops queue tables.
func TestMySQLLifecycle(t *testing.T) {
	dsn := os.Getenv("SYNCQUEUE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set SYNCQUEUE_TEST_MYSQL_DSN to a disposable MySQL database (parseTime=true)")
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
	if err := q.CheckSchema(ctx); err == nil {
		t.Fatal("missing schema accepted")
	}
	if err := q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.CheckSchema(ctx); err != nil {
		t.Fatal(err)
	}
	providerID, err := q.Enqueue(ctx, Request{
		RunID:         "dependency-run",
		AccountID:     "dependency-account",
		ProviderID:    "dependency-provider",
		ScopeType:     "provider",
		ResourceGroup: "provider",
		RangeJSON:     `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	regionID, err := q.Enqueue(ctx, Request{
		RunID:      "dependency-run",
		AccountID:  "dependency-account",
		ProviderID: "dependency-provider",
		RegionID:   "dependency-region",
		RangeJSON:  `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	providerJob, err := q.Claim(ctx, "dependency-worker", time.Minute)
	if err != nil || providerJob == nil || providerJob.ID != providerID {
		t.Fatalf("provider dependency must claim first: job=%v err=%v", providerJob, err)
	}
	activeProviders, err := q.ActiveProviderJobCounts(ctx, []string{"dependency-provider", "missing-provider"})
	if err != nil || activeProviders["dependency-provider"] != 1 || activeProviders["missing-provider"] != 0 {
		t.Fatalf("active provider counts: counts=%v err=%v", activeProviders, err)
	}
	if regionJob, err := q.Claim(ctx, "dependency-region-worker", time.Minute); err != nil || regionJob != nil {
		t.Fatalf("region job claimed before provider completed: job=%v err=%v", regionJob, err)
	}
	if err := q.Complete(ctx, providerJob, nil, 3, 0); err != nil {
		t.Fatal(err)
	}
	regionJob, err := q.Claim(ctx, "dependency-region-worker", time.Minute)
	if err != nil || regionJob == nil || regionJob.ID != regionID {
		t.Fatalf("region dependency must claim after provider completion: job=%v err=%v", regionJob, err)
	}
	if err := q.Complete(ctx, regionJob, nil, 3, 0); err != nil {
		t.Fatal(err)
	}
	r := Request{RunID: "run-one", AccountID: "a", ProviderID: "p", RegionID: "r", RangeJSON: `{"x":1}`}
	id, err := q.Enqueue(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	r.RunID = "run-two"
	id2, err := q.Enqueue(ctx, r)
	if err != nil || id != id2 {
		t.Fatalf("duplicate %s %v", id2, err)
	}
	runStats, err := q.StatsRun(ctx, "run-two")
	if err != nil || runStats[Waiting] != 1 {
		t.Fatal("coalesced run membership", runStats, err)
	}
	r.RangeJSON = `{"x":2}`
	follow, err := q.Enqueue(ctx, r)
	if err != nil || follow == id {
		t.Fatal("followup lost", err)
	}
	var wg sync.WaitGroup
	jobs := make(chan *Job, 2)
	errs := make(chan error, 2)
	for _, worker := range []string{"one", "two"} {
		wg.Add(1)
		go func(w string) { defer wg.Done(); j, e := q.Claim(ctx, w, time.Minute); jobs <- j; errs <- e }(worker)
	}
	wg.Wait()
	close(jobs)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var claimed *Job
	n := 0
	for j := range jobs {
		if j != nil {
			claimed = j
			n++
		}
	}
	if n != 1 {
		t.Fatalf("concurrent owners: %d", n)
	}
	stale := *claimed
	if _, err := db.Exec("UPDATE sync_queue_jobs SET lease_until=DATE_SUB(UTC_TIMESTAMP(6), INTERVAL 1 SECOND) WHERE id=?", claimed.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err = q.Claim(ctx, "replacement", time.Minute)
	if err != nil || claimed == nil {
		t.Fatal("reclaim", err)
	}
	if err := q.Heartbeat(ctx, &stale, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("stale heartbeat", err)
	}
	if err := q.Complete(ctx, &stale, nil, 3, 0); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("stale completion", err)
	}
	if err := q.Complete(ctx, claimed, errors.New("failure"), 3, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		j, e := q.Claim(ctx, "last", time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if j == nil {
			break
		}
		if e = q.Complete(ctx, j, errors.New("failure"), 3, 0); e != nil {
			t.Fatal(e)
		}
	}
	stats, err := q.Stats(ctx, "a")
	if err != nil || stats["failed"] != 2 {
		t.Fatalf("stats=%v err=%v", stats, err)
	}
	listed, err := q.List(ctx, "a", 10)
	if err != nil || len(listed) != 2 {
		t.Fatal("list", err)
	}
	job, err := q.Get(ctx, id)
	if err != nil || job.State != "failed" {
		t.Fatal("get", err)
	}
	r.RangeJSON = `{"x":1}`
	again, err := q.Enqueue(ctx, r)
	if err != nil || again == id {
		t.Fatal("terminal dedup key not released", err)
	}
	runStats, err = q.StatsRun(ctx, "run-one")
	if err != nil || runStats[Failed] != 1 {
		t.Fatal("first coalesced run outcome", runStats, err)
	}
	runStats, err = q.StatsRun(ctx, "run-two")
	if err != nil || runStats[Failed] != 2 || runStats[Waiting] != 1 {
		t.Fatal("followup run outcomes", runStats, err)
	}
	j, err := q.Claim(ctx, "success", time.Minute)
	if err != nil || j == nil {
		t.Fatal("claim success", err)
	}
	if err = q.Heartbeat(ctx, j, time.Minute); err != nil {
		t.Fatal("live heartbeat", err)
	}
	other := r
	other.AccountID = "other"
	other.ProviderID = "other-provider"
	other.RegionID = "other-region"
	other.RunID = "other-run"
	if _, err = q.Enqueue(ctx, other); err != nil {
		t.Fatal(err)
	}
	parallel, err := q.Claim(ctx, "parallel", time.Minute)
	if err != nil || parallel == nil || parallel.AccountID != "other" {
		t.Fatal("independent resource blocked", err)
	}
	if err = q.Complete(ctx, parallel, errors.New("delay"), 3, time.Hour); err != nil {
		t.Fatal(err)
	}
	if pending, err := q.Claim(ctx, "too-early", time.Minute); err != nil || pending != nil {
		t.Fatal("retry delay ignored", err)
	}
	if err = q.Complete(ctx, j, nil, 3, 0); err != nil {
		t.Fatal("successful completion", err)
	}
	isolated, err := q.List(ctx, "other", 10)
	if err != nil || len(isolated) != 1 {
		t.Fatal("account isolation", err)
	}
	if _, err = db.Exec("UPDATE sync_queue_schema SET version=999 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if err = q.CheckSchema(ctx); err == nil {
		t.Fatal("future schema accepted")
	}
	if err = q.Migrate(ctx); err == nil {
		t.Fatal("future schema migrated")
	}
	if _, err = db.Exec("UPDATE sync_queue_schema SET version=1 WHERE id=1"); err != nil {
		t.Fatal(err)
	}

}

func TestMySQLReplicaLimits(t *testing.T) {
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
	if err = q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("DELETE FROM sync_queue_jobs"); err != nil {
		t.Fatal(err)
	}
	q.GlobalLimit = 2
	q.AccountLimit = 1
	q.ProviderLimit = 1
	replica := New(db)
	replica.GlobalLimit = 2
	replica.AccountLimit = 1
	replica.ProviderLimit = 1
	for _, r := range []Request{{AccountID: "a", ProviderID: "p", RegionID: "1", RangeJSON: `{}`}, {AccountID: "a", ProviderID: "p", RegionID: "2", RangeJSON: `{}`}, {AccountID: "b", ProviderID: "q", RegionID: "1", RangeJSON: `{}`}, {AccountID: "c", ProviderID: "s", RegionID: "1", RangeJSON: `{}`}} {
		if _, err = q.Enqueue(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	first, err := q.Claim(ctx, "first", time.Minute)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	second, err := replica.Claim(ctx, "second", time.Minute)
	if err != nil || second == nil || first.AccountID == second.AccountID {
		t.Fatal("account/provider replica bound", err)
	}
	third, err := q.Claim(ctx, "third", time.Minute)
	if err != nil || third != nil {
		t.Fatal("global replica bound", err)
	}
	if err = q.Complete(ctx, first, nil, 3, 0); err != nil {
		t.Fatal(err)
	}
	third, err = q.Claim(ctx, "third", time.Minute)
	if err != nil || third == nil {
		t.Fatal("capacity not released", err)
	}
}
