package syncworker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"yunion.io/x/onecloud/pkg/compute/syncqueue"
)

// The explicit disposable DSN must allow CREATE DATABASE. A separate database
// prevents the queue package's destructive integration tests from racing these
// runner tests when go test executes packages concurrently.
func TestMySQLRunnerReplicasRetryAndDrain(t *testing.T) {
	dsn := os.Getenv("SYNCQUEUE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set SYNCQUEUE_TEST_MYSQL_DSN to a disposable MySQL DSN with CREATE DATABASE permission")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	database := fmt.Sprintf("syncworker_test_%d", time.Now().UnixNano())
	if _, err = admin.Exec("CREATE DATABASE " + database); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec("DROP DATABASE " + database); err != nil {
			t.Errorf("cleanup test database: %v", err)
		}
	}()
	cfg.DBName = database
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	queue := syncqueue.New(db)
	queue.GlobalLimit = 2
	queue.AccountLimit = 2
	queue.ProviderLimit = 2
	if err = queue.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	replica := syncqueue.New(db)
	replica.GlobalLimit = 2
	replica.AccountLimit = 2
	replica.ProviderLimit = 2
	enqueue := func(region, scope, run string) string {
		t.Helper()
		id, err := queue.Enqueue(ctx, syncqueue.Request{AccountID: "account", ProviderID: "provider", RegionID: region, RangeJSON: scope, RunID: run})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	firstID := enqueue("region-a", `"first"`, "run")
	if duplicate := enqueue("region-a", `"first"`, "coalesced-run"); duplicate != firstID {
		t.Fatal("duplicate did not coalesce")
	}
	enqueue("region-a", `"followup"`, "run")
	enqueue("region-b", `"independent"`, "run")

	var mu sync.Mutex
	active := map[string]int{}
	attempts := map[string]int{}
	workers := map[string]bool{}
	activeTotal, maxActive := 0, 0
	initialConcurrentReady := make(chan struct{})
	var initialReadyOnce sync.Once
	violations := make(chan string, 16)
	fatals := make(chan error, 4)
	drainStarted := make(chan *syncqueue.Job, 2)
	releaseDrain := make(chan struct{})
	var releaseOnce sync.Once
	execute := func(ctx context.Context, j *syncqueue.Job) error {
		mu.Lock()
		active[j.ResourceKey]++
		activeTotal++
		attempts[j.ID]++
		workers[j.WorkerID] = true
		if active[j.ResourceKey] > 1 {
			violations <- "overlapping executions for one provider-region"
		}
		if activeTotal > 2 {
			violations <- "global capacity exceeded"
		}
		if activeTotal == 2 {
			initialReadyOnce.Do(func() { close(initialConcurrentReady) })
		}
		if activeTotal > maxActive {
			maxActive = activeTotal
		}
		attempt := attempts[j.ID]
		mu.Unlock()
		defer func() { mu.Lock(); active[j.ResourceKey]--; activeTotal--; mu.Unlock() }()
		if j.RangeJSON == `"drain"` {
			drainStarted <- j
			<-releaseDrain
			if ctx.Err() != nil {
				violations <- "graceful shutdown canceled active execution"
				return ctx.Err()
			}
			return nil
		}
		select {
		case <-initialConcurrentReady:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("replicas did not reach concurrent execution")
		}
		time.Sleep(40 * time.Millisecond)
		if j.ID == firstID && attempt == 1 {
			return errors.New("transient test provider failure")
		}
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 2)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(releaseDrain) })
		finished := make(chan struct{})
		go func() { wg.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(6 * time.Second):
			t.Error("runner cleanup timed out")
		}
	}()
	for i, store := range []*syncqueue.Queue{queue, replica} {
		config := Config{WorkerID: fmt.Sprintf("replica-%d", i), Concurrency: 1, PollInterval: 10 * time.Millisecond, HeartbeatInterval: 500 * time.Millisecond, LeaseDuration: 5 * time.Second, MaxAttempts: 3, RetryDelay: 10 * time.Millisecond, ShutdownTimeout: 5 * time.Second}
		runner, err := New(config, store, execute, func(err error) { fatals <- err })
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() { defer wg.Done(); done <- runner.Run(runCtx) }()
	}
	deadline := time.Now().Add(8 * time.Second)
	for {
		stats, err := queue.StatsRun(ctx, "run")
		if err != nil {
			t.Fatal(err)
		}
		if stats[syncqueue.Succeeded] == 3 {
			break
		}
		select {
		case err := <-fatals:
			t.Fatalf("runner fatal: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("work did not complete: %v", stats)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stats, err := queue.StatsRun(ctx, "coalesced-run")
	if err != nil || stats[syncqueue.Succeeded] != 1 {
		t.Fatalf("coalesced progress %v, %v", stats, err)
	}
	first, err := queue.Get(ctx, firstID)
	if err != nil || first.Attempts != 2 || first.State != syncqueue.Succeeded {
		t.Fatalf("retry result %+v, %v", first, err)
	}
	mu.Lock()
	observedAttempts, observedWorkers, observedMax := attempts[firstID], len(workers), maxActive
	mu.Unlock()
	if observedAttempts != 2 || observedWorkers != 2 || observedMax != 2 {
		t.Fatalf("attempts=%d workers=%d concurrency=%d", observedAttempts, observedWorkers, observedMax)
	}

	enqueue("region-a", `"drain"`, "drain-run")
	enqueue("region-b", `"drain"`, "drain-run")
	var draining []*syncqueue.Job
	for len(draining) < 2 {
		select {
		case j := <-drainStarted:
			draining = append(draining, j)
		case err := <-fatals:
			t.Fatal(err)
		case <-time.After(5 * time.Second):
			t.Fatal("drain jobs did not start")
		}
	}
	cancel()
	// Hold execution beyond a heartbeat interval: cancellation must retain leases.
	select {
	case err := <-done:
		t.Fatalf("runner returned before active jobs drained: %v", err)
	case <-time.After(750 * time.Millisecond):
	}
	for _, j := range draining {
		current, err := queue.Get(ctx, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State != syncqueue.Running || !current.LeaseUntil.Time.After(j.LeaseUntil.Time) {
			t.Fatalf("lease was not renewed during drain: %+v", current)
		}
	}
	releaseOnce.Do(func() { close(releaseDrain) })
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("runner did not drain")
		}
	}
	stats, err = queue.StatsRun(ctx, "drain-run")
	if err != nil || stats[syncqueue.Succeeded] != 2 {
		t.Fatalf("drain outcomes %v, %v", stats, err)
	}
	select {
	case err := <-fatals:
		t.Fatalf("unexpected fatal: %v", err)
	default:
	}
	select {
	case violation := <-violations:
		t.Fatal(violation)
	default:
	}
}
