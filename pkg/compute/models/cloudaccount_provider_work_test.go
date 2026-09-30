package models

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"yunion.io/x/onecloud/pkg/cloudcommon/db/lockman"
	"yunion.io/x/pkg/tristate"
)

func awaitProviderWork(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("provider work did not progress")
		return ""
	}
}

func TestCloudProviderWorkSlowProviderDoesNotOccupySiblingSlots(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	started := make(chan string, 8)
	ready := make(chan string, 8)
	result := make(chan error, 1)
	steps := []cloudProviderWork{
		{providerID: "slow", run: func(context.Context) error { started <- "slow"; <-release; return nil }},
		{providerID: "slow", run: func(context.Context) error { started <- "slow-again"; return nil }},
		{providerID: "fast", run: func(context.Context) error { started <- "fast"; return nil }},
	}
	go func() {
		result <- runCloudProviderWork(context.Background(), steps, 2, func(ctx context.Context, id string) error {
			ready <- id
			return nil
		})
	}()
	first, second := awaitProviderWork(t, started), awaitProviderWork(t, started)
	if first == "slow-again" || second == "slow-again" || first == second {
		t.Fatalf("same-provider work overlapped or blocked a sibling: %q %q", first, second)
	}
	if id := awaitProviderWork(t, ready); id != "fast" {
		t.Fatalf("ready=%s, want fast while slow still runs", id)
	}
	select {
	case err := <-result:
		t.Fatalf("producer returned before slow work drained: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	if id := awaitProviderWork(t, started); id != "slow-again" {
		t.Fatalf("next work=%s", id)
	}
	if id := awaitProviderWork(t, ready); id != "slow" {
		t.Fatalf("ready=%s", id)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-ready:
		t.Fatalf("duplicate completion for %s", id)
	default:
	}
}

func TestCloudProviderWorkBoundedAndSerialPerProvider(t *testing.T) {
	var mu sync.Mutex
	active := map[string]int{}
	seen := map[string]int{}
	total, peak := 0, 0
	var jobs []cloudProviderWork
	for i := 0; i < 40; i++ {
		id := fmt.Sprint(i % 7)
		jobs = append(jobs, cloudProviderWork{providerID: id, run: func(context.Context) error {
			mu.Lock()
			active[id]++
			total++
			if total > peak {
				peak = total
			}
			if active[id] != 1 || total > 3 {
				t.Errorf("active[%s]=%d total=%d", id, active[id], total)
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			active[id]--
			total--
			seen[id]++
			mu.Unlock()
			return nil
		}})
	}
	ready := map[string]int{}
	err := runCloudProviderWork(context.Background(), jobs, 3, func(ctx context.Context, id string) error {
		mu.Lock()
		defer mu.Unlock()
		ready[id]++
		if active[id] != 0 {
			t.Errorf("completion before provider drained: %s", id)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if peak < 2 || peak > 3 || len(ready) != 7 {
		t.Fatalf("peak=%d ready=%v", peak, ready)
	}
	count := 0
	for id, n := range seen {
		count += n
		if ready[id] != 1 {
			t.Errorf("provider %s completed %d times", id, ready[id])
		}
	}
	if count != len(jobs) {
		t.Fatalf("executed %d/%d jobs", count, len(jobs))
	}
}

func TestCloudProviderWorkFailureStillDrainsAndCompletesProviders(t *testing.T) {
	first, last := errors.New("probe failed"), errors.New("enqueue failed")
	var completed atomic.Int32
	jobs := []cloudProviderWork{
		{providerID: "bad", run: func(context.Context) error { return first }},
		{providerID: "bad", run: func(context.Context) error { return nil }},
		{providerID: "good"}, // No discovery candidates must still reach planning.
	}
	err := runCloudProviderWork(context.Background(), jobs, 2, func(ctx context.Context, id string) error {
		completed.Add(1)
		if id == "bad" {
			return last
		}
		return nil
	})
	if !errors.Is(err, first) || !errors.Is(err, last) || completed.Load() != 2 {
		t.Fatalf("errors=%v completed=%d", err, completed.Load())
	}
}

func TestCloudProviderWorkCancellationStopsPendingAndDrainsRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var pending, ready atomic.Int32
	done := make(chan error, 1)
	jobs := []cloudProviderWork{
		{providerID: "slow", run: func(context.Context) error { started <- "slow"; <-release; return nil }},
		{providerID: "slow", run: func(context.Context) error { pending.Add(1); return nil }},
		{providerID: "other", run: func(context.Context) error { pending.Add(1); return nil }},
	}
	go func() {
		done <- runCloudProviderWork(ctx, jobs, 1, func(context.Context, string) error { ready.Add(1); return nil })
	}()
	awaitProviderWork(t, started)
	cancel()
	select {
	case err := <-done:
		t.Fatalf("canceled producer abandoned admitted SDK work: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if pending.Load() != 0 || ready.Load() != 0 {
		t.Fatalf("ran pending=%d callbacks=%d after cancellation", pending.Load(), ready.Load())
	}
}

func TestCloudProviderWorkRoundRobinAndSafeDefault(t *testing.T) {
	var order []string
	jobs := []cloudProviderWork{
		{providerID: "a", run: func(context.Context) error { order = append(order, "a1"); return nil }},
		{providerID: "a", run: func(context.Context) error { order = append(order, "a2"); return nil }},
		{providerID: "b", run: func(context.Context) error { order = append(order, "b1"); return nil }},
	}
	if err := runCloudProviderWork(context.Background(), jobs, 0, nil); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(order) != "[a1 b1 a2]" {
		t.Fatalf("nonpositive limit must run fairly with one slot: %v", order)
	}
}

func TestPrepareAccountProviderRegionsFiltersDisabledAndCollectsFailures(t *testing.T) {
	providers := []SCloudprovider{{}, {}, {}}
	providers[0].Id, providers[1].Id, providers[2].Id = "bad", "disabled", "good"
	providers[0].Enabled, providers[2].Enabled = tristate.True, tristate.True
	var calls atomic.Int32
	failure := errors.New("prepare failed")
	err := prepareAccountProviderRegions(context.Background(), providers, 2, func(ctx context.Context, p *SCloudprovider) error {
		calls.Add(1)
		if p.Id == "disabled" {
			t.Error("prepared disabled provider")
		}
		if p.Id == "bad" {
			return failure
		}
		return nil
	})
	if !errors.Is(err, failure) || calls.Load() != 2 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestCloudProviderWorkPanicDoesNotAbandonSiblings(t *testing.T) {
	var completed atomic.Int32
	err := runCloudProviderWork(context.Background(), []cloudProviderWork{
		{providerID: "panic", run: func(context.Context) error { panic("broken SDK") }},
		{providerID: "good"},
	}, 2, func(context.Context, string) error { completed.Add(1); return nil })
	if err == nil || completed.Load() != 2 {
		t.Fatalf("err=%v completed=%d", err, completed.Load())
	}
}

func TestCloudProviderWorkPreservesSharedReconciliationLock(t *testing.T) {
	lm := lockman.NewInMemoryLockManager()
	attempted := make(chan string, 2)
	entered := make(chan string, 2)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	work := func(id string) cloudProviderWork {
		return cloudProviderWork{providerID: id, run: func(ctx context.Context) error {
			attempted <- id
			lm.LockKey(ctx, "shared-region-prefix")
			defer lm.UnlockKey(ctx, "shared-region-prefix")
			entered <- id
			<-release
			return nil
		}}
	}
	done := make(chan error, 1)
	go func() {
		done <- runCloudProviderWork(context.Background(), []cloudProviderWork{work("a"), work("b")}, 2, nil)
	}()
	awaitProviderWork(t, attempted)
	awaitProviderWork(t, attempted)
	awaitProviderWork(t, entered)
	select {
	case id := <-entered:
		t.Errorf("provider %s bypassed shared reconciliation lock via context reentrancy", id)
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCloudProviderWorkIsolatesEtcdLockOwners(t *testing.T) {
	var mu sync.Mutex
	owners := map[string]context.Context{}
	type contextKey struct{}
	parent := context.WithValue(context.Background(), contextKey{}, "preserved")
	var jobs []cloudProviderWork
	for _, id := range []string{"a", "b", "a"} {
		jobs = append(jobs, cloudProviderWork{providerID: id, run: func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			if ctx == parent {
				t.Error("parallel SDK work reused parent etcd lock owner")
			}
			if ctx.Value(contextKey{}) != "preserved" {
				t.Error("context values lost")
			}
			if old := owners[id]; old != nil && old != ctx {
				t.Error("cached provider driver lost its context between probes")
			}
			owners[id] = ctx
			return nil
		}})
	}
	if err := runCloudProviderWork(parent, jobs, 2, func(ctx context.Context, id string) error {
		mu.Lock()
		defer mu.Unlock()
		if ctx != owners[id] || ctx.Err() != nil {
			t.Errorf("completion lost live provider lock context: %s", id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if owners["a"] == owners["b"] {
		t.Error("distinct providers shared an etcd lock owner")
	}
}
