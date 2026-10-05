package syncworker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"yunion.io/x/onecloud/pkg/compute/syncqueue"
	"yunion.io/x/onecloud/pkg/compute/synctrace"
)

type traceEvents struct {
	mu     sync.Mutex
	events []synctrace.Event
}

func (c *traceEvents) add(event synctrace.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)
}

func (c *traceEvents) end(phase string) []synctrace.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	var events []synctrace.Event
	for _, event := range c.events {
		if event.Phase == phase && event.Kind != "start" {
			events = append(events, event)
		}
	}
	return events
}

func TestTraceAttemptAndCompletionAreSeparate(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		execution, completion error
	}{
		{"success", nil, nil},
		{"execution failure", errors.New("timeout"), nil},
		{"completion failure", nil, errors.New("db failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{completeErr: tc.completion}
			record := &traceEvents{}
			ctx := synctrace.WithSink(context.Background(), record.add)
			now := time.Now()
			job := &syncqueue.Job{ID: "job", Attempts: 2, Version: 3, CreatedAt: now.Add(-3 * time.Hour), UpdatedAt: now,
				Request: syncqueue.Request{AccountID: "account", ProviderID: "provider", ResourceGroup: "core"}}
			runner, err := New(config(), store, func(context.Context, *syncqueue.Job) error { return tc.execution }, func(error) {})
			if err != nil {
				t.Fatal(err)
			}
			var fatal error
			runner.runJob(ctx, job, func(err error) { fatal = err })
			attempt := record.end("attempt")
			completion := record.end("complete")
			age := record.end("job_age")
			if len(attempt) != 1 || len(completion) != 1 || len(age) != 1 {
				t.Fatalf("missing events: %+v", record.events)
			}
			if attempt[0].JobID != "job" || attempt[0].Attempt != 2 || attempt[0].Version != 3 || attempt[0].ResourceGroup != "core" {
				t.Fatalf("attempt identity lost: %+v", attempt[0])
			}
			if attempt[0].DurationMS >= age[0].DurationMS || age[0].DurationMS != 3*60*60*1000 {
				t.Fatalf("attempt confused with job age: %+v", record.events)
			}
			if (attempt[0].Outcome == "error") != (tc.execution != nil) || (completion[0].Outcome == "error") != (tc.completion != nil) || (fatal != nil) != (tc.completion != nil) {
				t.Fatalf("outcome changed: %+v fatal=%v", record.events, fatal)
			}
		})
	}
}

func TestTracePanicAttemptIsNotSuccess(t *testing.T) {
	record := &traceEvents{}
	ctx := synctrace.WithSink(context.Background(), record.add)
	runner, _ := New(config(), &fakeStore{}, func(context.Context, *syncqueue.Job) error { panic("broken") }, func(error) {})
	runner.runJob(ctx, &syncqueue.Job{ID: "job"}, func(err error) { t.Errorf("fatal: %v", err) })
	events := record.end("attempt")
	if len(events) != 1 || events[0].Outcome != "error" {
		t.Fatalf("panic outcome: %+v", record.events)
	}
}

func TestTraceIdlePollingIsQuiet(t *testing.T) {
	record := &traceEvents{}
	ctx, cancel := context.WithTimeout(synctrace.WithSink(context.Background(), record.add), 15*time.Millisecond)
	defer cancel()
	runner, _ := New(config(), &fakeStore{}, func(context.Context, *syncqueue.Job) error { return nil }, func(error) {})
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(record.events) != 0 {
		t.Fatalf("idle poll logs: %+v", record.events)
	}
}

func TestTraceSuccessfulClaim(t *testing.T) {
	record := &traceEvents{}
	ctx, cancel := context.WithCancel(synctrace.WithSink(context.Background(), record.add))
	defer cancel()
	store := &fakeStore{jobs: []*syncqueue.Job{{ID: "job"}}}
	runner, _ := New(config(), store, func(context.Context, *syncqueue.Job) error { cancel(); return nil }, func(error) {})
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	events := record.end("claim")
	if len(events) != 1 || events[0].JobID != "job" || events[0].Outcome != "ok" {
		t.Fatalf("missing claim timing: %+v", record.events)
	}
}

func TestTraceQueueWaitExcludesRetryBackoffAndOldExecution(t *testing.T) {
	now := time.Now()
	for _, state := range []string{syncqueue.Waiting, syncqueue.Retry, syncqueue.Running} {
		t.Run(state, func(t *testing.T) {
			record := &traceEvents{}
			ctx := synctrace.WithSink(context.Background(), record.add)
			job := &syncqueue.Job{ID: "job", UpdatedAt: now, ClaimTiming: &syncqueue.ClaimTiming{
				PreviousState: state, EligibleAt: now.Add(-5 * time.Second), RetryBackoff: time.Minute,
			}}
			runner, _ := New(config(), &fakeStore{}, func(context.Context, *syncqueue.Job) error { return nil }, func(error) {})
			runner.runJob(ctx, job, func(err error) { t.Errorf("fatal: %v", err) })
			phase := "queue_wait"
			if state == syncqueue.Running {
				phase = "lease_recovery_wait"
			}
			events := record.end(phase)
			if len(events) != 1 || events[0].DurationMS != 5000 {
				t.Fatalf("invalid %s: %+v", phase, record.events)
			}
			backoff := record.end("retry_backoff")
			if state == syncqueue.Retry {
				if len(backoff) != 1 || backoff[0].DurationMS != 60000 {
					t.Fatalf("missing retry delay: %+v", record.events)
				}
			} else if len(backoff) != 0 {
				t.Fatalf("backoff on non-retry job: %+v", record.events)
			}
		})
	}
}
