package synctrace

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"yunion.io/x/onecloud/pkg/compute/syncqueue"
)

func capture() (context.Context, *[]Event) {
	events := []Event{}
	return WithSink(context.Background(), func(e Event) { events = append(events, e) }), &events
}

func TestTracePhaseSafetyAndIdentity(t *testing.T) {
	ctx, events := capture()
	ctx = WithIdentity(ctx, Identity{RunID: "run", AccountID: "account"})
	err := Measure(ctx, "prepare", func() error { return errors.New("access denied: SECRET_TOKEN") })
	if err == nil || len(*events) != 2 {
		t.Fatalf("err=%v events=%+v", err, *events)
	}
	start, end := (*events)[0], (*events)[1]
	if start.Kind != "start" || end.Kind != "end" || end.Outcome != "error" || end.ErrorClass != syncqueue.ErrorClassPermission || end.DurationMS < 0 {
		t.Fatalf("invalid phase events: %+v", *events)
	}
	if end.AccountID != "account" || end.RunID != "run" || end.Schema != 1 || end.At.IsZero() {
		t.Fatalf("missing correlation: %+v", end)
	}
	data, _ := json.Marshal(events)
	if strings.Contains(string(data), "SECRET_TOKEN") || strings.Contains(string(data), "access denied") {
		t.Fatalf("raw error leaked: %s", data)
	}
}

func TestTraceIdentityDoesNotMutateSiblings(t *testing.T) {
	ctx, events := capture()
	ctx = WithIdentity(ctx, Identity{RunID: "run", AccountID: "account"})
	for _, id := range []string{"a", "b"} {
		Duration(WithIdentity(ctx, Identity{ProviderID: id}), "prepare", time.Second, nil)
	}
	Duration(ctx, "prepare", time.Second, nil)
	if (*events)[0].ProviderID != "a" || (*events)[1].ProviderID != "b" || (*events)[2].ProviderID != "" || (*events)[1].RunID != "run" {
		t.Fatalf("context identities leaked: %+v", *events)
	}
}

func TestTraceJobUsesOnlyAllowlistedFields(t *testing.T) {
	ctx, events := capture()
	ctx = WithIdentity(ctx, Identity{RunID: "unrelated-run", ProviderID: "old-provider"})
	job := &syncqueue.Job{ID: "job", Attempts: 2, Version: 3, LastError: "SECRET_ERROR",
		Request: syncqueue.Request{AccountID: "account", ProviderID: "provider", ResourceGroup: "core", RangeJSON: "SECRET_RANGE", RequestedBy: "SECRET_REQUESTER"}}
	Duration(WithJob(ctx, job), "attempt", time.Second, nil)
	e := (*events)[0]
	data, _ := json.Marshal(e)
	if strings.Contains(string(data), "SECRET") || e.RunID != "" || e.JobID != "job" || e.Attempt != 2 || e.Version != 3 || e.ProviderID != "provider" {
		t.Fatalf("invalid job event: %s", data)
	}
}

func TestTraceAgeMissingFutureAndDuration(t *testing.T) {
	ctx, events := capture()
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	Age(ctx, "job_age", time.Time{}, now)
	Age(ctx, "job_age", now.Add(time.Second), now)
	Age(ctx, "job_age", now.Add(-2*time.Second), now)
	Duration(ctx, "claim", -time.Second, nil)
	if len(*events) != 2 || (*events)[0].DurationMS != 2000 || (*events)[1].DurationMS != 0 {
		t.Fatalf("invalid durations: %+v", *events)
	}
}

func TestTraceFinishOnceAndCanceled(t *testing.T) {
	ctx, events := capture()
	finish := Start(ctx, "prepare")
	finish(context.Canceled)
	finish(nil)
	if len(*events) != 2 || (*events)[1].Outcome != "canceled" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

func TestTracePanicRethrownWithoutPayload(t *testing.T) {
	ctx, events := capture()
	func() {
		defer func() {
			if p := recover(); p != "SECRET_PANIC" {
				t.Errorf("panic changed: %v", p)
			}
		}()
		_ = Measure(ctx, "prepare", func() error { panic("SECRET_PANIC") })
	}()
	if len(*events) != 2 || (*events)[1].Outcome != "error" {
		t.Fatalf("panic reported as success: %+v", *events)
	}
	data, _ := json.Marshal(events)
	if strings.Contains(string(data), "SECRET_PANIC") {
		t.Fatal("panic payload leaked")
	}
}

func TestTraceSinkCannotChangeWorkResult(t *testing.T) {
	ctx := WithSink(context.Background(), func(Event) { panic("broken logger") })
	want := errors.New("work error")
	if err := Measure(ctx, "prepare", func() error { return want }); err != want {
		t.Fatalf("work result changed: %v", err)
	}
}
