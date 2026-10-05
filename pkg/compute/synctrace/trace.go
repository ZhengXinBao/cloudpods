// Package synctrace emits bounded, payload-free synchronization phase diagnostics.
// It does not control scheduling, retries, leases, or synchronization outcomes.
package synctrace

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"yunion.io/x/log"

	"yunion.io/x/onecloud/pkg/compute/syncqueue"
)

type Identity struct {
	RunID         string `json:"run_id,omitempty"`
	AccountID     string `json:"account_id,omitempty"`
	ProviderID    string `json:"provider_id,omitempty"`
	RegionID      string `json:"region_id,omitempty"`
	JobID         string `json:"job_id,omitempty"`
	WorkerID      string `json:"worker_id,omitempty"`
	ResourceGroup string `json:"resource_group,omitempty"`
	Resource      string `json:"resource,omitempty"`
	Attempt       int    `json:"attempt,omitempty"`
	Version       int64  `json:"version,omitempty"`
}

type Event struct {
	Identity
	Schema     int                  `json:"schema"`
	At         time.Time            `json:"at"`
	Phase      string               `json:"phase"`
	Kind       string               `json:"kind"`
	DurationMS int64                `json:"duration_ms"`
	Outcome    string               `json:"outcome,omitempty"`
	ErrorClass syncqueue.ErrorClass `json:"error_class,omitempty"`
}

type identityKey struct{}
type sinkKey struct{}

// WithIdentity adds only nonempty fields to this diagnostic scope. Do not pass a
// new context to SDK/lock code just to attach metadata: lock ownership may depend
// on context identity. Use a separate traceCtx for those diagnostics.
func WithIdentity(ctx context.Context, add Identity) context.Context {
	id, _ := ctx.Value(identityKey{}).(Identity)
	for _, field := range []struct {
		dst *string
		src string
	}{
		{&id.RunID, add.RunID}, {&id.AccountID, add.AccountID},
		{&id.ProviderID, add.ProviderID}, {&id.RegionID, add.RegionID},
		{&id.JobID, add.JobID}, {&id.WorkerID, add.WorkerID},
		{&id.ResourceGroup, add.ResourceGroup}, {&id.Resource, add.Resource},
	} {
		if field.src != "" {
			*field.dst = field.src
		}
	}
	if add.Attempt != 0 {
		id.Attempt = add.Attempt
	}
	if add.Version != 0 {
		id.Version = add.Version
	}
	return context.WithValue(ctx, identityKey{}, id)
}

// WithJob starts a fresh job identity. Durable jobs can be shared by multiple
// runs; no run ID may be inferred from a parent context or the first requester.
func WithJob(ctx context.Context, job *syncqueue.Job) context.Context {
	return context.WithValue(ctx, identityKey{}, Identity{
		AccountID: job.AccountID, ProviderID: job.ProviderID, RegionID: job.RegionID,
		JobID: job.ID, WorkerID: job.WorkerID, ResourceGroup: job.ResourceGroup,
		Attempt: job.Attempts, Version: job.Version,
	})
}

// WithSink sets a scope-local sink. It must be concurrency-safe and nonblocking.
// A sink panic is isolated from synchronization; no global mutable sink is used.
func WithSink(ctx context.Context, sink func(Event)) context.Context {
	return context.WithValue(ctx, sinkKey{}, sink)
}

func emit(ctx context.Context, event Event) {
	defer func() { _ = recover() }() // Diagnostics must not fail or panic the job.
	event.Identity, _ = ctx.Value(identityKey{}).(Identity)
	event.Schema = 1
	event.At = time.Now().UTC()
	if sink, _ := ctx.Value(sinkKey{}).(func(Event)); sink != nil {
		sink(event)
		return
	}
	data, err := json.Marshal(event)
	if err == nil {
		log.Infof("cloud_sync_trace %s", data)
	}
}

func duration(ctx context.Context, phase, kind string, elapsed time.Duration, err error) {
	event := Event{Phase: phase, Kind: kind, DurationMS: max(0, elapsed.Milliseconds()), Outcome: "ok"}
	if err != nil {
		event.Outcome = "error"
		event.ErrorClass = syncqueue.ClassifyError(err)
		if errors.Is(err, context.Canceled) {
			event.Outcome = "canceled"
		}
	}
	emit(ctx, event)
}

func Duration(ctx context.Context, phase string, elapsed time.Duration, err error) {
	duration(ctx, phase, "duration", elapsed, err)
}

// Cost is a timer observation, not an assertion that a resource inventory
// completed successfully. Existing resource timers can cover failed operations.
func Cost(ctx context.Context, phase string, elapsed time.Duration) {
	emit(ctx, Event{Phase: phase, Kind: "cost", DurationMS: max(0, elapsed.Milliseconds())})
}

func Enabled(ctx context.Context) bool {
	id, _ := ctx.Value(identityKey{}).(Identity)
	return id.AccountID != "" || id.JobID != "" || id.RunID != ""
}

// Age omits unknown/future timestamps. Age is wall-clock lifecycle information,
// never a substitute for attempt execution time or pure queue wait.
func Age(ctx context.Context, phase string, since, now time.Time) {
	if since.IsZero() || now.Before(since) {
		return
	}
	duration(ctx, phase, "age", now.Sub(since), nil)
}

func Start(ctx context.Context, phase string) func(error) {
	start := time.Now()
	emit(ctx, Event{Phase: phase, Kind: "start"})
	var once sync.Once
	return func(err error) {
		once.Do(func() { duration(ctx, phase, "end", time.Since(start), err) })
	}
}

// End must be deferred directly. It records and rethrows a panic without logging
// its potentially sensitive payload, preserving the caller's recovery behavior.
func End(finish func(error), result *error) {
	if p := recover(); p != nil {
		finish(errors.New("synchronization phase panicked"))
		panic(p)
	}
	finish(*result)
}

func Measure(ctx context.Context, phase string, work func() error) (err error) {
	defer End(Start(ctx, phase), &err)
	return work()
}
