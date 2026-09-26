package syncworker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"yunion.io/x/onecloud/pkg/compute/syncqueue"
)

type fakeStore struct {
	mu           sync.Mutex
	jobs         []*syncqueue.Job
	completed    []*syncqueue.Job
	results      []error
	heartbeatErr error
	completeErr  error
	heartbeats   int
}

func (s *fakeStore) Claim(ctx context.Context, worker string, lease time.Duration) (*syncqueue.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(s.jobs) == 0 {
		return nil, nil
	}
	j := s.jobs[0]
	s.jobs = s.jobs[1:]
	j.WorkerID = worker
	return j, nil
}
func (s *fakeStore) Heartbeat(ctx context.Context, j *syncqueue.Job, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats++
	return s.heartbeatErr
}
func (s *fakeStore) Complete(ctx context.Context, j *syncqueue.Job, result error, max int, delay time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed = append(s.completed, j)
	s.results = append(s.results, result)
	return s.completeErr
}
func config() Config {
	return Config{WorkerID: "worker", Concurrency: 2, PollInterval: time.Millisecond, HeartbeatInterval: 10 * time.Millisecond, LeaseDuration: time.Second, MaxAttempts: 3, RetryDelay: time.Millisecond, ShutdownTimeout: time.Second}
}
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
		var z T
		return z
	}
}
func TestConcurrencyAndDrain(t *testing.T) {
	s := &fakeStore{}
	for i := 0; i < 5; i++ {
		s.jobs = append(s.jobs, &syncqueue.Job{ID: fmt.Sprint(i), Version: int64(i + 1)})
	}
	started := make(chan *syncqueue.Job, 5)
	release := make(chan struct{})
	fatal := make(chan error, 1)
	r, err := New(config(), s, func(ctx context.Context, j *syncqueue.Job) error {
		started <- j
		<-release
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}, func(e error) { fatal <- e })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	first := await(t, started)
	second := await(t, started)
	select {
	case <-started:
		t.Fatal("concurrency exceeded")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case e := <-done:
		t.Fatalf("returned before drain: %v", e)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if e := await(t, done); e != nil {
		t.Fatal(e)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.completed) != 2 || len(s.jobs) != 3 {
		t.Fatalf("completed=%d queued=%d", len(s.completed), len(s.jobs))
	}
	for i, j := range s.completed {
		if j != first && j != second {
			t.Fatal("completion changed job ownership")
		}
		if s.results[i] != nil {
			t.Fatalf("shutdown canceled execution: %v", s.results[i])
		}
	}
	if s.heartbeats == 0 {
		t.Fatal("no heartbeat during execution")
	}
}
func TestRenewalFailureTerminatesWithoutCompletion(t *testing.T) {
	lost := errors.New("lease lost")
	s := &fakeStore{jobs: []*syncqueue.Job{{ID: "job"}}, heartbeatErr: lost}
	fatal := make(chan error, 1)
	canceled := make(chan struct{})
	r, _ := New(config(), s, func(ctx context.Context, j *syncqueue.Job) error { <-ctx.Done(); close(canceled); return ctx.Err() }, func(e error) { fatal <- e })
	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()
	if e := await(t, fatal); !errors.Is(e, lost) {
		t.Fatal(e)
	}
	await(t, canceled)
	if e := await(t, done); !errors.Is(e, lost) {
		t.Fatal(e)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.completed) != 0 {
		t.Fatal("completed after lease loss")
	}
}
func TestShutdownDeadlineIsFatal(t *testing.T) {
	s := &fakeStore{jobs: []*syncqueue.Job{{ID: "job"}}}
	started := make(chan struct{})
	release := make(chan struct{})
	fatal := make(chan error, 1)
	cfg := config()
	cfg.ShutdownTimeout = 20 * time.Millisecond
	r, _ := New(cfg, s, func(ctx context.Context, j *syncqueue.Job) error { close(started); <-release; return nil }, func(e error) { fatal <- e })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	await(t, started)
	cancel()
	if e := await(t, fatal); !strings.Contains(e.Error(), "shutdown") {
		t.Fatal(e)
	}
	if e := await(t, done); e == nil {
		t.Fatal("missing fatal error")
	}
	close(release)
}
func TestPanicIsRecordedAsFailure(t *testing.T) {
	s := &fakeStore{jobs: []*syncqueue.Job{{ID: "job"}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, _ := New(config(), s, func(context.Context, *syncqueue.Job) error { cancel(); panic("broken") }, func(e error) { t.Errorf("fatal: %v", e) })
	if e := r.Run(ctx); e != nil {
		t.Fatal(e)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.results) != 1 || !strings.Contains(s.results[0].Error(), "broken") || !strings.Contains(s.results[0].Error(), "goroutine") {
		t.Fatalf("panic not preserved: %v", s.results)
	}
}
func TestCompletionFailureIsFatal(t *testing.T) {
	failure := errors.New("completion failed")
	s := &fakeStore{jobs: []*syncqueue.Job{{ID: "job"}}, completeErr: failure}
	fatal := make(chan error, 1)
	r, _ := New(config(), s, func(context.Context, *syncqueue.Job) error { return nil }, func(e error) { fatal <- e })
	if e := r.Run(context.Background()); !errors.Is(e, failure) {
		t.Fatal(e)
	}
	await(t, fatal)
}
func TestValidation(t *testing.T) {
	s := &fakeStore{}
	execute := func(context.Context, *syncqueue.Job) error { return nil }
	fatal := func(error) {}
	if _, e := New(config(), nil, execute, fatal); e == nil {
		t.Fatal("nil store accepted")
	}
	if _, e := New(config(), s, nil, fatal); e == nil {
		t.Fatal("nil execute accepted")
	}
	if _, e := New(config(), s, execute, nil); e == nil {
		t.Fatal("nil fatal accepted")
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.WorkerID = "" }, func(c *Config) { c.Concurrency = 0 }, func(c *Config) { c.PollInterval = 0 }, func(c *Config) { c.HeartbeatInterval = 0 }, func(c *Config) { c.LeaseDuration = c.HeartbeatInterval * 2 }, func(c *Config) { c.MaxAttempts = 0 }, func(c *Config) { c.RetryDelay = -1 }, func(c *Config) { c.ShutdownTimeout = 0 }} {
		c := config()
		mutate(&c)
		if _, e := New(c, s, execute, fatal); e == nil {
			t.Fatalf("invalid config accepted: %+v", c)
		}
	}
}

type stalledHeartbeatStore struct {
	fakeStore
	release <-chan struct{}
}

func (s *stalledHeartbeatStore) Heartbeat(context.Context, *syncqueue.Job, time.Duration) error {
	<-s.release
	return nil
}

func TestHeartbeatTimeoutTerminatesEvenIfStoreIgnoresContext(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	s := &stalledHeartbeatStore{fakeStore: fakeStore{jobs: []*syncqueue.Job{{ID: "job"}}}, release: release}
	fatal := make(chan error, 1)
	canceled := make(chan struct{})
	r, _ := New(config(), s, func(ctx context.Context, j *syncqueue.Job) error { <-ctx.Done(); close(canceled); return ctx.Err() }, func(e error) { fatal <- e })
	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()
	if e := await(t, fatal); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	await(t, canceled)
	if e := await(t, done); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.completed) != 0 {
		t.Fatal("completed timed-out lease")
	}
}

func TestCanceledExecutionDoesNotStartLegacyWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := make(chan struct{}, 1)
	r, _ := New(config(), &fakeStore{}, func(context.Context, *syncqueue.Job) error { started <- struct{}{}; return nil }, func(error) {})
	r.runJob(ctx, &syncqueue.Job{ID: "job"}, func(error) {})
	select {
	case <-started:
		t.Fatal("started execution after fatal cancellation")
	case <-time.After(20 * time.Millisecond):
	}
}
