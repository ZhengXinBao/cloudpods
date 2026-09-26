// Package syncworker runs leased synchronization jobs independently of the API server.
package syncworker

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"yunion.io/x/onecloud/pkg/compute/syncqueue"
)

// Store must preserve the job's worker and version when renewing or completing it.
type Store interface {
	Claim(context.Context, string, time.Duration) (*syncqueue.Job, error)
	Heartbeat(context.Context, *syncqueue.Job, time.Duration) error
	Complete(context.Context, *syncqueue.Job, error, int, time.Duration) error
}
type ExecuteFunc func(context.Context, *syncqueue.Job) error

type Config struct {
	WorkerID          string
	Concurrency       int
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	LeaseDuration     time.Duration
	MaxAttempts       int
	RetryDelay        time.Duration
	ShutdownTimeout   time.Duration
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.WorkerID) == "" {
		return errors.New("worker ID is required")
	}
	if c.Concurrency <= 0 || c.PollInterval <= 0 || c.HeartbeatInterval <= 0 || c.LeaseDuration <= 0 || c.MaxAttempts <= 0 || c.RetryDelay < 0 || c.ShutdownTimeout <= 0 {
		return errors.New("invalid worker concurrency, attempt count or duration")
	}
	if c.HeartbeatInterval >= c.LeaseDuration/3 {
		return errors.New("heartbeat interval must be less than one third of the lease duration")
	}
	return nil
}

type Runner struct {
	config  Config
	store   Store
	execute ExecuteFunc
	fatal   func(error)
	// OnError receives transient claim errors and execution failures. Set before Run.
	OnError func(error)
}

// New requires a fatal callback that terminates the production process. Legacy
// synchronization may ignore cancellation; returning from Fatal cannot fence it.
func New(config Config, store Store, execute ExecuteFunc, fatal func(error)) (*Runner, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if store == nil || execute == nil || fatal == nil {
		return nil, errors.New("store, execute and fatal callbacks are required")
	}
	return &Runner{config: config, store: store, execute: execute, fatal: fatal}, nil
}
func (r *Runner) report(err error) {
	if r.OnError != nil {
		r.OnError(err)
	}
}

// Run stops claims on cancellation and drains current executions while keeping
// their leases alive. A lease failure or drain timeout terminates the process
// through Fatal before a replacement worker can safely take ownership.
func (r *Runner) Run(ctx context.Context) error {
	claimCtx, stopClaims := context.WithCancel(ctx)
	defer stopClaims()
	executionCtx, stopExecution := context.WithCancel(context.WithoutCancel(ctx))
	defer stopExecution()
	failed := make(chan error, 1)
	var once sync.Once
	fail := func(err error) { once.Do(func() { stopClaims(); stopExecution(); r.fatal(err); failed <- err }) }
	var wg sync.WaitGroup
	for i := 0; i < r.config.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for claimCtx.Err() == nil {
				job, err := r.store.Claim(claimCtx, r.config.WorkerID, r.config.LeaseDuration)
				if err != nil {
					if claimCtx.Err() != nil {
						return
					}
					r.report(fmt.Errorf("claim sync job: %w", err))
				}
				if job != nil && err == nil {
					r.runJob(executionCtx, job, fail)
					continue
				}
				timer := time.NewTimer(r.config.PollInterval)
				select {
				case <-claimCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case err := <-failed:
		return err
	case <-done:
		select {
		case err := <-failed:
			return err
		default:
			return nil
		}
	case <-ctx.Done():
	}
	timer := time.NewTimer(r.config.ShutdownTimeout)
	defer timer.Stop()
	select {
	case err := <-failed:
		return err
	case <-done:
		select {
		case err := <-failed:
			return err
		default:
			return nil
		}
	case <-timer.C:
		err := errors.New("sync worker shutdown deadline exceeded; active sync cannot be safely abandoned")
		fail(err)
		return <-failed
	}
}

func (r *Runner) runJob(ctx context.Context, job *syncqueue.Job, fail func(error)) {
	if ctx.Err() != nil {
		return
	}
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("sync job %s panic: %v\n%s", job.ID, p, debug.Stack())
			}
			result <- err
		}()
		if jobCtx.Err() != nil {
			err = jobCtx.Err()
			return
		}
		err = r.execute(jobCtx, job)
	}()
	ticker := time.NewTicker(r.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := r.bounded(jobCtx, func(opCtx context.Context) error { return r.store.Heartbeat(opCtx, job, r.config.LeaseDuration) })
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				fail(fmt.Errorf("renew sync job %s lease: %w", job.ID, err))
				return
			}
		case err := <-result:
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				r.report(err)
			}
			completionErr := r.bounded(jobCtx, func(opCtx context.Context) error {
				return r.store.Complete(opCtx, job, err, r.config.MaxAttempts, r.config.RetryDelay)
			})
			if completionErr != nil && ctx.Err() == nil {
				fail(fmt.Errorf("complete sync job %s: %w", job.ID, completionErr))
			}
			return
		}
	}
}

// The outer select enforces the deadline even if a store implementation ignores
// cancellation. Such a timeout is fatal, so no later operation uses that lease.
func (r *Runner) bounded(ctx context.Context, operation func(context.Context) error) error {
	opCtx, cancel := context.WithTimeout(ctx, r.config.HeartbeatInterval)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- operation(opCtx) }()
	select {
	case err := <-done:
		return err
	case <-opCtx.Done():
		return opCtx.Err()
	}
}
