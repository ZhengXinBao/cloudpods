package models

import (
	"context"
	"errors"
	"sync"
)

// cloudaccountSyncAdmission serializes account preparation and status probes.
// A manual preparation waits for an occupied slot and then does its own work:
// completing a status probe is not equivalent to preparing regions for a sync.
type cloudaccountSyncAdmission struct {
	mu      sync.Mutex
	pending map[string]chan struct{}
}

func (a *cloudaccountSyncAdmission) acquire(ctx context.Context, id string, wait bool) (func(), error) {
	for {
		a.mu.Lock()
		if err := ctx.Err(); err != nil {
			a.mu.Unlock()
			return nil, &cloudaccountSyncNotAdmittedError{err}
		}
		if done, exists := a.pending[id]; exists {
			a.mu.Unlock()
			if !wait {
				return nil, nil
			}
			select {
			case <-ctx.Done():
				return nil, &cloudaccountSyncNotAdmittedError{ctx.Err()}
			case <-done:
				continue
			}
		}
		if a.pending == nil {
			a.pending = make(map[string]chan struct{})
		}
		done := make(chan struct{})
		a.pending[id] = done
		a.mu.Unlock()
		return func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			delete(a.pending, id)
			close(done)
		}, nil
	}
}

func deliverCloudaccountSyncResult(ctx context.Context, result chan error, err error) {
	if result == nil {
		return
	}
	// A buffered synchronous caller must always receive completion, even after
	// cancellation, so it can drain admitted work before cleaning up its state.
	select {
	case result <- err:
		return
	default:
	}
	select {
	case result <- err:
	case <-ctx.Done():
	}
}

type cloudaccountSyncNotAdmittedError struct{ error }

func (e *cloudaccountSyncNotAdmittedError) Unwrap() error { return e.error }
func (e *cloudaccountSyncNotAdmittedError) Cause() error  { return e.error }

// CloudaccountSyncNotAdmitted reports that a request never owned preparation
// and therefore must not clean up another request's synchronization state.
func CloudaccountSyncNotAdmitted(err error) bool {
	var notAdmitted *cloudaccountSyncNotAdmittedError
	return errors.As(err, &notAdmitted)
}

func waitCloudaccountSyncResult(ctx context.Context, result <-chan error) error {
	// Once admitted, preparation must drain before its caller cleans up state.
	// A canceled admission itself completes promptly with a not-admitted error.
	if err := <-result; err != nil {
		return err
	}
	return ctx.Err()
}
