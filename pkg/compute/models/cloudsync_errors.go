package models

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/log"
	"yunion.io/x/pkg/errors"
)

type cloudSyncErrorsKey struct{}

// CloudSyncErrors captures otherwise log-only failures for one worker execution.
// Collection is bounded because a large cloud may generate many resource errors.
type CloudSyncErrors struct {
	mu       sync.Mutex
	count    int
	messages []string
}

// WithCloudSyncErrors opts an execution into failure collection. Ordinary legacy
// callers retain their existing logging and return-value behavior.
func WithCloudSyncErrors(ctx context.Context) (context.Context, *CloudSyncErrors) {
	collector := &CloudSyncErrors{}
	return context.WithValue(ctx, cloudSyncErrorsKey{}, collector), collector
}
func (c *CloudSyncErrors) Error() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.count == 0 {
		return nil
	}
	return fmt.Errorf("%d cloud synchronization errors: %s", c.count, strings.Join(c.messages, "; "))
}
func cloudSyncError(ctx context.Context, format string, args ...interface{}) {
	log.Errorf(format, args...)
	collector, _ := ctx.Value(cloudSyncErrorsKey{}).(*CloudSyncErrors)
	if collector == nil {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.count++
	if len(collector.messages) >= 8 {
		return
	}
	message := fmt.Sprintf(format, args...)
	if len(message) > 512 {
		// Decoder errors often end with the actionable cause after a large
		// response body. Keep both operation context and that cause bounded.
		message = message[:256] + "..." + message[len(message)-256:]
	}
	collector.messages = append(collector.messages, message)
}

func syncResultSetError(results SSyncResultSet) error {
	for resource, result := range results {
		if result == nil {
			continue
		}
		if result.IsError() {
			return fmt.Errorf("resource sync %s has reconciliation errors: %w", resource, result.AllError())
		}
		if result.AddErrCnt+result.UpdateErrCnt+result.DelErrCnt > 0 {
			return fmt.Errorf("resource sync %s has reconciliation errors", resource)
		}
	}
	return nil
}

// skipOptionalCloudSync is only for optional SDK fetches, before reconciliation.
// An unavailable capability must not be reconciled as an empty cloud inventory.
// Match SDK sentinels, never error text: permission and request errors must fail.
func skipOptionalCloudSync(err error, operation string) bool {
	cause := errors.Cause(err)
	if cause != cloudprovider.ErrNotImplemented && cause != cloudprovider.ErrNotSupported {
		return false
	}
	log.Infof("Skip optional cloud synchronization %s: %v", operation, err)
	return true
}
