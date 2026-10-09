package models

import (
	"context"
	stderrors "errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/log"
	"yunion.io/x/pkg/errors"
	"yunion.io/x/pkg/util/compare"

	"yunion.io/x/onecloud/pkg/compute/syncqueue"
)

type cloudSyncErrorsKey struct{}

// CloudSyncErrors captures otherwise log-only failures for one worker execution.
// Collection is bounded because a large cloud may generate many resource errors.
type CloudSyncErrors struct {
	mu       sync.Mutex
	count    int
	messages []string
	classes  map[syncqueue.ErrorClass]int
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
	return syncqueue.NewErrorSummary(fmt.Sprintf("%d cloud synchronization errors: %s", c.count, strings.Join(c.messages, "; ")), c.classes)
}

// Merge retains an executor's original cause while including otherwise log-only
// failures. With no collected failures, callback error identity stays unchanged.
func (c *CloudSyncErrors) Merge(err error) error {
	collected := c.Error()
	if collected == nil {
		return err
	}
	if err == nil {
		return collected
	}
	return stderrors.Join(err, collected)
}

func cloudSyncError(ctx context.Context, format string, args ...interface{}) {
	collector, _ := ctx.Value(cloudSyncErrorsKey{}).(*CloudSyncErrors)
	if collector == nil {
		log.Errorf(format, args...)
		return
	}
	// Capture structured causes before either diagnostic formatter destroys type
	// information. Never retain the provider error or use its rendered body to
	// override an unknown SDK code. Collect every argument, even after message #8.
	classes := make(map[syncqueue.ErrorClass]int)
	for _, arg := range args {
		if err, ok := arg.(error); ok && err != nil {
			addCloudSyncClasses(classes, syncqueue.ErrorClassCounts(err))
		}
	}
	message := fmt.Sprintf(format, args...)
	if len(classes) == 0 {
		classes = syncqueue.ErrorClassCounts(stderrors.New(message))
	}
	collector.add(message, classes)
	log.Errorf(format, args...)
}

func addCloudSyncClasses(dst, src map[syncqueue.ErrorClass]int) {
	for class, count := range src {
		dst[class] += count
	}
}

func (c *CloudSyncErrors) add(message string, classes map[syncqueue.ErrorClass]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
	if c.classes == nil {
		c.classes = make(map[syncqueue.ErrorClass]int)
	}
	addCloudSyncClasses(c.classes, classes)
	if len(c.messages) < 8 {
		c.messages = append(c.messages, boundedCloudSyncMessage(message))
	}
}

func boundedCloudSyncMessage(message string) string {
	return syncqueue.BoundedErrorMessage(message, 512)
}

func syncResultSetError(results SSyncResultSet) error {
	resources := make([]string, 0, len(results))
	for resource, result := range results {
		if result != nil && (result.IsError() || result.AddErrCnt > 0 || result.UpdateErrCnt > 0 || result.DelErrCnt > 0) {
			resources = append(resources, resource)
		}
	}
	sort.Strings(resources)
	collector := &CloudSyncErrors{}
	for _, resource := range resources {
		result := results[resource]
		message := fmt.Sprintf("resource sync %s has reconciliation errors", resource)
		classes := map[syncqueue.ErrorClass]int{syncqueue.ErrorClassUnknown: 1}
		if cause := summarizeSyncResultErrors(&result.SyncResult); cause != nil {
			classes = syncqueue.ErrorClassCounts(cause)
			message += ": " + cause.Error()
		}
		collector.add(message, classes)
	}
	return collector.Error()
}

// Capture classifications before either Add or final result formatting loses
// SDK types. Retain only fixed-vocabulary counts and the first eight sorted,
// bounded diagnostics, never the SDK errors or a flattened-body classification.
func summarizeSyncResultErrors(result *compare.SyncResult) error {
	causes := result.Errors()
	if len(causes) == 0 {
		return nil
	}
	classes := make(map[syncqueue.ErrorClass]int)
	parts := make([]string, 0, 8)
	for _, cause := range causes {
		part := "unknown cloud synchronization error"
		if cause == nil {
			classes[syncqueue.ErrorClassUnknown]++
		} else {
			addCloudSyncClasses(classes, syncqueue.ErrorClassCounts(cause))
			part = boundedCloudSyncMessage(boundedSyncResultMessage(cause.Error()))
		}
		idx := sort.SearchStrings(parts, part)
		if idx < len(parts) && parts[idx] == part {
			continue
		}
		if idx >= 8 {
			continue
		}
		parts = append(parts, "")
		copy(parts[idx+1:], parts[idx:])
		parts[idx] = part
		if len(parts) > 8 {
			parts = parts[:8]
		}
	}
	return syncqueue.NewErrorSummary(fmt.Sprintf("%d reconciliation causes: %s", len(causes), strings.Join(parts, "; ")), classes)
}

// Normalize legacy flattened diagnostics for display only. Classification is
// already captured from the original causes and never uses these fragments.
func boundedSyncResultMessage(message string) string {
	parts := make([]string, 0, 8)
	for message != "" {
		part, rest, _ := strings.Cut(message, ";")
		message = rest
		part = boundedCloudSyncMessage(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		idx := sort.SearchStrings(parts, part)
		if idx >= 8 || idx < len(parts) && parts[idx] == part {
			continue
		}
		parts = append(parts, "")
		copy(parts[idx+1:], parts[idx:])
		parts[idx] = part
		if len(parts) > 8 {
			parts = parts[:8]
		}
	}
	return strings.Join(parts, "; ")
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
