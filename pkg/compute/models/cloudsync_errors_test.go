package models

import (
	"context"
	"strings"
	"sync"
	"testing"
)

func TestCloudSyncErrorsConcurrentAndIsolated(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	_, other := WithCloudSyncErrors(context.Background())
	if collector.Error() != nil {
		t.Fatal("empty collector reports failure")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); cloudSyncError(ctx, "cloud request failed: %s", "timeout") }()
	}
	wg.Wait()
	if err := collector.Error(); err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatal("lost cloud failures", err)
	}
	if other.Error() != nil {
		t.Fatal("collector leaked into another job")
	}
	cloudSyncError(context.Background(), "legacy cloud failure")
	if other.Error() != nil {
		t.Fatal("normal context collected an error")
	}
}
func TestCloudSyncErrorsBounded(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	for i := 0; i < 100; i++ {
		cloudSyncError(ctx, "failure %d", i)
	}
	err := collector.Error()
	if err == nil || len(err.Error()) > 8192 || !strings.Contains(err.Error(), "100") {
		t.Fatalf("unbounded or missing failure count: %v", err)
	}
}

func TestCloudSyncErrorsKeepsRootCauseSuffix(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	cloudSyncError(ctx, "DescribeCluster: %s: tags type mismatch", strings.Repeat("payload", 200))
	err := collector.Error()
	if err == nil || !strings.Contains(err.Error(), "DescribeCluster") || !strings.Contains(err.Error(), "tags type mismatch") {
		t.Fatalf("truncation hid operation or root cause: %v", err)
	}
	if len(err.Error()) > 600 {
		t.Fatalf("unbounded diagnostic: %d", len(err.Error()))
	}
}
