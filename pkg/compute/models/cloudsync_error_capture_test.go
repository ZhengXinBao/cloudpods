package models

import (
	"context"
	"errors"
	"strings"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/pkg/util/compare"
)

type failingSyncRegion struct {
	cloudprovider.ICloudRegion
	failure error
}

func (r failingSyncRegion) GetName() string                                { return "test region" }
func (r failingSyncRegion) GetIZones() ([]cloudprovider.ICloudZone, error) { return nil, r.failure }
func (r failingSyncRegion) GetICloudAccessGroups() ([]cloudprovider.ICloudAccessGroup, error) {
	return nil, r.failure
}

func TestCloudSyncCollectorCapturesFetchFailureWithoutResultCounters(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	results := SSyncResultSet{}
	_, _, _ = syncRegionZones(ctx, nil, results, &SCloudprovider{}, &SCloudregion{}, failingSyncRegion{failure: errors.New("cloud unavailable")}, false)
	if err := collector.Error(); err == nil || !strings.Contains(err.Error(), "cloud unavailable") {
		t.Fatalf("lost otherwise ignored zone failure: %v", err)
	}
	for _, result := range results {
		if result.AddErrCnt+result.UpdateErrCnt+result.DelErrCnt != 0 {
			t.Fatal("test did not reproduce missing result counters")
		}
	}
}
func TestCloudSyncCollectorPreservesUnsupportedResourceSkip(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	syncRegionAccessGroups(ctx, nil, SSyncResultSet{}, &SCloudprovider{}, &SCloudregion{}, failingSyncRegion{failure: cloudprovider.ErrNotSupported}, &SSyncRange{})
	if err := collector.Error(); err != nil {
		t.Fatalf("optional unsupported API treated as failure: %v", err)
	}
	syncRegionAccessGroups(ctx, nil, SSyncResultSet{}, &SCloudprovider{}, &SCloudregion{}, failingSyncRegion{failure: errors.New("access group request failed")}, &SSyncRange{})
	if err := collector.Error(); err == nil {
		t.Fatal("lost access group fetch failure")
	}
}

type failingSyncVpc struct {
	cloudprovider.ICloudVpc
	failure error
}

func (v failingSyncVpc) GetId() string { return "test-vpc" }
func (v failingSyncVpc) GetICloudIPv6Gateways() ([]cloudprovider.ICloudIPv6Gateway, error) {
	return nil, v.failure
}

func TestIPv6GatewayOptionalAPIErrors(t *testing.T) {
	for _, failure := range []error{cloudprovider.ErrNotImplemented, cloudprovider.ErrNotSupported, errors.New("permission denied")} {
		ctx, collector := WithCloudSyncErrors(context.Background())
		syncIPv6Gateways(ctx, nil, SSyncResultSet{}, &SCloudprovider{}, &SVpc{}, failingSyncVpc{failure: failure}, &SSyncRange{})
		wantFailure := failure != cloudprovider.ErrNotImplemented && failure != cloudprovider.ErrNotSupported
		if (collector.Error() != nil) != wantFailure {
			t.Errorf("API error %v: collected=%v, want failure=%v", failure, collector.Error(), wantFailure)
		}
	}
}

func (r failingSyncRegion) GetISecurityGroups() ([]cloudprovider.ICloudSecurityGroup, error) {
	return nil, r.failure
}

func TestRegionSecurityGroupOptionalAPIErrors(t *testing.T) {
	for _, failure := range []error{cloudprovider.ErrNotImplemented, cloudprovider.ErrNotSupported, errors.New("permission denied")} {
		ctx, collector := WithCloudSyncErrors(context.Background())
		syncRegionSecGroup(ctx, nil, SSyncResultSet{}, &SCloudprovider{}, &SCloudregion{}, failingSyncRegion{failure: failure}, &SSyncRange{})
		wantFailure := failure != cloudprovider.ErrNotImplemented && failure != cloudprovider.ErrNotSupported
		if (collector.Error() != nil) != wantFailure {
			t.Errorf("API error %v: collected=%v, want failure=%v", failure, collector.Error(), wantFailure)
		}
	}
}

func TestSyncResultSetErrorUsesCurrentExecution(t *testing.T) {
	results := SSyncResultSet{"guests": &SyncResult{}}
	results["guests"].UpdateErrCnt = 1
	if err := syncResultSetError(results); err == nil {
		t.Fatal("current reconciliation errors must fail the queue job")
	}
	if err := syncResultSetError(SSyncResultSet{"guests": &SyncResult{}}); err != nil {
		t.Fatalf("clean current result set reported failure: %v", err)
	}
}

func TestSyncResultSetErrorKeepsNonCounterErrors(t *testing.T) {
	var result compare.SyncResult
	result.Error(errors.New("compare query failed"))
	results := SSyncResultSet{}
	results.Add(ExternalProjectManager, result)
	if err := syncResultSetError(results); err == nil || !strings.Contains(err.Error(), "project") {
		t.Fatalf("non-counter reconciliation error was lost: %v", err)
	}
}

func TestSyncResultSetErrorPreservesUnderlyingFailure(t *testing.T) {
	var result compare.SyncResult
	result.UpdateError(errors.New("AccessDenied: sts:AssumeRole"))
	results := SSyncResultSet{}
	results.Add(ExternalProjectManager, result)
	err := syncResultSetError(results)
	if err == nil || !strings.Contains(err.Error(), "AccessDenied: sts:AssumeRole") {
		t.Fatalf("lost actionable reconciliation cause: %v", err)
	}
}
