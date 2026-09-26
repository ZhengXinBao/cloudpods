package models

import (
	"context"
	"fmt"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/cloudmux/pkg/multicloud/aws"
	"yunion.io/x/pkg/errors"
)

type failingKubeNodeCluster struct {
	cloudprovider.ICloudKubeCluster
	failure error
}

func (c failingKubeNodeCluster) GetIKubeNodes() ([]cloudprovider.ICloudKubeNode, error) {
	return nil, c.failure
}

func TestOptionalKubeNodeFetchPreservesExistingNodes(t *testing.T) {
	for _, tc := range []struct {
		failure error
		skip    bool
	}{
		{cloudprovider.ErrNotImplemented, true},
		{cloudprovider.ErrNotSupported, true},
		{errors.Wrap(cloudprovider.ErrNotImplemented, "SDK"), true},
		{errors.Wrap(cloudprovider.ErrNotSupported, "SDK"), true},
		{fmt.Errorf("NotImplementedError"), false},
		{fmt.Errorf("AccessDenied"), false},
		{fmt.Errorf("connection reset"), false},
	} {
		t.Run(tc.failure.Error(), func(t *testing.T) {
			ctx, collector := WithCloudSyncErrors(context.Background())
			results := SSyncResultSet{}
			err := syncKubeClusterNodes(ctx, nil, results, &SKubeCluster{}, failingKubeNodeCluster{failure: tc.failure})
			if (err == nil) != tc.skip || (collector.Error() == nil) != tc.skip {
				t.Fatalf("error=%v collected=%v wantSkip=%v", err, collector.Error(), tc.skip)
			}
			if !tc.skip && errors.Cause(err) != tc.failure {
				t.Fatalf("original failure lost: %v", err)
			}
			for _, result := range results {
				if result.SqlCost != "" || result.AddCnt+result.UpdateCnt+result.DelCnt+result.AddErrCnt+result.UpdateErrCnt+result.DelErrCnt != 0 {
					t.Fatalf("failed/unsupported fetch entered node reconciliation: %#v", result)
				}
			}
		})
	}
}

func TestAWSKubeNodeStubIsOptional(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	results := SSyncResultSet{}
	err := syncKubeClusterNodes(ctx, nil, results, &SKubeCluster{}, &aws.SKubeCluster{})
	if err != nil || collector.Error() != nil {
		t.Fatalf("AWS unimplemented node fetch reported failure: %v / %v", err, collector.Error())
	}
	if results[KubeNodeManager.KeywordPlural()].SqlCost != "" {
		t.Fatal("AWS stub must never reconcile/delete existing nodes")
	}
}
