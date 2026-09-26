package models

import (
	"context"
	"errors"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/onecloud/pkg/mcclient"
)

type inventoryFailureRegion struct {
	cloudprovider.ICloudRegion
	failure error
}

func (r inventoryFailureRegion) GetName() string { return "inventory-test" }
func (r inventoryFailureRegion) GetILoadBalancers() ([]cloudprovider.ICloudLoadbalancer, error) {
	return []cloudprovider.ICloudLoadbalancer{nil}, r.failure
}
func (r inventoryFailureRegion) GetILoadBalancerAcls() ([]cloudprovider.ICloudLoadbalancerAcl, error) {
	return nil, r.failure
}
func (r inventoryFailureRegion) GetILoadBalancerCertificates() ([]cloudprovider.ICloudLoadbalancerCertificate, error) {
	return nil, r.failure
}
func (r inventoryFailureRegion) GetILoadBalancerHealthChecks() ([]cloudprovider.ICloudLoadbalancerHealthCheck, error) {
	return nil, r.failure
}

func TestLoadbalancerInventoryFetchFailuresAndUnsupported(t *testing.T) {
	for _, failure := range []error{errors.New("page 2 permission denied"), cloudprovider.ErrNotImplemented, cloudprovider.ErrNotSupported} {
		for _, sync := range []func(context.Context, mcclient.TokenCredential, SSyncResultSet, *SCloudprovider, *SCloudregion, cloudprovider.ICloudRegion, *SSyncRange){syncRegionLoadbalancers, syncRegionLoadbalancerAcls, syncRegionLoadbalancerCertificates, syncRegionLoadbalancerHealthChecks} {
			ctx, collector := WithCloudSyncErrors(context.Background())
			sync(ctx, nil, SSyncResultSet{}, &SCloudprovider{}, &SCloudregion{}, inventoryFailureRegion{failure: failure}, &SSyncRange{})
			want := failure != cloudprovider.ErrNotImplemented && failure != cloudprovider.ErrNotSupported
			if (collector.Error() != nil) != want {
				t.Errorf("fetch %v: collector=%v want failure=%v", failure, collector.Error(), want)
			}
		}
	}
}

func (r inventoryFailureRegion) GetIEips() ([]cloudprovider.ICloudEIP, error) { return nil, r.failure }
func (r inventoryFailureRegion) GetIDBInstances() ([]cloudprovider.ICloudDBInstance, error) {
	return nil, r.failure
}
func (r inventoryFailureRegion) GetIElasticcaches() ([]cloudprovider.ICloudElasticcache, error) {
	return nil, r.failure
}

func TestRegionalInventoryFetchFailuresAndUnsupported(t *testing.T) {
	for _, failure := range []error{errors.New("page 2 permission denied"), cloudprovider.ErrNotImplemented, cloudprovider.ErrNotSupported} {
		for _, sync := range []func(context.Context, mcclient.TokenCredential, SSyncResultSet, *SCloudprovider, *SCloudregion, cloudprovider.ICloudRegion, *SSyncRange){syncRegionEips, syncRegionDBInstances, syncElasticcaches} {
			ctx, collector := WithCloudSyncErrors(context.Background())
			sync(ctx, nil, SSyncResultSet{}, &SCloudprovider{}, &SCloudregion{}, inventoryFailureRegion{failure: failure}, &SSyncRange{})
			want := failure != cloudprovider.ErrNotImplemented && failure != cloudprovider.ErrNotSupported
			if (collector.Error() != nil) != want {
				t.Errorf("fetch %v: collector=%v want failure=%v", failure, collector.Error(), want)
			}
		}
	}
}
