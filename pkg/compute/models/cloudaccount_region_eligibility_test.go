package models

import (
	"errors"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
)

type optInCloudRegion struct {
	*fakeCloudRegion
	optInStatus string
}

func (r optInCloudRegion) GetRegionOptInStatus() string { return r.optInStatus }

func TestCloudRegionDiscoveryUsesAccountOptInStatus(t *testing.T) {
	for _, status := range []string{"not-opted-in", "opted-in", "opt-in-not-required", "", "unknown"} {
		t.Run(status, func(t *testing.T) {
			inventory := &fakeCloudRegion{vms: []cloudprovider.ICloudVM{nil}}
			has, err := cloudRegionHasResources(optInCloudRegion{inventory, status})
			if err != nil {
				t.Fatal(err)
			}
			if status == "not-opted-in" {
				if has || inventory.vmCalls != 0 {
					t.Fatal("disabled region queried inventory or reported resources")
				}
			} else if !has || inventory.vmCalls != 1 {
				t.Fatal("eligible or unknown region was skipped")
			}
		})
	}
}

func TestCloudRegionDiscoveryPreservesActiveRegionAuthFailure(t *testing.T) {
	for _, status := range []string{"opted-in", "opt-in-not-required", ""} {
		for _, message := range []string{"AuthFailure", "InvalidClientTokenId", "AccessDenied: sts:AssumeRole", "RegionDisabled"} {
			failure := errors.New(message)
			has, err := cloudRegionHasResources(optInCloudRegion{&fakeCloudRegion{vmErr: failure}, status})
			if has || err != failure {
				t.Fatalf("status=%q failure=%q: has=%v err=%v", status, message, has, err)
			}
		}
	}
}
