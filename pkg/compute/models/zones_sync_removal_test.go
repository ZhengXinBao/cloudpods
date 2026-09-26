package models

import (
	"errors"
	"strings"
	"testing"

	_ "yunion.io/x/cloudmux/pkg/multicloud/aws/provider"
	_ "yunion.io/x/cloudmux/pkg/multicloud/openstack/provider"
	"yunion.io/x/pkg/util/compare"
)

func TestMissingPublicZoneIsRetainedWithoutDeleteCount(t *testing.T) {
	calls := 0
	var result compare.SyncResult
	reconcileMissingCloudZones(&SCloudregion{Provider: "Aws"}, []SZone{{}}, &result, func(*SZone) error {
		calls++
		return errors.New("contains 47 networks")
	})
	if calls != 0 || result.DelCnt != 0 || result.DelErrCnt != 0 || result.IsError() {
		t.Fatalf("account omission must retain shared zone: calls=%d result=%s", calls, result.Result())
	}
}

func TestMissingPrivateZonePreservesDeletionAndErrors(t *testing.T) {
	for _, failure := range []error{nil, errors.New("database unavailable"), errors.New("contains 3 networks")} {
		calls := 0
		var result compare.SyncResult
		reconcileMissingCloudZones(&SCloudregion{Provider: "OpenStack"}, []SZone{{}}, &result, func(*SZone) error { calls++; return failure })
		if calls != 1 {
			t.Fatalf("private cloud deletion calls=%d", calls)
		}
		if failure == nil {
			if result.DelCnt != 1 || result.IsError() {
				t.Fatalf("deletion not counted: %s", result.Result())
			}
		} else if result.DelCnt != 0 || result.DelErrCnt != 1 || !strings.Contains(result.AllError().Error(), failure.Error()) {
			t.Fatalf("real deletion error lost: %s", result.Result())
		}
	}
}

func TestMissingZoneUnknownOwnershipFailsWithoutDeleting(t *testing.T) {
	var result compare.SyncResult
	calls := 0
	reconcileMissingCloudZones(&SCloudregion{Provider: "unregistered-cloud"}, []SZone{{}}, &result, func(*SZone) error { calls++; return nil })
	if calls != 0 || !result.IsError() || result.DelCnt != 0 {
		t.Fatalf("unknown ownership must not authorize deletion: calls=%d result=%s", calls, result.Result())
	}
}
