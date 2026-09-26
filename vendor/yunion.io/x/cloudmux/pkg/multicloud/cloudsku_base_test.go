package multicloud

import (
	"testing"

	api "yunion.io/x/cloudmux/pkg/apis/compute"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
)

func TestNewSCloudSkuDefaults(t *testing.T) {
	var sku cloudprovider.ICloudSku = NewSCloudSku("m7i.large")

	if sku.GetId() != "m7i.large" || sku.GetName() != "m7i.large" || sku.GetGlobalId() != "m7i.large" {
		t.Fatalf("unexpected identity: %s/%s/%s", sku.GetId(), sku.GetName(), sku.GetGlobalId())
	}
	if sku.GetStatus() != api.SkuStatusAvailable {
		t.Fatalf("unexpected status: %s", sku.GetStatus())
	}
	if sku.GetPrepaidStatus() != api.SkuStatusSoldout || sku.GetPostpaidStatus() != api.SkuStatusAvailable {
		t.Fatalf("unexpected billing status: %s/%s", sku.GetPrepaidStatus(), sku.GetPostpaidStatus())
	}
	if sku.GetOsName() != "Any" || !sku.GetSysDiskResizable() {
		t.Fatalf("unexpected disk/os defaults")
	}
	if sku.GetAttachedDiskType() != "iscsi" || sku.GetDataDiskMaxCount() != 6 {
		t.Fatalf("unexpected disk defaults")
	}
	if sku.GetNicType() != "vpc" || sku.GetNicMaxCount() != 1 {
		t.Fatalf("unexpected network defaults")
	}
	if sku.GetGpuAttachable() {
		t.Fatalf("gpu should be disabled by default")
	}
}
