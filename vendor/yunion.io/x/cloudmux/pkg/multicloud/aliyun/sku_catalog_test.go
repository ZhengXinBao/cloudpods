package aliyun

import "testing"

func TestNewCloudSkuFromInstanceType(t *testing.T) {
	sku := newCloudSkuFromInstanceType(SInstanceType{
		InstanceTypeId:     "ecs.g7.large",
		InstanceTypeFamily: "g7",
		CpuCoreCount:       2,
		MemorySize:         8,
		EniQuantity:        3,
	})
	if sku.GetId() != "ecs.g7.large" || sku.GetCpuCoreCount() != 2 || sku.GetMemorySizeMB() != 8192 || sku.GetNicMaxCount() != 3 {
		t.Fatalf("unexpected sku %s cpu=%d memory=%d nic=%d", sku.GetId(), sku.GetCpuCoreCount(), sku.GetMemorySizeMB(), sku.GetNicMaxCount())
	}
}
