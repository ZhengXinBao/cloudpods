package aws

import "testing"

func TestNewCloudSkuFromInstanceType(t *testing.T) {
	sku := newCloudSkuFromInstanceType(Sku{
		InstanceType: "m7i.large",
		MemoryInfo: struct {
			SizeInMiB int `xml:"sizeInMiB"`
		}{SizeInMiB: 8192},
		VCpuInfo: struct {
			DefaultVCpus int `xml:"defaultVCpus"`
		}{DefaultVCpus: 2},
	})
	if sku.GetId() != "m7i.large" || sku.GetCpuCoreCount() != 2 || sku.GetMemorySizeMB() != 8192 {
		t.Fatalf("unexpected sku %s cpu=%d memory=%d", sku.GetId(), sku.GetCpuCoreCount(), sku.GetMemorySizeMB())
	}
}
