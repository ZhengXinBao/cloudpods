package ksyun

import (
	"testing"

	api "yunion.io/x/cloudmux/pkg/apis/compute"
)

func TestDiskFormat(t *testing.T) {
	for _, volumeType := range []string{"SSD3.0", "EHDD", api.STORAGE_KSYUN_LOCAL_SSD} {
		disk := SDisk{VolumeType: volumeType}
		if got := disk.GetDiskFormat(); got != "vhd" {
			t.Fatalf("volume type %s: got disk format %q, want vhd", volumeType, got)
		}
	}
}
