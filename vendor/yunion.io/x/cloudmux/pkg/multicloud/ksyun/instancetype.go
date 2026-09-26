// Copyright 2019 Yunion
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ksyun

import (
	"fmt"

	api "yunion.io/x/cloudmux/pkg/apis"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/cloudmux/pkg/multicloud"
	"yunion.io/x/pkg/errors"
)

type SInstanceType struct {
	InstanceType          string
	InstanceFamily        string
	InstanceFamilyName    string
	CPU                   int
	Memory                int
	NetworkInterfaceQuota struct {
		NetworkInterfaceCount int
	}
	PrivateIpQuota struct {
		PrivateIpCount int
	}
	AvailabilityZoneSet []struct {
		AzCode string
	}
	SystemDiskQuotaSet []struct {
		SystemDiskType string
	}
	DataDiskQuotaSet []struct {
		DataDiskType        string
		DataDiskMinSize     int
		DataDiskMaxsize     int
		DataDiskCount       int
		AvailabilityZoneSet []struct {
			AzCode string
		}
	}
}

func (region *SRegion) GetInstanceTypes() ([]SInstanceType, error) {
	params := make(map[string]interface{})
	params["Region"] = region.Region
	zones, err := region.GetZones()
	if err != nil {
		return nil, err
	}

	params["Filter.1.Name.1"] = "availability-zone"
	for i, zone := range zones {
		params[fmt.Sprintf("Filter.1.Value.%d", i+1)] = zone.AvailabilityZone
	}

	resp, err := region.ecsRequest("DescribeInstanceTypeConfigs", params)
	if err != nil {
		return nil, err
	}
	ret := []SInstanceType{}
	err = resp.Unmarshal(&ret, "InstanceTypeConfigSet")
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal instance types")
	}

	return ret, nil
}

func newCloudSkuFromInstanceType(instanceType SInstanceType) *multicloud.SCloudSku {
	if instanceType.InstanceType == "" || instanceType.CPU <= 0 || instanceType.Memory <= 0 {
		return nil
	}
	sku := multicloud.NewSCloudSku(instanceType.InstanceType)
	sku.InstanceTypeFamily = instanceType.InstanceFamily
	sku.InstanceTypeCategory = instanceType.InstanceFamily
	sku.CpuCoreCount = instanceType.CPU
	sku.MemorySizeMB = instanceType.Memory * 1024
	sku.CpuArch = api.OS_ARCH_X86_64
	if count := instanceType.NetworkInterfaceQuota.NetworkInterfaceCount; count > 0 {
		sku.NicMaxCount = count
	}
	for _, disk := range instanceType.DataDiskQuotaSet {
		if disk.DataDiskCount > sku.DataDiskMaxCount {
			sku.DataDiskMaxCount = disk.DataDiskCount
		}
	}
	return sku
}

func (region *SRegion) GetISkus() ([]cloudprovider.ICloudSku, error) {
	instanceTypes, err := region.GetInstanceTypes()
	if err != nil {
		return nil, errors.Wrap(err, "GetInstanceTypes")
	}
	ret := make([]cloudprovider.ICloudSku, 0, len(instanceTypes))
	seen := make(map[string]bool)
	for _, instanceType := range instanceTypes {
		if seen[instanceType.InstanceType] {
			continue
		}
		sku := newCloudSkuFromInstanceType(instanceType)
		if sku == nil {
			continue
		}
		seen[instanceType.InstanceType] = true
		ret = append(ret, sku)
	}
	return ret, nil
}
