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

package aliyun

import (
	"fmt"

	api "yunion.io/x/cloudmux/pkg/apis"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/cloudmux/pkg/multicloud"
	"yunion.io/x/log"
	"yunion.io/x/pkg/errors"
)

// {"CpuCoreCount":1,"EniQuantity":1,"GPUAmount":0,"GPUSpec":"","InstanceTypeFamily":"ecs.t1","InstanceTypeId":"ecs.t1.xsmall","LocalStorageCategory":"","MemorySize":0.500000}
// InstanceBandwidthRx":26214400,"InstanceBandwidthTx":26214400,"InstancePpsRx":4500000,"InstancePpsTx":4500000

type SInstanceType struct {
	BaselineCredit       int
	CpuCoreCount         int
	MemorySize           float32
	EniQuantity          int // 实例规格支持网卡数量
	GPUAmount            int
	GPUSpec              string
	InstanceTypeFamily   string
	InstanceFamilyLevel  string
	InstanceTypeId       string
	LocalStorageCategory string
	LocalStorageAmount   int
	LocalStorageCapacity int64
	InstanceBandwidthRx  int
	InstanceBandwidthTx  int
	InstancePpsRx        int
	InstancePpsTx        int
}

func (self *SRegion) GetInstanceTypes() ([]SInstanceType, error) {
	params := make(map[string]string)
	params["RegionId"] = self.RegionId

	body, err := self.ecsRequest("DescribeInstanceTypes", params)
	if err != nil {
		log.Errorf("GetInstanceTypes fail %s", err)
		return nil, err
	}

	instanceTypes := make([]SInstanceType, 0)
	err = body.Unmarshal(&instanceTypes, "InstanceTypes", "InstanceType")
	if err != nil {
		log.Errorf("Unmarshal instance type details fail %s", err)
		return nil, err
	}
	return instanceTypes, nil
}

func (self *SInstanceType) memoryMB() int {
	return int(self.MemorySize * 1024)
}

func newCloudSkuFromInstanceType(instanceType SInstanceType) *multicloud.SCloudSku {
	if instanceType.InstanceTypeId == "" || instanceType.CpuCoreCount <= 0 || instanceType.MemorySize <= 0 {
		return nil
	}
	sku := multicloud.NewSCloudSku(instanceType.InstanceTypeId)
	sku.InstanceTypeFamily = instanceType.InstanceTypeFamily
	sku.InstanceTypeCategory = instanceType.InstanceTypeFamily
	sku.CpuCoreCount = instanceType.CpuCoreCount
	sku.MemorySizeMB = instanceType.memoryMB()
	sku.CpuArch = api.OS_ARCH_X86_64
	if instanceType.EniQuantity > 0 {
		sku.NicMaxCount = instanceType.EniQuantity
	}
	if instanceType.GPUAmount > 0 {
		sku.GpuAttachable = true
		sku.GpuSpec = instanceType.GPUSpec
		sku.GpuCount = fmt.Sprintf("%d", instanceType.GPUAmount)
		sku.GpuMaxCount = instanceType.GPUAmount
	}
	return sku
}

func (self *SRegion) GetISkus() ([]cloudprovider.ICloudSku, error) {
	instanceTypes, err := self.GetInstanceTypes()
	if err != nil {
		return nil, errors.Wrap(err, "GetInstanceTypes")
	}
	ret := make([]cloudprovider.ICloudSku, 0, len(instanceTypes))
	seen := map[string]bool{}
	for _, instanceType := range instanceTypes {
		if seen[instanceType.InstanceTypeId] {
			continue
		}
		sku := newCloudSkuFromInstanceType(instanceType)
		if sku == nil {
			continue
		}
		seen[instanceType.InstanceTypeId] = true
		ret = append(ret, sku)
	}
	return ret, nil
}
