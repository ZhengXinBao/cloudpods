// Copyright 2023 Yunion
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

package volcengine

import (
	"strconv"

	api "yunion.io/x/cloudmux/pkg/apis"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/cloudmux/pkg/multicloud"
	"yunion.io/x/pkg/errors"
)

type SInstanceType struct {
	InstanceTypeId     string
	InstanceTypeFamily string
	Memory             struct {
		Size int
	}
	Processor struct {
		Cpus int
	}
	Network struct {
		MaximumNetworkInterfaces int
	}
	Gpu struct {
		GpuDevices []struct {
			Count       int
			ProductName string
		}
	}
}

func newCloudSkuFromInstanceType(instanceType SInstanceType) *multicloud.SCloudSku {
	if instanceType.InstanceTypeId == "" || instanceType.Processor.Cpus <= 0 || instanceType.Memory.Size <= 0 {
		return nil
	}
	family := instanceType.InstanceTypeFamily
	if family == "" {
		family = instanceType.InstanceTypeId
	}
	sku := multicloud.NewSCloudSku(instanceType.InstanceTypeId)
	sku.InstanceTypeFamily = family
	sku.InstanceTypeCategory = family
	sku.CpuCoreCount = instanceType.Processor.Cpus
	sku.MemorySizeMB = instanceType.Memory.Size
	sku.CpuArch = api.OS_ARCH_X86_64
	if instanceType.Network.MaximumNetworkInterfaces > 0 {
		sku.NicMaxCount = instanceType.Network.MaximumNetworkInterfaces
	}
	gpuCount := 0
	for _, gpu := range instanceType.Gpu.GpuDevices {
		if gpu.Count <= 0 {
			continue
		}
		gpuCount += gpu.Count
		if sku.GpuSpec == "" {
			sku.GpuSpec = gpu.ProductName
		}
	}
	if gpuCount > 0 {
		sku.GpuAttachable = true
		sku.GpuCount = strconv.Itoa(gpuCount)
		sku.GpuMaxCount = gpuCount
	}
	return sku
}

func (region *SRegion) GetISkus() ([]cloudprovider.ICloudSku, error) {
	params := map[string]string{"MaxResults": "100"}
	ret := make([]cloudprovider.ICloudSku, 0)
	seen := make(map[string]bool)
	seenTokens := make(map[string]bool)
	token := ""
	for {
		if token != "" {
			if seenTokens[token] {
				return nil, errors.Errorf("DescribeInstanceTypes returned repeated next token %q", token)
			}
			seenTokens[token] = true
			params["NextToken"] = token
		}
		resp, err := region.ecsRequest("DescribeInstanceTypes", params)
		if err != nil {
			return nil, errors.Wrap(err, "DescribeInstanceTypes")
		}
		part := struct {
			InstanceTypes []SInstanceType
			NextToken     string
		}{}
		if err := resp.Unmarshal(&part); err != nil {
			return nil, errors.Wrap(err, "unmarshal instance types")
		}
		for _, instanceType := range part.InstanceTypes {
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
		if part.NextToken == "" {
			break
		}
		token = part.NextToken
	}
	return ret, nil
}
