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

package azure

import (
	"strconv"
	"strings"

	api "yunion.io/x/cloudmux/pkg/apis"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/cloudmux/pkg/multicloud"
	"yunion.io/x/pkg/errors"
)

/*
{
	"capabilities":[
		{"name":"MaxResourceVolumeMB","value":"286720"},
		{"name":"OSVhdSizeMB","value":"1047552"},
		{"name":"vCPUs","value":"20"},
		{"name":"MemoryGB","value":"140"},
		{"name":"MaxDataDiskCount","value":"64"},
		{"name":"LowPriorityCapable","value":"True"},
		{"name":"PremiumIO","value":"True"},
		{"name":"EphemeralOSDiskSupported","value":"True"}
	],
	"family":"standardDSv2Family",
	"locations":["CentralUSEUAP"],
	"name":"Standard_DS15_v2",
	"resourceType":"virtualMachines",
	"restrictions":[],
	"size":"DS15_v2",
	"tier":"Standard"
}
*/

type SResourceSkuCapability struct {
	Name  string
	Value string
}

type TResourceSkuCapacityScaleType string

const (
	ResourceSkuCapacityScaleTypeAutomatic = TResourceSkuCapacityScaleType("Automatic")
	ResourceSkuCapacityScaleTypeManual    = TResourceSkuCapacityScaleType("Manual")
	ResourceSkuCapacityScaleTypeNone      = TResourceSkuCapacityScaleType("None")
)

type SResourceSkuCapacity struct {
	Default   int
	Maximum   int
	Minimum   int
	ScaleType TResourceSkuCapacityScaleType
}

type SResourceSkuLocationInfo struct {
	Location string
	Zones    []string
}

type TResourceSkuRestrictionsType string

const (
	ResourceSkuRestrictionsTypeLocation = TResourceSkuRestrictionsType("Location")
	ResourceSkuRestrictionsTypeZone     = TResourceSkuRestrictionsType("Zone")
)

type TResourceSkuRestrictionsReasonCode string

const (
	ResourceSkuRestrictionsReasonCodeNotAvailable = TResourceSkuRestrictionsReasonCode("NotAvailableForSubscription")
	ResourceSkuRestrictionsReasonCodeQuotaId      = TResourceSkuRestrictionsReasonCode("QuotaId")
)

type SResourceSkuRestrictionInfo struct {
	Locations []string
	Zones     []string
}

type SResourceSkuRestrictions struct {
	ReasonCode      TResourceSkuRestrictionsReasonCode
	RestrictionInfo SResourceSkuRestrictionInfo
	Type            TResourceSkuRestrictionsType
	Values          []string
}

type SResourceSku struct {
	Capabilities []SResourceSkuCapability
	Capacity     *SResourceSkuCapacity
	Family       string
	Kind         string
	LocationInfo []SResourceSkuLocationInfo
	Locations    []string
	Name         string
	ResourceType string
	Restrictions []SResourceSkuRestrictions
	Size         string
	Tier         string
}

type SResourceSkusResult struct {
	NextLink string
	Value    []SResourceSku
}

func (self *SAzureClient) ListResourceSkus() ([]SResourceSku, error) {
	skus := []SResourceSku{}
	resource := "Microsoft.Compute/skus"
	return skus, self.list(resource, nil, &skus)
}

func resourceSkuCapability(sku SResourceSku, name string) string {
	for _, capability := range sku.Capabilities {
		if strings.EqualFold(capability.Name, name) {
			return capability.Value
		}
	}
	return ""
}

func resourceSkuRegionAvailable(sku SResourceSku, region string) bool {
	region = strings.ToLower(region)
	if len(sku.Locations) > 0 {
		ok := false
		for _, location := range sku.Locations {
			if strings.EqualFold(location, region) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if len(sku.LocationInfo) > 0 {
		ok := false
		for _, location := range sku.LocationInfo {
			if strings.EqualFold(location.Location, region) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	for _, restriction := range sku.Restrictions {
		if restriction.Type != ResourceSkuRestrictionsTypeLocation {
			continue
		}
		for _, location := range append(restriction.Values, restriction.RestrictionInfo.Locations...) {
			if strings.EqualFold(location, region) {
				return false
			}
		}
	}
	return true
}

func newCloudSkuFromResourceSku(instanceType SResourceSku) *multicloud.SCloudSku {
	if instanceType.Name == "" {
		return nil
	}
	cpu, err := strconv.Atoi(resourceSkuCapability(instanceType, "vCPUs"))
	if err != nil || cpu <= 0 {
		return nil
	}
	memoryGB, err := strconv.ParseFloat(resourceSkuCapability(instanceType, "MemoryGB"), 64)
	if err != nil || memoryGB <= 0 {
		return nil
	}
	sku := multicloud.NewSCloudSku(instanceType.Name)
	sku.InstanceTypeFamily = instanceType.Family
	sku.InstanceTypeCategory = instanceType.Size
	sku.CpuCoreCount = cpu
	sku.MemorySizeMB = int(memoryGB * 1024)
	sku.CpuArch = api.OS_ARCH_X86_64
	if arch := strings.ToLower(resourceSkuCapability(instanceType, "CpuArchitecture")); strings.Contains(arch, "arm") {
		sku.CpuArch = api.OS_ARCH_ARM
	}
	if count, err := strconv.Atoi(resourceSkuCapability(instanceType, "MaxDataDiskCount")); err == nil && count > 0 {
		sku.DataDiskMaxCount = count
	}
	if gpu := resourceSkuCapability(instanceType, "GPUs"); gpu == "" {
		gpu = resourceSkuCapability(instanceType, "GPU")
		if count, err := strconv.Atoi(gpu); err == nil && count > 0 {
			sku.GpuAttachable = true
			sku.GpuCount = strconv.Itoa(count)
			sku.GpuMaxCount = count
			sku.GpuSpec = resourceSkuCapability(instanceType, "GPUName")
		}
	} else if count, err := strconv.Atoi(gpu); err == nil && count > 0 {
		sku.GpuAttachable = true
		sku.GpuCount = strconv.Itoa(count)
		sku.GpuMaxCount = count
		sku.GpuSpec = resourceSkuCapability(instanceType, "GPUName")
	}
	return sku
}

func (self *SRegion) GetISkus() ([]cloudprovider.ICloudSku, error) {
	resourceSkus, err := self.client.ListResourceSkus()
	if err != nil {
		return nil, errors.Wrap(err, "ListResourceSkus")
	}
	ret := make([]cloudprovider.ICloudSku, 0, len(resourceSkus))
	seen := map[string]bool{}
	for _, resourceSku := range resourceSkus {
		if !strings.EqualFold(resourceSku.ResourceType, "virtualMachines") ||
			!resourceSkuRegionAvailable(resourceSku, self.Name) ||
			seen[resourceSku.Name] {
			continue
		}
		sku := newCloudSkuFromResourceSku(resourceSku)
		if sku == nil {
			continue
		}
		seen[resourceSku.Name] = true
		ret = append(ret, sku)
	}
	return ret, nil
}
