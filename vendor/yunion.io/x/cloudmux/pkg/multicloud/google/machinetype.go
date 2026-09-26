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

package google

import (
	"fmt"
	"strings"
	"time"

	api "yunion.io/x/cloudmux/pkg/apis"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/cloudmux/pkg/multicloud"
	"yunion.io/x/pkg/errors"
)

type SMachineType struct {
	Id                           string
	CreationTimestamp            time.Time
	Name                         string
	Description                  string
	GuestCpus                    int
	MemoryMb                     int
	ImageSpaceGb                 int
	MaximumPersistentDisks       int
	MaximumPersistentDisksSizeGb int
	Zone                         string
	SelfLink                     string
	IsSharedCpu                  bool
	Kind                         string
}

func (region *SRegion) GetMachineTypes(zone string, maxResults int, pageToken string) ([]SMachineType, error) {
	machines := []SMachineType{}
	params := map[string]string{}
	if len(zone) == 0 {
		return nil, cloudprovider.ErrNotFound
	}
	resource := fmt.Sprintf("zones/%s/machineTypes", zone)
	return machines, region.List(resource, params, maxResults, pageToken, &machines)
}

func (region *SRegion) GetMachineType(id string) (*SMachineType, error) {
	machine := &SMachineType{}
	err := region.client.ecsGet("machineTypes", id, machine)
	if err != nil {
		return nil, err
	}
	return machine, nil
}

func newCloudSkuFromMachineType(machine SMachineType) *multicloud.SCloudSku {
	if machine.Name == "" || machine.GuestCpus <= 0 || machine.MemoryMb <= 0 {
		return nil
	}
	family := machine.Name
	if idx := strings.LastIndexByte(family, '-'); idx > 0 {
		family = family[:idx]
	}
	sku := multicloud.NewSCloudSku(machine.Name)
	sku.InstanceTypeFamily = family
	sku.InstanceTypeCategory = family
	sku.CpuCoreCount = machine.GuestCpus
	sku.MemorySizeMB = machine.MemoryMb
	sku.CpuArch = api.OS_ARCH_X86_64
	return sku
}

func (region *SRegion) GetISkus() ([]cloudprovider.ICloudSku, error) {
	zones, err := region.GetZones(region.Name, 0, "")
	if err != nil {
		return nil, errors.Wrap(err, "GetZones")
	}

	ret := make([]cloudprovider.ICloudSku, 0)
	seen := make(map[string]bool)
	for _, zone := range zones {
		machines, err := region.GetMachineTypes(zone.Name, 0, "")
		if err != nil {
			return nil, errors.Wrapf(err, "GetMachineTypes(%s)", zone.Name)
		}
		for _, machine := range machines {
			if seen[machine.Name] {
				continue
			}
			sku := newCloudSkuFromMachineType(machine)
			if sku == nil {
				continue
			}
			seen[machine.Name] = true
			ret = append(ret, sku)
		}
	}
	return ret, nil
}
