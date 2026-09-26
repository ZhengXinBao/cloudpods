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

package aws

import (
	"fmt"
	"strconv"
	"strings"

	api "yunion.io/x/cloudmux/pkg/apis"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/cloudmux/pkg/multicloud"
	"yunion.io/x/pkg/errors"
)

type Product struct {
	ProductFamily string     `json:"productFamily"`
	Attributes    Attributes `json:"attributes"`
	Sku           string     `json:"sku"`
}

type Attributes struct {
	Availabilityzone            string `json:"availabilityzone"`
	Classicnetworkingsupport    string `json:"classicnetworkingsupport"`
	GPUMemory                   string `json:"gpuMemory"`
	Instancesku                 string `json:"instancesku"`
	Marketoption                string `json:"marketoption"`
	RegionCode                  string `json:"regionCode"`
	Vpcnetworkingsupport        string `json:"vpcnetworkingsupport"`
	EnhancedNetworkingSupported string `json:"enhancedNetworkingSupported"`
	IntelTurboAvailable         string `json:"intelTurboAvailable"`
	Memory                      string `json:"memory"`
	DedicatedEbsThroughput      string `json:"dedicatedEbsThroughput"`
	Vcpu                        int    `json:"vcpu"`
	Gpu                         int    `json:"gpu"`
	Capacitystatus              string `json:"capacitystatus"`
	LocationType                string `json:"locationType"`
	Storage                     string `json:"storage"`
	InstanceFamily              string `json:"instanceFamily"`
	OperatingSystem             string `json:"operatingSystem"`
	IntelAvx2Available          string `json:"intelAvx2Available"`
	PhysicalProcessor           string `json:"physicalProcessor"`
	ClockSpeed                  string `json:"clockSpeed"`
	Ecu                         string `json:"ecu"`
	NetworkPerformance          string `json:"networkPerformance"`
	Servicename                 string `json:"servicename"`
	InstanceType                string `json:"instanceType"`
	Tenancy                     string `json:"tenancy"`
	Usagetype                   string `json:"usagetype"`
	NormalizationSizeFactor     string `json:"normalizationSizeFactor"`
	IntelAvxAvailable           string `json:"intelAvxAvailable"`
	ProcessorFeatures           string `json:"processorFeatures"`
	Servicecode                 string `json:"servicecode"`
	LicenseModel                string `json:"licenseModel"`
	CurrentGeneration           string `json:"currentGeneration"`
	PreInstalledSw              string `json:"preInstalledSw"`
	Location                    string `json:"location"`
	ProcessorArchitecture       string `json:"processorArchitecture"`
	Operation                   string `json:"operation"`
	VolumeApiName               string `json:"volumeApiName"`
}

type Terms struct {
	OnDemand map[string]Term `json:"OnDemand"`
	Reserved map[string]Term `json:"Reserved"`
}

type Term struct {
	PriceDimensions map[string]Dimension `json:"priceDimensions"`
	Sku             string               `json:"sku"`
	EffectiveDate   string               `json:"effectiveDate"`
	OfferTermCode   string               `json:"offerTermCode"`
	TermAttributes  TermAttributes       `json:"termAttributes"`
}

type Dimension struct {
	Unit         string       `json:"unit"`
	EndRange     string       `json:"endRange"`
	Description  string       `json:"description"`
	AppliesTo    []string     `json:"appliesTo"`
	RateCode     string       `json:"rateCode"`
	BeginRange   string       `json:"beginRange"`
	PricePerUnit PricePerUnit `json:"pricePerUnit"`
}

type TermAttributes struct {
	LeaseContractLength string `json:"LeaseContractLength"`
	OfferingClass       string `json:"OfferingClass"`
	PurchaseOption      string `json:"PurchaseOption"`
}

type PricePerUnit struct {
	Usd float64 `json:"USD"`
	CNY float64 `json:"CNY"`
}

type SInstanceType struct {
	Product         Product `json:"product"`
	ServiceCode     string  `json:"serviceCode"`
	Terms           Terms   `json:"terms"`
	Version         string  `json:"version"`
	PublicationDate string  `json:"publicationDate"`
}

type InstanceType struct {
	InstanceType string `xml:"instanceType"`
	MemoryInfo   struct {
		SizeInMiB int `xml:"sizeInMiB"`
	} `xml:"memoryInfo"`
	VCpuInfo struct {
		DefaultVCpus int `xml:"defaultVCpus"`
	} `xml:"vCpuInfo"`
}

func (self *SRegion) GetInstanceType(name string) (*InstanceType, error) {
	params := map[string]string{
		"InstanceType.1": name,
	}
	ret := struct {
		InstanceTypeSet []InstanceType `xml:"instanceTypeSet>item"`
		NextToken       string         `xml:"nextToken"`
	}{}
	err := self.ec2Request("DescribeInstanceTypes", params, &ret)
	if err != nil {
		return nil, err
	}
	for i := range ret.InstanceTypeSet {
		if ret.InstanceTypeSet[i].InstanceType == name {
			return &ret.InstanceTypeSet[i], nil
		}
	}
	return nil, errors.Wrapf(cloudprovider.ErrNotFound, "%s", name)
}

func (self *SRegion) GetInstanceTypes() ([]SInstanceType, error) {
	filters := map[string]string{
		"regionCode":      self.RegionId,
		"operatingSystem": "Linux",
		"licenseModel":    "No License required",
		"productFamily":   "Compute Instance",
		"operation":       "RunInstances",
		"preInstalledSw":  "NA",
		"tenancy":         "Shared",
		"capacitystatus":  "Used",
	}

	params := []ProductFilter{}

	for k, v := range filters {
		params = append(params, ProductFilter{
			Type:  "TERM_MATCH",
			Field: k,
			Value: v,
		})
	}

	ret := []SInstanceType{}
	var nextToken string
	for {
		parts, _nextToken, err := self.GetProducts("AmazonEC2", params, nextToken)
		if err != nil {
			return nil, err
		}
		ret = append(ret, parts...)
		if len(_nextToken) == 0 || len(parts) == 0 {
			break
		}
		nextToken = _nextToken
	}
	return ret, nil
}

type Sku struct {
	InstanceType string `xml:"instanceType"`
	MemoryInfo   struct {
		SizeInMiB int `xml:"sizeInMiB"`
	} `xml:"memoryInfo"`
	VCpuInfo struct {
		DefaultVCpus int `xml:"defaultVCpus"`
	} `xml:"vCpuInfo"`
	ProcessorInfo struct {
		SupportedArchitectures []string `xml:"supportedArchitectures>item"`
	} `xml:"processorInfo"`
	NetworkInfo struct {
		MaximumNetworkInterfaces int `xml:"maximumNetworkInterfaces"`
	} `xml:"networkInfo"`
	GpuInfo struct {
		Gpus []struct {
			Count int    `xml:"count"`
			Name  string `xml:"name"`
		} `xml:"gpus>item"`
	} `xml:"gpuInfo"`
}

func (self *SRegion) DescribeInstanceTypes(arch string, nextToken string) ([]Sku, string, error) {
	params := map[string]string{}
	if len(nextToken) > 0 {
		params["NextToken"] = nextToken
	}
	params["MaxResults"] = "100"
	idx := 1
	if len(arch) > 0 {
		params[fmt.Sprintf("Filter.%d.Name", idx)] = "processor-info.supported-architecture"
		params[fmt.Sprintf("Filter.%d.Value.1", idx)] = arch
		idx++
	}
	ret := struct {
		InstanceTypeSet []Sku  `xml:"instanceTypeSet>item"`
		NextToken       string `xml:"nextToken"`
	}{}
	err := self.ec2Request("DescribeInstanceTypes", params, &ret)
	if err != nil {
		return nil, "", err
	}
	return ret.InstanceTypeSet, ret.NextToken, nil
}

func newCloudSkuFromInstanceType(instanceType Sku) *multicloud.SCloudSku {
	if instanceType.InstanceType == "" || instanceType.VCpuInfo.DefaultVCpus <= 0 || instanceType.MemoryInfo.SizeInMiB <= 0 {
		return nil
	}
	sku := multicloud.NewSCloudSku(instanceType.InstanceType)
	family := instanceType.InstanceType
	if idx := strings.IndexByte(family, '.'); idx > 0 {
		family = family[:idx]
	}
	sku.InstanceTypeFamily = family
	sku.InstanceTypeCategory = family
	sku.CpuCoreCount = instanceType.VCpuInfo.DefaultVCpus
	sku.MemorySizeMB = instanceType.MemoryInfo.SizeInMiB
	sku.CpuArch = api.OS_ARCH_X86_64
	if len(instanceType.ProcessorInfo.SupportedArchitectures) > 0 {
		switch instanceType.ProcessorInfo.SupportedArchitectures[0] {
		case "arm64", "aarch64":
			sku.CpuArch = api.OS_ARCH_ARM
		}
	}
	if instanceType.NetworkInfo.MaximumNetworkInterfaces > 0 {
		sku.NicMaxCount = instanceType.NetworkInfo.MaximumNetworkInterfaces
	}
	for _, gpu := range instanceType.GpuInfo.Gpus {
		if gpu.Count > 0 {
			sku.GpuAttachable = true
			sku.GpuSpec = gpu.Name
			sku.GpuCount = strconv.Itoa(gpu.Count)
			sku.GpuMaxCount = gpu.Count
			break
		}
	}
	return sku
}

func (self *SRegion) GetISkus() ([]cloudprovider.ICloudSku, error) {
	ret := []cloudprovider.ICloudSku{}
	seen := map[string]bool{}
	seenTokens := map[string]bool{}
	nextToken := ""
	for {
		part, token, err := self.DescribeInstanceTypes("", nextToken)
		if err != nil {
			return nil, errors.Wrap(err, "DescribeInstanceTypes")
		}
		for _, item := range part {
			if seen[item.InstanceType] {
				continue
			}
			sku := newCloudSkuFromInstanceType(item)
			if sku == nil {
				continue
			}
			seen[item.InstanceType] = true
			ret = append(ret, sku)
		}
		if token == "" {
			break
		}
		if seenTokens[token] {
			return nil, errors.Errorf("DescribeInstanceTypes returned repeated next token %q", token)
		}
		seenTokens[token] = true
		nextToken = token
	}
	return ret, nil
}
