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
	"strings"

	api "yunion.io/x/cloudmux/pkg/apis/compute"
	"yunion.io/x/cloudmux/pkg/multicloud"
	"yunion.io/x/jsonutils"
	"yunion.io/x/pkg/errors"
)

type SNodeGroup struct {
	multicloud.SResourceBase
	region *SRegion
	Tags   map[string]string `json:"tags"`

	ClusterName   string
	NodegroupName string
	DiskSize      int
	Status        string
	InstanceTypes []string
	Subnets       []string
	ScalingConfig struct {
		DesiredSize int
		MaxSize     int
		MinSize     int
	}
}

func (self *SNodeGroup) GetId() string {
	return self.NodegroupName
}

func (self *SNodeGroup) GetName() string {
	return self.NodegroupName
}

func (self *SNodeGroup) GetGlobalId() string {
	return self.NodegroupName
}

func (self *SNodeGroup) Refresh() error {
	ng, err := self.region.GetNodegroup(self.ClusterName, self.NodegroupName)
	if err != nil {
		return err
	}
	self.InstanceTypes = nil
	self.Subnets = nil
	return jsonutils.Update(self, ng)
}

func (self *SNodeGroup) GetStatus() string {
	switch strings.ToLower(self.Status) {
	case "active":
		return api.KUBE_CLUSTER_STATUS_RUNNING
	case "creating":
		return api.KUBE_CLUSTER_STATUS_CREATING
	case "", "degraded":
		return api.KUBE_CLUSTER_STATUS_ABNORMAL
	}
	return strings.ToLower(self.Status)
}

func (self *SNodeGroup) GetMinInstanceCount() int {
	return self.ScalingConfig.MinSize
}

func (self *SNodeGroup) GetMaxInstanceCount() int {
	return self.ScalingConfig.MaxSize
}

func (self *SNodeGroup) GetDesiredInstanceCount() int {
	return self.ScalingConfig.DesiredSize
}

func (self *SNodeGroup) GetRootDiskSizeGb() int {
	return self.DiskSize
}

func (self *SNodeGroup) Delete() error {
	return self.region.DeleteNodegroup(self.ClusterName, self.NodegroupName)
}

func (self *SNodeGroup) GetNetworkIds() []string {
	return self.Subnets
}

func (self *SNodeGroup) GetInstanceTypes() []string {
	return self.InstanceTypes
}

func (self *SRegion) GetNodegroup(cluster, name string) (*SNodeGroup, error) {
	params := map[string]interface{}{
		"name":          cluster,
		"nodegroupName": name,
	}
	ret := struct {
		Nodegroup SNodeGroup
	}{}
	err := self.eksRequest("DescribeNodegroup", "/clusters/{name}/node-groups/{nodegroupName}", params, &ret)
	if err != nil {
		return nil, errors.Wrapf(err, "DescribeNodegroup")
	}
	ret.Nodegroup.region = self
	return &ret.Nodegroup, nil
}

func (self *SRegion) GetNodegroups(cluster, nextToken string) ([]SNodeGroup, string, error) {
	params := map[string]interface{}{}
	if len(nextToken) > 0 {
		params["nextToken"] = nextToken
	}
	ret := struct {
		Nodegroups []string
		NextToken  string
	}{}
	resource := fmt.Sprintf("/clusters/%s/node-groups", cluster)
	err := self.eksRequest("ListNodegroups", resource, params, &ret)
	if err != nil {
		return nil, "", errors.Wrapf(err, "ListNodegroups")
	}
	result, err := describeNodegroups(ret.Nodegroups, func(name string) (*SNodeGroup, error) {
		return self.GetNodegroup(cluster, name)
	})
	if err != nil {
		return nil, "", err
	}
	return result, ret.NextToken, nil
}

// ListNodegroups supplies names only. Complete the inventory before returning
// it so Describe failures are surfaced instead of lost inside value getters.
func describeNodegroups(names []string, describe func(string) (*SNodeGroup, error)) ([]SNodeGroup, error) {
	result := make([]SNodeGroup, 0, len(names))
	for _, name := range names {
		group, err := describe(name)
		if err != nil {
			return nil, errors.Wrapf(err, "DescribeNodegroup(%s)", name)
		}
		if group == nil || len(group.Status) == 0 {
			return nil, fmt.Errorf("DescribeNodegroup(%s) returned no nodegroup status", name)
		}
		result = append(result, *group)
	}
	return result, nil
}

func (self *SRegion) DeleteNodegroup(cluster, name string) error {
	params := map[string]interface{}{
		"name":          cluster,
		"nodegroupName": name,
	}
	ret := struct {
		Nodegroup SNodeGroup
	}{}
	return self.eksRequest("DeleteNodegroup", "/clusters/{name}/node-groups/{nodegroupName}", params, &ret)
}

func (self *SNodeGroup) GetDescription() string {
	return self.awsTags().GetDescription()
}

func (self *SNodeGroup) awsTags() *AwsTags {
	tags := &AwsTags{}
	for key, value := range self.Tags {
		tags.Tags = append(tags.Tags, SAwsLbTag{Key: key, Value: value})
	}
	return tags
}

func (self *SNodeGroup) GetTags() (map[string]string, error) {
	return self.awsTags().GetTags()
}

func (self *SNodeGroup) GetSysTags() map[string]string {
	return self.awsTags().GetSysTags()
}

func (self *SNodeGroup) SetTags(tags map[string]string, replace bool) error {
	return self.awsTags().SetTags(tags, replace)
}
