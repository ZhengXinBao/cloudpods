package aws

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"yunion.io/x/jsonutils"
)

func TestNodegroupTagMapResponse(t *testing.T) {
	response, err := jsonutils.ParseString(`{"nodegroup":{"clusterName":"cluster","nodegroupName":"workers","status":"ACTIVE","subnets":["subnet-a"],"instanceTypes":["m5.large"],"diskSize":40,"scalingConfig":{"minSize":1,"maxSize":4,"desiredSize":2},"tags":{"eks":"workers","karpenter.sh/discovery":"cluster","Description":"worker pool","aws:eks:cluster-name":"cluster"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	var result struct{ Nodegroup SNodeGroup }
	if err := response.Unmarshal(&result); err != nil {
		t.Fatalf("decode EKS nodegroup: %v", err)
	}
	var group SNodeGroup
	if err := jsonutils.Update(&group, &result.Nodegroup); err != nil {
		t.Fatal(err)
	}
	tags, err := group.GetTags()
	if err != nil || len(tags) != 2 || tags["eks"] != "workers" || group.GetSysTags()["aws:eks:cluster-name"] != "cluster" || group.GetDescription() != "worker pool" {
		t.Fatalf("lost tag metadata: %v, %v", tags, err)
	}
	if group.GetStatus() != "running" || group.GetMinInstanceCount() != 1 || group.GetMaxInstanceCount() != 4 || group.GetDesiredInstanceCount() != 2 || group.GetRootDiskSizeGb() != 40 {
		t.Fatalf("lost nodegroup details: %+v", group)
	}
}

func TestDescribeNodegroupsRequiresDetails(t *testing.T) {
	for _, tc := range []struct {
		name  string
		group *SNodeGroup
		err   error
		want  string
	}{
		{"denied", nil, errors.New("AccessDeniedException"), "AccessDeniedException"},
		{"missing status", &SNodeGroup{NodegroupName: "workers"}, nil, "no nodegroup status"},
		{"absent group", nil, nil, "no nodegroup status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groups, err := describeNodegroups([]string{"workers"}, func(string) (*SNodeGroup, error) { return tc.group, tc.err })
			if err == nil || !strings.Contains(err.Error(), tc.want) || len(groups) != 0 {
				t.Fatalf("groups=%v error=%v", groups, err)
			}
		})
	}
	groups, err := describeNodegroups([]string{"workers"}, func(name string) (*SNodeGroup, error) {
		return &SNodeGroup{NodegroupName: name, Status: "ACTIVE", DiskSize: 80}, nil
	})
	if err != nil || len(groups) != 1 || groups[0].GetStatus() != "running" || groups[0].GetRootDiskSizeGb() != 80 {
		t.Fatalf("unhydrated groups=%v error=%v", groups, err)
	}
}

func TestNodegroupStatusAndOptionalFields(t *testing.T) {
	for _, tc := range []struct{ cloud, want string }{
		{"", "abnormal"}, {"ACTIVE", "running"}, {"CREATING", "creating"},
		{"DEGRADED", "abnormal"}, {"UPDATING", "updating"}, {"DELETING", "deleting"},
		{"CREATE_FAILED", "create_failed"}, {"DELETE_FAILED", "delete_failed"},
	} {
		group := SNodeGroup{Status: tc.cloud}
		if got := group.GetStatus(); got != tc.want {
			t.Fatalf("status %q: got %q, want %q", tc.cloud, got, tc.want)
		}
	}
	// Launch-template based pools may omit instanceTypes and diskSize. Value
	// getters must not retry Describe behind an interface that cannot return errors.
	group := SNodeGroup{Status: "ACTIVE"}
	if len(group.GetInstanceTypes()) != 0 || group.GetRootDiskSizeGb() != 0 || len(group.GetNetworkIds()) != 0 || group.GetMinInstanceCount() != 0 || group.GetMaxInstanceCount() != 0 || group.GetDesiredInstanceCount() != 0 {
		t.Fatal("optional fields unexpectedly populated")
	}
}

func TestKubeNodePoolsRejectsPartialInventory(t *testing.T) {
	pools, err := listKubeNodePools(func(token string) ([]SNodeGroup, string, error) {
		if token == "" {
			return []SNodeGroup{{NodegroupName: "first", Status: "ACTIVE"}}, "page-2", nil
		}
		return nil, "", errors.New("DescribeNodegroup AccessDeniedException")
	})
	if err == nil || !strings.Contains(err.Error(), "AccessDeniedException") || len(pools) != 0 {
		t.Fatalf("partial inventory accepted: pools=%v error=%v", pools, err)
	}
}

func TestKubeNodePoolsAdvancesPagination(t *testing.T) {
	calls := 0
	pools, err := listKubeNodePools(func(token string) ([]SNodeGroup, string, error) {
		calls++
		if calls == 1 && token == "" {
			return []SNodeGroup{{NodegroupName: "first", Status: "ACTIVE"}}, "page-2", nil
		}
		if calls == 2 && token == "page-2" {
			return []SNodeGroup{{NodegroupName: "second", Status: "ACTIVE"}}, "", nil
		}
		return nil, "", fmt.Errorf("wrong pagination token %q at call %d", token, calls)
	})
	if err != nil || calls != 2 || len(pools) != 2 {
		t.Fatalf("calls=%d pools=%v error=%v", calls, pools, err)
	}
	if pools[0].GetName() != "first" || pools[1].GetName() != "second" {
		t.Fatalf("wrong pages: %v", pools)
	}
}
