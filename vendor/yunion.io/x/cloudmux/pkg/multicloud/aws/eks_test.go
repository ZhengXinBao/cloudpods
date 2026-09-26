package aws

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	api "yunion.io/x/cloudmux/pkg/apis/compute"
	"yunion.io/x/jsonutils"
)

func TestCloudKubeClustersAdvancesPagination(t *testing.T) {
	region := &SRegion{}
	calls := 0
	clusters, err := region.listCloudKubeClusters(func(token string) ([]SKubeCluster, string, error) {
		calls++
		if calls == 1 && token == "" {
			return []SKubeCluster{{Name: "first", Status: "ACTIVE"}}, "page-2", nil
		}
		if calls == 2 && token == "page-2" {
			return []SKubeCluster{{Name: "second", Status: "ACTIVE"}}, "", nil
		}
		return nil, "", fmt.Errorf("wrong pagination token %q at call %d", token, calls)
	})
	if err != nil || calls != 2 || len(clusters) != 2 {
		t.Fatalf("calls=%d clusters=%v error=%v", calls, clusters, err)
	}
	for i, name := range []string{"first", "second"} {
		cluster := clusters[i].(*SKubeCluster)
		if cluster.Name != name || cluster.region != region {
			t.Fatalf("cluster name/region changed: %+v", cluster)
		}
	}
}

func TestCloudKubeClustersRejectsPartialInventory(t *testing.T) {
	region := &SRegion{}
	clusters, err := region.listCloudKubeClusters(func(token string) ([]SKubeCluster, string, error) {
		if token == "" {
			return []SKubeCluster{{Name: "first", Status: "ACTIVE"}}, "page-2", nil
		}
		return nil, "", errors.New("DescribeCluster AccessDeniedException")
	})
	if err == nil || !strings.Contains(err.Error(), "AccessDeniedException") || len(clusters) != 0 {
		t.Fatalf("partial inventory accepted: clusters=%v error=%v", clusters, err)
	}
}

func TestKubeClusterStatus(t *testing.T) {
	for _, tc := range []struct{ cloud, want string }{
		{"ACTIVE", api.KUBE_CLUSTER_STATUS_RUNNING},
		{"CREATING", api.KUBE_CLUSTER_STATUS_CREATING},
		{"UPDATING", api.KUBE_CLUSTER_STATUS_UPDATING},
		{"DELETING", api.KUBE_CLUSTER_STATUS_DELETING},
		{"FAILED", api.KUBE_CLUSTER_STATUS_ABNORMAL},
		{"", api.KUBE_CLUSTER_STATUS_ABNORMAL},
	} {
		t.Run(tc.cloud, func(t *testing.T) {
			cluster := SKubeCluster{Status: tc.cloud}
			if got := cluster.GetStatus(); got != tc.want {
				t.Fatalf("status %q: got %q, want %q", tc.cloud, got, tc.want)
			}
		})
	}
}

func TestDescribeKubeClusterTagMap(t *testing.T) {
	response, err := jsonutils.ParseString(`{"cluster":{"name":"tra-dev","status":"ACTIVE","accessConfig":{"authenticationMode":"API_AND_CONFIG_MAP","bootstrapClusterCreatorAdminPermissions":null},"certificateAuthority":{"active":{},"data":"Y2VydA=="},"tags":{"eks":"tra-dev","karpenter.sh/discovery":"tra-dev","Description":"test cluster","Name":"display","aws:eks:cluster-name":"tra-dev"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	var result struct{ Cluster SKubeCluster }
	if err := response.Unmarshal(&result); err != nil {
		t.Fatalf("decode EKS tag map: %v", err)
	}
	// Refresh uses jsonutils.Update; tags must survive that round trip too.
	var cluster SKubeCluster
	if err := jsonutils.Update(&cluster, &result.Cluster); err != nil {
		t.Fatal(err)
	}
	tags, err := cluster.GetTags()
	if err != nil || len(tags) != 2 || tags["eks"] != "tra-dev" || tags["karpenter.sh/discovery"] != "tra-dev" {
		t.Fatalf("custom tags=%v, error=%v", tags, err)
	}
	if cluster.GetSysTags()["aws:eks:cluster-name"] != "tra-dev" || cluster.GetName() != "tra-dev" {
		t.Fatalf("tag metadata or cluster name lost: %+v", cluster)
	}
}

func TestDescribeKubeClustersPropagatesFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cluster *SKubeCluster
		err     error
		want    string
	}{
		{"denied", nil, errors.New("AccessDeniedException"), "AccessDeniedException"},
		{"empty", &SKubeCluster{Name: "cluster"}, nil, "no cluster status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clusters, err := describeKubeClusters([]string{"cluster"}, func(name string) (*SKubeCluster, error) {
				return tc.cluster, tc.err
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) || len(clusters) != 0 {
				t.Fatalf("failed describe yielded clusters=%v error=%v", clusters, err)
			}
		})
	}
}

func TestDescribeKubeClustersLoadsDetails(t *testing.T) {
	clusters, err := describeKubeClusters([]string{"first", "second"}, func(name string) (*SKubeCluster, error) {
		return &SKubeCluster{Name: name, Status: "ACTIVE", Version: "1.31"}, nil
	})
	if err != nil || len(clusters) != 2 {
		t.Fatalf("clusters=%v error=%v", clusters, err)
	}
	for i, name := range []string{"first", "second"} {
		if clusters[i].Name != name || clusters[i].GetStatus() != api.KUBE_CLUSTER_STATUS_RUNNING || clusters[i].GetVersion() != "1.31" {
			t.Fatalf("cluster not hydrated: %+v", clusters[i])
		}
	}
}
