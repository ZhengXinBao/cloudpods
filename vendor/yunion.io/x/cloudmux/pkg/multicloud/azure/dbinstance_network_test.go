package azure

import (
	"encoding/json"
	"strings"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
)

func TestDBInstanceDelegatedSubnetMetadata(t *testing.T) {
	subnet := "/subscriptions/SUB/resourceGroups/RG/providers/Microsoft.Network/virtualNetworks/VNET/subnets/MYSQL"
	for _, input := range []string{
		`{"properties":{"delegatedSubnetArguments":{"subnetArmResourceId":"` + subnet + `"}}}`,
		`{"properties":{"network":{"delegatedSubnetResourceId":"` + subnet + `"}}}`,
	} {
		var instance SDBInstance
		if err := json.Unmarshal([]byte(input), &instance); err != nil {
			t.Fatal(err)
		}
		wantVpc := strings.Split(strings.ToLower(subnet), "/subnets/")[0]
		if got := instance.GetIVpcId(); got != wantVpc {
			t.Fatalf("VPC must use same subnet metadata: got=%q want=%q", got, wantVpc)
		}
		networks, err := instance.GetDBNetworks()
		if err != nil || len(networks) != 1 || networks[0].NetworkId != strings.ToLower(subnet) || networks[0].IP != "" {
			t.Fatalf("subnet metadata must preserve unknown IP and normalize ID: %#v, %v", networks, err)
		}
	}
}

func TestDBInstanceMissingSubnetIsNotEmptyInventory(t *testing.T) {
	instance := SDBInstance{}
	networks, err := instance.GetDBNetworks()
	if err != cloudprovider.ErrNotSupported || len(networks) != 0 {
		t.Fatalf("missing metadata cannot reconcile an empty ID or delete existing networks: %#v, %v", networks, err)
	}
}
