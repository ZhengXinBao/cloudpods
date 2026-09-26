package models

import (
	"context"
	"fmt"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/onecloud/pkg/mcclient"
	"yunion.io/x/pkg/errors"
)

type optionalDBInstance struct {
	cloudprovider.ICloudDBInstance
	failure error
}

func (d optionalDBInstance) GetDBNetworks() ([]cloudprovider.SDBInstanceNetwork, error) {
	return nil, d.failure
}
func (d optionalDBInstance) GetSecurityGroupIds() ([]string, error) { return nil, d.failure }
func (d optionalDBInstance) GetIDBInstanceParameters() ([]cloudprovider.ICloudDBInstanceParameter, error) {
	return nil, d.failure
}
func (d optionalDBInstance) GetIDBInstanceDatabases() ([]cloudprovider.ICloudDBInstanceDatabase, error) {
	return nil, d.failure
}
func (d optionalDBInstance) GetIDBInstanceAccounts() ([]cloudprovider.ICloudDBInstanceAccount, error) {
	return nil, d.failure
}
func (d optionalDBInstance) GetIDBInstanceBackups() ([]cloudprovider.ICloudDBInstanceBackup, error) {
	return nil, d.failure
}

type optionalDBAccount struct {
	cloudprovider.ICloudDBInstanceAccount
	failure error
}

func (d optionalDBAccount) GetIDBInstanceAccountPrivileges() ([]cloudprovider.ICloudDBInstanceAccountPrivilege, error) {
	return nil, d.failure
}

func TestOptionalDBFetchErrors(t *testing.T) {
	calls := map[string]func(context.Context, mcclient.TokenCredential, SSyncResultSet, *SDBInstance, cloudprovider.ICloudDBInstance) error{
		"networks":        syncDBInstanceNetwork,
		"security groups": syncDBInstanceSecgroups,
		"parameters":      syncDBInstanceParameters,
		"databases":       syncDBInstanceDatabases,
		"accounts":        syncDBInstanceAccounts,
		"backups":         syncDBInstanceBackups,
		"privileges": func(ctx context.Context, cred mcclient.TokenCredential, results SSyncResultSet, _ *SDBInstance, remote cloudprovider.ICloudDBInstance) error {
			return syncDBInstanceAccountPrivileges(ctx, cred, results, &SDBInstanceAccount{}, optionalDBAccount{failure: remote.(optionalDBInstance).failure})
		},
	}
	cases := []struct {
		failure error
		skip    bool
	}{
		{cloudprovider.ErrNotImplemented, true},
		{cloudprovider.ErrNotSupported, true},
		{errors.Wrap(cloudprovider.ErrNotSupported, "provider capability"), true},
		{fmt.Errorf("access denied"), false},
		{fmt.Errorf("invalid credential"), false},
		{fmt.Errorf("NotSupportedError"), false},
		{fmt.Errorf("timeout"), false},
	}
	for name, call := range calls {
		for _, tc := range cases {
			t.Run(name+"/"+tc.failure.Error(), func(t *testing.T) {
				results := SSyncResultSet{}
				err := call(context.Background(), nil, results, nil, optionalDBInstance{failure: tc.failure})
				if (err == nil) != tc.skip {
					t.Errorf("error=%v; want skip=%v", err, tc.skip)
				}
				if !tc.skip && errors.Cause(err) != tc.failure {
					t.Errorf("lost original API failure: %v", err)
				}
				if len(results) != 0 {
					t.Fatal("failed/unsupported fetch must not reconcile existing resources")
				}
			})
		}
	}
}
