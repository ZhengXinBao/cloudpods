package qcloud

import (
	"fmt"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/pkg/errors"
)

func TestMySQLInstanceOptionalErrorOnlySkipsUnsupportedReplicaOperations(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		skip      bool
		wantCause error
	}{
		{
			name:      "master-only database api",
			err:       fmt.Errorf("[TencentCloudSDKError] Code=OperationDenied, Message=The action only supports for master"),
			skip:      true,
			wantCause: cloudprovider.ErrNotSupported,
		},
		{
			name:      "unsupported instance type",
			err:       fmt.Errorf("[TencentCloudSDKError] Code=OperationDenied.InstTypeNotSupport"),
			skip:      true,
			wantCause: cloudprovider.ErrNotSupported,
		},
		{
			name:      "generic operation denied remains an error",
			err:       fmt.Errorf("[TencentCloudSDKError] Code=OperationDenied, Message=permission denied"),
			skip:      false,
			wantCause: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mysqlInstanceOptionalError("DescribeDatabases", tc.err)
			if tc.skip && errors.Cause(got) != tc.wantCause {
				t.Fatalf("cause=%v, want %v", errors.Cause(got), tc.wantCause)
			}
			if !tc.skip && got != tc.err {
				t.Fatalf("got=%v, want original error", got)
			}
		})
	}
}
