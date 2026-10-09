package models

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	aliyunerrors "github.com/aliyun/alibaba-cloud-sdk-go/sdk/errors"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/smithy-go"
	tencenterrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/onecloud/pkg/compute/syncqueue"
	"yunion.io/x/pkg/util/compare"
)

func TestSyncResultSetAddPreservesActualSDKCauses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		want  syncqueue.ErrorClass
	}{
		{"aws v1", awserr.New("InternalError", "upstream returned an unsupported protocol", nil), syncqueue.ErrorClassTemporary},
		{"aws v2", &smithy.GenericAPIError{Code: "InternalError", Message: "upstream returned an unsupported protocol"}, syncqueue.ErrorClassTemporary},
		{"aws status", awserr.NewRequestFailure(awserr.New("UnknownProviderCode", "AccessDenied", nil), 503, "request"), syncqueue.ErrorClassTemporary},
		{"aws original cause", awserr.New("UnknownProviderCode", "opaque", context.DeadlineExceeded), syncqueue.ErrorClassTimeout},
		{"aws batch causes", awserr.NewBatchError("UnknownProviderCode", "opaque", []error{errors.New("unclassified"), tencenterrors.NewTencentCloudSDKError("UnauthorizedOperation", "opaque", "")}), syncqueue.ErrorClassPermission},
		{"aliyun", aliyunerrors.NewServerError(503, `{"Code":"InternalError","Message":"unsupported protocol"}`, ""), syncqueue.ErrorClassTemporary},
		{"tencent", tencenterrors.NewTencentCloudSDKError("AuthFailure", `decode {"body":"unfinished`, ""), syncqueue.ErrorClassAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := compare.SyncResult{}
			source.AddError(tc.cause)
			source.Update()
			set := SSyncResultSet{}
			set.Add(BucketManager, source)
			got := set[BucketManager.KeywordPlural()]
			if got.AddErrCnt != 1 || got.UpdateCnt != 1 {
				t.Fatal("Add lost counters")
			}
			ctx, collector := WithCloudSyncErrors(context.Background())
			cloudSyncError(ctx, "request: %v", tc.cause)
			for _, err := range []error{syncResultSetError(set), collector.Merge(syncResultSetError(set))} {
				if class := syncqueue.ClassifyError(err); class != tc.want {
					t.Fatalf("real Add path class=%s want=%s", class, tc.want)
				}
			}
		})
	}
}

func TestSyncResultSetAddBoundedAllClassesAndIndependentErrors(t *testing.T) {
	for i := 0; i < 100; i++ {
		source := compare.SyncResult{}
		causes := []error{awserr.New("InternalError", "unsupported protocol; AccessDenied", nil), tencenterrors.NewTencentCloudSDKError("UnauthorizedOperation", "opaque", "")}
		if i%2 != 0 {
			causes[0], causes[1] = causes[1], causes[0]
		}
		for n := 0; n < 30; n++ {
			source.Error(awserr.New("NewCode", strings.Repeat("x", 10000), nil))
		}
		for _, cause := range causes {
			source.Error(cause)
		}
		set := SSyncResultSet{}
		set.Add(BucketManager, source)
		result := syncResultSetError(set)
		if class := syncqueue.ClassifyError(result); class != syncqueue.ErrorClassPermission {
			t.Fatalf("class=%s", class)
		}
		counts := syncqueue.ErrorClassCounts(result)
		if counts[syncqueue.ErrorClassUnknown] != 30 || counts[syncqueue.ErrorClassTemporary] != 1 || counts[syncqueue.ErrorClassPermission] != 1 {
			t.Fatalf("lost causes: %v", counts)
		}
		if len(result.Error()) > 8192 {
			t.Fatal("unbounded summary")
		}
	}
}

func TestSyncResultSetDirectCausesBeforeFormatting(t *testing.T) {
	source := &SyncResult{}
	source.Error(awserr.New("InternalError", "unsupported protocol", nil))
	source.Error(errors.New(`decode {"body":"unfinished`))
	err := syncResultSetError(SSyncResultSet{"buckets": source})
	if got := syncqueue.ClassifyError(err); got != syncqueue.ErrorClassTemporary {
		t.Fatalf("direct SDK cause overwritten: %s", got)
	}
}

type failingBucketRegion struct {
	cloudprovider.ICloudRegion
	cause error
}

func (r *failingBucketRegion) GetIBuckets() ([]cloudprovider.ICloudBucket, error) {
	return nil, r.cause
}
func (r *failingBucketRegion) GetName() string { return "test-region" }
func TestSyncRegionBucketsCollectorKeepsSDKCause(t *testing.T) {
	cause := awserr.New("InternalError", "upstream returned an unsupported protocol", nil)
	ctx, collector := WithCloudSyncErrors(context.Background())
	syncRegionBuckets(ctx, nil, SSyncResultSet{}, &SCloudprovider{}, &SCloudregion{}, &failingBucketRegion{cause: cause}, false)
	if got := syncqueue.ClassifyError(collector.Error()); got != syncqueue.ErrorClassTemporary {
		t.Fatalf("bucket collector lost cause: %s", got)
	}
}
func TestSyncResultAllErrorDoesNotPoisonCollector(t *testing.T) {
	source := compare.SyncResult{}
	source.Error(awserr.New("InternalError", "upstream returned an unsupported protocol", nil))
	set := SSyncResultSet{}
	set.Add(BucketManager, source)
	for _, err := range []error{source.AllError(), set[BucketManager.KeywordPlural()].AllError()} {
		ctx, collector := WithCloudSyncErrors(context.Background())
		cloudSyncError(ctx, "sync operation: %v", err)
		if got := syncqueue.ClassifyError(collector.Merge(syncResultSetError(set))); got != syncqueue.ErrorClassTemporary {
			t.Fatalf("AllError poisoned collector: %s", got)
		}
	}
}

// The behavioral bucket test exercises the real failed-fetch path; this source
// guard prevents its former pre-formatting pattern reappearing in sibling paths.
func TestSyncCollectorCallsitesKeepFormatArguments(t *testing.T) {
	files, err := filepath.Glob("cloudsync*.go")
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, "cloudproviderregions.go", "cloudaccounts.go", "cloudaccount_region_discover.go")
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn, ok := call.Fun.(*ast.Ident)
			if !ok || fn.Name != "cloudSyncError" {
				return true
			}
			if len(call.Args) < 2 {
				t.Errorf("%s: missing format", fs.Position(call.Pos()))
				return true
			}
			format, ok := call.Args[1].(*ast.BasicLit)
			if !ok || format.Kind != token.STRING {
				t.Errorf("%s: preformatted collector message", fs.Position(call.Pos()))
				return true
			}
			if format.Value == `"%s"` && len(call.Args) == 3 {
				t.Errorf("%s: opaque collector argument drops cause types", fs.Position(call.Pos()))
			}
			return true
		})
	}
}
