package models

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
	"time"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/jsonutils"
	api "yunion.io/x/onecloud/pkg/apis/compute"
	"yunion.io/x/sqlchemy"
)

// The SDK boundary double detects extra remote calls. Assertions exercise the
// real snapshot/apply path; no database or cloud credentials are required.
type snapshotTestBucket struct {
	cloudprovider.ICloudBucket
	stats                     cloudprovider.SBucketStats
	statsCalls, propertyCalls int
	created                   time.Time
	urls                      []cloudprovider.SBucketAccessUrl
	denyCalls                 bool
}

func (b *snapshotTestBucket) called() {
	if b.denyCalls {
		panic("remote getter called while applying snapshot")
	}
}
func (b *snapshotTestBucket) GetStats() cloudprovider.SBucketStats {
	b.called()
	b.statsCalls++
	return b.stats
}
func (b *snapshotTestBucket) GetCreatedAt() time.Time { b.called(); return b.created }
func (b *snapshotTestBucket) GetLimit() cloudprovider.SBucketStats {
	b.called()
	b.propertyCalls++
	return cloudprovider.SBucketStats{SizeBytes: 900, ObjectCount: 90}
}
func (b *snapshotTestBucket) LimitSupport() cloudprovider.SBucketStats {
	b.called()
	b.propertyCalls++
	return cloudprovider.SBucketStats{SizeBytes: 1, ObjectCount: 1}
}
func (b *snapshotTestBucket) GetAcl() cloudprovider.TBucketACLType {
	b.called()
	b.propertyCalls++
	return cloudprovider.TBucketACLType("private")
}
func (b *snapshotTestBucket) GetLocation() string { b.called(); b.propertyCalls++; return "region-a" }
func (b *snapshotTestBucket) GetStorageClass() string {
	b.called()
	b.propertyCalls++
	return "standard"
}
func (b *snapshotTestBucket) GetAccessUrls() []cloudprovider.SBucketAccessUrl {
	b.called()
	b.propertyCalls++
	return b.urls
}

func TestBucketSnapshotKeepsUnknownStats(t *testing.T) {
	for _, tc := range []struct {
		name         string
		remote, want cloudprovider.SBucketStats
		wantError    bool
	}{
		{"unknown-size", cloudprovider.SBucketStats{-1, 0}, cloudprovider.SBucketStats{91, 0}, true},
		{"unknown-count", cloudprovider.SBucketStats{0, -1}, cloudprovider.SBucketStats{0, 7}, true},
		{"both-unknown", cloudprovider.SBucketStats{-1, -1}, cloudprovider.SBucketStats{91, 7}, true},
		{"actual-zero", cloudprovider.SBucketStats{0, 0}, cloudprovider.SBucketStats{0, 0}, false},
		{"actual-stats", cloudprovider.SBucketStats{100, 3}, cloudprovider.SBucketStats{100, 3}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := &snapshotTestBucket{stats: tc.remote}
			snapshot := captureCloudBucketSnapshot(remote, true)
			remote.denyCalls = true
			local := SBucket{SizeBytes: 91, ObjectCnt: 7}
			snapshot.apply(&local)
			if got := local.getStats(); got != tc.want {
				t.Fatalf("stats=%+v want=%+v", got, tc.want)
			}
			if (snapshot.statsError() != nil) != tc.wantError {
				t.Fatalf("error=%v", snapshot.statsError())
			}
			if remote.statsCalls != 1 || remote.propertyCalls != 0 {
				t.Fatalf("stats=%d properties=%d", remote.statsCalls, remote.propertyCalls)
			}
		})
	}
}

func TestBucketSnapshotCopiesPropertiesBeforeApply(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	remote := &snapshotTestBucket{
		stats: cloudprovider.SBucketStats{10, 2}, created: created,
		urls: []cloudprovider.SBucketAccessUrl{{Url: "https://bucket.invalid", Primary: true}},
	}
	snapshot := captureCloudBucketSnapshot(remote, false)
	remote.urls[0].Url = "https://mutated.invalid"
	remote.stats = cloudprovider.SBucketStats{20, 4}
	remote.denyCalls = true
	local := SBucket{}
	snapshot.apply(&local)
	if local.SizeBytes != 10 || local.ObjectCnt != 2 || local.CreatedAt != created ||
		local.Acl != "private" || local.Location != "region-a" || local.StorageClass != "standard" ||
		local.SizeBytesLimit != 900 || local.ObjectCntLimit != 90 || local.Status != api.BUCKET_STATUS_READY {
		t.Fatalf("snapshot values not applied: %+v", local)
	}
	if local.AccessUrls == nil || !strings.Contains(local.AccessUrls.String(), "https://bucket.invalid") {
		t.Fatalf("URL slice was not copied: %v", local.AccessUrls)
	}
	if remote.statsCalls != 1 || remote.propertyCalls != 6 {
		t.Fatalf("stats=%d properties=%d", remote.statsCalls, remote.propertyCalls)
	}
}

func TestBucketSnapshotStatsOnlyPreservesOtherFields(t *testing.T) {
	remote := &snapshotTestBucket{stats: cloudprovider.SBucketStats{4, 5}}
	local := SBucket{Location: "existing", Acl: "public-read", StorageClass: "cold", SizeBytesLimit: 9, ObjectCntLimit: 8}
	local.Status = "existing-status"
	local.CreatedAt = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	want := local
	want.SizeBytes, want.ObjectCnt = 4, 5
	snapshot := captureCloudBucketSnapshot(remote, true)
	remote.denyCalls = true
	snapshot.apply(&local)
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("statsOnly changed other fields: got=%+v want=%+v", local, want)
	}
}

func TestBucketSnapshotPreservesEmptyURLRepresentation(t *testing.T) {
	for _, urls := range [][]cloudprovider.SBucketAccessUrl{nil, {}} {
		snapshot := captureCloudBucketSnapshot(&snapshotTestBucket{urls: urls}, false)
		local := SBucket{}
		snapshot.apply(&local)
		want := jsonutils.Marshal(urls).String()
		if got := local.AccessUrls.String(); got != want {
			t.Errorf("URL serialization changed: got=%s want=%s", got, want)
		}
	}
}

func TestBucketSnapshotUnknownNewBucketIsIncomplete(t *testing.T) {
	remote := &snapshotTestBucket{stats: cloudprovider.SBucketStats{-1, -1}}
	snapshot := captureCloudBucketSnapshot(remote, false)
	local := SBucket{}
	snapshot.apply(&local)
	if local.SizeBytes != 0 || local.ObjectCnt != 0 || snapshot.statsError() == nil {
		t.Fatalf("new unknown stats became successful: stats=%+v error=%v", local.getStats(), snapshot.statsError())
	}
}

func TestBucketSnapshotUnknownDoesNotFabricateStatsChange(t *testing.T) {
	local := SBucket{SizeBytes: 91, ObjectCnt: 7}
	old := local.getStats()
	snapshot := captureCloudBucketSnapshot(&snapshotTestBucket{stats: cloudprovider.SBucketStats{-1, -1}}, true)
	snapshot.apply(&local)
	if !old.Equals(local.getStats()) {
		t.Fatal("unknown values generated a stats change")
	}
}

func TestBucketSnapshotWiredIntoUpdateAndInsert(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "buckets.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"syncWithCloudBucket", "newFromCloudBucket"} {
		var body *ast.BlockStmt
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
				body = fn.Body
			}
		}
		if body == nil {
			t.Fatalf("missing %s", name)
		}
		captures, applies, errors := 0, 0, 0
		ast.Inspect(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "captureCloudBucketSnapshot" {
				captures++
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "extBucket" && sel.Sel.Name == "GetStats" {
					t.Errorf("%s bypasses the single snapshot", name)
				}
				if sel.Sel.Name == "apply" {
					applies++
				}
				if sel.Sel.Name == "statsError" {
					errors++
				}
				if sel.Sel.Name == "UpdateWithLock" {
					ast.Inspect(call, func(inner ast.Node) bool {
						if s, ok := inner.(*ast.SelectorExpr); ok {
							if id, ok := s.X.(*ast.Ident); ok && id.Name == "extBucket" {
								t.Errorf("remote getter inside object lock: %s", s.Sel.Name)
							}
						}
						return true
					})
				}
			}
			return true
		})
		if captures != 1 || applies != 1 || errors != 1 {
			t.Errorf("%s: capture=%d apply=%d errors=%d", name, captures, applies, errors)
		}
	}
}

func TestBucketSnapshotOperationsIgnoreOnlyStandaloneIncompleteStats(t *testing.T) {
	incomplete := captureCloudBucketSnapshot(&snapshotTestBucket{stats: cloudprovider.SBucketStats{-1, -1}}, true).statsError()
	if incomplete == nil {
		t.Fatal("reconciliation lost incomplete statistics")
	}
	if got := bucketOperationSyncError(incomplete); got != nil {
		t.Fatalf("successful operation failed for optional stats: %v", got)
	}
	for _, err := range []error{nil, errors.New(incomplete.Error()), errors.New("database update failed"), fmt.Errorf("write failed: %w", incomplete), errors.Join(incomplete, errors.New("other failure"))} {
		if got := bucketOperationSyncError(err); got != err {
			t.Fatalf("operation error changed: got=%v want=%v", got, err)
		}
	}
}

func TestBucketSnapshotStatsEventUsesOnlyWrittenColumns(t *testing.T) {
	for _, tc := range []struct {
		diff    sqlchemy.UpdateDiffs
		changed bool
	}{
		{nil, false}, {sqlchemy.UpdateDiffs{"acl": {}, "status": {}}, false},
		{sqlchemy.UpdateDiffs{"size_bytes": {}}, true}, {sqlchemy.UpdateDiffs{"object_cnt": {}}, true},
		{sqlchemy.UpdateDiffs{"size_bytes_limit": {}}, false},
	} {
		if got := bucketStatsChanged(tc.diff); got != tc.changed {
			t.Errorf("diff=%v changed=%v", tc.diff, got)
		}
	}
}

func TestBucketSnapshotStrictReconciliationAndCompatibleOperationsAreWired(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "buckets.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ caller, callee string }{
		{"syncBuckets", "syncWithCloudBucket"},
		{"SyncWithCloudBucket", "syncWithCloudBucket"},
		{"SyncWithCloudBucket", "bucketOperationSyncError"},
		{"syncWithCloudBucket", "bucketStatsChanged"},
	} {
		found := 0
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != tc.caller {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					switch fun := call.Fun.(type) {
					case *ast.Ident:
						if fun.Name == tc.callee {
							found++
						}
					case *ast.SelectorExpr:
						if fun.Sel.Name == tc.callee {
							found++
						}
					}
				}
				return true
			})
		}
		if found != 1 {
			t.Errorf("%s must call %s once, found %d", tc.caller, tc.callee, found)
		}
	}
}
