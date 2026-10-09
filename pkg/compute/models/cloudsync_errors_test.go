package models

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	aliyunerrors "github.com/aliyun/alibaba-cloud-sdk-go/sdk/errors"
	tencenterrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	tencenthttp "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/http"

	"yunion.io/x/onecloud/pkg/compute/syncqueue"
)

func TestCloudSyncErrorsConcurrentAndIsolated(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	_, other := WithCloudSyncErrors(context.Background())
	if collector.Error() != nil {
		t.Fatal("empty collector reports failure")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); cloudSyncError(ctx, "cloud request failed: %s", "timeout") }()
	}
	wg.Wait()
	if err := collector.Error(); err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatal("lost cloud failures", err)
	}
	if other.Error() != nil {
		t.Fatal("collector leaked into another job")
	}
	cloudSyncError(context.Background(), "legacy cloud failure")
	if other.Error() != nil {
		t.Fatal("normal context collected an error")
	}
}

type collectorSDKError struct{ code, message string }

func (e *collectorSDKError) Error() string     { return e.message }
func (e *collectorSDKError) ErrorCode() string { return e.code }

func cloudErrorCounts(t *testing.T, err error) map[syncqueue.ErrorClass]int {
	t.Helper()
	summary, ok := err.(interface {
		ErrorClassCounts() map[syncqueue.ErrorClass]int
	})
	if !ok {
		t.Fatalf("error has no class summary: %T", err)
	}
	return summary.ErrorClassCounts()
}

func TestCloudSyncErrorsRetainsEveryClassBeyondMessageLimit(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	for i := 0; i < 9; i++ {
		cloudSyncError(ctx, "initial failure: %v", errors.New("missing relation"))
	}
	for _, code := range []string{"InternalError", "RequestTimeout", "RequestLimitExceeded", "NotSupportedError", "RegionDisabled", "AccessDenied", "AuthFailure"} {
		cloudSyncError(ctx, "request failed: %v", &collectorSDKError{code, "opaque response"})
	}
	err := collector.Error()
	if got := syncqueue.ClassifyError(err); got != syncqueue.ErrorClassAuth {
		t.Fatalf("class after eighth error=%s", got)
	}
	counts := cloudErrorCounts(t, err)
	if counts[syncqueue.ErrorClassUnknown] != 9 || len(counts) != 8 {
		t.Fatalf("incomplete counts: %v", counts)
	}
	for class, count := range counts {
		if class != syncqueue.ErrorClassUnknown && count != 1 {
			t.Fatalf("%s count=%d", class, count)
		}
	}
	counts[syncqueue.ErrorClassAuth] = 0
	if cloudErrorCounts(t, err)[syncqueue.ErrorClassAuth] != 1 {
		t.Fatal("summary exposed mutable counts")
	}
	if len(err.Error()) > 8192 || !strings.Contains(err.Error(), "16 cloud synchronization errors") {
		t.Fatalf("bad bounded summary: %v", err)
	}
}

func TestCloudSyncErrorsClassifiesArgumentsBeforeFormatting(t *testing.T) {
	for _, tc := range []struct {
		code string
		want syncqueue.ErrorClass
	}{
		{"RequestLimitExceeded", syncqueue.ErrorClassThrottle},
		{"DecodeFailure", syncqueue.ErrorClassUnknown},
	} {
		ctx, collector := WithCloudSyncErrors(context.Background())
		original := &collectorSDKError{tc.code, strings.Repeat("x", 1200) + " AuthFailure " + strings.Repeat("y", 1200)}
		cloudSyncError(ctx, "decode forbidden data: %v", fmt.Errorf("wrapper: %w", original))
		original.code = "AuthFailure"
		err := collector.Error()
		if got := syncqueue.ClassifyError(err); got != tc.want {
			t.Fatalf("%s: got %s", tc.code, got)
		}
		if len(err.Error()) > 600 || strings.Contains(err.Error(), "AuthFailure") {
			t.Fatalf("raw cause retained in text: %v", err)
		}
		if errors.Is(err, original) {
			t.Fatal("collector retained raw provider error")
		}
	}
	ctx, collector := WithCloudSyncErrors(context.Background())
	cloudSyncError(ctx, "failures: %v / %v", &collectorSDKError{"RequestTimeout", "opaque"}, &collectorSDKError{"AccessDenied", "opaque"})
	counts := cloudErrorCounts(t, collector.Error())
	if counts[syncqueue.ErrorClassTimeout] != 1 || counts[syncqueue.ErrorClassPermission] != 1 {
		t.Fatalf("multiple arguments: %v", counts)
	}
}

func TestCloudSyncErrorsConservativeStringFallback(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	cloudSyncError(ctx, "decode field: %s", `{"timeout":30,"code":"AccessDenied"}`)
	if got := syncqueue.ClassifyError(collector.Error()); got != syncqueue.ErrorClassUnknown {
		t.Fatalf("payload classified as %s", got)
	}
	cloudSyncError(ctx, "operation failed: %s", "NotSupportedError")
	if got := syncqueue.ClassifyError(collector.Error()); got != syncqueue.ErrorClassUnsupported {
		t.Fatalf("serialized error lost: %s", got)
	}
}

func TestSyncResultSetErrorDeterministicMixedResources(t *testing.T) {
	results := SSyncResultSet{"clean": {}, "nil": nil, "counter-only": {}}
	results["counter-only"].UpdateErrCnt = 1
	for name, messages := range map[string][]string{
		"zones":  {"RequestTimeout", "missing relation"},
		"guests": {"RequestLimitExceeded", "AccessDenied"},
		"disks":  {"NotSupportedError", "duplicate entry"},
	} {
		result := &SyncResult{}
		for _, message := range messages {
			result.Error(errors.New(message))
		}
		results[name] = result
	}
	wantText := ""
	var wantCounts map[syncqueue.ErrorClass]int
	for i := 0; i < 40; i++ {
		err := syncResultSetError(results)
		if got := syncqueue.ClassifyError(err); got != syncqueue.ErrorClassPermission {
			t.Fatalf("mixed resources class=%s", got)
		}
		text := err.Error()
		last := -1
		for _, name := range []string{"counter-only", "disks", "guests", "zones"} {
			pos := strings.Index(text, "resource sync "+name+" has reconciliation errors")
			if pos <= last {
				t.Fatalf("resource absent or unordered: %s", text)
			}
			last = pos
		}
		if strings.Contains(text, "resource sync clean") || strings.Contains(text, "resource sync nil") {
			t.Fatalf("included successful resource: %s", text)
		}
		counts := cloudErrorCounts(t, err)
		if len(counts) != 5 {
			t.Fatalf("lost mixed classes: %v", counts)
		}
		if i == 0 {
			wantText, wantCounts = text, counts
		}
		if text != wantText || !reflect.DeepEqual(counts, wantCounts) {
			t.Fatalf("nondeterministic summary: %s, %v", text, counts)
		}
	}
}

func TestSyncResultSetErrorBoundedAndCannotRecoverLostTypes(t *testing.T) {
	results := SSyncResultSet{}
	for i := 0; i < 30; i++ {
		result := &SyncResult{}
		result.Error(errors.New(strings.Repeat("payload", 200)))
		results[fmt.Sprintf("resource-%02d", i)] = result
	}
	results["resource-29"].Error(errors.New("AuthFailure"))
	err := syncResultSetError(results)
	if len(err.Error()) > 8192 || syncqueue.ClassifyError(err) != syncqueue.ErrorClassAuth {
		t.Fatalf("bounded late resource: %v", err)
	}
	if cloudErrorCounts(t, err)[syncqueue.ErrorClassAuth] != 1 {
		t.Fatal("late resource class missing")
	}
	lost := &SyncResult{}
	lost.Error(&collectorSDKError{"AccessDenied", "opaque failure"})
	if got := syncqueue.ClassifyError(syncResultSetError(SSyncResultSet{"lost": lost})); got != syncqueue.ErrorClassPermission {
		t.Fatalf("lost class before result formatting: %s", got)
	}
}

func TestCloudSyncErrorsMergePreservesExecutorCause(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	merger, ok := interface{}(collector).(interface{ Merge(error) error })
	if !ok {
		t.Fatal("collector cannot merge an existing executor failure")
	}
	original := errors.New("missing relation")
	if merger.Merge(original) != original {
		t.Fatal("empty collector changed original cause")
	}
	cloudSyncError(ctx, "request failed: %v", &collectorSDKError{"AccessDenied", "opaque response"})
	err := merger.Merge(original)
	if !errors.Is(err, original) {
		t.Fatal("executor cause lost")
	}
	if got := syncqueue.ClassifyError(err); got != syncqueue.ErrorClassPermission {
		t.Fatalf("collector failure suppressed by executor: %s", got)
	}
}
func TestCloudSyncErrorsBounded(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	for i := 0; i < 100; i++ {
		cloudSyncError(ctx, "failure %d", i)
	}
	err := collector.Error()
	if err == nil || len(err.Error()) > 8192 || !strings.Contains(err.Error(), "100") {
		t.Fatalf("unbounded or missing failure count: %v", err)
	}
}

func TestCloudSyncErrorsKeepsRootCauseSuffix(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	cloudSyncError(ctx, "DescribeCluster: %s: tags type mismatch", strings.Repeat("payload", 200))
	err := collector.Error()
	if err == nil || !strings.Contains(err.Error(), "DescribeCluster") || !strings.Contains(err.Error(), "tags type mismatch") {
		t.Fatalf("truncation hid operation or root cause: %v", err)
	}
	if len(err.Error()) > 600 {
		t.Fatalf("unbounded diagnostic: %d", len(err.Error()))
	}
}

func TestSyncResultSetErrorMalformedFlattenedPermutations(t *testing.T) {
	var display string
	for i := 0; i < 1000; i++ {
		result := &SyncResult{}
		messages := []string{"AccessDenied", `decode {"body":"unfinished`}
		if i%2 != 0 {
			messages[0], messages[1] = messages[1], messages[0]
		}
		for _, message := range messages {
			result.Error(errors.New(message))
		}
		if got := syncqueue.ClassifyError(syncResultSetError(SSyncResultSet{"guests": result})); got != syncqueue.ErrorClassPermission {
			t.Fatalf("iteration %d: independent permission cause lost: %s", i, got)
		}
		legacy := &SyncResult{}
		legacy.Error(errors.New(result.AllError().Error()))
		err := syncResultSetError(SSyncResultSet{"guests": legacy})
		if got := syncqueue.ClassifyError(err); got != syncqueue.ErrorClassUnknown {
			t.Fatalf("iteration %d: got %s, want unknown", i, got)
		}
		counts := cloudErrorCounts(t, err)
		if len(counts) != 1 || counts[syncqueue.ErrorClassUnknown] != 1 {
			t.Fatalf("iteration %d: ambiguous counts %v", i, counts)
		}
		if i == 0 {
			display = err.Error()
		}
		if err.Error() != display {
			t.Fatalf("iteration %d: display changed: %s", i, err)
		}
		// Vendor map iteration is exercised again without changing the result.
		if got := syncqueue.ClassifyError(syncResultSetError(SSyncResultSet{"guests": legacy})); got != syncqueue.ErrorClassUnknown {
			t.Fatalf("repeat %d: got %s", i, got)
		}
	}
}

func TestSyncResultSetErrorValidPayloadSemicolons(t *testing.T) {
	for i := 0; i < 1000; i++ {
		result := &SyncResult{}
		messages := []string{`decode {"body":"first; AccessDenied; \"; AuthFailure","timeout":30}`, "missing relation"}
		if i%2 != 0 {
			messages[0], messages[1] = messages[1], messages[0]
		}
		for _, message := range messages {
			result.Error(errors.New(message))
		}
		if got := syncqueue.ClassifyError(syncResultSetError(SSyncResultSet{"guests": result})); got != syncqueue.ErrorClassUnknown {
			t.Fatalf("iteration %d: payload injected %s", i, got)
		}
		result.Error(errors.New("RequestLimitExceeded"))
		if got := syncqueue.ClassifyError(syncResultSetError(SSyncResultSet{"guests": result})); got != syncqueue.ErrorClassThrottle {
			t.Fatalf("iteration %d: lost unambiguous failure: %s", i, got)
		}
	}
}

func TestCloudSyncErrorsMalformedTextDoesNotOverrideStructuredClasses(t *testing.T) {
	ctx, collector := WithCloudSyncErrors(context.Background())
	cloudSyncError(ctx, "request: %v", &collectorSDKError{"AccessDenied", `decode {"body":"unfinished`})
	ambiguous := &SyncResult{}
	ambiguous.Error(errors.New(`AuthFailure; decode {"body":"unfinished`))
	results := SSyncResultSet{"guests": ambiguous}
	if got := syncqueue.ClassifyError(collector.Merge(syncResultSetError(results))); got != syncqueue.ErrorClassPermission {
		t.Fatalf("lost collector class: %s", got)
	}
	unambiguous := &SyncResult{}
	unambiguous.Error(errors.New("RequestLimitExceeded"))
	results["disks"] = unambiguous
	if got := syncqueue.ClassifyError(syncResultSetError(results)); got != syncqueue.ErrorClassThrottle {
		t.Fatalf("lost separate resource class: %s", got)
	}
}

func TestSyncResultSetErrorSerializedTencentSDK(t *testing.T) {
	for _, tc := range []struct {
		code, body string
		want       syncqueue.ErrorClass
	}{
		{"AuthFailure", "request failed", syncqueue.ErrorClassAuth},
		{"UnauthorizedOperation", "request failed", syncqueue.ErrorClassPermission},
		{"RequestLimitExceeded", `{"body":"AuthFailure; UnauthorizedOperation","timeout":30}`, syncqueue.ErrorClassThrottle},
		{"DecodeFailure", `{"body":"AuthFailure; UnauthorizedOperation","timeout":30}`, syncqueue.ErrorClassUnknown},
		{"AuthFailure", `decode {"body":"unfinished`, syncqueue.ErrorClassUnknown},
	} {
		for _, requestID := range []string{"", "request-id"} {
			result := &SyncResult{}
			sdkErr := tencenterrors.NewTencentCloudSDKError(tc.code, tc.body, requestID)
			result.Error(sdkErr)
			for i := 0; i < 20; i++ {
				flattened := errors.New(result.AllError().Error())
				if got := syncqueue.ClassifyError(flattened); got != tc.want {
					t.Fatalf("%s, flattened iteration %d: got %s, want %s", tc.code, i, got, tc.want)
				}
				if got := syncqueue.ClassifyError(syncResultSetError(SSyncResultSet{"guests": result})); got != syncqueue.ClassifyError(sdkErr) {
					t.Fatalf("%s, resultset iteration %d: lost typed class, got %s", tc.code, i, got)
				}
			}
		}
	}
}

func TestSyncResultSetErrorSerializedAliyunSDK(t *testing.T) {
	for _, tc := range []struct {
		code string
		want syncqueue.ErrorClass
	}{
		{aliyunerrors.TimeoutErrorCode, syncqueue.ErrorClassTimeout},
		{aliyunerrors.UnsupportedCredentialErrorCode, syncqueue.ErrorClassUnsupported},
		{aliyunerrors.UnsupportedParamPositionErrorCode, syncqueue.ErrorClassUnsupported},
	} {
		result := &SyncResult{}
		result.Error(aliyunerrors.NewClientError(tc.code, "opaque failure", nil))
		for i := 0; i < 20; i++ {
			if got := syncqueue.ClassifyError(errors.New(result.AllError().Error())); got != tc.want {
				t.Fatalf("%s, flattened iteration %d: got %s", tc.code, i, got)
			}
			if got := syncqueue.ClassifyError(syncResultSetError(SSyncResultSet{"guests": result})); got != tc.want {
				t.Fatalf("%s, resultset iteration %d: got %s", tc.code, i, got)
			}
		}
	}
}

func TestCloudSyncSDKCodeSurvivesSerializationAndMerge(t *testing.T) {
	for _, tc := range []struct {
		name               string
		err                error
		direct, serialized syncqueue.ErrorClass
	}{
		{"aliyun temporary", aliyunerrors.NewServerError(503, `{"Code":"InternalError","Message":"upstream returned an unsupported protocol"}`, ""), syncqueue.ErrorClassTemporary, syncqueue.ErrorClassTemporary},
		{"aliyun throttle", aliyunerrors.NewServerError(429, `{"Code":"Throttling","Message":"upstream returned AccessDenied and forbidden content"}`, ""), syncqueue.ErrorClassThrottle, syncqueue.ErrorClassThrottle},
		{"aliyun unknown", aliyunerrors.NewServerError(400, `{"Code":"NewProviderCode","Message":"AuthFailure not supported"}`, ""), syncqueue.ErrorClassUnknown, syncqueue.ErrorClassUnknown},
		{"aliyun status lost", aliyunerrors.NewServerError(503, `{"Code":"NewProviderCode","Message":"AuthFailure not supported"}`, ""), syncqueue.ErrorClassTemporary, syncqueue.ErrorClassUnknown},
		{"aliyun recommendation", aliyunerrors.NewServerError(503, `{"Code":"InternalError","Message":"request failed","Recommend":"AuthFailure"}`, "AccessDenied"), syncqueue.ErrorClassTemporary, syncqueue.ErrorClassTemporary},
		{"aliyun client timeout", aliyunerrors.NewClientError(aliyunerrors.TimeoutErrorCode, "unsupported protocol AuthFailure", nil), syncqueue.ErrorClassTimeout, syncqueue.ErrorClassTimeout},
		{"tencent temporary", tencenterrors.NewTencentCloudSDKError("InternalError", "upstream returned an unsupported protocol", "AuthFailure"), syncqueue.ErrorClassTemporary, syncqueue.ErrorClassTemporary},
		{"tencent throttle", tencenterrors.NewTencentCloudSDKError("RequestLimitExceeded", "AuthFailure not supported", ""), syncqueue.ErrorClassThrottle, syncqueue.ErrorClassThrottle},
		{"tencent unknown", tencenterrors.NewTencentCloudSDKError("NewProviderCode", "AccessDenied unsupported", ""), syncqueue.ErrorClassUnknown, syncqueue.ErrorClassUnknown},
		{"tencent nested SDK text", tencenterrors.NewTencentCloudSDKError("RequestLimitExceeded", "SDK.ServerError\nErrorCode: AuthFailure\nRecommend: \nRequestId: \nMessage: failure", ""), syncqueue.ErrorClassThrottle, syncqueue.ErrorClassThrottle},
		{"tencent nested bracket label", tencenterrors.NewTencentCloudSDKError("RequestLimitExceeded", "[TencentCloudSDKError] Code=AuthFailure, Message=failure", ""), syncqueue.ErrorClassThrottle, syncqueue.ErrorClassUnknown},
		{"tencent HTTP 503 HTML", tencenthttp.ParseFromHttpResponse(&http.Response{StatusCode: 503, Status: "503 Service Unavailable", Body: io.NopCloser(strings.NewReader("<html>unsupported protocol AccessDenied</html>"))}, &tencenthttp.BaseResponse{}), syncqueue.ErrorClassUnknown, syncqueue.ErrorClassUnknown},
		{"known permission", tencenterrors.NewTencentCloudSDKError("UnauthorizedOperation", "temporary failure", ""), syncqueue.ErrorClassPermission, syncqueue.ErrorClassPermission},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err == nil {
				t.Fatal("SDK did not return an error")
			}
			ctx, collector := WithCloudSyncErrors(context.Background())
			cloudSyncError(ctx, "%v", tc.err)
			result := &SyncResult{}
			result.Error(tc.err)
			resourceErr := syncResultSetError(SSyncResultSet{"guests": result})
			for _, stage := range []struct {
				name string
				err  error
				want syncqueue.ErrorClass
			}{
				{"typed", tc.err, tc.direct},
				{"serialized", errors.New(tc.err.Error()), tc.serialized},
				{"AllError", result.AllError(), tc.direct},
				{"resource summary", resourceErr, tc.direct},
				{"collector", collector.Error(), tc.direct},
				{"Merge", collector.Merge(resourceErr), tc.direct},
			} {
				if got := syncqueue.ClassifyError(stage.err); got != stage.want {
					t.Errorf("%s: got %s retryable=%v, want %s", stage.name, got, syncqueue.Retryable(got), stage.want)
				}
			}
		})
	}
}

func TestCloudSyncSDKMessageBoundaryAmbiguityPreservesIndependentFailures(t *testing.T) {
	transient := aliyunerrors.NewServerError(503, `{"Code":"InternalError","Message":"upstream unsupported protocol"}`, "")
	permanent := tencenterrors.NewTencentCloudSDKError("UnauthorizedOperation", "request denied", "")
	ctx, collector := WithCloudSyncErrors(context.Background())
	cloudSyncError(ctx, "%v", transient)
	cloudSyncError(ctx, "%v", permanent)
	for i := 0; i < 1000; i++ {
		mixed := &SyncResult{}
		causes := []error{transient, permanent}
		if i%2 != 0 {
			causes[0], causes[1] = causes[1], causes[0]
		}
		for _, cause := range causes {
			mixed.Error(cause)
		}
		if got := syncqueue.ClassifyError(syncResultSetError(SSyncResultSet{"guests": mixed})); got != syncqueue.ErrorClassPermission {
			t.Fatalf("typed mixed causes lost at %d: %s", i, got)
		}
		legacy := &SyncResult{}
		legacy.Error(errors.New(mixed.AllError().Error()))
		flattened := syncResultSetError(SSyncResultSet{"guests": legacy})
		if got := syncqueue.ClassifyError(flattened); got != syncqueue.ErrorClassUnknown {
			t.Fatalf("ambiguous iteration %d: got %s", i, got)
		}
		if got := syncqueue.ClassifyError(collector.Merge(flattened)); got != syncqueue.ErrorClassPermission {
			t.Fatalf("collector permanent cause lost at %d: %s", i, got)
		}
	}
	transientResult, permanentResult := &SyncResult{}, &SyncResult{}
	transientResult.Error(transient)
	permanentResult.Error(permanent)
	for _, err := range []error{
		errors.Join(transient, permanent),
		errors.Join(errors.New(transient.Error()), errors.New("AccessDenied")),
		syncResultSetError(SSyncResultSet{"guests": transientResult, "disks": permanentResult}),
		errors.New("InternalError;AccessDenied"),
	} {
		if got := syncqueue.ClassifyError(err); got != syncqueue.ErrorClassPermission {
			t.Errorf("independent permanent failure lost: %s", got)
		}
	}
}
