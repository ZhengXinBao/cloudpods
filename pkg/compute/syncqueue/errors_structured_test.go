package syncqueue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	aliyunerrors "github.com/aliyun/alibaba-cloud-sdk-go/sdk/errors"
	"github.com/aws/aws-sdk-go/aws/awserr"
	tencenterrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
)

type sdkCodeError struct{ code, body string }

func (e sdkCodeError) Error() string     { return e.body }
func (e sdkCodeError) ErrorCode() string { return e.code }

type sdkLegacyCodeError struct{ code, body string }

func (e sdkLegacyCodeError) Error() string { return e.body }
func (e sdkLegacyCodeError) Code() string  { return e.code }

type sdkGetCodeError struct{ code, body string }

func (e sdkGetCodeError) Error() string   { return e.body }
func (e sdkGetCodeError) GetCode() string { return e.code }

type httpStatusError int

func (e httpStatusError) Error() string   { return `body: {"AccessDenied":true}` }
func (e httpStatusError) StatusCode() int { return int(e) }

type httpResponseError int

func (e httpResponseError) Error() string       { return "opaque response" }
func (e httpResponseError) HTTPStatusCode() int { return int(e) }

type causeError struct{ cause error }

func (e causeError) Error() string { return "opaque wrapper" }
func (e causeError) Cause() error  { return e.cause }

type aggregateError []error

func (e aggregateError) Error() string   { return "opaque aggregate" }
func (e aggregateError) Errors() []error { return e }

type cyclicError struct{}

func (e *cyclicError) Error() string { return "opaque cycle" }
func (e *cyclicError) Unwrap() error { return e }

type summarizedError map[ErrorClass]int

func (e summarizedError) Error() string                        { return "AuthFailure in discarded response data" }
func (e summarizedError) ErrorClassCounts() map[ErrorClass]int { return e }

func TestTimeoutParameterIsNotTimeout(t *testing.T) {
	err := errors.New(`decode field: {"timeout":30,"name":"broken"}`)
	if got := ClassifyError(err); got != ErrorClassUnknown {
		t.Fatalf("got %s", got)
	}
}

func TestClassifyStructuredCausesBeforeBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want ErrorClass
	}{
		{"deadline", fmt.Errorf("decode AuthFailure: %w", context.DeadlineExceeded), ErrorClassTimeout},
		{"net timeout", &net.DNSError{Err: "opaque failure", IsTimeout: true}, ErrorClassTimeout},
		{"net temporary", &net.DNSError{Err: "opaque failure", IsTemporary: true}, ErrorClassTemporary},
		{"EOF", fmt.Errorf("read: %w", io.EOF), ErrorClassTimeout},
		{"unexpected EOF", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), ErrorClassTimeout},
		{"SDK ErrorCode", fmt.Errorf("call: %w", sdkCodeError{"ThrottlingException", "AccessDenied timeout"}), ErrorClassThrottle},
		{"SDK Code", sdkLegacyCodeError{"InvalidAccessKeyId.NotFound", "opaque failure"}, ErrorClassAuth},
		{"SDK GetCode", sdkGetCodeError{"UnauthorizedOperation", "opaque failure"}, ErrorClassPermission},
		{"SDK unknown", fmt.Errorf("NotSupportedError: %w", sdkCodeError{"DecodeFailure", "AuthFailure forbidden timeout"}), ErrorClassUnknown},
		{"SDK unknown GetCode", sdkGetCodeError{"MissingRelation", "AccessDenied"}, ErrorClassUnknown},
		{"status 401", httpStatusError(401), ErrorClassAuth},
		{"status 403", httpStatusError(403), ErrorClassPermission},
		{"status 408", httpResponseError(408), ErrorClassTimeout},
		{"status 429", httpStatusError(429), ErrorClassThrottle},
		{"status 503", httpResponseError(503), ErrorClassTemporary},
		{"status 504", httpResponseError(504), ErrorClassTimeout},
		{"status 404", httpStatusError(404), ErrorClassUnknown},
		{"Cause", causeError{sdkCodeError{"RegionDisabled", "opaque failure"}}, ErrorClassRegionDisabled},
		{"aggregate", aggregateError{errors.New("unclassified"), causeError{sdkGetCodeError{"NotSupportedError", "opaque failure"}}}, ErrorClassUnsupported},
		{"summary", fmt.Errorf("wrapper: %w", summarizedError{ErrorClassThrottle: 2, ErrorClassUnknown: 9}), ErrorClassThrottle},
		{"cycle", &cyclicError{}, ErrorClassUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyError(tc.err); got != tc.want {
				t.Fatalf("class=%s, want %s", got, tc.want)
			}
		})
	}
}

func TestClassifyTextIgnoresPayloadAndPartialTokens(t *testing.T) {
	for _, message := range []string{
		`decode {"AuthFailure":"AccessDenied","RegionDisabled":true,"unsupported":true,"timeout":30}`,
		`decode [{"message":"forbidden; EOF; internalerror"}]`,
		`decode {"nested":{"x":"escaped \\"},"timeout":30}`,
		`decode {"body":"unfinished AccessDenied timeout`,
		"decode timeoutSeconds=30; unsupportedFeature=true; AccessDeniedCount=4; EOFMarker=1",
		"missing relation: vpc not found",
		"duplicate entry for external_id",
	} {
		if got := ClassifyError(errors.New(message)); got != ErrorClassUnknown {
			t.Errorf("%q: got %s", message, got)
		}
	}
	for _, tc := range []struct {
		message string
		want    ErrorClass
	}{
		{`AccessDenied: response {"timeout":30}`, ErrorClassPermission},
		{`decode {"message":"AccessDenied"}: i/o timeout`, ErrorClassTimeout},
		{`decode {"payload":"` + strings.Repeat("x", 10000) + `","error":"AuthFailure"}: NotSupportedError`, ErrorClassUnsupported},
		{"status code: 503", ErrorClassTemporary},
		{"Throttling.User", ErrorClassThrottle},
	} {
		if got := ClassifyError(errors.New(tc.message)); got != tc.want {
			t.Errorf("got %s, want %s", got, tc.want)
		}
	}
}

func TestClassifyMixedFailuresStablePrecedence(t *testing.T) {
	codes := []string{"AuthFailure", "AccessDenied", "RegionDisabled", "NotSupportedError", "RequestLimitExceeded", "RequestTimeout", "InternalError", "DecodeFailure"}
	classes := []ErrorClass{ErrorClassAuth, ErrorClassPermission, ErrorClassRegionDisabled, ErrorClassUnsupported, ErrorClassThrottle, ErrorClassTimeout, ErrorClassTemporary, ErrorClassUnknown}
	for i, code := range codes {
		for j := i; j < len(codes); j++ {
			a, b := sdkCodeError{code, "opaque"}, sdkCodeError{codes[j], "opaque"}
			for _, err := range []error{errors.Join(a, b), errors.Join(b, a), aggregateError{b, causeError{a}}} {
				if got := ClassifyError(err); got != classes[i] {
					t.Errorf("%s/%s: got %s, want %s", code, codes[j], got, classes[i])
				}
			}
		}
	}
	children := make([]error, 40)
	for i := range children {
		children[i] = errors.New("unclassified")
	}
	children[39] = sdkCodeError{"AuthFailure", "opaque"}
	if got := ClassifyError(errors.Join(children...)); got != ErrorClassAuth {
		t.Fatalf("late cause: %s", got)
	}
	var deep error = &cyclicError{}
	for i := 0; i < 200; i++ {
		deep = causeError{deep}
	}
	if got := ClassifyError(deep); got != ErrorClassUnknown {
		t.Fatalf("deep cause: %s", got)
	}
}

func TestRetryablePolicyRegression(t *testing.T) {
	for _, class := range []ErrorClass{ErrorClassAuth, ErrorClassPermission, ErrorClassRegionDisabled, ErrorClassUnsupported} {
		if Retryable(class) {
			t.Errorf("permanent class %s became retryable", class)
		}
	}
	for _, class := range []ErrorClass{ErrorClassThrottle, ErrorClassTimeout, ErrorClassTemporary, ErrorClassUnknown, "", "future-class"} {
		if !Retryable(class) {
			t.Errorf("class %s became terminal", class)
		}
	}
	if ClassifyError(nil) != "" {
		t.Fatal("nil error classified as failure")
	}
}

func TestClassifyDynamicallyNonComparableCause(t *testing.T) {
	err := causeError{aggregateError{sdkCodeError{"AccessDenied", "opaque"}}}
	if got := ClassifyError(err); got != ErrorClassPermission {
		t.Fatalf("dynamic aggregate cause: %s", got)
	}
}

func TestClassifyConfigurationAndActivationAreNotFailures(t *testing.T) {
	for _, message := range []string{
		"decode configuration timeout=30",
		"decode configuration timeout: 30",
		"timeout = 30; invalid input",
		"timeout:30ms; missing relation",
		"service not activated",
		"feature not activated for this service",
	} {
		if got := ClassifyError(errors.New(message)); got != ErrorClassUnknown {
			t.Errorf("%q: got %s, want unknown", message, got)
		}
	}
	for _, message := range []string{"i/o timeout", "request timeout", "region is not activated"} {
		want := ErrorClassTimeout
		if strings.HasPrefix(message, "region") {
			want = ErrorClassRegionDisabled
		}
		if got := ClassifyError(errors.New(message)); got != want {
			t.Errorf("%q: got %s, want %s", message, got, want)
		}
	}
}

func TestClassifyTextWindowBoundaries(t *testing.T) {
	for _, padding := range []int{889, 890, 891, 892, 893, 894, 895, 896, 990, 1000, 1008, 1016, 1024} {
		for _, message := range []string{
			strings.Repeat("x", padding) + " timeout =30",
			strings.Repeat("x", padding) + " timeout" + strings.Repeat(" ", 200) + ":30",
			`not{"timeout":30}implemented`,
		} {
			if got := ClassifyError(errors.New(message)); got != ErrorClassUnknown {
				t.Errorf("padding=%d: got %s for %q", padding, got, message)
			}
		}
		if got := ClassifyError(errors.New(strings.Repeat("x", padding) + " i/o timeout; " + strings.Repeat("y", 200))); got != ErrorClassTimeout {
			t.Errorf("lost timeout at boundary %d: %s", padding, got)
		}
	}
}

func TestClassifyMalformedFlattenedTextIsAlwaysUnknown(t *testing.T) {
	for _, payload := range []string{
		`decode {"body":"unfinished`,
		`decode {"body":"finished"`,
		`decode "unfinished`,
		`decode [{"body":"finished"}] }`,
		`decode {"body":]`,
		`decode {"body":invalid}`,
		`decode "invalid\qescape"`,
	} {
		t.Run(payload, func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				messages := []string{"AccessDenied", payload}
				if i%2 != 0 {
					messages[0], messages[1] = messages[1], messages[0]
				}
				// Force earlier matches through multiple bounded text windows.
				text := strings.Join(messages, ";"+strings.Repeat("x", 2048)+";")
				if got := ClassifyError(errors.New(text)); got != ErrorClassUnknown {
					t.Fatalf("permutation %d: got %s, want unknown", i, got)
				}
			}
		})
	}
}

func TestClassifyValidPayloadSemicolonsAndStructuredCauses(t *testing.T) {
	payload := `decode {"body":"first; AccessDenied; \"; AuthFailure","nested":[{"timeout":30}]}`
	for _, tc := range []struct {
		err  error
		want ErrorClass
	}{
		{errors.New(payload), ErrorClassUnknown},
		{errors.New(payload + ";RequestLimitExceeded"), ErrorClassThrottle},
		{errors.New("RequestLimitExceeded;" + payload), ErrorClassThrottle},
		{sdkCodeError{"AccessDenied", `decode {"body":"unfinished`}, ErrorClassPermission},
		{sdkCodeError{"DecodeFailure", `AuthFailure; decode {"body":"unfinished`}, ErrorClassUnknown},
		{NewErrorSummary(`decode {"body":"unfinished`, map[ErrorClass]int{ErrorClassAuth: 2}), ErrorClassAuth},
		{errors.Join(errors.New(`decode {"body":"unfinished`), errors.New("AccessDenied")), ErrorClassPermission},
	} {
		if got := ClassifyError(tc.err); got != tc.want {
			t.Errorf("%T: got %s, want %s", tc.err, got, tc.want)
		}
	}
}

func TestClassifySerializedAll5xxStatuses(t *testing.T) {
	for status := 500; status <= 599; status++ {
		message := fmt.Sprintf("request failed: status code: %d", status)
		if got := ClassifyError(errors.New(message)); got != ErrorClassTemporary {
			t.Errorf("%s: got %s, want temporary", message, got)
		}
	}
	for _, message := range []string{
		"status code: 200", "status code: 401", "status code: 404", "status code: 499", "status code: 600",
		"status code: 5", "status code: 50", "status code: 5000", "status code: 599suffix", "xstatus code: 501",
		`decode {"body":"status code: 501; status code: 505; status code: 599"}`,
	} {
		if got := ClassifyError(errors.New(message)); got != ErrorClassUnknown {
			t.Errorf("%q: got %s, want unknown", message, got)
		}
	}
	for _, padding := range []int{880, 889, 895, 896, 1015, 1020, 1024} {
		message := strings.Repeat("x", padding) + " status code: 599; " + strings.Repeat("y", 200)
		if got := ClassifyError(errors.New(message)); got != ErrorClassTemporary {
			t.Errorf("padding %d: got %s", padding, got)
		}
	}
}

func TestClassifySerializedTencentSDKError(t *testing.T) {
	for _, tc := range []struct {
		code      string
		want      ErrorClass
		retryable bool
	}{
		{"AuthFailure", ErrorClassAuth, false},
		{"UnauthorizedOperation", ErrorClassPermission, false},
		{"RequestLimitExceeded", ErrorClassThrottle, true},
	} {
		for _, requestID := range []string{"", "request-id"} {
			sdkErr := tencenterrors.NewTencentCloudSDKError(tc.code, "request failed", requestID)
			serialized := errors.New(sdkErr.Error())
			if got := ClassifyError(serialized); got != tc.want || Retryable(got) != tc.retryable {
				t.Errorf("%q: class=%s retryable=%v, want %s/%v", serialized, got, Retryable(got), tc.want, tc.retryable)
			}
		}
	}
}

func TestClassifyTencentPrefixDoesNotBypassPayloadRules(t *testing.T) {
	validBody := `{"body":"AuthFailure; UnauthorizedOperation","timeout":30}`
	for _, tc := range []struct {
		code, body string
		want       ErrorClass
	}{
		{"RequestLimitExceeded", validBody, ErrorClassThrottle},
		{"DecodeFailure", validBody, ErrorClassUnknown},
		{"AuthFailure", `decode {"body":"unfinished`, ErrorClassUnknown},
	} {
		serialized := tencenterrors.NewTencentCloudSDKError(tc.code, tc.body, "request-id").Error()
		if got := ClassifyError(errors.New(serialized)); got != tc.want {
			t.Errorf("%q: got %s, want %s", serialized, got, tc.want)
		}
	}
	serialized := tencenterrors.NewTencentCloudSDKError("AuthFailure", "request failed", "").Error()
	for _, message := range []string{
		`[OtherSDKError] Code=AuthFailure`,
		`[TencentCloudSDKErrorExtra] Code=AuthFailure`,
		`[TencentCloudSDKError] Other=AuthFailure`,
		`[[TencentCloudSDKError] Code=AuthFailure]`,
		`["[TencentCloudSDKError] Code=AuthFailure"]`,
		`{"body":"[TencentCloudSDKError] Code=AuthFailure"}`,
		serialized + `;decode {"body":"unfinished`,
		`decode {"body":"unfinished;` + serialized,
	} {
		if got := ClassifyError(errors.New(message)); got != ErrorClassUnknown {
			t.Errorf("%q: payload bypass classified %s", message, got)
		}
	}
}

func TestClassifyActualAliyunSDKClientErrors(t *testing.T) {
	for _, tc := range []struct {
		code, message string
		want          ErrorClass
	}{
		{aliyunerrors.TimeoutErrorCode, fmt.Sprintf(aliyunerrors.TimeoutErrorMessage, "3", "1"), ErrorClassTimeout},
		{aliyunerrors.UnsupportedCredentialErrorCode, fmt.Sprintf(aliyunerrors.UnsupportedCredentialErrorMessage, "opaque"), ErrorClassUnsupported},
		{aliyunerrors.UnsupportedParamPositionErrorCode, fmt.Sprintf(aliyunerrors.UnsupportedParamPositionErrorMessage, "opaque"), ErrorClassUnsupported},
	} {
		sdkErr := aliyunerrors.NewClientError(tc.code, tc.message, nil)
		for _, err := range []error{sdkErr, fmt.Errorf("request: %w", sdkErr), errors.New(sdkErr.Error())} {
			if got := ClassifyError(err); got != tc.want {
				t.Errorf("%s (%T): got %s, want %s", tc.code, err, got, tc.want)
			}
		}
	}
	origin := aliyunerrors.NewClientError(aliyunerrors.JsonUnmarshalErrorCode, "opaque failure", context.DeadlineExceeded)
	if got := ClassifyError(fmt.Errorf("request: %w", origin)); got != ErrorClassTimeout {
		t.Errorf("OriginError lost: %s", got)
	}
	unknown := aliyunerrors.NewClientError("SDK.UnknownFailure", "AuthFailure not supported", nil)
	if got := ClassifyError(unknown); got != ErrorClassUnknown {
		t.Errorf("unknown structured SDK code inferred from body: %s", got)
	}
}

func TestClassifyActualAliyunSDKHttpStatus(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   ErrorClass
	}{
		{401, ErrorClassAuth}, {403, ErrorClassPermission}, {429, ErrorClassThrottle},
		{503, ErrorClassTemporary}, {504, ErrorClassTimeout}, {404, ErrorClassUnknown},
	} {
		err := aliyunerrors.NewServerError(tc.status, `{"Code":"OpaqueServerFailure","Message":"AuthFailure"}`, "")
		if got := ClassifyError(fmt.Errorf("request: %w", err)); got != tc.want {
			t.Errorf("HttpStatus %d: got %s, want %s", tc.status, got, tc.want)
		}
	}
}

func TestClassifyAliyunLabelsDoNotBypassPayloadRules(t *testing.T) {
	for _, message := range []string{
		`[SDK.TimeoutErrorExtra] opaque`, `[SDK.UnknownFailure] not supported`,
		`[[SDK.TimeoutError] opaque]`, `["[SDK.TimeoutError] opaque"]`,
		`{"body":"[SDK.TimeoutError] opaque"}`,
		aliyunerrors.NewClientError(aliyunerrors.TimeoutErrorCode, `decode {"body":"unfinished`, nil).Error(),
	} {
		if got := ClassifyError(errors.New(message)); got != ErrorClassUnknown {
			t.Errorf("%q: got %s", message, got)
		}
	}
	err := aliyunerrors.NewClientError(aliyunerrors.TimeoutErrorCode, `{"body":"AuthFailure; not supported"}`, nil)
	if got := ClassifyError(errors.New(err.Error())); got != ErrorClassTimeout {
		t.Errorf("JSON body overrode timeout label: %s", got)
	}
}

func TestClassifySerializedSDKUsesOnlyCodeFields(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want ErrorClass
	}{
		{aliyunerrors.NewServerError(503, `{"Code":"InternalError","Message":"upstream returned an unsupported protocol"}`, "AccessDenied"), ErrorClassTemporary},
		{aliyunerrors.NewServerError(429, `{"Code":"Throttling","Message":"AuthFailure not supported"}`, ""), ErrorClassThrottle},
		{aliyunerrors.NewServerError(400, `{"Code":"NewProviderCode","Message":"AuthFailure not supported"}`, ""), ErrorClassUnknown},
		{tencenterrors.NewTencentCloudSDKError("RequestLimitExceeded", "AuthFailure not supported", "AccessDenied"), ErrorClassThrottle},
		{tencenterrors.NewTencentCloudSDKError("NewProviderCode", "AuthFailure not supported", ""), ErrorClassUnknown},
		{aliyunerrors.NewClientError(aliyunerrors.TimeoutErrorCode, "AuthFailure not supported", nil), ErrorClassTimeout},
		{aliyunerrors.NewClientError(aliyunerrors.UnsupportedCredentialErrorCode, "AuthFailure", nil), ErrorClassUnsupported},
	} {
		for _, prefix := range []string{"", "DescribeInstances: "} {
			if got := ClassifyError(errors.New(prefix + tc.err.Error())); got != tc.want {
				t.Errorf("%q: got %s, want %s", prefix+tc.err.Error(), got, tc.want)
			}
		}
	}
}

func TestClassifySDKFlattenedBoundariesAreAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		body string
		want ErrorClass
	}{
		{"request failed; AccessDenied", ErrorClassUnknown},
		{"request failed; [TencentCloudSDKError] Code=AuthFailure, Message=failure", ErrorClassUnknown},
		{"[TencentCloudSDKError] Code=AuthFailure, Message=failure", ErrorClassUnknown},
		{"SDK.ServerError\nErrorCode: AuthFailure\nRecommend: \nRequestId: \nMessage: failure", ErrorClassThrottle},
	} {
		err := tencenterrors.NewTencentCloudSDKError("RequestLimitExceeded", tc.body, "")
		if got := ClassifyError(errors.New(err.Error())); got != tc.want {
			t.Errorf("ambiguous SDK body: %s", got)
		}
	}
	for _, sdkErr := range []error{
		tencenterrors.NewTencentCloudSDKError("InternalError", "unsupported protocol", ""),
		aliyunerrors.NewServerError(503, `{"Code":"InternalError","Message":"unsupported protocol"}`, ""),
	} {
		for _, message := range []string{sdkErr.Error() + ";AccessDenied", "AccessDenied;" + sdkErr.Error()} {
			if got := ClassifyError(errors.New(message)); got != ErrorClassUnknown {
				t.Errorf("ambiguous flattened order: %s", got)
			}
		}
	}
}

func TestClassifyAWSBatchVisitsOriginalsOnce(t *testing.T) {
	err := awserr.NewBatchError("UnknownBatch", "opaque", []error{
		awserr.New("InternalError", "unsupported protocol", nil),
		awserr.New("AccessDenied", "opaque", nil),
	})
	got := ErrorClassCounts(err)
	if got[ErrorClassUnknown] != 1 || got[ErrorClassTemporary] != 1 || got[ErrorClassPermission] != 1 || len(got) != 3 {
		t.Fatalf("AWS batch synthesized duplicate causes: %v", got)
	}
}
