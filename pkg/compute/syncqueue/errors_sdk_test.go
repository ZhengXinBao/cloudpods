package syncqueue

import (
	"errors"
	"testing"
)

func TestClassifySerializedSDKUnsupportedErrors(t *testing.T) {
	for _, message := range []string{
		"GetSecurityGroupIds: NotImplementedError",
		"GetDBNetworks: NotSupportedError",
	} {
		if got := ClassifyError(errors.New(message)); got != ErrorClassUnsupported {
			t.Errorf("%q: class=%q, want unsupported", message, got)
		}
	}
	for _, tc := range []struct {
		message string
		class   ErrorClass
	}{
		{"NotSupportedError; AccessDenied", ErrorClassPermission},
		{"NotImplementedError; invalid credential", ErrorClassAuth},
	} {
		if got := ClassifyError(errors.New(tc.message)); got != tc.class {
			t.Errorf("%q: class=%q, want %q", tc.message, got, tc.class)
		}
	}
}

func TestClassifyCommonCloudSDKPermanentErrors(t *testing.T) {
	for _, tc := range []struct {
		message string
		class   ErrorClass
	}{
		{"AuthFailure: SecretIdNotFound", ErrorClassAuth},
		{"InvalidAccessKeyId.NotFound", ErrorClassAuth},
		{"SignatureFailure", ErrorClassAuth},
		{"UnauthorizedOperation: not authorized", ErrorClassPermission},
		{"RegionDisabled: opt-in required", ErrorClassRegionDisabled},
		{"OptInRequired: region is not enabled", ErrorClassRegionDisabled},
	} {
		if got := ClassifyError(errors.New(tc.message)); got != tc.class {
			t.Errorf("%q: class=%q, want %q", tc.message, got, tc.class)
		}
	}
}
