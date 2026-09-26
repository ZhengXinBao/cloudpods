package models

import (
	"fmt"
	"testing"

	"yunion.io/x/pkg/errors"
	"yunion.io/x/pkg/util/httputils"
)

func TestUnsupportedAutoKubeImportRequiresExactStructuredError(t *testing.T) {
	const detail = "Not found support driver by import/aws/guest"
	unsupported := &httputils.JSONClientError{Code: 404, Class: "NotFoundError", Details: detail}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"exact", unsupported, true},
		{"wrapped create", errors.Wrapf(unsupported, "Create"), true},
		{"wrapped reimport", errors.Wrap(errors.Wrap(unsupported, "Create"), "import cluster again when updating"), true},
		{"nil", nil, false},
		{"unstructured", fmt.Errorf("404 NotFoundError: %s", detail), false},
		{"general404", &httputils.JSONClientError{Code: 404, Class: "NotFoundError", Details: "kubecluster was not found"}, false},
		{"permission", &httputils.JSONClientError{Code: 403, Class: "NotFoundError", Details: detail}, false},
		{"wrongclass", &httputils.JSONClientError{Code: 404, Class: "ForbiddenError", Details: detail}, false},
		{"otherprovider", &httputils.JSONClientError{Code: 404, Class: "NotFoundError", Details: "Not found support driver by import/azure/guest"}, false},
		{"differentmode", &httputils.JSONClientError{Code: 404, Class: "NotFoundError", Details: "Not found support driver by selfbuild/aws/guest"}, false},
		{"extrasuffix", &httputils.JSONClientError{Code: 404, Class: "NotFoundError", Details: detail + ": permission denied"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isUnsupportedAutoKubeImport(tc.err); got != tc.want {
				t.Fatalf("got %v, want %v for %v", got, tc.want, tc.err)
			}
		})
	}
}
