package models

import (
	"errors"

	"yunion.io/x/pkg/util/httputils"
)

// This compatibility exception is restricted to automatic inventory sync. The
// kubeserver driver registry uses this exact response for missing AWS import
// support; other 404s and manual import requests must remain failures.
func isUnsupportedAutoKubeImport(err error) bool {
	var response *httputils.JSONClientError
	// Cause() reduces JSONClientError to its class and discards Code/Details.
	return errors.As(err, &response) && response != nil &&
		response.Code == 404 && response.Class == "NotFoundError" &&
		response.Details == "Not found support driver by import/aws/guest"
}
