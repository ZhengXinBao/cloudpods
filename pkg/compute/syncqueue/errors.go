package syncqueue

import "strings"

type ErrorClass string

const (
	ErrorClassAuth           ErrorClass = "auth"
	ErrorClassPermission     ErrorClass = "permission"
	ErrorClassRegionDisabled ErrorClass = "region_disabled"
	ErrorClassUnsupported    ErrorClass = "unsupported"
	ErrorClassThrottle       ErrorClass = "throttle"
	ErrorClassTimeout        ErrorClass = "timeout"
	ErrorClassTemporary      ErrorClass = "temporary"
	ErrorClassUnknown        ErrorClass = "unknown"
)

func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ""
	}
	message := strings.ToLower(err.Error())
	switch {
	case containsAny(message, "invalidclienttoken", "invalid access key", "invalid credential", "invalidaccesskeyid", "authfailure", "signaturedoesnotmatch", "signaturefailure", "authentication failed", "unauthenticated"):
		return ErrorClassAuth
	case containsAny(message, "accessdenied", "access denied", "authorizationerror", "permissiondenied", "permission denied", "not authorized", "unauthorizedoperation", "unauthorized", "forbidden"):
		return ErrorClassPermission
	case containsAny(message, "region is not enabled", "region not enabled", "regiondisabled", "not activated", "not-opted-in", "notoptedin", "opt-in", "opted-in", "optinrequired", "region disabled", "sync region disabled"):
		return ErrorClassRegionDisabled
	case containsAny(message, "not implemented", "not supported", "unsupported", "notimplementederror", "notsupportederror"):
		return ErrorClassUnsupported
	case containsAny(message, "throttl", "rate exceeded", "requestlimitexceeded", "too many requests", "slowdown"):
		return ErrorClassThrottle
	case containsAny(message, "timeout", "deadline exceeded", "connection reset", "connection refused", "eof"):
		return ErrorClassTimeout
	case containsAny(message, "temporarily unavailable", "service unavailable", "internalerror", "internal server error", "bad gateway", "gateway timeout", "status code: 5"):
		return ErrorClassTemporary
	default:
		return ErrorClassUnknown
	}
}

func Retryable(class ErrorClass) bool {
	switch class {
	case ErrorClassAuth, ErrorClassPermission, ErrorClassRegionDisabled, ErrorClassUnsupported:
		return false
	default:
		return true
	}
}

func containsAny(s string, values ...string) bool {
	for _, value := range values {
		if strings.Contains(s, value) {
			return true
		}
	}
	return false
}
