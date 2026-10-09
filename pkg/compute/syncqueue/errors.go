package syncqueue

import (
	"context"
	"encoding/json"
	"io"
	"reflect"
	"regexp"
	"strings"
	"syscall"
)

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

// Precedence describes the whole failed job. A permanent + transient mixture
// remains a failure; it does not imply that individual shards can be retried.
var errorClassOrder = [...]ErrorClass{
	ErrorClassAuth, ErrorClassPermission, ErrorClassRegionDisabled, ErrorClassUnsupported,
	ErrorClassThrottle, ErrorClassTimeout, ErrorClassTemporary, ErrorClassUnknown,
}

// ErrorClassSummary shares bounded classifications without retaining SDK objects.
// Counts are class observations, not necessarily distinct operations: an
// aggregate or serialized error can contain more than one class.
type ErrorClassSummary interface {
	ErrorClassCounts() map[ErrorClass]int
}

type classSummaryError struct {
	message string
	counts  [len(errorClassOrder)]int
}

func (e *classSummaryError) Error() string { return e.message }
func (e *classSummaryError) ErrorClassCounts() map[ErrorClass]int {
	counts := make(map[ErrorClass]int)
	for i, class := range errorClassOrder {
		if e.counts[i] > 0 {
			counts[class] = e.counts[i]
		}
	}
	return counts
}

// NewErrorSummary copies only a fixed vocabulary of counts and bounded text.
// Classifications must be captured before the caller truncates diagnostics.
func NewErrorSummary(message string, counts map[ErrorClass]int) error {
	if len(message) > 8192 {
		message = message[:4094] + "..." + message[len(message)-4095:]
	}
	e := &classSummaryError{message: message}
	for i, class := range errorClassOrder {
		if counts[class] > 0 {
			e.counts[i] = counts[class]
		}
	}
	return e
}

func ClassifyError(err error) ErrorClass {
	counts := ErrorClassCounts(err)
	for _, class := range errorClassOrder {
		if counts[class] > 0 {
			return class
		}
	}
	return ""
}

// ErrorClassCounts inspects causes before compatibility text. Walking is bounded
// even for cyclic, non-comparable or adversarial error graphs. Shared causes are
// visited once per graph; independent collector calls still count independently.
func ErrorClassCounts(err error) map[ErrorClass]int {
	counts := make(map[ErrorClass]int)
	seen := make(map[error]bool)
	remaining := 1024
	var visit func(error, int)
	visit = func(err error, depth int) {
		if err == nil {
			return
		}
		if depth >= 64 || remaining <= 0 {
			counts[ErrorClassUnknown]++
			return
		}
		remaining--
		if reflect.ValueOf(err).Comparable() {
			if seen[err] {
				return
			}
			seen[err] = true
		}
		if summary, ok := err.(ErrorClassSummary); ok {
			values := summary.ErrorClassCounts()
			for _, class := range errorClassOrder {
				if values[class] > 0 {
					counts[class] += values[class]
				}
			}
			return
		}
		class, structured := structuredErrorClass(err)
		if structured {
			counts[class]++
		}
		hasChild := false
		child := func(cause error) {
			if cause != nil {
				hasChild = true
				visit(cause, depth+1)
			}
		}
		children := func(causes []error) {
			for _, cause := range causes {
				child(cause)
				if remaining <= 0 {
					break
				}
			}
		}
		if wrapped, ok := err.(interface{ Unwrap() error }); ok {
			child(wrapped.Unwrap())
		}
		if wrapped, ok := err.(interface{ Cause() error }); ok {
			child(wrapped.Cause())
		}
		if wrapped, ok := err.(interface{ OriginError() error }); ok {
			child(wrapped.OriginError())
		}
		if aggregate, ok := err.(interface{ OrigErrs() []error }); ok {
			// AWS OrigErr may synthesize a lossy batch wrapper. Prefer the
			// complete original list instead of traversing both representations.
			children(aggregate.OrigErrs())
		} else if wrapped, ok := err.(interface{ OrigErr() error }); ok {
			child(wrapped.OrigErr())
		}
		if aggregate, ok := err.(interface{ Unwrap() []error }); ok {
			children(aggregate.Unwrap())
		}
		if aggregate, ok := err.(interface{ Errors() []error }); ok {
			children(aggregate.Errors())
		}
		// Wrapper text includes rendered bodies and must not override real causes,
		// including unknown SDK codes. Only unstructured leaves use text.
		if !structured && !hasChild {
			for class := range textErrorClasses(err.Error()) {
				counts[class]++
			}
		}
	}
	visit(err, 0)
	if err != nil && len(counts) == 0 {
		counts[ErrorClassUnknown] = 1
	}
	return counts
}

func structuredErrorClass(err error) (ErrorClass, bool) {
	structured := false
	codes := []string{}
	if coded, ok := err.(interface{ ErrorCode() string }); ok {
		structured = true
		codes = append(codes, coded.ErrorCode())
	}
	if coded, ok := err.(interface{ Code() string }); ok {
		structured = true
		codes = append(codes, coded.Code())
	}
	if coded, ok := err.(interface{ GetCode() string }); ok {
		structured = true
		codes = append(codes, coded.GetCode())
	}
	codeClasses := make(map[ErrorClass]bool)
	for _, code := range codes {
		codeClasses[errorCodeClass(code)] = true
	}
	for _, class := range errorClassOrder[:len(errorClassOrder)-1] {
		if codeClasses[class] {
			return class, true
		}
	}
	status := 0
	if response, ok := err.(interface{ HTTPStatusCode() int }); ok {
		structured, status = true, response.HTTPStatusCode()
	} else if response, ok := err.(interface{ StatusCode() int }); ok {
		structured, status = true, response.StatusCode()
	} else if response, ok := err.(interface{ HttpStatus() int }); ok {
		structured, status = true, response.HttpStatus()
	}
	switch status {
	case 401:
		return ErrorClassAuth, true
	case 403:
		return ErrorClassPermission, true
	case 429:
		return ErrorClassThrottle, true
	case 408, 504:
		return ErrorClassTimeout, true
	}
	if status >= 500 && status <= 599 {
		return ErrorClassTemporary, true
	}
	if err == context.DeadlineExceeded || err == io.EOF || err == io.ErrUnexpectedEOF || err == syscall.ECONNRESET || err == syscall.ECONNREFUSED {
		return ErrorClassTimeout, true
	}
	if timeout, ok := err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
		return ErrorClassTimeout, true
	}
	if temporary, ok := err.(interface{ Temporary() bool }); ok && temporary.Temporary() {
		return ErrorClassTemporary, true
	}
	return ErrorClassUnknown, structured
}

func errorCodeClass(code string) ErrorClass {
	if len(code) > 128 {
		return ErrorClassUnknown
	}
	// Alibaba client codes must match in full, before legacy suffix handling.
	for _, known := range aliyunClientErrorCodes {
		if strings.EqualFold(code, known.code) {
			return known.class
		}
	}
	root, _, _ := strings.Cut(strings.ToLower(code), ".")
	for i, tokens := range errorTokens {
		for _, token := range tokens {
			if root == token {
				return errorClassOrder[i]
			}
		}
	}
	return ErrorClassUnknown
}

// Exact serialized tokens/phrases retain legacy SDK compatibility without
// matching fields such as timeoutSeconds, EOFMarker or unsupportedFeature.
var errorTokens = [...][]string{
	{"invalidclienttoken", "invalidclienttokenid", "invalid access key", "invalid credential", "invalidaccesskeyid", "authfailure", "signaturedoesnotmatch", "signaturefailure", "authentication failed", "unauthenticated"},
	{"accessdenied", "accessdeniedexception", "access denied", "authorizationerror", "permissiondenied", "permission denied", "not authorized", "unauthorizedoperation", "unauthorized", "forbidden"},
	{"region is not enabled", "region not enabled", "regiondisabled", "region not activated", "region is not activated", "not-opted-in", "notoptedin", "opt-in", "opted-in", "optinrequired", "region disabled"},
	{"not implemented", "not supported", "unsupported", "notimplemented", "notsupported", "notimplementederror", "notsupportederror"},
	{"throttle", "throttled", "throttling", "throttlingexception", "throttlingerror", "rate exceeded", "requestlimitexceeded", "too many requests", "slowdown"},
	{"timeout", "requesttimeout", "requesttimeoutexception", "deadline exceeded", "connection reset", "connection refused", "eof"},
	{"temporarily unavailable", "service unavailable", "internalerror", "internal server error", "bad gateway", "gateway timeout"},
}

var serializedServerStatusPattern = regexp.MustCompile(`status code: 5[0-9]{2}`)

// Exact codes emitted by the vendored Alibaba SDK ClientError. Unknown SDK
// codes/labels remain unknown; their message bodies cannot supply a typed code.
var aliyunClientErrorCodes = [...]struct {
	code  string
	class ErrorClass
}{
	{"SDK.TimeoutError", ErrorClassTimeout},
	{"SDK.UnsupportedCredential", ErrorClassUnsupported},
	{"SDK.UnsupportedParamPosition", ErrorClassUnsupported},
}

// Read only the code field of these actual SDK formats. The remaining message
// and metadata are opaque: none can supply a class or another SDK error header.
// The skip length exempts the exact diagnostic label from JSON-array parsing.
func serializedSDKHeader(message string) (class ErrorClass, skip int, found bool) {
	const tencentLabel = "[TencentCloudSDKError]"
	const tencentPrefix = tencentLabel + " Code="
	const aliyunLabel = "SDK.ServerError"
	const aliyunPrefix = aliyunLabel + "\nErrorCode: "
	var code string
	var valid bool
	switch {
	case strings.HasPrefix(message, tencentPrefix):
		code, _, valid = strings.Cut(message[len(tencentPrefix):], ", Message=")
		skip = len(tencentLabel)
	case strings.HasPrefix(message, aliyunPrefix):
		var rest string
		code, rest, valid = strings.Cut(message[len(aliyunPrefix):], "\nRecommend: ")
		_, rest, hasRequestID := strings.Cut(rest, "\nRequestId: ")
		_, _, hasMessage := strings.Cut(rest, "\nMessage: ")
		valid = valid && hasRequestID && hasMessage
		skip = len(aliyunLabel)
	default:
		for _, known := range aliyunClientErrorCodes {
			label := "[" + known.code + "]"
			if strings.HasPrefix(message, label+" ") {
				return known.class, len(label), true
			}
		}
		return ErrorClassUnknown, 0, false
	}
	if !valid || len(code) > 128 {
		return ErrorClassUnknown, skip, true
	}
	for i := 0; i < len(code); i++ {
		b := code[i]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '.' || b == '_' || b == '-') {
			return ErrorClassUnknown, skip, true
		}
	}
	return errorCodeClass(code), skip, true
}

func tokenByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '_' || b == '-' || b >= 128
}

// Text matching uses a fixed-size window and overlap larger than any phrase.
// Scan the entire string in bounded matching windows so a JSON body cannot hide an
// operation suffix. JSON objects, arrays and quoted fields are skipped. A
// malformed/unclosed payload anywhere invalidates ALL text matches: flattened
// errors no longer have reliable boundaries, and their order may vary upstream.
// Structured causes and class summaries do not use this compatibility fallback.
func textErrorClasses(message string) map[ErrorClass]int {
	counts := make(map[ErrorClass]int)
	window := make([]byte, 0, 1024)
	first := true
	match := func(last bool) {
		text := string(window)
		wholeToken := func(start, end int) bool {
			left := start == 0 && first || start > 0 && !tokenByte(text[start-1])
			right := end == len(text) && last || end < len(text) && !tokenByte(text[end])
			// Leave the overlap for the next window, including lookahead.
			return left && right && (last || start < len(text)-128)
		}
		for i, tokens := range errorTokens {
			for _, token := range tokens {
				for offset := 0; offset < len(text); {
					idx := strings.Index(text[offset:], token)
					if idx < 0 {
						break
					}
					start := offset + idx
					end := start + len(token)
					// A configuration value is data, not evidence of a failed call.
					tail := strings.TrimLeft(text[end:], " \t")
					parameter := len(tail) > 0 && (tail[0] == '=' || tail[0] == ':')
					// Defer the overlap until the next window, so lookahead cannot
					// mistake a split configuration assignment for a timeout.
					complete := last || token != "timeout" || len(tail) > 0
					if complete && wholeToken(start, end) && !(token == "timeout" && parameter) {
						counts[errorClassOrder[i]] = 1
					}
					offset = start + 1
				}
			}
		}
		// The window bounds both regexp work and its returned match list.
		for _, location := range serializedServerStatusPattern.FindAllStringIndex(text, -1) {
			if wholeToken(location[0], location[1]) {
				counts[ErrorClassTemporary] = 1
			}
		}
	}
	depth := 0
	payloadStart := 0
	quoted, escaped := false, false
	malformed := false
	sdkClass, sdkFound := ErrorClassUnknown, false
	hasSeparator := false
	for i := 0; i < len(message); i++ {
		b := message[i]
		if quoted {
			if escaped {
				escaped = false
			} else if b == '\\' {
				escaped = true
			} else if b == '"' {
				quoted = false
				if depth == 0 && !json.Valid([]byte(message[payloadStart:i+1])) {
					malformed = true
				}
			}
			continue
		}
		if depth == 0 && b == ';' {
			hasSeparator = true
		}
		if !sdkFound && depth == 0 && (b == '[' || b == 'S') {
			if class, skip, found := serializedSDKHeader(message[i:]); found {
				sdkClass, sdkFound = class, true
				i += skip - 1
				b = 0
			}
		}
		if b == '"' {
			if depth == 0 {
				payloadStart = i
			}
			quoted = true
			b = 0
		}
		if b == '{' || b == '[' {
			if depth == 0 {
				payloadStart = i
			}
			depth++
			b = 0
		} else if b == '}' || b == ']' {
			if depth > 0 {
				depth--
				if depth == 0 && !json.Valid([]byte(message[payloadStart:i+1])) {
					malformed = true
				}
			} else {
				malformed = true
			}
			// A barrier prevents words on either side of a removed payload
			// from joining into a supported phrase (not{...}implemented).
			b = 0
		}
		if depth > 0 {
			continue
		}
		// Continue validating payload syntax, but never classify SDK Message,
		// Recommend, RequestId, or a nested diagnostic label as another cause.
		if sdkFound {
			continue
		}
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		window = append(window, b)
		if len(window) == cap(window) {
			match(false)
			// Keep one preceding byte for the next window's left boundary.
			copy(window, window[len(window)-129:])
			window = window[:129]
			first = false
		}
	}
	// Check the entire flattened message before returning any earlier matches.
	// Splitting at semicolons cannot repair it: they may belong to JSON strings.
	if malformed || quoted || depth != 0 {
		return map[ErrorClass]int{ErrorClassUnknown: 1}
	}
	if sdkFound {
		// AllError joins with unescaped semicolons; they may instead be SDK
		// body text. The original error boundaries cannot be recovered here.
		// This affects this leaf only, not separate resources/collector/Join.
		if hasSeparator {
			return map[ErrorClass]int{ErrorClassUnknown: 1}
		}
		return map[ErrorClass]int{sdkClass: 1}
	}
	match(true)
	if len(counts) == 0 {
		counts[ErrorClassUnknown] = 1
	}
	return counts
}

func Retryable(class ErrorClass) bool {
	switch class {
	case ErrorClassAuth, ErrorClassPermission, ErrorClassRegionDisabled, ErrorClassUnsupported:
		return false
	default:
		return true
	}
}
