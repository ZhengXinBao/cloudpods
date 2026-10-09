package syncqueue

import (
	"strings"
	"unicode/utf8"
)

// BoundedErrorMessage makes display text valid UTF-8 and bounds its byte length.
// Long diagnostics retain both operation context and the trailing root cause.
// Replacement characters and the ellipsis count toward maxBytes. Classifications
// must come from the original error, never from this lossy display string.
func BoundedErrorMessage(message string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	message = strings.ToValidUTF8(message, "\uFFFD")
	if len(message) <= maxBytes {
		// Replacement can shrink a huge invalid run to one rune while retaining
		// ToValidUTF8's input-sized buffer. Keep the retained storage bounded too.
		return strings.Clone(message)
	}
	const ellipsis = "..."
	if maxBytes < len(ellipsis) {
		return strings.Clone(utf8Prefix(message, maxBytes))
	}
	budget := maxBytes - len(ellipsis)
	head := budget / 2
	return utf8Prefix(message, head) + ellipsis + utf8Suffix(message, budget-head)
}

// boundedErrorPrefix is the storage guard for errors that bypass summaries.
func boundedErrorPrefix(message string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	message = strings.ToValidUTF8(message, "\uFFFD")
	// Do not retain a large SDK response through a small prefix slice.
	return strings.Clone(utf8Prefix(message, maxBytes))
}

// These boundary helpers require valid UTF-8 and a nonnegative byte budget.
func utf8Prefix(message string, maxBytes int) string {
	if len(message) <= maxBytes {
		return message
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(message[end]) {
		end--
	}
	return message[:end]
}

func utf8Suffix(message string, maxBytes int) string {
	if len(message) <= maxBytes {
		return message
	}
	start := len(message) - maxBytes
	for start < len(message) && !utf8.RuneStart(message[start]) {
		start++
	}
	return message[start:]
}
