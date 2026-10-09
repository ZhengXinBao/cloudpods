package syncqueue

import (
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestNewErrorSummaryUTF8ReleasesCollapsedBuffer(t *testing.T) {
	// ToValidUTF8 can collapse a large invalid byte run to one rune while its
	// Builder still owns a buffer sized for the input. Checking len alone cannot
	// detect that retained memory. Keep only the summary alive across GC.
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := collapsedUTF8Summary()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(err)
	if err.Error() != "\uFFFD" {
		t.Fatalf("unexpected collapsed diagnostic: bytes=%d", len(err.Error()))
	}
	// Leave ample room for runtime/test bookkeeping, but not the 16 MiB input.
	if after.HeapAlloc > before.HeapAlloc+(8<<20) {
		t.Fatalf("three-byte summary retained %d heap bytes", after.HeapAlloc-before.HeapAlloc)
	}
}

func collapsedUTF8Summary() error {
	return NewErrorSummary(strings.Repeat("\xff", 16<<20), nil)
}

func TestBoundedErrorMessageUTF8Boundaries(t *testing.T) {
	for _, tc := range []struct {
		message string
		limit   int
		summary string
		prefix  string
	}{
		{"hello", -1, "", ""},
		{"hello", 0, "", ""},
		{"hello", 1, "h", "h"},
		{"hello", 2, "he", "he"},
		{"hello", 3, "...", "hel"},
		{"abcdefgh", 7, "ab...gh", "abcdefg"},
		{"abcdefgh", 8, "abcdefgh", "abcdefgh"},
		{"é中🙂", 2, "é", "é"},
		{"中中", 2, "", ""},
		{"🙂🙂", 7, "...", "🙂"},
		{"é中🙂", 9, "é中🙂", "é中🙂"},
		{"\xff", 3, "\uFFFD", "\uFFFD"},
		{"\xff\xfe", 3, "\uFFFD", "\uFFFD"},
		{"\xff中\xfe", 9, "\uFFFD中\uFFFD", "\uFFFD中\uFFFD"},
		{"a\xe4\xb8", 4, "a\uFFFD", "a\uFFFD"},
	} {
		if got := BoundedErrorMessage(tc.message, tc.limit); got != tc.summary {
			t.Errorf("summary(%q,%d)=%q want %q", tc.message, tc.limit, got, tc.summary)
		}
		if got := boundedErrorPrefix(tc.message, tc.limit); got != tc.prefix {
			t.Errorf("prefix(%q,%d)=%q want %q", tc.message, tc.limit, got, tc.prefix)
		}
	}
}

func FuzzBoundedErrorMessageUTF8(f *testing.F) {
	for _, message := range []string{"", "abc", "é中🙂", "\xffx\xe4\xb8", strings.Repeat("错误🙂", 1000)} {
		for _, limit := range []int{-1, 0, 1, 2, 3, 4, 5, 8, 512, 8192} {
			f.Add(message, limit)
		}
	}
	f.Fuzz(func(t *testing.T, message string, limit int) {
		limit %= 8193
		budget := limit
		if budget < 0 {
			budget = 0
		}
		valid := strings.ToValidUTF8(message, "\uFFFD")
		for _, bound := range []func(string, int) string{BoundedErrorMessage, boundedErrorPrefix} {
			got := bound(message, limit)
			if !utf8.ValidString(got) || len(got) > budget {
				t.Fatalf("budget=%d bytes=%d valid=%v", budget, len(got), utf8.ValidString(got))
			}
			if len(valid) <= budget && got != valid {
				t.Fatal("short valid diagnostic changed")
			}
			if bound(got, limit) != got {
				t.Fatal("not idempotent")
			}
			if utf8.ValidString(message) && !strings.Contains(message, "\uFFFD") && strings.Contains(got, "\uFFFD") {
				t.Fatal("valid input was corrupted")
			}
		}
		if !strings.HasPrefix(valid, boundedErrorPrefix(message, limit)) {
			t.Fatal("storage guard did not preserve the prefix")
		}
	})
}

func TestNewErrorSummaryUTF8(t *testing.T) {
	for _, tc := range []struct {
		name, message string
	}{
		{"empty", ""},
		{"short", "调用失败：é🙂"},
		{"exact ASCII", strings.Repeat("a", 8192)},
		{"exact mixed", strings.Repeat("a", 8183) + "é中🙂"},
		{"long ASCII", strings.Repeat("a", 9000)},
		{"Chinese", strings.Repeat("错误", 1500)},
		{"mixed", "操作:" + strings.Repeat("é中🙂", 1200) + ":原因"},
		{"invalid short", "\xff开始\xfe\xff结束\xe4\xb8"},
		{"invalid exact", strings.Repeat("a", 8191) + "\xff"},
		{"expanding replacement", strings.Repeat("a\xff", 3000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counts := map[ErrorClass]int{ErrorClassPermission: 2, ErrorClassTimeout: 3, ErrorClassUnknown: 1}
			err := NewErrorSummary(tc.message, counts)
			if err == nil {
				t.Fatal("non-nil summary contract lost")
			}
			got := err.Error()
			if !utf8.ValidString(got) || len(got) > 8192 {
				t.Fatalf("invalid/big summary: bytes=%d valid=%v", len(got), utf8.ValidString(got))
			}
			if want := strings.ToValidUTF8(tc.message, "\uFFFD"); len(want) <= 8192 && got != want {
				t.Fatal("short diagnostic changed or invalid bytes were not replaced")
			}
			if utf8.ValidString(tc.message) && strings.Contains(got, "\uFFFD") {
				t.Fatal("valid input was corrupted during truncation")
			}
			if !reflect.DeepEqual(ErrorClassCounts(err), counts) || ClassifyError(err) != ErrorClassPermission {
				t.Fatalf("summary classes changed: %v", ErrorClassCounts(err))
			}
			counts[ErrorClassPermission] = 0
			if ErrorClassCounts(err)[ErrorClassPermission] != 2 {
				t.Fatal("summary retained mutable input counts")
			}
			if NewErrorSummary(got, nil).Error() != got {
				t.Fatal("summary is not idempotent")
			}
		})
	}
	message := "操作开始:" + strings.Repeat("错误🙂", 1500) + ":根本原因"
	got := NewErrorSummary(message, nil).Error()
	if !strings.HasPrefix(got, "操作开始:") || !strings.HasSuffix(got, ":根本原因") || !strings.Contains(got, "...") {
		t.Fatal("head/tail context missing")
	}
}
