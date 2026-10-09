package models

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"yunion.io/x/onecloud/pkg/compute/syncqueue"
	"yunion.io/x/pkg/util/compare"
)

func TestBoundedCloudSyncMessageUTF8(t *testing.T) {
	for _, message := range []string{
		"", "调用失败：é🙂", strings.Repeat("a", 512), strings.Repeat("a", 513),
		strings.Repeat("a", 503) + "é中🙂", strings.Repeat("错误", 150),
		"操作:" + strings.Repeat("é中🙂", 150) + ":原因",
		"\xff开始\xfe\xff结束\xe4\xb8", strings.Repeat("a", 511) + "\xff",
		strings.Repeat("a\xff", 200),
	} {
		got := boundedCloudSyncMessage(message)
		if !utf8.ValidString(got) || len(got) > 512 {
			t.Errorf("input bytes=%d: output bytes=%d valid=%v", len(message), len(got), utf8.ValidString(got))
		}
		if want := strings.ToValidUTF8(message, "\uFFFD"); len(want) <= 512 && got != want {
			t.Error("short diagnostic changed or invalid bytes were not replaced")
		}
		if boundedCloudSyncMessage(got) != got {
			t.Error("not idempotent")
		}
		if utf8.ValidString(message) && strings.Contains(got, "\uFFFD") {
			t.Error("valid input was corrupted")
		}
	}
	got := boundedCloudSyncMessage("操作开始:" + strings.Repeat("错误🙂", 150) + ":根本原因")
	if !strings.HasPrefix(got, "操作开始:") || !strings.HasSuffix(got, ":根本原因") || !strings.Contains(got, "...") {
		t.Fatal("lost head/tail context")
	}
}

func TestCloudSyncUTF8RealAggregationAndMerge(t *testing.T) {
	for _, message := range []string{strings.Repeat("错误🙂", 150), "\xff原因\xfe"} {
		// An unknown SDK code must not be reclassified from its display body.
		cause := &collectorSDKError{"DecodeFailure", "AuthFailure " + message}
		source := compare.SyncResult{}
		source.AddError(cause)
		set := SSyncResultSet{}
		set.Add(BucketManager, source)
		aggregated := syncResultSetError(set)
		if !utf8.ValidString(aggregated.Error()) || len(aggregated.Error()) > 8192 {
			t.Error("real Add path returned invalid/unbounded diagnostic")
		}
		ctx, collector := WithCloudSyncErrors(context.Background())
		cloudSyncError(ctx, "request: %v", cause)
		if !utf8.ValidString(collector.Error().Error()) {
			t.Error("collector returned invalid UTF-8")
		}
		merged := collector.Merge(aggregated)
		if !errors.Is(merged, aggregated) {
			t.Fatal("Merge lost original cause identity")
		}
		want := map[syncqueue.ErrorClass]int{syncqueue.ErrorClassUnknown: 2}
		if got := syncqueue.ErrorClassCounts(merged); !reflect.DeepEqual(got, want) {
			t.Fatalf("classes/counts changed through Add/collector/Merge: %v", got)
		}
		if !utf8.ValidString(merged.Error()) {
			t.Error("merged summaries are not UTF-8")
		}
		// A direct error is deliberately not rebuilt or sanitized by Merge:
		// Complete must guard storage even when callers bypass the summaries.
		original := errors.New("\xff原始错误")
		_, empty := WithCloudSyncErrors(context.Background())
		if empty.Merge(original) != original || !errors.Is(collector.Merge(original), original) {
			t.Fatal("raw executor error identity changed")
		}
	}
}
