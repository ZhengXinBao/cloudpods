package models

import (
	"errors"
	"strings"
	"testing"
)

func TestCloudInventoryDetectsMissingIDsAtEqualCounts(t *testing.T) {
	err := verifyCloudInventory([]string{"lb-a", "lb-b"}, func() ([]string, error) { return []string{"lb-a", "lb-stale"}, nil })
	if err == nil || !strings.Contains(err.Error(), "lb-b") {
		t.Fatalf("equal counts concealed omitted ID: %v", err)
	}
}

func TestCloudInventoryUsesOpaqueIDsAndIgnoresLocalExtras(t *testing.T) {
	for _, ids := range [][]string{{"/ID/A", " id "}, {"lb-a", "lb-a"}, {}} {
		local := append(append([]string{}, ids...), "stale")
		if err := verifyCloudInventory(ids, func() ([]string, error) { return local, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := verifyCloudInventory([]string{"/ID/A"}, func() ([]string, error) { return []string{"/id/a"}, nil }); err == nil {
		t.Fatal("opaque ID case was normalized")
	}
}

func TestCloudInventoryRejectsQueryFailureAndEmptyRemoteID(t *testing.T) {
	failure := errors.New("inventory database unavailable")
	if err := verifyCloudInventory([]string{"lb-a"}, func() ([]string, error) { return nil, failure }); !errors.Is(err, failure) {
		t.Fatalf("query error lost: %v", err)
	}
	if err := verifyCloudInventory([]string{""}, func() ([]string, error) { return []string{""}, nil }); err == nil {
		t.Fatal("empty identity accepted")
	}
}

func TestCloudInventoryBoundsMissingReport(t *testing.T) {
	ids := make([]string, 100)
	for i := range ids {
		ids[i] = strings.Repeat("x", 1000) + string(rune('A'+i))
	}
	err := verifyCloudInventory(ids, func() ([]string, error) { return nil, nil })
	if err == nil || !strings.Contains(err.Error(), "missing=100") || len(err.Error()) > 1500 {
		t.Fatalf("unbounded or incomplete report: %v", err)
	}
}
