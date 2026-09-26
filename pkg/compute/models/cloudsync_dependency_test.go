package models

import (
	"database/sql"
	"strings"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/sqlchemy"
)

func TestResolveGlobalVpcIDRequiresExistingDependency(t *testing.T) {
	got, err := resolveGlobalVpcID("global-1", "provider-1", func() (string, error) {
		return "local-global-1", nil
	})
	if err != nil {
		t.Fatalf("resolve existing global vpc: %v", err)
	}
	if got != "local-global-1" {
		t.Fatalf("got %q, want local-global-1", got)
	}

	_, err = resolveGlobalVpcID("global-1", "provider-1", func() (string, error) {
		return "", sql.ErrNoRows
	})
	if err == nil || !strings.Contains(err.Error(), "global-1") {
		t.Fatalf("missing global vpc should be retryable dependency error, got %v", err)
	}
	if !strings.Contains(err.Error(), cloudprovider.ErrNotFound.Error()) {
		t.Fatalf("missing global vpc should preserve not-found cause, got %v", err)
	}
}

func TestResolveGlobalVpcIDAllowsVpcWithoutGlobalDependency(t *testing.T) {
	called := false
	got, err := resolveGlobalVpcID("", "provider-1", func() (string, error) {
		called = true
		return "unexpected", nil
	})
	if err != nil {
		t.Fatalf("empty global vpc id: %v", err)
	}
	if got != "" {
		t.Fatalf("got %q, want empty", got)
	}
	if called {
		t.Fatal("empty global vpc id should not query the dependency")
	}
}

func TestProviderIpSetQueryExcludesRegionIpSets(t *testing.T) {
	sqlchemy.SetupMockDatabaseBackend()
	sql := providerIpSetQuery("provider-1").String()
	if !strings.Contains(sql, "cloudregion_id") {
		t.Fatalf("provider ipset query must constrain cloudregion_id: %s", sql)
	}
}
