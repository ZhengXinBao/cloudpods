package syncworker

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/onecloud/pkg/compute/syncqueue"
)

func TestExecutionLockKey(t *testing.T) {
	a := &syncqueue.Job{Request: syncqueue.Request{ProviderID: "p/a", RegionID: "r"}}
	b := &syncqueue.Job{Request: syncqueue.Request{ProviderID: "p", RegionID: "a/r"}}
	if executionLockKey("/sync", a) == executionLockKey("/sync", b) {
		t.Fatal("ambiguous resource identity")
	}
	before := executionLockKey("/sync", a)
	a.RangeJSON = `{"changed":true}`
	if before != executionLockKey("/sync/", a) {
		t.Fatal("range or trailing slash changed lock identity")
	}
	a.ScopeType = "provider"
	a.ResourceGroup = "provider"
	before = executionLockKey("/sync", a)
	a.RegionID = ""
	if before != executionLockKey("/sync", a) {
		t.Fatal("provider scope must ignore the region")
	}
	b.ProviderID, b.RegionID = a.ProviderID, a.RegionID
	b.ScopeType, b.ResourceGroup = "region", a.ResourceGroup
	if before == executionLockKey("/sync", b) {
		t.Fatal("provider and region scopes must not share a lock")
	}
}

func TestRequestedExecutionLockConflictsWithItsResourceGroups(t *testing.T) {
	core := &syncqueue.Job{Request: syncqueue.Request{
		ProviderID: "provider", RegionID: "region", ResourceGroup: "core",
	}}
	services := &syncqueue.Job{Request: syncqueue.Request{
		ProviderID: "provider", RegionID: "region", ResourceGroup: "services",
	}}
	for _, resource := range []string{"compute", "network"} {
		requested := &syncqueue.Job{Request: syncqueue.Request{
			ProviderID: "provider", RegionID: "region", ResourceGroup: "requested",
			RangeJSON: `{"resources":["` + resource + `"]}`,
		}}
		if executionLockKey("/sync", requested) != executionLockKey("/sync", core) {
			t.Fatalf("%s request must share the core execution lock", resource)
		}
	}
	for _, resource := range []string{"loadbalancer", "rds", "cache"} {
		requested := &syncqueue.Job{Request: syncqueue.Request{
			ProviderID: "provider", RegionID: "region", ResourceGroup: "requested",
			RangeJSON: `{"resources":["` + resource + `"]}`,
		}}
		if executionLockKey("/sync", requested) != executionLockKey("/sync", services) {
			t.Fatalf("%s request must share the services execution lock", resource)
		}
	}
	mixed := &syncqueue.Job{Request: syncqueue.Request{
		ProviderID: "provider", RegionID: "region", ResourceGroup: "requested",
		RangeJSON: `{"resources":["compute","rds"]}`,
	}}
	keys := executionLockKeys("/sync", mixed)
	if len(keys) != 2 {
		t.Fatalf("mixed request must acquire both group locks, got %d", len(keys))
	}
	seen := map[string]bool{}
	for _, key := range keys {
		seen[key] = true
	}
	if !seen[executionLockKey("/sync", core)] || !seen[executionLockKey("/sync", services)] {
		t.Fatalf("mixed request locks=%v, want core and services locks", keys)
	}
}

func TestRequestedExecutionLockUsesCapabilityConstantsAndConservativeFallback(t *testing.T) {
	tests := []struct {
		resource string
		group    string
	}{
		{cloudprovider.CLOUD_CAPABILITY_VPC_PEER, "core"},
		{cloudprovider.CLOUD_CAPABILITY_MODELARTES, "extended"},
	}
	for _, tt := range tests {
		job := &syncqueue.Job{Request: syncqueue.Request{
			ProviderID: "provider", RegionID: "region", ResourceGroup: "requested",
			RangeJSON: `{"resources":["` + tt.resource + `"]}`,
		}}
		groups := executionLockGroups(job)
		if len(groups) != 1 || groups[0] != tt.group {
			t.Fatalf("%s groups=%v, want [%s]", tt.resource, groups, tt.group)
		}
	}
	for _, resource := range []string{cloudprovider.CLOUD_CAPABILITY_CERT, cloudprovider.CLOUD_CAPABILITY_AI_GATEWAY} {
		job := &syncqueue.Job{Request: syncqueue.Request{
			ProviderID: "provider", ScopeType: "provider", ResourceGroup: "provider",
			RangeJSON: `{"resources":["` + resource + `"]}`,
		}}
		if groups := executionLockGroups(job); len(groups) != 1 || groups[0] != "provider" {
			t.Fatalf("%s provider groups=%v, want [provider]", resource, groups)
		}
	}
	for _, rangeJSON := range []string{"", `{}`, `{"resources":["unknown"]}`, "not-json"} {
		job := &syncqueue.Job{Request: syncqueue.Request{
			ProviderID: "provider", RegionID: "region", ResourceGroup: "requested",
			RangeJSON: rangeJSON,
		}}
		groups := executionLockGroups(job)
		if len(groups) != 4 {
			t.Fatalf("range %q groups=%v, want all region groups", rangeJSON, groups)
		}
	}
}

func TestEmptyRegionResourceGroupLocksAllRegionGroups(t *testing.T) {
	job := &syncqueue.Job{Request: syncqueue.Request{
		ProviderID: "provider", RegionID: "region", ResourceGroup: "",
	}}
	groups := executionLockGroups(job)
	if len(groups) != 4 {
		t.Fatalf("empty resource group groups=%v, want all region groups", groups)
	}
}
func TestEtcdLockerValidation(t *testing.T) {
	if _, e := NewEtcdLocker(nil, "/sync", 10, func(error) {}); e == nil {
		t.Fatal("nil client accepted")
	}
}

// Opt in with endpoints of a disposable etcd instance. The test uses its own prefix.
func TestEtcdLockExclusionAndCancellation(t *testing.T) {
	endpoints := os.Getenv("SYNCWORKER_TEST_ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("set SYNCWORKER_TEST_ETCD_ENDPOINTS for etcd integration")
	}
	cli, e := clientv3.New(clientv3.Config{Endpoints: strings.Split(endpoints, ","), DialTimeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	defer cli.Close()
	fatal := make(chan error, 10)
	locker, e := NewEtcdLocker(cli, "/syncworker-tests/"+time.Now().Format("20060102150405.000000000"), 10, func(e error) { fatal <- e })
	if e != nil {
		t.Fatal(e)
	}
	job := &syncqueue.Job{Request: syncqueue.Request{ProviderID: "provider", RegionID: "region"}}
	ctx, cancel := context.WithCancel(context.Background())
	release, e := locker.Acquire(ctx, job)
	if e != nil {
		t.Fatal(e)
	}
	cancel() // Cancellation must not release an acquired lock protecting legacy work.
	otherCtx, otherCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer otherCancel()
	if otherRelease, e := locker.Acquire(otherCtx, job); e == nil {
		otherRelease()
		t.Fatal("same resource locked concurrently")
	}
	release()
	release()
	nextCtx, nextCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer nextCancel()
	nextRelease, e := locker.Acquire(nextCtx, job)
	if e != nil {
		t.Fatal(e)
	}
	nextRelease()
	select {
	case e := <-fatal:
		t.Fatalf("intentional release was fatal: %v", e)
	default:
	}
}

func TestEtcdLeaseRevocationIsFatal(t *testing.T) {
	endpoints := os.Getenv("SYNCWORKER_TEST_ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("set SYNCWORKER_TEST_ETCD_ENDPOINTS for etcd integration")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(endpoints, ","), DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	fatal := make(chan error, 1)
	locker, err := NewEtcdLocker(cli, "/syncworker-tests/revoke-"+time.Now().Format("20060102150405.000000000"), 4, func(err error) { fatal <- err })
	if err != nil {
		t.Fatal(err)
	}
	job := &syncqueue.Job{Request: syncqueue.Request{ProviderID: "p", RegionID: "r"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release, err := locker.Acquire(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	keys, err := cli.Get(ctx, executionLockKey(locker.prefix, job), clientv3.WithPrefix())
	if err != nil || len(keys.Kvs) != 1 {
		t.Fatalf("lock keys: %v %v", keys, err)
	}
	if _, err = cli.Revoke(ctx, clientv3.LeaseID(keys.Kvs[0].Lease)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-fatal:
		if !strings.Contains(err.Error(), "lost") {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("revoked session did not invoke fatal")
	}
}
