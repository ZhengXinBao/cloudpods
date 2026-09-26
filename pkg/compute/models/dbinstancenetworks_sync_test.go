package models

import (
	"errors"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
)

func TestDBNetworkSubnetOnlyAndFailedReconciliation(t *testing.T) {
	for _, tc := range []struct {
		name, ip            string
		existing            bool
		resolveErr, addErr  error
		wantAdd, wantDelete int
		wantError           bool
	}{
		{name: "subnet only creates association", wantAdd: 1},
		{name: "subnet only preserves known IP", existing: true},
		{name: "unresolved subnet preserves association", existing: true, resolveErr: errors.New("sql: no rows"), wantError: true},
		{name: "invalid IP preserves association", existing: true, ip: "bad-ip", wantError: true},
		{name: "outside subnet preserves association", existing: true, ip: "192.168.1.1", wantError: true},
		{name: "database insert failure preserves association", existing: true, ip: "10.0.0.9", addErr: errors.New("database unavailable"), wantError: true},
		{name: "known replacement IP reconciles", existing: true, ip: "10.0.0.9", wantAdd: 1, wantDelete: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			network := &SNetwork{GuestIpStart: "10.0.0.1", GuestIpEnd: "10.0.0.254"}
			network.Id = "local-subnet"
			local := []SDBInstanceNetwork{}
			if tc.existing {
				local = append(local, SDBInstanceNetwork{NetworkId: network.Id, IpAddr: "10.0.0.8"})
			}
			addCalls, deletes := 0, 0
			remote := []cloudprovider.SDBInstanceNetwork{{NetworkId: "remote-subnet", IP: tc.ip}}
			result := reconcileDBInstanceNetworks(local, remote, func(id string) (*SNetwork, error) { return network, tc.resolveErr }, func(id, ip string) error {
				addCalls++
				if id != network.Id || ip != tc.ip {
					t.Fatalf("fabricated network/IP: %q %q", id, ip)
				}
				return tc.addErr
			}, func(*SDBInstanceNetwork) error { deletes++; return nil })
			if result.AddCnt != tc.wantAdd || deletes != tc.wantDelete || result.IsError() != tc.wantError {
				t.Fatalf("add calls=%d delete calls=%d result=%s", addCalls, deletes, result.Result())
			}
		})
	}
}

func TestDBNetworkSubnetOnlyRetryIsIdempotent(t *testing.T) {
	network := &SNetwork{}
	network.Id = "subnet-id"
	rows := []SDBInstanceNetwork{}
	adds := 0
	for attempt := 0; attempt < 2; attempt++ {
		result := reconcileDBInstanceNetworks(rows, []cloudprovider.SDBInstanceNetwork{{NetworkId: "remote-subnet"}}, func(string) (*SNetwork, error) { return network, nil }, func(id, ip string) error {
			adds++
			rows = append(rows, SDBInstanceNetwork{NetworkId: id, IpAddr: ip})
			return nil
		}, func(*SDBInstanceNetwork) error { t.Fatal("unexpected deletion"); return nil })
		if result.IsError() {
			t.Fatal(result.AllError())
		}
	}
	if adds != 1 || len(rows) != 1 || rows[0].IpAddr != "" {
		t.Fatalf("subnet-only retry created duplicates: adds=%d rows=%#v", adds, rows)
	}
}
