package models

import (
	"database/sql"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/pkg/errors"
	"yunion.io/x/sqlchemy"
)

type backSyncPeer struct {
	cloudprovider.ICloudVpcPeeringConnection
	id string
}

func (p backSyncPeer) GetId() string { return p.id }

func TestBackSyncPeerSkipsMissingAndContinues(t *testing.T) {
	for _, missing := range []error{sql.ErrNoRows, errors.ErrNotFound} {
		existingSeen := false
		result := backSyncVpcPeeringConnectionsVpc([]cloudprovider.ICloudVpcPeeringConnection{
			backSyncPeer{id: "missing"}, backSyncPeer{id: "existing"},
		}, func(id string) (*SVpcPeeringConnection, error) {
			if id == "missing" {
				return nil, errors.Wrap(missing, "lookup peer")
			}
			existingSeen = true
			return &SVpcPeeringConnection{PeerVpcId: "accepter-vpc"}, nil
		}, func(*SVpcPeeringConnection) error {
			t.Fatal("changed an existing accepter binding")
			return nil
		})
		if result.IsError() || !existingSeen {
			t.Fatalf("missing %v: errors=%v existingSeen=%v", missing, result.AllError(), existingSeen)
		}
	}
}

func TestBackSyncPeerPreservesLookupErrors(t *testing.T) {
	for _, failure := range []error{sql.ErrConnDone, sqlchemy.ErrDuplicateEntry} {
		result := backSyncVpcPeeringConnectionsVpc([]cloudprovider.ICloudVpcPeeringConnection{backSyncPeer{id: "peer"}}, func(string) (*SVpcPeeringConnection, error) {
			return nil, errors.Wrap(failure, "lookup peer")
		}, func(*SVpcPeeringConnection) error {
			t.Fatal("updated a missing or ambiguous peer")
			return nil
		})
		if !result.IsError() {
			t.Fatalf("lost lookup error %v", failure)
		}
	}
}

func TestBackSyncPeerPreservesUpdateError(t *testing.T) {
	result := backSyncVpcPeeringConnectionsVpc([]cloudprovider.ICloudVpcPeeringConnection{backSyncPeer{id: "peer"}}, func(string) (*SVpcPeeringConnection, error) {
		return &SVpcPeeringConnection{}, nil
	}, func(*SVpcPeeringConnection) error { return sql.ErrConnDone })
	if !result.IsError() {
		t.Fatal("lost database update failure")
	}
}
