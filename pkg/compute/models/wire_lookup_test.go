package models

import (
	"database/sql"
	"testing"
	"yunion.io/x/onecloud/pkg/httperrors"
	"yunion.io/x/pkg/errors"
)

func wireCandidate(id, vpcID string) SWire {
	w := SWire{}
	w.Id, w.VpcId, w.ExternalId = id, vpcID, "westus3/subscription/same-name"
	return w
}

func TestWireLookupScopesSameNameWiresToVpc(t *testing.T) {
	candidates := []SWire{wireCandidate("ops-monitor", "vpc-monitor"), wireCandidate("ops-archery", "vpc-archery"), wireCandidate("common-uva", "vpc-common")}
	got, err := selectWireByExternalID(candidates, candidates[0].ExternalId, []string{"vpc-archery"})
	if err != nil || got.Id != "ops-archery" {
		t.Fatalf("scoped lookup=%v error=%v", got, err)
	}
}

func TestWireLookupPreservesGenuineAmbiguity(t *testing.T) {
	candidates := []SWire{wireCandidate("wire-a", "vpc-a"), wireCandidate("wire-b", "vpc-a")}
	for _, vpcIDs := range [][]string{nil, {"vpc-a"}} {
		_, err := selectWireByExternalID(candidates, candidates[0].ExternalId, vpcIDs)
		if errors.Cause(err) != httperrors.ErrDuplicateId {
			t.Fatalf("ambiguous lookup accepted: %v", err)
		}
	}
}

func TestWireLookupDoesNotFallBackToAnotherVpc(t *testing.T) {
	candidates := []SWire{wireCandidate("wire-a", "vpc-a")}
	for _, vpcIDs := range [][]string{{}, {"vpc-missing"}} {
		_, err := selectWireByExternalID(candidates, candidates[0].ExternalId, vpcIDs)
		if errors.Cause(err) != sql.ErrNoRows {
			t.Fatalf("unmatched VPC fell back to another wire: %v", err)
		}
	}
}
