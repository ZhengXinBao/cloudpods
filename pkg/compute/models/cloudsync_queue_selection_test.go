package models

import (
	"testing"
	"time"
)

func TestIndependentRegionSelectionRejectsMissingScope(t *testing.T) {
	regions := []SCloudproviderregion{{Enabled: true}}
	regions[0].CloudregionId = "present"
	if _, err := independentRegionCandidates(regions, []string{"present", "missing"}); err == nil {
		t.Fatal("partial requested scope must not be reported as fully planned")
	}
	regions[0].Enabled = false
	if _, err := independentRegionCandidates(regions, []string{"present"}); err == nil {
		t.Fatal("disabled requested region must not silently become an empty successful run")
	}
}

func TestIndependentRegionSelectionRetainsInterruptedAndActiveScopes(t *testing.T) {
	regions := []SCloudproviderregion{{Enabled: true}}
	regions[0].CloudregionId = "present"
	got, err := independentRegionCandidates(regions, []string{"present"})
	if err != nil || len(got) != 1 {
		t.Fatalf("explicit region selection = %v, %v", got, err)
	}
	regions[0].SyncStatus = "syncing"
	got, err = independentRegionCandidates(regions, []string{"present"})
	if err != nil || len(got) != 1 {
		t.Fatal("active scope must join the existing queue job, not escape run membership")
	}
	regions[0].LastSync = time.Now().Add(-time.Hour)
	got, err = independentRegionCandidates(regions, nil)
	if err != nil || len(got) != 1 {
		t.Fatal("interrupted region must remain eligible for queue recovery")
	}
	regions[0].LastSync = time.Time{}
	got, err = independentRegionCandidates(regions, nil)
	if err != nil || len(got) != 0 {
		t.Fatal("untouched region without a discovery hit must still be discovered first")
	}
}
