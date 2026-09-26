package service

import (
	"testing"

	"yunion.io/x/sqlchemy"

	"yunion.io/x/onecloud/pkg/cloudcommon/db"
	"yunion.io/x/onecloud/pkg/compute/models"
)

func TestSyncWorkerAppliesConfiguredChecksumPolicy(t *testing.T) {
	sqlchemy.SetupMockDatabaseBackend()
	manager := models.GuestManager
	original := manager.EnableRecordChecksum()
	defer manager.SetEnableRecordChecksum(original)
	guest := &models.SGuest{}
	guest.SetModelManager(manager, guest)
	managers := map[string]db.IModelManager{"server": manager}
	manager.SetEnableRecordChecksum(true)
	configureSyncWorkerChecksums(managers, false)
	if err := db.CheckRecordChecksumConsistent(guest); err != nil {
		t.Fatalf("worker rejected legacy guest although configured checksum checks are disabled: %v", err)
	}
	if guest.RecordChecksum != "" {
		t.Fatal("worker initialization rewrote record checksum")
	}
}

func TestSyncWorkerEnabledChecksumsRejectMissingOrCorruptChecksum(t *testing.T) {
	sqlchemy.SetupMockDatabaseBackend()
	manager := models.GuestManager
	original := manager.EnableRecordChecksum()
	defer manager.SetEnableRecordChecksum(original)
	manager.SetEnableRecordChecksum(false)
	configureSyncWorkerChecksums(map[string]db.IModelManager{"server": manager}, true)
	guest := &models.SGuest{}
	guest.SetModelManager(manager, guest)
	for _, checksum := range []string{"", "corrupt"} {
		guest.RecordChecksum = checksum
		if err := db.CheckRecordChecksumConsistent(guest); err == nil {
			t.Fatalf("worker accepted checksum %q with verification enabled", checksum)
		}
		if guest.RecordChecksum != checksum {
			t.Fatal("worker rewrote invalid checksum")
		}
	}
	expected, err := db.CalculateModelChecksum(guest)
	if err != nil {
		t.Fatal(err)
	}
	guest.RecordChecksum = expected
	if err := db.CheckRecordChecksumConsistent(guest); err != nil {
		t.Fatalf("valid checksum rejected: %v", err)
	}
}
