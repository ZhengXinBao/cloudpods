package service

import "yunion.io/x/onecloud/pkg/cloudcommon/db"

// configureSyncWorkerChecksums mirrors the runtime policy normally applied by
// db.CheckSync in region. Workers skip that schema/data migration path, but must
// still honor the same option rather than manager constructors' default true.
// It never initializes or rewrites checksums; when enabled, missing or corrupt
// checksums remain errors and must be addressed through region's initialization.
func configureSyncWorkerChecksums(managers map[string]db.IModelManager, enabled bool) {
	for _, manager := range managers {
		if checksummed, ok := manager.(db.IRecordChecksumModelManager); ok {
			checksummed.SetEnableRecordChecksum(enabled)
		}
	}
}
