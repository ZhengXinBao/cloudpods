package models

import (
	"errors"
	"slices"
	"time"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/jsonutils"
	"yunion.io/x/sqlchemy"

	api "yunion.io/x/onecloud/pkg/apis/compute"
)

// cloudBucketSnapshot is scoped to one reconciliation. It owns only values,
// never an SDK client, so applying it under the object lock cannot fetch data.
type cloudBucketSnapshot struct {
	stats        cloudprovider.SBucketStats
	createdAt    time.Time
	statsOnly    bool
	limit        cloudprovider.SBucketStats
	limitSupport cloudprovider.SBucketStats
	acl          string
	location     string
	storageClass string
	accessURLs   jsonutils.JSONObject
}

var errCloudBucketStatsUnavailable = errors.New("bucket statistics unavailable")

// Statistics refresh is ancillary to create/ACL/status operations. Suppress only
// our standalone incomplete-statistics result, never a database or joined error.
// Inventory reconciliation calls the strict method directly and retains it.
func bucketOperationSyncError(err error) error {
	if err == errCloudBucketStatsUnavailable {
		return nil
	}
	return err
}

// SQLAlchemy can reload values written by another caller after the update.
// Only columns changed by this update may generate a statistics-change event.
func bucketStatsChanged(diff sqlchemy.UpdateDiffs) bool {
	_, size := diff["size_bytes"]
	_, count := diff["object_cnt"]
	return size || count
}

func captureCloudBucketSnapshot(ext cloudprovider.ICloudBucket, statsOnly bool) cloudBucketSnapshot {
	snapshot := cloudBucketSnapshot{
		stats:     ext.GetStats(),
		createdAt: ext.GetCreatedAt(),
		statsOnly: statsOnly,
	}
	if !statsOnly {
		snapshot.limit = ext.GetLimit()
		snapshot.limitSupport = ext.LimitSupport()
		snapshot.acl = string(ext.GetAcl())
		snapshot.location = ext.GetLocation()
		snapshot.storageClass = ext.GetStorageClass()
		urls := slices.Clone(ext.GetAccessUrls())
		snapshot.accessURLs = jsonutils.Marshal(urls)
	}
	return snapshot
}

func (s cloudBucketSnapshot) apply(bucket *SBucket) {
	// Negative SDK statistics mean unknown, not zero. Each metric is independent:
	// a known zero must still replace an old value even if the other fetch failed.
	if s.stats.SizeBytes >= 0 {
		bucket.SizeBytes = s.stats.SizeBytes
	}
	if s.stats.ObjectCount >= 0 {
		bucket.ObjectCnt = s.stats.ObjectCount
	}
	if !s.createdAt.IsZero() {
		bucket.CreatedAt = s.createdAt
	}
	if s.statsOnly {
		return
	}
	if s.limitSupport.SizeBytes > 0 {
		bucket.SizeBytesLimit = s.limit.SizeBytes
	}
	if s.limitSupport.ObjectCount > 0 {
		bucket.ObjectCntLimit = s.limit.ObjectCount
	}
	bucket.Acl = s.acl
	bucket.Location = s.location
	bucket.StorageClass = s.storageClass
	bucket.AccessUrls = s.accessURLs
	bucket.Status = api.BUCKET_STATUS_READY
}

func (s cloudBucketSnapshot) statsError() error {
	if s.stats.SizeBytes < 0 || s.stats.ObjectCount < 0 {
		// The legacy interface does not expose the underlying provider error.
		// Report incomplete statistics without fabricating a cause or success.
		return errCloudBucketStatsUnavailable
	}
	return nil
}
