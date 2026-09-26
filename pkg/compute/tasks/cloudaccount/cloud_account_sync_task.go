// Copyright 2019 Yunion
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudaccount

import (
	"context"
	"strings"

	"yunion.io/x/jsonutils"
	"yunion.io/x/log"
	"yunion.io/x/pkg/errors"

	api "yunion.io/x/onecloud/pkg/apis/compute"
	"yunion.io/x/onecloud/pkg/cloudcommon/db"
	"yunion.io/x/onecloud/pkg/cloudcommon/db/taskman"
	"yunion.io/x/onecloud/pkg/compute/models"
	"yunion.io/x/onecloud/pkg/httperrors"
	"yunion.io/x/onecloud/pkg/util/logclient"
)

type CloudAccountSyncInfoTask struct {
	taskman.STask
}

func init() {
	taskman.RegisterTask(CloudAccountSyncInfoTask{})
}

func (self *CloudAccountSyncInfoTask) OnInit(ctx context.Context, obj db.IStandaloneModel, body jsonutils.JSONObject) {
	cloudaccount := obj.(*models.SCloudaccount)
	if retryOf, _ := self.Params.GetString("retry_of"); retryOf != "" {
		self.prepareRetrySync(ctx, cloudaccount, retryOf)
		return
	}

	if cloudaccount.Provider == api.CLOUD_PROVIDER_VMWARE || cloudaccount.Provider == api.CLOUD_PROVIDER_PROXMOX {
		cloudaccount.SetStatus(ctx, self.UserCred, api.CLOUD_PROVIDER_SYNC_NETWORK, "StartSyncOnPremiseNetworkTask")
		zone, _ := self.Params.GetString("zone")
		var err error
		if cloudaccount.Provider == api.CLOUD_PROVIDER_PROXMOX {
			err = cloudaccount.PrepareProxmoxHostNetwork(ctx, self.UserCred, zone)
		} else {
			err = cloudaccount.PrepareEsxiHostNetwork(ctx, self.UserCred, zone)
		}
		if err != nil {
			d := jsonutils.NewDict()
			d.Set("error", jsonutils.NewString(err.Error()))
			db.OpsLog.LogEvent(cloudaccount, db.ACT_SYNC_NETWORK_FAILED, d, self.UserCred)
			cloudaccount.SetStatus(ctx, self.UserCred, api.CLOUD_PROVIDER_SYNC_NETWORK_FAILED, "sync network failed")
			logclient.AddActionLogWithStartable(self, cloudaccount, logclient.ACT_CLOUDACCOUNT_SYNC_NETWORK, d, self.UserCred, false)
			cloudaccount.MarkEndSync(self.UserCred, false)
			self.SetStageFailed(ctx, d)
			return
		} else {
			cloudaccount.SetStatus(ctx, self.UserCred, api.CLOUD_PROVIDER_INIT, "sync network sucess")
			logclient.AddActionLogWithStartable(self, cloudaccount, logclient.ACT_CLOUDACCOUNT_SYNC_NETWORK, cloudaccount.GetShortDesc(ctx), self.UserCred, true)
		}
	}

	db.OpsLog.LogEvent(cloudaccount, db.ACT_SYNCING_HOST, "", self.UserCred)

	self.SetStage("OnCloudaccountSyncReady", nil)

	taskman.LocalTaskRun(self, func() (jsonutils.JSONObject, error) {
		// do sync
		err := cloudaccount.SyncCallSyncAccountTask(ctx, self.UserCred)
		if err != nil {
			if errors.Cause(err) == httperrors.ErrConflict {
				log.Errorf("account %s(%s) alread in syncing", cloudaccount.Name, cloudaccount.Provider)
				return nil, errors.Wrap(err, "SyncCallSyncAccountTask")
			}
			// A canceled admission never owned preparation. Only admitted work
			// may clean up synchronization state, after its worker has drained.
			if !models.CloudaccountSyncNotAdmitted(err) {
				cloudaccount.MarkEndSyncWithLock(ctx, self.UserCred, false)
			}
			return nil, errors.Wrap(err, "SyncCallSyncAccountTask")
		}
		return nil, nil
	})
}

func (self *CloudAccountSyncInfoTask) OnCloudaccountSyncReadyFailed(ctx context.Context, obj db.IStandaloneModel, err jsonutils.JSONObject) {
	cloudaccount := obj.(*models.SCloudaccount)
	if !strings.Contains(err.String(), "ConflictError") {
		db.OpsLog.LogEvent(cloudaccount, db.ACT_SYNC_HOST_FAILED, err, self.UserCred)
		logclient.AddActionLogWithStartable(self, cloudaccount, logclient.ACT_CLOUD_SYNC, err, self.UserCred, false)
	}
	self.SetStageFailed(ctx, err)
}

func (self *CloudAccountSyncInfoTask) GetSyncRange(ctx context.Context) models.SSyncRange {
	syncRange := models.SSyncRange{}
	syncRangeJson, _ := self.Params.Get("sync_range")
	if syncRangeJson != nil {
		syncRangeJson.Unmarshal(&syncRange)
	} else {
		syncRange.FullSync = true
	}
	return syncRange
}

func (self *CloudAccountSyncInfoTask) OnCloudaccountSyncReady(ctx context.Context, obj db.IStandaloneModel, body jsonutils.JSONObject) {
	cloudaccount := obj.(*models.SCloudaccount)

	syncRange := self.GetSyncRange(ctx)
	if err := cloudaccount.EnableCloudproviderRegions(syncRange.Region); err != nil {
		log.Errorf("EnableCloudproviderRegions %s: %v", cloudaccount.Name, err)
		if models.IndependentSyncAccount(cloudaccount.Id) {
			self.OnCloudaccountSyncCompleteFailed(ctx, obj, jsonutils.NewString(err.Error()))
			return
		}
	}

	if !syncRange.NeedSyncInfo() {
		self.OnCloudaccountSyncComplete(ctx, obj, nil)
		return
	}

	if models.IndependentSyncAccount(cloudaccount.Id) {
		self.prepareIndependentSync(ctx, cloudaccount, syncRange)
		return
	}

	// Cheap-scan never-synced regions first; each provider submits compute-first
	// sync as soon as its own scan finishes, without waiting for sibling accounts.
	if err := cloudaccount.DiscoverCloudproviderRegions(ctx, self.UserCred, true, true); err != nil {
		log.Errorf("DiscoverCloudproviderRegions %s: %v", cloudaccount.Name, err)
	}
	if err := cloudaccount.RefreshEnabledRegionsAfterDiscover(ctx, self.UserCred, syncRange.Region); err != nil {
		log.Errorf("RefreshEnabledCloudproviderRegions after discover %s: %v", cloudaccount.Name, err)
	}

	// Account probe + cheap discover are done. Do not wait for provider-region resource sync.
	self.OnCloudaccountSyncComplete(ctx, obj, nil)

	providerRange := models.AccountFanoutSyncRange(syncRange)
	cloudproviders := cloudaccount.GetEnabledCloudproviders()
	for i := range cloudproviders {
		if !cloudproviders[i].HasEnabledCloudproviderRegion() {
			continue
		}
		err := cloudproviders[i].StartSyncCloudProviderInfoTask(ctx, self.UserCred, &providerRange, "")
		if err != nil {
			log.Errorf("StartSyncCloudProviderInfoTask %s: %v", cloudproviders[i].Name, err)
		}
	}
}

func (self *CloudAccountSyncInfoTask) OnCloudaccountSyncComplete(ctx context.Context, obj db.IStandaloneModel, data jsonutils.JSONObject) {
	cloudaccount := obj.(*models.SCloudaccount)
	syncRange := self.GetSyncRange(ctx)
	cloudaccount.MarkEndSyncWithLock(ctx, self.UserCred, syncRange.NeedSyncInfo())
	db.OpsLog.LogEvent(cloudaccount, db.ACT_SYNC_HOST_COMPLETE, "", self.UserCred)
	self.SetStageComplete(ctx, nil)
	logclient.AddActionLogWithStartable(self, cloudaccount, logclient.ACT_CLOUD_SYNC, syncRange, self.UserCred, true)
}

func (self *CloudAccountSyncInfoTask) OnCloudaccountSyncCompleteFailed(ctx context.Context, obj db.IStandaloneModel, err jsonutils.JSONObject) {
	cloudaccount := obj.(*models.SCloudaccount)
	syncRange := self.GetSyncRange(ctx)
	cloudaccount.MarkEndSyncWithLock(ctx, self.UserCred, syncRange.NeedSyncInfo())
	db.OpsLog.LogEvent(cloudaccount, db.ACT_SYNC_HOST_FAILED, err, self.UserCred)
	self.SetStageFailed(ctx, err)
	logclient.AddActionLogWithStartable(self, cloudaccount, logclient.ACT_CLOUD_SYNC, err, self.UserCred, false)
}
