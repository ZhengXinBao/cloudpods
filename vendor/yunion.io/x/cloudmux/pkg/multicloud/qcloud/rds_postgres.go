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

package qcloud

import (
	"fmt"
	"time"

	billingapi "yunion.io/x/cloudmux/pkg/apis/billing"
	api "yunion.io/x/cloudmux/pkg/apis/compute"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/cloudmux/pkg/multicloud"
	"yunion.io/x/jsonutils"
	"yunion.io/x/pkg/errors"
	"yunion.io/x/pkg/util/billing"
)

const (
	postgresPageLimit    = 100
	postgresTimeLayout   = "2006-01-02 15:04:05"
	postgresNetTypePriv  = "private"
	postgresNetTypePub   = "public"
	postgresNetTypeInner = "inner"
)

type SPostgresNetInfo struct {
	Address  string
	Ip       string
	Port     int
	NetType  string
	Status   string
	VpcId    string
	SubnetId string
}

type SPostgresNode struct {
	Role string
	Zone string
}

// SPostgreSQL 对应腾讯云 postgres DescribeDBInstances 返回的 DBInstance
type SPostgreSQL struct {
	multicloud.SDBInstanceBase
	QcloudTags

	Region             string
	Zone               string
	ProjectId          string
	VpcId              string
	SubnetId           string
	DBInstanceId       string
	DBInstanceName     string
	DBInstanceStatus   string
	DBInstanceMemory   int // GB
	DBInstanceStorage  int // GB
	DBInstanceCpu      int
	DBInstanceClass    string
	DBMajorVersion     string
	DBVersion          string
	DBKernelVersion    string
	DBInstanceType     string // primary / readonly
	DBInstanceVersion  string // standard
	DBCharset          string
	CreateTime         string
	UpdateTime         string
	ExpireTime         string
	IsolatedTime       string
	PayType            string // prepaid / postpaid
	AutoRenew          int
	DBInstanceNetInfo  []SPostgresNetInfo
	Type               string
	MasterDBInstanceId string
	DBNodeSet          []SPostgresNode

	region *SRegion
}

// 已进入回收站或下线的实例不再同步，与 SQLServer 过滤回收站实例的行为保持一致
var postgresSkipStatus = map[string]bool{
	"isolated":  true,
	"recycling": true,
	"recycled":  true,
	"offlining": true,
	"offlined":  true,
}

var postgresStatusMap = map[string]string{
	"applying":   api.DBINSTANCE_DEPLOYING,
	"initing":    api.DBINSTANCE_DEPLOYING,
	"running":    api.DBINSTANCE_RUNNING,
	"readonly":   api.DBINSTANCE_RUNNING,
	"isolating":  api.DBINSTANCE_ISOLATING,
	"isolated":   api.DBINSTANCE_ISOLATE,
	"recycling":  api.DBINSTANCE_DELETING,
	"recycled":   api.DBINSTANCE_DELETING,
	"offlining":  api.DBINSTANCE_DELETING,
	"offlined":   api.DBINSTANCE_DELETING,
	"deleting":   api.DBINSTANCE_DELETING,
	"restarting": api.DBINSTANCE_REBOOTING,
	"migrating":  api.DBINSTANCE_MIGRATING,
	"switching":  api.DBINSTANCE_MIGRATING,
	"expanding":  api.DBINSTANCE_CHANGE_CONFIG,
	"waitSwitch": api.DBINSTANCE_CHANGE_CONFIG,
	"modifying":  api.DBINSTANCE_CHANGE_CONFIG,
	"upgrading":  api.DBINSTANCE_UPGRADING,
	"restoring":  api.DBINSTANCE_RESTORING,
	"cloning":    api.DBINSTANCE_CLONING,
}

// parsePostgresTime 解析腾讯云返回的北京时间，"0000-00-00 00:00:00" 或空值返回零值
func parsePostgresTime(s string) time.Time {
	if len(s) == 0 || s == "0000-00-00 00:00:00" {
		return time.Time{}
	}
	t, err := time.ParseInLocation(postgresTimeLayout, s, time.FixedZone("CST", 8*3600))
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

func (pg *SPostgreSQL) GetId() string {
	return pg.DBInstanceId
}

func (pg *SPostgreSQL) GetGlobalId() string {
	return pg.DBInstanceId
}

func (pg *SPostgreSQL) GetName() string {
	if len(pg.DBInstanceName) > 0 {
		return pg.DBInstanceName
	}
	return pg.DBInstanceId
}

func (pg *SPostgreSQL) GetStatus() string {
	if status, ok := postgresStatusMap[pg.DBInstanceStatus]; ok {
		return status
	}
	return api.DBINSTANCE_UNKNOWN
}

func (pg *SPostgreSQL) Refresh() error {
	rds, err := pg.region.GetPostgreSQL(pg.DBInstanceId)
	if err != nil {
		return errors.Wrapf(err, "GetPostgreSQL(%s)", pg.DBInstanceId)
	}
	return jsonutils.Update(pg, rds)
}

func (pg *SPostgreSQL) GetEngine() string {
	return api.DBINSTANCE_TYPE_POSTGRESQL
}

func (pg *SPostgreSQL) GetEngineVersion() string {
	if len(pg.DBMajorVersion) > 0 {
		return pg.DBMajorVersion
	}
	return pg.DBVersion
}

func (pg *SPostgreSQL) GetInstanceType() string {
	if len(pg.DBInstanceClass) > 0 {
		return pg.DBInstanceClass
	}
	return fmt.Sprintf("%d核%dGB", pg.DBInstanceCpu, pg.DBInstanceMemory)
}

func (pg *SPostgreSQL) GetVcpuCount() int {
	return pg.DBInstanceCpu
}

func (pg *SPostgreSQL) GetVmemSizeMB() int {
	return pg.DBInstanceMemory * 1024
}

func (pg *SPostgreSQL) GetDiskSizeGB() int {
	return pg.DBInstanceStorage
}

func (pg *SPostgreSQL) GetCategory() string {
	if len(pg.DBNodeSet) == 1 {
		return api.QCLOUD_DBINSTANCE_CATEGORY_BASIC
	}
	return api.QCLOUD_DBINSTANCE_CATEGORY_HA
}

func (pg *SPostgreSQL) GetStorageType() string {
	return api.QCLOUD_DBINSTANCE_STORAGE_TYPE_CLOUD_SSD
}

func (pg *SPostgreSQL) GetMaintainTime() string {
	return ""
}

func (pg *SPostgreSQL) findNetInfo(netTypes ...string) *SPostgresNetInfo {
	for _, netType := range netTypes {
		for i := range pg.DBInstanceNetInfo {
			if pg.DBInstanceNetInfo[i].NetType == netType {
				return &pg.DBInstanceNetInfo[i]
			}
		}
	}
	return nil
}

func (pg *SPostgreSQL) GetIVpcId() string {
	return pg.VpcId
}

func (pg *SPostgreSQL) GetDBNetworks() ([]cloudprovider.SDBInstanceNetwork, error) {
	ret := []cloudprovider.SDBInstanceNetwork{}
	netInfo := pg.findNetInfo(postgresNetTypePriv)
	if netInfo == nil || len(netInfo.Ip) == 0 {
		return ret, nil
	}
	subnetId := netInfo.SubnetId
	if len(subnetId) == 0 {
		subnetId = pg.SubnetId
	}
	ret = append(ret, cloudprovider.SDBInstanceNetwork{NetworkId: subnetId, IP: netInfo.Ip})
	return ret, nil
}

func (pg *SPostgreSQL) GetConnectionStr() string {
	netInfo := pg.findNetInfo(postgresNetTypePub)
	if netInfo == nil || netInfo.Status != "opened" {
		return ""
	}
	host := netInfo.Address
	if len(host) == 0 {
		host = netInfo.Ip
	}
	if len(host) == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", host, netInfo.Port)
}

func (pg *SPostgreSQL) GetInternalConnectionStr() string {
	netInfo := pg.findNetInfo(postgresNetTypePriv, postgresNetTypeInner)
	if netInfo == nil {
		return ""
	}
	host := netInfo.Ip
	if len(host) == 0 {
		host = netInfo.Address
	}
	if len(host) == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", host, netInfo.Port)
}

func (pg *SPostgreSQL) GetPort() int {
	netInfo := pg.findNetInfo(postgresNetTypePriv, postgresNetTypeInner, postgresNetTypePub)
	if netInfo == nil {
		return 0
	}
	return netInfo.Port
}

func (pg *SPostgreSQL) GetZone1Id() string {
	return pg.Zone
}

// GetZone2Id 返回备节点可用区（跨可用区部署时与主节点不同）
func (pg *SPostgreSQL) GetZone2Id() string {
	for _, node := range pg.DBNodeSet {
		if node.Role == "Standby" {
			return node.Zone
		}
	}
	return ""
}

func (pg *SPostgreSQL) GetZone3Id() string {
	return ""
}

func (pg *SPostgreSQL) GetMasterInstanceId() string {
	if pg.DBInstanceType == "readonly" {
		return pg.MasterDBInstanceId
	}
	return ""
}

func (pg *SPostgreSQL) GetProjectId() string {
	return pg.ProjectId
}

func (pg *SPostgreSQL) GetBillingType() string {
	if pg.PayType == "prepaid" {
		return billingapi.BILLING_TYPE_PREPAID
	}
	return billingapi.BILLING_TYPE_POSTPAID
}

func (pg *SPostgreSQL) IsAutoRenew() bool {
	return pg.AutoRenew == 1
}

func (pg *SPostgreSQL) GetCreatedAt() time.Time {
	return parsePostgresTime(pg.CreateTime)
}

func (pg *SPostgreSQL) GetExpiredAt() time.Time {
	if pg.GetBillingType() != billingapi.BILLING_TYPE_PREPAID {
		return time.Time{}
	}
	return parsePostgresTime(pg.ExpireTime)
}

func (pg *SPostgreSQL) GetSecurityGroupIds() ([]string, error) {
	return pg.region.DescribePostgresDBSecurityGroups(pg.DBInstanceId)
}

func (pg *SPostgreSQL) SetSecurityGroups(ids []string) error {
	return pg.region.ModifyPostgresDBSecurityGroups(pg.DBInstanceId, ids)
}

func (pg *SPostgreSQL) SetAutoRenew(bc billing.SBillingCycle) error {
	return cloudprovider.ErrNotImplemented
}

func (region *SRegion) DescribePostgresDBSecurityGroups(id string) ([]string, error) {
	params := map[string]string{
		"DBInstanceId": id,
	}
	resp, err := region.postgresRequest("DescribeDBInstanceSecurityGroups", params)
	if err != nil {
		return nil, errors.Wrapf(err, "DescribeDBInstanceSecurityGroups")
	}
	ret := struct {
		SecurityGroupSet []struct {
			SecurityGroupId string
		}
	}{}
	err = resp.Unmarshal(&ret)
	if err != nil {
		return nil, errors.Wrapf(err, "Unmarshal")
	}
	groups := []string{}
	for i := range ret.SecurityGroupSet {
		groups = append(groups, ret.SecurityGroupSet[i].SecurityGroupId)
	}
	return groups, nil
}

func (region *SRegion) ModifyPostgresDBSecurityGroups(id string, secIds []string) error {
	params := map[string]string{
		"DBInstanceId": id,
	}
	for idx, secId := range secIds {
		params[fmt.Sprintf("SecurityGroupIdSet.%d", idx)] = secId
	}
	_, err := region.postgresRequest("ModifyDBInstanceSecurityGroups", params)
	return err
}

// GetPostgreSQLs 分页拉取 PostgreSQL 实例，id 非空时按实例 ID 过滤
// Offset 为数据偏移量，按已拉取条数递增；同时按 ID 去重以防分页语义异常导致死循环
func (region *SRegion) GetPostgreSQLs(id string) ([]SPostgreSQL, error) {
	params := map[string]string{
		"Limit": fmt.Sprintf("%d", postgresPageLimit),
	}
	if len(id) > 0 {
		params["Filters.0.Name"] = "db-instance-id"
		params["Filters.0.Values.0"] = id
	}
	ret := []SPostgreSQL{}
	seen := map[string]bool{}
	offset := 0
	for {
		params["Offset"] = fmt.Sprintf("%d", offset)
		resp, err := region.postgresRequest("DescribeDBInstances", params)
		if err != nil {
			return nil, errors.Wrapf(err, "DescribeDBInstances")
		}
		part := struct {
			DBInstanceSet []SPostgreSQL
			TotalCount    int
		}{}
		err = resp.Unmarshal(&part)
		if err != nil {
			return nil, errors.Wrapf(err, "resp.Unmarshal")
		}
		added := 0
		for i := range part.DBInstanceSet {
			instance := part.DBInstanceSet[i]
			if seen[instance.DBInstanceId] {
				continue
			}
			seen[instance.DBInstanceId] = true
			added++
			if postgresSkipStatus[instance.DBInstanceStatus] {
				continue
			}
			instance.region = region
			ret = append(ret, instance)
		}
		offset += len(part.DBInstanceSet)
		if added == 0 || offset >= part.TotalCount {
			break
		}
	}
	return ret, nil
}

func (region *SRegion) GetIPostgreSQLs() ([]cloudprovider.ICloudDBInstance, error) {
	ret := []cloudprovider.ICloudDBInstance{}
	pgs, err := region.GetPostgreSQLs("")
	if err != nil {
		// 该地域未开通 PostgreSQL 服务时视为无实例
		if errors.Cause(err) == cloudprovider.ErrNotSupported {
			return ret, nil
		}
		return nil, errors.Wrapf(err, "GetPostgreSQLs")
	}
	for i := range pgs {
		ret = append(ret, &pgs[i])
	}
	return ret, nil
}

func (region *SRegion) GetPostgreSQL(id string) (*SPostgreSQL, error) {
	pgs, err := region.GetPostgreSQLs(id)
	if err != nil {
		return nil, errors.Wrapf(err, "GetPostgreSQLs")
	}
	for i := range pgs {
		if pgs[i].DBInstanceId == id {
			return &pgs[i], nil
		}
	}
	return nil, errors.Wrapf(cloudprovider.ErrNotFound, "id: [%s]", id)
}
