package multicloud

import (
	"time"

	api "yunion.io/x/cloudmux/pkg/apis/compute"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
)

// SCloudSku is the small common adapter used by public-cloud instance catalogs.
type SCloudSku struct {
	SResourceBase
	STagBase

	Id       string
	Name     string
	GlobalId string
	Status   string

	InstanceTypeFamily   string
	InstanceTypeCategory string
	PrepaidStatus        string
	PostpaidStatus       string
	CpuArch              string
	CpuCoreCount         int
	MemorySizeMB         int
	OsName               string
	SysDiskResizable     bool
	SysDiskType          string
	SysDiskMinSizeGB     int
	SysDiskMaxSizeGB     int
	AttachedDiskType     string
	AttachedDiskSizeGB   int
	AttachedDiskCount    int
	DataDiskTypes        string
	DataDiskMaxCount     int
	NicType              string
	NicMaxCount          int
	GpuAttachable        bool
	GpuSpec              string
	GpuCount             string
	GpuMaxCount          int
}

func NewSCloudSku(name string) *SCloudSku {
	return &SCloudSku{
		Id:               name,
		Name:             name,
		GlobalId:         name,
		Status:           api.SkuStatusAvailable,
		PrepaidStatus:    api.SkuStatusSoldout,
		PostpaidStatus:   api.SkuStatusAvailable,
		OsName:           "Any",
		SysDiskResizable: true,
		AttachedDiskType: "iscsi",
		DataDiskMaxCount: 6,
		NicType:          "vpc",
		NicMaxCount:      1,
	}
}

var _ cloudprovider.ICloudSku = (*SCloudSku)(nil)

func (s *SCloudSku) GetId() string                   { return s.Id }
func (s *SCloudSku) GetName() string                 { return s.Name }
func (s *SCloudSku) GetGlobalId() string             { return s.GlobalId }
func (s *SCloudSku) GetStatus() string               { return s.Status }
func (s *SCloudSku) GetCreatedAt() time.Time         { return time.Time{} }
func (s *SCloudSku) GetInstanceTypeFamily() string   { return s.InstanceTypeFamily }
func (s *SCloudSku) GetInstanceTypeCategory() string { return s.InstanceTypeCategory }
func (s *SCloudSku) GetPrepaidStatus() string        { return s.PrepaidStatus }
func (s *SCloudSku) GetPostpaidStatus() string       { return s.PostpaidStatus }
func (s *SCloudSku) GetCpuArch() string              { return s.CpuArch }
func (s *SCloudSku) GetCpuCoreCount() int            { return s.CpuCoreCount }
func (s *SCloudSku) GetMemorySizeMB() int            { return s.MemorySizeMB }
func (s *SCloudSku) GetOsName() string               { return s.OsName }
func (s *SCloudSku) GetSysDiskResizable() bool       { return s.SysDiskResizable }
func (s *SCloudSku) GetSysDiskType() string          { return s.SysDiskType }
func (s *SCloudSku) GetSysDiskMinSizeGB() int        { return s.SysDiskMinSizeGB }
func (s *SCloudSku) GetSysDiskMaxSizeGB() int        { return s.SysDiskMaxSizeGB }
func (s *SCloudSku) GetAttachedDiskType() string     { return s.AttachedDiskType }
func (s *SCloudSku) GetAttachedDiskSizeGB() int      { return s.AttachedDiskSizeGB }
func (s *SCloudSku) GetAttachedDiskCount() int       { return s.AttachedDiskCount }
func (s *SCloudSku) GetDataDiskTypes() string        { return s.DataDiskTypes }
func (s *SCloudSku) GetDataDiskMaxCount() int        { return s.DataDiskMaxCount }
func (s *SCloudSku) GetNicType() string              { return s.NicType }
func (s *SCloudSku) GetNicMaxCount() int             { return s.NicMaxCount }
func (s *SCloudSku) GetGpuAttachable() bool          { return s.GpuAttachable }
func (s *SCloudSku) GetGpuSpec() string              { return s.GpuSpec }
func (s *SCloudSku) GetGpuCount() string             { return s.GpuCount }
func (s *SCloudSku) GetGpuMaxCount() int             { return s.GpuMaxCount }
func (s *SCloudSku) Delete() error                   { return nil }
