# 金山云地域级云盘查询实现计划

> 按用户确认的范围在当前会话执行，使用 executing-plans；不部署、不触发线上同步、不修改云权限。

**Goal:** 补齐 `SRegion.GetIDiskById`，消除金山云磁盘同步核对阶段的 `NotImplementedError`。

**Architecture:** 复用 `SRegion.GetDisk` 的按 ID 查询及分页。由响应中的 `AvailabilityZone`、`VolumeType` 构造 `SZone` 和 `SStorage`，保持既有存储全局 ID 规则。不查询可购买目录，不把元数据缺失转换成 `ErrNotFound`。该对象只表示已有磁盘的归属，不推断存储当前是否可购买。

**Tech Stack:** Go 1.24、cloudmux 金山云驱动、标准库 HTTP transport 测试、cloudpods vendor。

## 边界

- 修改 SDK 地域级查询入口，不改现有存储级查询与共享删除流程。
- 空 ID 在发送请求之前返回参数错误。
- 成功查询但无对应 ID 时沿用 `GetDisk` 的 `ErrNotFound`；HTTP、权限、解析失败保留错误。
- 返回磁盘存在但可用区或盘类型为空时返回非 `ErrNotFound` 错误，以免 `syncRemoveCloudDisk` 进入删除分支。
- 快照重复、服务未开通、其他云厂商接口不在本次范围。
- 不更改依赖版本，不重新生成整个 vendor；仅同步此次变更文件。后续正式升级依赖必须包含 SDK 修复。

## 文件

- SDK 工作树：`/Users/janetyoun/.config/superpowers/worktrees/cloudmux/ksyun-region-disk-lookup`
- 修改 SDK `pkg/multicloud/ksyun/disks.go`：地域级接口。
- 新增 SDK `pkg/multicloud/ksyun/region_disk_test.go`：模拟 HTTP 响应，经过实际请求编码、JSON 解析与接口调用，不连接云端。
- 修改 SDK `pkg/multicloud/ksyun/ksyun.go`：测试发现成功 HTTP 返回空/非 JSON 内容时 `JsonClient.Send` 返回 `(nil, nil)`，原请求层在 `resp.GetMap()` 崩溃。仅增加 nil 响应检查，返回非 `ErrNotFound` 的查询错误。
- 验证后同步到 `/opt/tcl/cmp/forks/cloudmux/pkg/multicloud/ksyun/` 及 `/opt/tcl/cmp/cloudpods/vendor/yunion.io/x/cloudmux/pkg/multicloud/ksyun/` 的同名文件。

## 执行步骤

- [x] 基线：SDK 工作树运行 `go test -mod=vendor ./pkg/multicloud/ksyun -count=1`。
- [x] RED：通过 `cloudprovider.ICloudRegion` 调用接口；测试精确 ID、分页、关联全局 ID、空结果、未匹配 ID、403、500、网络错误、非法 JSON、空 ID、可用区/类型缺失。运行 `go test -mod=vendor ./pkg/multicloud/ksyun -run TestRegionGetIDiskById -count=1 -v`，确认最初11个场景失败来自原有 `NotImplementedError`。
- [x] GREEN：增加接口，顺序为校验 ID → `GetDisk(id)` → 校验关联字段 → 构造 storage → 返回 disk。保持真正的查询错误及 `ErrNotFound` 可区分。非 JSON 用例另外复现原请求层空指针崩溃，添加 nil 检查后通过；追加截断 JSON 与空响应测试。
- [x] 回归：金山云 SDK 包全部测试、race、vet；测试使用自定义 HTTP transport，不访问公网。
- [x] 集成：比较原始文件后同步最小补丁，直接测试 cloudpods vendor 包，检查 diff 和格式；不声称已通过线上验证。
- [x] 记录验证结果与未覆盖风险，不提交、不部署。

## 验证结果（2026-10-06）

SDK 工作树，以下命令全部退出0：

```sh
go test -mod=vendor ./pkg/multicloud/ksyun -count=1 -v
go test -mod=vendor -race ./pkg/multicloud/ksyun -count=1
go vet -mod=vendor ./pkg/multicloud/ksyun
```

项目 `/opt/tcl/cmp/cloudpods`，以下命令全部退出0：

```sh
go test -mod=vendor yunion.io/x/cloudmux/pkg/multicloud/ksyun -count=1 -v
go test -mod=vendor -race yunion.io/x/cloudmux/pkg/multicloud/ksyun -count=1
go vet -mod=vendor yunion.io/x/cloudmux/pkg/multicloud/ksyun
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -mod=vendor yunion.io/x/cloudmux/pkg/multicloud/ksyun -c -o /tmp/cmp-ksyun-region-disk-linux-amd64.test
git diff --check
```

新增测试13个场景全部通过。SDK 还通过既有 `TestDiskFormat`。网络异常用例会由底层 HTTP 工具打印测试请求错误，这是模拟错误场景，不是真实公网请求。

## 验证边界与后续发布注意事项

- 未执行 cloudpods 全项目测试、未构建完整服务镜像、未访问真实云 API；Linux 验证仅为金山云包的测试二进制交叉编译。
- 未修改数据库、未触发云同步、未开通服务；线上原失败任务不会因本地改动自动恢复。
- 未改共享 `syncRemoveCloudDisk`：它在云盘归属的存储 ID 无法匹配本地存储时仍有既有删除分支，本次不宣称解决所有本地存储缺失情形。
- 本次仅补地域级 EBS 查询。存储级本地盘模拟逻辑、快照去重、API通用错误码映射均未改动。
- SDK 源码与 cloudpods vendor 同步，但依赖版本未更新。后续发布应包含 vendor 补丁；将来重新执行 `go mod vendor` 前，需要先发布包含修复的 SDK 并更新依赖版本。
- 所有变更保留为未提交工作区文件，SDK隔离工作树保留供后续审阅。

## 后续授权（2026-10-06）

上述“不提交、不部署”为实现阶段的边界。用户随后明确要求“提交修改并部署”。
本次提交仅包含本接口、请求层空响应保护及其测试/计划，不包含其他已有文档或构建产物。
发布沿用测试机 `10.20.30.43`，切换前需重新验证队列、账号及任务空闲；
有在途任务时只准备制品，不强杀、不修改任务状态，不将镜像构建完成当作已部署。
