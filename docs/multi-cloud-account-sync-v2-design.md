# 多云账号同步 V2 总体架构方案

## 1. 文档定位

- 状态：Draft
- 代码基线：Cloudpods `release/4.0.4`，commit `92e18c0e076b628f55a10e98b92663b815db5a60`
- 范围：阿里云、腾讯云、AWS、Azure、GCP、华为云、火山引擎、金山云及 Cloudpods 已支持的私有云
- AWS 专项分析保留在 `docs/cloud-account-sync-v2-design.md`，本文件是多云架构的权威总方案
- 本阶段只定义方案，不修改同步业务代码

## 2. 核心结论

多云同步不能用一套 AWS Region 探测逻辑覆盖所有厂商。通用核心只负责：

1. 同步运行生命周期；
2. 任务规划与依赖；
3. 公平调度和背压；
4. 状态聚合、重试和恢复；
5. 指标、审计和灰度。

厂商差异必须通过可选 provider adapter 提供：

1. 账号/订阅/项目拓扑；
2. Region、Zone 和全局资源作用域；
3. 轻量资源存在性探针；
4. API 限流模型；
5. 错误分类；
6. 增量游标或事件源。

没有实现 V2 adapter 的云厂商继续走现有同步链路，确保渐进迁移。

## 3. 当前通用链路的问题

Cloudpods 当前同步路径大致为：

```text
CloudAccount
  -> GetSubAccounts
  -> CloudProvider × N
  -> GetIRegions
  -> CloudProviderRegion × N
  -> syncPublicCloudProviderInfo / syncOnPremiseCloudProviderInfo
  -> 各资源类型串行同步
```

通用问题包括：

- 默认将 provider-region 作为唯一调度单元，无法表达账号级和全局资源；
- 工作队列是进程内状态，重启恢复能力有限；
- 防重依赖进程内 map，不能覆盖多 region 实例；
- 默认 5 个 Region worker，且缺少账号级公平性；
- 父任务等待的是入队而不是实际完成；
- 单 Region 内大量资源串行执行；
- 不同厂商的限流、分页、错误和资源作用域被压进相同流程；
- 状态只有 queued/syncing/idle，无法表达 partial、retry、throttled；
- API 获取与数据库 reconcile 没有独立并发预算。

## 4. 多云差异模型

### 4.1 账号层级差异

| 类型 | 代表厂商 | 典型层级 |
|---|---|---|
| 组织账号 | AWS、阿里云 | Organization/Resource Directory -> Member Account |
| 订阅模型 | Azure | Tenant -> Subscription |
| 项目模型 | GCP | Organization/Folder -> Project |
| 企业项目/委托 | 华为云 | Account -> Enterprise Project / Agency |
| 单账号多 Region | 腾讯云、火山、金山等 | Account -> Region |
| 私有云 | VMware、OpenStack、ZStack、Cloudpods 等 | Endpoint -> Region/Zone/Cluster |
| 全局对象存储 | S3、CephFS、RemoteFile 等 | Endpoint/Bucket，未必存在 Region |

同步核心不能假定 `CloudProvider == 子账号`，也不能假定所有资源都属于 Region。

### 4.2 资源作用域差异

统一定义：

```text
GLOBAL        IAM、DNS、CDN、组织、全局 Bucket 等
ACCOUNT       账号、订阅、Project、Quota 等
REGION        VPC、EIP、LB、RDS、Cache、VM 等
ZONE          Host、Storage、部分 VM/磁盘等
ENDPOINT      私有云 Endpoint、对象存储 Endpoint
```

每种资源由 adapter 声明作用域，Planner 据此构建任务，不再把所有任务强制塞进 Region。

### 4.3 Region 可用性差异

| 厂商类型 | Region 候选依据 |
|---|---|
| AWS | `OptInStatus`、用户 allowlist、历史活跃度 |
| Azure | Subscription locations、资源提供者注册状态、历史资源 |
| GCP | Project 中启用的服务、Region/Zone、资产索引 |
| 阿里云/腾讯云/华为云/火山/金山 | 厂商 Region API、账号权限、历史活跃度 |
| 私有云 | Endpoint 返回的实际 Region/Zone，通常不做 dormant 探测 |
| 全局服务 | 不创建伪 Region，直接生成 GLOBAL/ENDPOINT 任务 |

### 4.4 错误语义差异

统一错误类别：

```go
type SyncErrorClass string

const (
    SyncErrorAuth         SyncErrorClass = "auth"
    SyncErrorPermission   SyncErrorClass = "permission"
    SyncErrorThrottle     SyncErrorClass = "throttle"
    SyncErrorTimeout      SyncErrorClass = "timeout"
    SyncErrorUnavailable  SyncErrorClass = "unavailable"
    SyncErrorUnsupported  SyncErrorClass = "unsupported"
    SyncErrorNotFound     SyncErrorClass = "not_found"
    SyncErrorInternal     SyncErrorClass = "internal"
)
```

Auth/Permission 错误不能被解释为“Region 为空”；Throttle/Timeout 不改变活跃状态；Unsupported 才允许跳过资源类型。

## 5. 目标架构

```text
ScheduledTask / Manual API / Event
                |
                v
        SyncRun Coordinator
    - persistent single-flight
    - trigger/mode/scope
    - build or resume plan
                |
                v
          SyncPlan Builder
    +-----------+-------------+
    |                         |
    v                         v
Provider Adapter       Historical Activity Index
- topology             - active/dormant/error
- scopes               - last success/change
- probes               - next probe time
- error classifier     - cost/latency EWMA
- rate profile
    |                         |
    +-----------+-------------+
                v
          Fair Dispatcher
    - account round-robin
    - provider round-robin
    - global/account/provider/API budgets
    - backpressure and retry queue
                |
                v
          SyncUnit Executor
    - GLOBAL/ACCOUNT/REGION/ZONE/ENDPOINT
    - Resource DAG
    - context cancellation
                |
                v
          Batch Reconciler
    - API data -> compare -> batch upsert
                |
                v
          SyncRun Aggregator
    - progress/status/error summary
```

## 6. Provider Adapter 设计

在现有 `ICloudProvider` 之上增加可选接口，不破坏旧 provider：

```go
type ICloudSyncPlanner interface {
    DescribeSyncTopology(ctx context.Context, req TopologyRequest) (*SyncTopology, error)
    BuildProbe(ctx context.Context, scope SyncScope) (ICloudScopeProbe, error)
    ClassifySyncError(err error) SyncErrorClass
    SyncRateProfile() SyncRateProfile
    ResourceGraph() ResourceGraph
}

type ICloudScopeProbe interface {
    Probe(ctx context.Context, req ProbeRequest) (*ProbeResult, error)
}
```

未实现时使用 `LegacySyncAdapter`：

- 继续调用 `GetSubAccounts/GetIRegions/GetCapabilities`；
- 使用现有 provider-region 同步；
- 仍进入新的 SyncRun 和 Dispatcher，以获得防重、恢复和真实进度；
- 不启用厂商特有的轻量探针。

### 6.1 SyncTopology

```go
type SyncTopology struct {
    Providers []ProviderNode
    Scopes    []SyncScope
}

type SyncScope struct {
    Type       ScopeType
    ExternalID string
    ParentID   string
    Status     ScopeStatus
    Pinned     bool
    Metadata   map[string]string
}
```

Topology 只描述结构，不拉取完整资源。

### 6.2 SyncRateProfile

```go
type SyncRateProfile struct {
    GlobalConcurrency      int
    PerAccountConcurrency  int
    PerProviderConcurrency int
    PerScopeConcurrency    int
    APILimits              map[string]RateLimit
}
```

厂商 adapter 提供默认值，运维配置可以覆盖。

## 7. SyncRun 与 SyncUnit

### 7.1 SyncRun

```text
id
cloudaccount_id
provider_type
trigger: scheduled/manual/event/recovery
mode: discover/inventory/incremental/full
status: queued/planning/running/retrying/succeeded/partial_success/failed/cancelled
plan_version
total/queued/running/succeeded/failed/retrying
started_at/finished_at
error_summary
```

同一 cloudaccount 默认只允许一个 inventory/full SyncRun；事件型单资源 reconcile 可以按资源键合并。

### 7.2 SyncUnit

```text
sync_run_id
cloudprovider_id
scope_type
scope_id
resource_group
status
attempts
next_retry_at
lease_owner
lease_expire_at
request_cost
sql_cost
error_class
error_message
```

唯一键：

```text
(sync_run_id, cloudprovider_id, scope_type, scope_id, resource_group)
```

SyncUnit 使用现有 taskman 或新增表持久化。worker 通过 lease 获取任务，进程退出后 lease 超时可恢复。

## 8. Planner：按能力构建计划

Planner 输入：

- trigger 和 mode；
- 用户指定的 provider/region/resource 范围；
- adapter topology；
- Activity Index；
- provider capabilities；
- 最近一次成功同步和变更游标；
- 当前并发预算。

Planner 输出 SyncUnit DAG，而不是立即提交 goroutine。

### 8.1 计划模式

| 模式 | 用途 | 主要内容 |
|---|---|---|
| discover | 发现账号和作用域 | 组织、订阅、Project、Region、Endpoint |
| inventory | 首次低成本纳管 | 身份、作用域、关键资源索引 |
| incremental | 高频同步 | 活跃作用域、变化资源、状态字段 |
| full | 低频校准 | 删除检测、低频资源、完整 compare |
| reconcile | 单资源修复 | 由事件或人工触发 |

### 8.2 计划裁剪

裁剪优先级：

1. 用户明确指定的 scope；
2. provider capability 不支持的资源；
3. adapter 明确 unavailable 的 scope；
4. dormant 且未到 next_probe_at 的 scope；
5. SkipSyncResources；
6. incremental 模式下无变更游标的低频资源。

## 9. Scope Activity Index

Activity Index 不局限于 Region：

```text
unknown
candidate
active
dormant
denied
unavailable
error
```

字段：

- provider/scope identity；
- last_probe_at；
- last_probe_success_at；
- last_resource_seen_at；
- empty_streak；
- next_probe_at；
- last_error_class；
- user_pinned；
- estimated_cost；
- duration_ewma。

规则：

- user_pinned 永远参与同步；
- Auth/Permission 不改变 active/dormant；
- 连续多次成功空结果才进入 dormant；
- dormant 使用 1h、6h、24h、72h 退避；
- 发现资源立即切回 active；
- 私有云默认不执行 dormant 扫描，除非 adapter 明确支持。

## 10. 轻量探针

通用核心禁止直接调用返回完整列表的 `GetIVMs/GetIEips` 来判断是否有资源。

探针要求：

1. 使用厂商支持的最小分页参数，如 `MaxResults=1`、`PageSize=1`；
2. 找到任一资源立即短路；
3. 使用 context-aware 请求，超时后真正取消；
4. 返回标准化错误类别；
5. 记录 API 成本和耗时；
6. 首次发现也受 batch 和并发预算限制。

没有廉价探针的 provider：

- 使用历史活跃 scope；
- 用户指定 allowlist；
- 分批执行最小 inventory；
- 不允许一次性扫描所有 scope 的所有资源。

## 11. 公平调度与背压

调度层级：

```text
cloudaccount -> cloudprovider -> scope -> resource group
```

算法：

1. cloudaccount 加权 round-robin；
2. 同一账号内 provider round-robin；
3. provider 内优先 active、用户触发和历史耗时短的 scope；
4. 同一 API 类型共享 token bucket；
5. 数据库 reconcile 使用独立 semaphore；
6. 队列满时保持 queued 并返回 backpressure，禁止静默丢弃。

通用初始配置：

```yaml
cloud_sync_global_concurrency: 12
cloud_sync_per_account_concurrency: 6
cloud_sync_per_provider_concurrency: 2
cloud_sync_per_scope_concurrency: 3
cloud_sync_discovery_concurrency: 8
cloud_sync_db_reconcile_concurrency: 4
cloud_sync_queue_capacity: 4096
```

adapter 可降低或提高默认值，例如私有云可能限制单 Endpoint 并发为 1。

## 12. Resource DAG

通用资源组：

```text
identity/global
catalog: region, zone, quota, sku
network: vpc, subnet, route, security-group, eip
compute: host, storage, vm, disk
image: image, snapshot, cached-image
edge: lb, waf, cdn, dns
 data: rds, cache, mongodb, elasticsearch, kafka
platform: bucket, nas, k8s, app, tablestore
```

DAG 由 adapter 调整：

- 公有云通常 `catalog -> network -> compute/image`；
- 私有云可能是 `catalog -> host/storage -> network -> vm`；
- 全局服务绕过 Region；
- 不支持的组不生成 SyncUnit。

每个资源组使用独立结果集，完成后 merge。当前 `SSyncResultSet` 是非线程安全 map，在 DAG 并行前必须改造。

## 13. API 获取与数据库写入解耦

每个 SyncUnit 分两段：

```text
FetchSnapshot -> ReconcileSnapshot
```

原则：

- Fetch 受厂商 API 限流；
- Reconcile 受数据库并发限制；
- 同 manager + scope 的写入串行；
- 100～500 条批量 upsert；
- 无变化时跳过 update/OpsLog；
- 大结果可分页写入 staging，不在内存长期保留；
- 分别记录 request_cost 和 sql_cost。

## 14. 状态与进度

不得在子任务仍运行时提前结束 CloudAccount 任务。

状态从 SyncRun 聚合：

```text
CloudAccount <- all providers
CloudProvider <- all scopes
Scope <- all resource groups
```

页面展示：

```text
计划 960
完成 238
运行 12
排队 694
重试 8
失败 8
```

partial_success 允许用户看到具体失败 provider/scope/resource group，而不是整个账号只有 syncing/error。

## 15. 厂商实施矩阵

### 第一批：高价值公有云

| Provider | Adapter 重点 |
|---|---|
| AWS | Organizations、OptInStatus、STS、区域/API token bucket |
| Azure | Tenant/Subscription、registered provider、location、ARM throttling |
| GCP | Organization/Folder/Project、enabled services、global/zonal scope |
| 阿里云 | Resource Directory、Region、RAM AssumeRole、API 限流 |
| 腾讯云 | Region、CAM、按产品 API 限流 |
| 华为云 | Account/Agency/Enterprise Project、Region、Project scope |
| 火山引擎 | Account/Region、产品限流与分页 |
| 金山云 | Account/Region、产品能力矩阵 |

### 第二批：私有云与专有云

- VMware、OpenStack、ZStack、HCS/HCSO、Apsara、Cloudpods、Nutanix、Proxmox 等；
- 默认使用 LegacySyncAdapter 进入 SyncRun；
- 不自动关闭 Region/Endpoint；
- Endpoint 级串行或低并发；
- 重点解决任务恢复、真实进度和数据库背压。

### 第三批：专用服务

- S3、Ceph、CephFS、RemoteFile、OceanBase 等；
- 以 GLOBAL/ENDPOINT scope 建模；
- 不创建无意义的 Region SyncUnit。

## 16. 对当前未提交 Region Discover 原型的处理

当前原型不应进入通用 models 主流程。建议：

1. 保留候选排序、provider round-robin 和状态字段思想；
2. 将 AWS 特有探测迁入 AWS SyncPlanner adapter；
3. 删除完整列表式通用探针；
4. 修复 timeout 后仍阻塞的问题；
5. AuthFailure 不再标记为空 Region；
6. NeverSynced 也必须受批次预算限制；
7. 不允许 CloudAccountSyncInfoTask 提前 complete；
8. 通用核心只消费 adapter 返回的 SyncTopology/ProbeResult。

## 17. 分阶段实施

### Phase 0：整理原型和契约

- 冻结当前未提交代码；
- 定义 SyncScope、SyncTopology、ProbeResult、SyncErrorClass；
- 实现 LegacySyncAdapter；
- 修正任务完成语义；
- 不改变现网同步结果。

### Phase 1：SyncRun 与公平调度

- 持久化 SyncRun/SyncUnit；
- DB/etcd single-flight；
- lease/recovery；
- account/provider/scope 公平调度；
- 真实进度和指标。

所有 provider 立即受益，不依赖厂商探针。

### Phase 2：第一批公有云 adapter

- AWS、Azure、GCP、阿里云、腾讯云；
- 轻量 topology 和 probe；
- provider-specific rate profile；
- Activity Index 和退避。

### Phase 3：Resource DAG 与批量写入

- 抽取通用资源组；
- adapter 定制依赖；
- 有限并行；
- 线程安全结果聚合；
- 批量 reconcile。

### Phase 4：其余云与事件驱动

- 华为、火山、金山和私有云 adapter；
- AWS Config/CloudTrail、Azure Resource Graph/Event Grid、GCP Asset Inventory/PubSub 等可选增量源；
- 事件同步仍配合周期性 full reconcile。

## 18. 测试要求

通用测试：

1. 48 providers × 30 scopes 不会一次全部执行；
2. 大账号不能饿死小账号；
3. 全局/账号/provider/scope 并发上限正确；
4. 队列满不丢任务；
5. 服务重启可恢复 lease 过期任务；
6. 手工和定时 full sync 不重叠；
7. 父任务只在子任务终态后完成；
8. partial_success 能定位失败单元；
9. context cancel 不泄漏 goroutine；
10. DAG 并发下 `go test -race` 无数据竞争。

Adapter contract tests：

1. topology 不拉完整资源；
2. probe 使用最小分页；
3. Auth/Permission/Throttle/Timeout 分类一致；
4. unsupported resource 不生成任务；
5. global resource 不依赖 Region；
6. provider rate profile 生效；
7. LegacySyncAdapter 与旧结果一致。

## 19. 可观测性

指标标签不得包含高基数资源 ID，详细定位进入结构化日志。

核心指标：

- `cloud_sync_runs_total{provider,trigger,mode,status}`
- `cloud_sync_run_duration_seconds{provider,mode}`
- `cloud_sync_units{provider,scope_type,resource_group,status}`
- `cloud_sync_queue_depth{provider}`
- `cloud_sync_worker_utilization{scope}`
- `cloud_sync_api_duration_seconds{provider,api}`
- `cloud_sync_api_throttle_total{provider,api}`
- `cloud_sync_sql_duration_seconds{resource_group}`
- `cloud_sync_probe_result_total{provider,result}`
- `cloud_sync_recovery_total{provider}`

日志统一携带 sync_run_id、cloudaccount_id、cloudprovider_id、scope_type、scope_id、resource_group。

## 20. 灰度与回滚

```yaml
cloud_sync_v2_enabled: false
cloud_sync_v2_provider_allowlist: []
cloud_sync_v2_planner_enabled: false
cloud_sync_v2_resource_dag_enabled: false
```

灰度顺序：

1. LegacySyncAdapter + SyncRun，只验证状态和恢复；
2. 小型账号启用公平调度；
3. 每个公有云选择一个测试账号启用 provider adapter；
4. 对比旧/新资源结果连续 3 轮；
5. 大型组织账号灰度；
6. 最后启用 Resource DAG。

关闭 feature flag 即回旧链路；SyncRun 和 Activity Index 保留只读历史，不影响旧模型。

## 21. 推荐优先级

建议优先实施：

1. SyncRun/SyncUnit 和真实完成语义；
2. 公平调度与分层背压；
3. LegacySyncAdapter，先让所有云获得恢复和进度能力；
4. AWS/阿里云/Azure/GCP/腾讯云轻量 Planner；
5. Resource DAG 和批量写入；
6. 其他 provider 与事件驱动。

这样不会把系统改造成 AWS 专用同步器，同时可以分阶段让所有云厂商获得确定收益。
