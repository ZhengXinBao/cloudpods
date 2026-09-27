# 云账号同步 V2 架构方案

## 1. 文档状态

- 状态：Draft
- 目标版本：Cloudpods `release/4.0.4`
- 审查基线：`92e18c0e076b628f55a10e98b92663b815db5a60`（`v4.0.4-20260914.0`）
- 适用场景：单个 AWS Organizations 云账号管理数十个成员账号，每个成员账号覆盖多个 Region
- 本阶段只定义方案，不直接修改同步业务代码

## 2. 背景与问题

当前一次云账号同步的主要路径为：

```text
CloudAccountSyncInfoTask
  -> probeAccountStatus / GetSubAccounts
  -> importAllSubaccounts
  -> prepareCloudproviderRegions
  -> CloudProviderSyncInfoTask × N providers
  -> SCloudproviderregion.submitSyncTask × N regions
  -> syncPublicCloudProviderInfo
  -> 各资源类型串行同步
```

线上场景包含约 48 个 AWS 订阅。当前实现存在以下放大效应：

1. AWS `DescribeRegions(AllRegions=true)` 返回的 `OptInStatus` 没有参与过滤，Region 被创建后默认启用。
2. `CloudSyncWorkerCount` 默认只有 5，Region 同步最多 5 路并发。
3. 工作队列按 provider-region RowId 哈希，但没有账号级公平性和分层限流。
4. `SyncCallSyncCloudproviderRegions` 的 WaitGroup 只包围入队动作，没有等待 Region 同步真正完成。
5. 单 Region 内二十余类资源大体串行执行，任一慢 API 都会占用整个 Region worker。
6. 同步队列是进程内状态；服务重启后缺少显式的同步运行恢复模型。
7. 账号级防重依赖进程内 map，不能覆盖多实例和进程重启。

结果是：48 个订阅可以展开为上千个 Region 工作单元，页面状态又可能早于实际工作完成。

## 3. 最新工作树代码审查

当前工作树已有一组未提交的“Region 资源发现”改造，主要包括：

- `pkg/compute/models/cloudaccount_region_discover.go`
- `pkg/compute/models/cloudaccount_sync_policy.go`
- `pkg/compute/models/cloudaccount_region_discover_test.go`
- `pkg/compute/models/cloudaccount_sync_policy_test.go`
- 以及 cloudaccount、cloudprovider、cloudproviderregion、cloudsync、options 和 task 的配套修改

这组代码试图先探测 Region 是否存在 VM、EIP、RDS、LB、Cache，再决定是否启用 Region。方向正确，但当前形态不建议直接合并，原因如下。

### 3.1 首次探测仍可能形成 API 洪峰

`selectDiscoverCandidateIndexes` 只限制重复探测批次，所有 `NeverSynced` Region 会绕过 batch 上限。48 个订阅首次接入时，仍可能一次提交全部 Region。

### 3.2 探针并不轻量

当前探针直接调用：

- `GetIVMs`
- `GetIEips`
- `GetIDBInstances`
- `GetILoadBalancers`
- `GetIElasticcaches`

这些接口返回完整资源列表并可能自动分页。探测一个空 Region 最多执行 5 类完整列表调用，最坏情况下比一次受控的增量同步还昂贵。

### 3.3 超时实现无效

`cloudRegionHasResourcesWithTimeout` 在 timer 触发后再次读取结果 channel，因此仍会等待后台调用结束。该函数并没有真正限制调用时长。

### 3.4 错误被错误解释为“Region 为空”

`InvalidClientTokenId`、`AuthFailure` 被当作 Region 无资源。凭据错误、STS 失败和 Region 未启用应分别记录；凭据错误不能导致 Region 被自动关闭。

### 3.5 状态语义被提前结束

当前改动在 provider-region 任务仍运行时就完成 `CloudAccountSyncInfoTask`，同时放宽 `CanSync` 并移除 provider-region 状态检查。这会造成：

- 页面提前显示完成；
- 定时同步与人工同步可能重叠；
- 同一账号可能重复生成 Region 任务；
- 失败无法准确汇总到账号级运行。

### 3.6 仍是进程内调度

现有改动没有解决重启恢复、队列持久化、跨 region 实例防重和任务级进度聚合。

## 4. 设计目标

1. 大型组织账号接入时，不产生全 Region、全资源 API 洪峰。
2. 同步任务能够公平分配，单个大账号不能占满全部 worker。
3. 支持全局、账号、订阅、API 类型四级并发与限流。
4. 账号同步状态必须反映真实子任务进度。
5. Region/API 局部失败不能阻塞其他订阅。
6. region 服务重启后可以恢复未完成同步。
7. 保持现有 CloudAccount、CloudProvider 和 CloudProviderRegion API 基本兼容。
8. 第一阶段不强制引入 Redis、RabbitMQ 或 NATS 等新中间件。

## 5. 非目标

1. 本期不实现所有云厂商的事件驱动同步。
2. 本期不重写现有资源 compare/sync 逻辑。
3. 本期不把 region 服务拆成独立微服务集群。
4. 本期不追求单次同步无限并发；必须优先保证 AWS API 和数据库稳定。

## 6. 总体架构

```text
ScheduledTask / Manual API
          |
          v
   SyncRun Coordinator
   - DB/etcd single-flight
   - 创建持久化运行记录
   - 构建 SyncPlan
          |
          +--------------------+
          |                    |
          v                    v
 Region Activity Index     Provider Catalog
 - active/dormant/error    - Organizations accounts
 - next_probe_at           - enabled AWS regions
 - empty_streak            - cached STS/session
          |                    |
          +----------+---------+
                     v
              Fair Dispatcher
      - cloudaccount round-robin
      - provider round-robin
      - global/account/provider/API limits
                     |
                     v
              Resource DAG Executor
      catalog -> network -> compute -> dependent
              \-> data/edge/other parallel
                     |
                     v
              Batch DB Reconciler
                     |
                     v
              SyncRun Aggregator
       queued/running/succeeded/partial/failed
```

## 7. 核心设计

### 7.1 Region Activity Index

为每个 provider-region 维护显式发现状态，而不是仅使用 `Enabled`：

```text
unknown       尚未探测
candidate     AWS 已启用，等待轻量探测
active        最近发现资源或用户强制启用
dormant       连续多次未发现资源
denied        当前角色无权限访问
unavailable   Region 未 opt-in
error         网络、限流或临时云 API 错误
```

建议字段：

- `discover_status`
- `last_discover_at`
- `last_discover_success_at`
- `last_discover_error`
- `empty_streak`
- `next_probe_at`
- `user_pinned`

`Enabled` 继续作为兼容字段，但由 Activity Index 和用户显式配置共同计算。

### 7.2 Region 候选集生成

按以下优先级生成候选集：

1. 用户 Region allowlist / 已固定启用 Region；
2. AWS `OptInStatus` 为 `opt-in-not-required` 或 `opted-in`；
3. 本地已有资源或过去同步成功的 Region；
4. Region Activity Index 到达 `next_probe_at` 的 dormant Region；
5. `not-opted-in` 直接标记 unavailable，不执行资源 API 探测。

这一步先消除确定无效的 Region，再进入资源发现。

### 7.3 轻量资源存在性探针

不要复用返回完整列表的 `GetIVMs` 等接口。新增 provider 可选能力：

```go
type ICloudRegionResourceProbe interface {
    ProbeResources(ctx context.Context, kinds []string) (ProbeResult, error)
}
```

AWS 实现要求：

- 每类 API 使用 `MaxResults=1` 或等价分页参数；
- 找到任一资源立即短路；
- 使用 context-aware SDK 请求；
- 对 Unsupported、Denied、Throttle、Timeout 分别分类；
- 探测预算按账号和 API 类型限流。

探测顺序建议根据命中率和成本动态调整，初始默认：EC2 -> RDS -> ELB -> ElastiCache -> EIP。

### 7.4 探测预算与退避

首次接入也必须有预算：

- 全局 discovery 并发：8；
- 单云账号 discovery 并发：4；
- 单订阅 discovery 并发：1；
- 单轮最多探测 100 个 provider-region；
- 未完成候选进入下一轮，而不是一次全部提交；
- dormant Region 使用 1h、6h、24h、72h 指数退避；
- Throttling 使用带 jitter 的短退避，不增加 empty_streak；
- Auth/STS 错误升级为账号或订阅错误，不修改 Region active/dormant 状态。

### 7.5 持久化 SyncRun

新增账号级同步运行模型：

```text
SyncRun
- id
- cloudaccount_id
- trigger: manual/scheduled/discovery/reconcile
- mode: inventory/full/incremental
- status
- total_units
- queued_units
- running_units
- succeeded_units
- failed_units
- started_at / finished_at
- error_summary
```

同步单元：

```text
SyncUnit
- sync_run_id
- cloudprovider_id
- cloudregion_id
- resource_group
- status
- attempts
- next_retry_at
- lease_owner / lease_expire_at
```

第一阶段可复用现有 taskman 表和父子任务能力，不新增外部队列。账号防重使用数据库唯一约束或 etcd lock，替代进程内 map。

### 7.6 公平调度器

当前按 RowId 哈希到固定 worker，无法保证账号公平。新调度器采用两级 round-robin：

1. cloudaccount 之间轮询；
2. 同一 cloudaccount 的 provider 之间轮询；
3. provider 内按 Region 活跃度和上次完成时间排序。

建议默认限制：

```yaml
cloud_sync_global_concurrency: 12
cloud_sync_per_account_concurrency: 8
cloud_sync_per_provider_concurrency: 2
cloud_sync_discovery_global_concurrency: 8
cloud_sync_discovery_per_provider_concurrency: 1
cloud_sync_queue_capacity: 4096
```

队列满时必须返回 backpressure 错误或保留 queued 状态，禁止静默丢任务。

### 7.7 Resource DAG

单 Region 同步拆为资源组 DAG：

```text
catalog: quota, zone, sku
              |
              v
network: vpc, subnet, security-group, route, eip
              |
              v
compute: storage, host, vm, disk
              |
              v
snapshot: snapshot-policy, snapshot, cached-image

independent after catalog:
- data: rds, cache, mongodb, elasticsearch, kafka
- edge: loadbalancer, waf, cdn, dns
- platform: bucket, nas, k8s, tablestore, app
```

DAG 规则：

- 单 Region 同时最多运行 3～4 个资源组；
- 同一资源组内保持现有顺序，先降低改造风险；
- 每个资源组使用独立结果收集器，结束后再聚合；
- 现有 `SSyncResultSet` 是普通 map，并行前必须改为线程安全结构或分组后 merge；
- 资源组失败只影响当前组，SyncRun 最终可进入 partial_success。

### 7.8 正确的完成语义

不得在 provider-region 任务仍运行时完成账号任务。

建议状态聚合：

```text
CloudAccount.SyncStatus = SyncRun.Status 聚合结果
CloudProvider.SyncStatus = 其所有 SyncUnit 聚合结果
CloudProviderRegion.SyncStatus = 当前 Region 资源组聚合结果
```

父任务完成条件：所有非延迟重试的 SyncUnit 进入终态。页面应展示：

```text
完成 238 / 960
运行 12
排队 694
重试 8
失败 8
```

### 7.9 数据库写入优化

在调高 API 并发前，必须限制数据库压力：

1. compare 结果按 100～500 条批量 upsert；
2. 相同 manager/region 的写入串行化；
3. API 获取和 DB reconcile 使用独立并发限制；
4. 记录 request_cost 与 sql_cost，自动判断瓶颈位于云 API 还是数据库；
5. 对无变化结果跳过 OpsLog 和无效 update。

### 7.10 增量同步

同步模式分为：

- inventory：首次发现，仅同步身份、Region 和轻量资源索引；
- incremental：定时同步活跃 Region 和高频资源；
- full：每日或每周校准，包括删除检测、镜像、快照和低频资源；
- discovery：按退避策略扫描 dormant Region。

后续可接入 AWS Config Aggregator、CloudTrail/EventBridge，事件触发单资源 reconcile；事件同步不能替代周期性全量校准。

## 8. 分阶段实施

### Phase 0：冻结并评审现有未提交改动

- 保留当前工作树，不直接覆盖；
- 将 Region 发现逻辑拆成独立提交候选；
- 修复无效超时、错误分类和首次无界探测；
- 不接受账号任务提前 complete 的状态语义。

验收：现有测试通过，并补充 48 providers × 30 regions 的调度仿真测试。

### Phase 1：Region 候选过滤与轻量探针

- 使用 AWS `OptInStatus`；
- 新增 provider-specific `ProbeResources(MaxResults=1)`；
- 引入 Activity Index、探测预算和退避；
- 继续使用现有 Region worker 执行正式同步。

预期：无效 Region 工作量减少 50%～90%。

### Phase 2：SyncRun 与公平调度

- 增加持久化运行和同步单元；
- 数据库/etcd single-flight；
- 替换 RowId hash queue；
- 增加全局、账号、provider 并发限制；
- 修复父子任务完成语义和页面进度。

预期：避免任务洪峰、状态提前完成和单账号独占。

### Phase 3：Resource DAG 与批量写入

- 抽取资源组；
- 在依赖关系允许范围内有限并行；
- 线程安全结果聚合；
- 批量 upsert 和写入限流。

预期：活跃 Region 同步再缩短 30%～60%。

### Phase 4：增量与事件驱动

- 高频增量、低频全量；
- 可选接入 AWS Config/CloudTrail；
- 根据历史耗时和限流动态调节并发。

## 9. 测试方案

必须新增以下测试：

1. 48 providers × 30 regions 首次发现不会一次全部提交；
2. 单 provider discovery 并发始终不超过 1；
3. 不同 cloudaccount 可以公平获得执行槽位；
4. `not-opted-in` Region 不执行资源 API；
5. `AuthFailure` 不会被记录为空 Region；
6. timeout 到期后调用可取消且不泄漏 goroutine；
7. Throttling 进入重试，不增加 empty_streak；
8. 服务重启后 queued/running SyncUnit 能被恢复；
9. 手工同步和定时同步不会重叠；
10. 父任务只在全部子任务终态后完成；
11. DAG 并发下运行 `go test -race` 不出现 map 数据竞争；
12. 账号被删除或禁用时，待执行任务可取消。

## 10. 可观测性

至少暴露以下指标：

- `cloud_sync_runs_total{trigger,status}`
- `cloud_sync_units{account,provider,status}`
- `cloud_sync_queue_depth{account}`
- `cloud_sync_active_workers{scope}`
- `cloud_sync_api_duration_seconds{provider,api}`
- `cloud_sync_sql_duration_seconds{resource}`
- `cloud_sync_throttle_total{provider,api}`
- `cloud_sync_discovery_result_total{result}`
- `cloud_sync_run_duration_seconds{mode}`

日志统一带：`sync_run_id`、`cloudaccount_id`、`cloudprovider_id`、`cloudregion_id`、`resource_group`。

## 11. 灰度与回滚

新增 feature flags：

```yaml
cloud_sync_v2_enabled: false
cloud_sync_region_discovery_v2_enabled: false
cloud_sync_resource_dag_enabled: false
```

灰度顺序：

1. 先选择 1 个小型 AWS 账号；
2. 再选择 1 个 48 订阅组织账号，但只开启 Region discovery V2；
3. 对比旧/新任务数、API 调用数、同步时长和资源差异；
4. 资源差异连续 3 轮为零后开启公平调度；
5. 最后启用 Resource DAG。

回滚只需关闭 feature flag；保留 Activity Index 和 SyncRun 历史数据用于审计，不影响旧同步逻辑。

## 12. 预期收益

基于 48 个订阅场景的保守预估：

| 改造项 | 预期收益 |
|---|---:|
| OptInStatus + Region Activity Index | Region 任务减少 50%～90% |
| 轻量 MaxResults=1 探针 | discovery API 数据量降低 80% 以上 |
| 公平调度与分层并发 | 总同步时间降低约 40%～70% |
| Resource DAG | 活跃 Region 同步再降低约 30%～60% |
| 综合 | 预计 4～10 倍，最终以压测为准 |

## 13. 推荐决策

不建议直接合并当前未提交的 Region discover 方案。推荐将其保留为原型，按以下顺序重构：

1. 先修 Region 候选过滤和轻量探针；
2. 再落地 SyncRun 与公平调度；
3. 修正真实完成语义；
4. 最后做 Region 内 DAG 并行。

这样既能尽快减少无效 AWS API，又不会通过“提前标记完成”掩盖实际同步时长和一致性问题。
