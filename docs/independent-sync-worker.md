# 独立云账号同步 worker

## 实现范围

region 继续负责 API 权限校验、账号验证、子账号与 Region 发现。新的 `sync-worker` 进程消费 MySQL 持久化队列，按 provider-region 调用原有 `DoSync`；不启动 region 主节点定时任务，不监听 region API，不执行全部模型的 InitializeData。

首版使用现有任务 ID 表示同步运行，通过 `sync_queue_run_memberships` 关联作业。账号/订阅任务在规划结束后进入持久化等待阶段，主节点每 10 秒汇总一次结果。服务重启保留这个等待阶段。规划途中重启会记录规划失败，并等待已提交作业结束；不会宣称完整成功。

默认关闭独立路由，空白名单不路由任何账号。相同 provider-region、相同同步范围的活跃请求合并，不同范围保存为后续作业。作业只携带 ID、范围、发起人审计字段；worker 使用服务身份读取新鲜的账号和凭据，重新检查账号/订阅/Region 的启用状态和账号归属。

## 部署前提

- MySQL（测试使用 MySQL 8.0.46），InnoDB；生产连接需支持 parseTime，使用 UTC 时间解释。
- region 与 worker 使用同一业务数据库、同一 etcd 集群和锁前缀；两端 `lockman_method: etcd`。
- 相同代码版本，region 先完成常规模型迁移。独立队列表使用单独的显式迁移命令。
- worker 需要数据库、etcd、认证服务、region 内部回调地址及相应云 API 的网络访问。
- 保留原 region 配置里的认证、加密/密钥、代理、ESXi 等必要配置；不要为 worker 新建不同的账号加密密钥。

## 构建

在 Linux 构建环境中使用现有 Makefile 通用目标：

```sh
make cmd/sync-worker
make cmd/region
```

产物位于 `_output/bin/`。也可在对应平台执行 `go build -o ./_output/bin/sync-worker ./cmd/sync-worker`。

## 配置

复制当前 region 配置为 `/etc/yunion/sync-worker.conf`，保留连接信息；以下是新增/相关字段，不是完整配置：

```yaml
lockman_method: etcd
sync_worker_concurrency: 4
sync_worker_global_limit: 16
sync_worker_account_limit: 8
sync_worker_provider_limit: 2
sync_worker_lease_seconds: 120
sync_worker_heartbeat_seconds: 15
sync_worker_poll_seconds: 2
sync_worker_max_attempts: 3
sync_worker_retry_seconds: 60
sync_worker_shutdown_seconds: 90
```

所有副本保持相同的 global/account/provider 限制。这些限制在数据库中跨副本计数，增加副本不会自动提高全局上限。它们限制的是执行作业数量，不是云 API 每秒请求数；厂商 API 配额仍需单独观察与控制。

同步范围按资源组拆分：`core`、`services`、`storage`、`extended` 和 `provider`。无资源请求仍按兼容路径执行；深度同步才增加存储和扩展组。不同资源组使用不同的 provider-region etcd 锁，可并行执行；同一资源组仍保持串行，避免重复写同一组资源。SKU 只在所属资源组执行，避免每个拆分作业重复拉取 SKU。

region 增加：

```yaml
independent_cloud_sync: true
independent_cloud_sync_accounts:
  - cloud-account-id-to-canary
lockman_method: etcd
```

`"*"` 可路由全部账号，首轮灰度建议使用明确的账号 ID。配置按进程启动应用；修改路由前先排空旧执行器在途同步，不能混跑不同版本/不同路由配置的 region 实例。

## 首次启用

1. 先构建并安装新 region 和 worker 二进制，独立路由保持关闭。
2. 运行显式队列表迁移：

   ```sh
   /opt/yunion/bin/sync-worker --config /etc/yunion/sync-worker.conf --sync-worker-migrate
   ```

   此命令只执行独立队列表的幂等增量 DDL，然后退出；不启动云 API 同步。

3. 启动两个 worker，每个并发 4。可使用随包提供的 `yunion-sync-worker.service`，安装到 systemd 后启动；systemd 的停止期限应大于 `sync_worker_shutdown_seconds`。
4. 确认待灰度账号的旧同步已结束，启用 region 白名单并重启/滚动更新所有相关 region 实例。
5. 手动触发一个账号同步，验证 queued/running/retry/succeeded/failed 变化、资源数量和错误信息。

队列表缺失或锁配置不符合要求时，新入口拒绝启动。入队失败会返回/记录错误，绝不降级成并行的本地执行。

## 查看进度

沿用 cloudaccount 的对象权限，新增详情：

```text
GET /cloudaccounts/<account-id>/sync-queue
```

返回 `counts` 和最近 100 个作业。账号列表还包含 `sync_queue_counts`，资源同步状态使用持久化队列活跃数量；队列不可用时提供 `sync_queue_error`。counts 是此账号保留作业的累计统计，不等于最近一次运行统计。单次运行的结果聚合记录在原账号/订阅任务中。

也可在部署机器使用配置对应的数据库查询：

```sh
/opt/yunion/bin/sync-worker --config /etc/yunion/sync-worker.conf --sync-worker-inspect-account <account-id>
```

worker 日志包含 job ID、账号、订阅、Region、attempt 和请求人 ID。队列保留终态作业供排障；当前没有自动历史清理，应根据容量和审计要求制定保留策略。

## 失败与恢复

- 按数据库服务器 UTC 计算租约。领取、续租、完成均校验执行归属/版本，旧版本不能覆盖任务结果。
- 同一 provider-region 的执行再由 etcd mutex 保护。等待锁时继续续租，拿锁后再次验证队列归属。
- 续租失败或执行锁失效时 worker 立即非零退出。旧驱动不一定响应 context，因此不能只取消 goroutine 后继续运行进程。
- 正常 SIGTERM 停止领取，保持续租并排空任务；超过 drain 期限强制退出。其他 worker 在租约过期后重试。
- 执行失败和崩溃恢复都计入尝试次数；超过次数转为 failed，后续新的同步请求可创建新作业。
- 规划失败也会计入父任务失败；部分作业成功不会把整次运行标为成功。
- 云调用错误收集覆盖资源发现、账号级资源同步、主同步、负载均衡、缓存调度函数及资源比较错误计数。更深层只记 action log、完全吞掉的错误仍沿用原实现，需要按厂商逐步验证。
- 资源组拆分任务按稳定顺序获取多把 etcd 锁；请求自定义范围无法解析或包含未知能力时保守锁住全部 region 资源组，避免与拆分任务并发写入。

**互斥边界：** 现有云 API 和资源写入没有全面实施下游 fencing token。失联退出和 etcd 互斥覆盖正常故障恢复，但不能证明被长时间暂停的旧进程恢复时绝不会产生旧写入。禁止人工暂停后在租约过期时恢复旧 worker；生产灰度必须包含进程终止、网络隔离和资源一致性演练。需要严格暂停/分区语义的环境，应先补充写入层 fencing，不能把版本化任务状态当成资源写入 fencing。

## 回滚

1. 暂停目标账号的新同步触发，保持新 region 的进度汇总仍运行。
2. 等待账号 waiting/running/retry 都为 0；不能只等待 running 为 0。
3. SIGTERM 排空并停止相关 worker。
4. 关闭 region 的独立路由，再恢复原同步触发。

不删除队列表，不自动把仍运行的作业转为本地执行。紧急情况下必须先确认旧 worker 已终止，再处理积压；直接切换可能造成新旧执行器竞争。

## 验证

```sh
go test ./pkg/cloudcommon/db/taskman ./pkg/compute/models ./pkg/compute/tasks/cloudaccount
SYNCQUEUE_TEST_MYSQL_DSN='<disposable mysql DSN>' go test -race ./pkg/compute/syncqueue ./pkg/compute/syncworker
SYNCWORKER_TEST_ETCD_ENDPOINTS='<disposable etcd endpoint>' go test -race ./pkg/compute/syncworker
```

只向测试变量提供可丢弃实例：队列测试会清理其表，worker MySQL 测试会创建并删除独立测试数据库。测试覆盖重复请求、多个消费者、跨副本限制、过期归属、重试、单次运行关联、排空期间续租及锁撤销。真实云账号端到端和性能提升需在灰度环境测量。

本地代码验证还包括：

```sh
go test -count=1 ./pkg/compute/...
go test -race -count=1 ./pkg/compute/models ./pkg/compute/syncqueue ./pkg/compute/syncworker
go test -count=1 ./cmd/region ./cmd/sync-worker ./pkg/cloudcommon/db/taskman
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./cmd/region
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./cmd/sync-worker
git diff --check
```

以上不包含真实 MySQL/etcd、云厂商 API 或测试环境的集成验收；一小时目标必须在灰度环境按账号数、Region 数、资源量和厂商限流实测。
