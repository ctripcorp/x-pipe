# Checker 与 Console 对 Keeper / KeeperContainer 的操作清单

> 日期：2026-09-15
> 用途：keepercontainer 容器化改造的配套分析，梳理 checker 与 console 当前对 keeper 和 keepercontainer 的全部操作。
> 相关文档：keepercontainer-hostname-impact-analysis.md

## 0. 部署拓扑前提

- **checker 按机房分组部署**（GroupChecker 分组、GroupCheckerLeaderElector 组内选主），与 redis master 实例同机房，只负责 master 在本机房的分片。
- **console 每 DC 一套**，通过 `consoleConfig.getConsoleDomains()` 互知各 DC 的 console 域名；跨机房数据现状走 console 间 HTTP 转发（已有接口 `/api/keepercontainer/redis/msg`）。
- keeper 注册的 ZK 每 DC 一套；checker、console、metaserver 处理本机房 keeper 时，连的都是本机房这套 ZK。

## 1. checker 对 keeper / keepercontainer 的操作

### 1.1 前提：checker 不给 keeper 实例建健康检查

checker 只为 `shardMeta.getRedises()` 建健康检查实例（`DefaultHealthCheckInstanceManager:177`、`DefaultHealthChecker:146`），从不遍历 `getKeepers()`。ping、delay、redisconf、gtidgap 等实例级检查都与 keeper 无关。keeper 实例的存活感知由 metaserver 负责（ZK survive 节点 + DeadKeeperChecker）。

### 1.2 sentinel 维度校验（仅 one-way 集群，本机房内完成）

one-way 集群下 keeper 分片挂在 sentinel 监控下（keeper 伪装成 redis 主从）。checker 采集本机房 sentinel 的 hello 报文并校验：

| 类 | 职责 | 与 keeper 的关系 |
|---|---|---|
| `SentinelCollector4Keeper` | 校验 sentinel 视角的 master 是否为该分片当前 active keeper（`metaCache.findMasterInSameShard`）、monitor name 是否正确；不一致时告警或触发 reset（SentinelCollectorAction） | 比对基准是 meta 中 keeper 的 ip:port，不直连 keeper |
| `SentinelInvalidSlaves` | 清理 sentinel 中已无效的 slave 记录 | 涉及 keeper 地址的排除判断 |
| `ResetSentinels:338` | 执行 sentinel reset 时用 `metaCache.getAllKeepers()` 取得 keeper 地址集合做排除 | 依赖 keeper 的 ip:port 集合 |

### 1.3 redis master 指标采集上报（服务于 keeper 负载分析）

注意：这一组操作的对象是 **redis master 实例，不是 keeper**。数据的用途是 console 侧的 keepercontainer 负载分析（keeper 负载取决于其承载分片的数据量与写入速率，只能从分片 master 采到）。

| 类 | 职责 |
|---|---|
| `RedisMsgCollector`（redisinfo action） | 只处理 one-way 集群的 master 实例（`onAction:46` 明确过滤），采集内存、masterReplOffset、输入流量；开关 `isKeeperMsgCollectOn` |
| `RedisMsgReporter`（GroupCheckerLeaderAware） | 组内 leader 定时将 `Map<HostPort, RedisMsg>` 上报 console（`CheckerConsoleService.reportKeeperContainerInfo`）；HostPort 是 redis master 的地址 |
| console 接收端 | `ConsoleCheckerController:171 updateKeeperContainerUsedInfo` |

### 1.4 直连 keepercontainer 的客户端类（定义在 checker 模块，运行在 console 进程）

`DefaultKeeperContainerService`（实现 `KeeperContainerCheckerService`）定义了 5 个直连容器的 REST 操作，但调用方全部在 console 进程（见 2.4）：

| 方法 | 目标路径 | 端口来源 |
|---|---|---|
| `getKeeperDiskInfo(ip)` | GET `/keepers/disk` | 硬编码 8080 |
| `getAllKeepers(ip)` | GET `/keepers` | 硬编码 8080 |
| `setKeeperContainerDiskIOLimit(ip, port, limit)` | POST `/keepers/limit/totalIO` | 调用方传入（meta 中的容器端口） |
| `resetKeeper(ip, replId)` | POST `/keepers/election/reset` | 硬编码 8080 |
| `releaseRdb(ip, port, replId)` | DELETE `/keepers/rdb/release` | 硬编码 8080 |

### 1.5 checker 明确不做的事

- 不对 keeper 实例做任何实例级健康检查
- 不负责 keeper failover（metaserver 与 ZK 选举的职责）
- 不负责 keepercontainer 调度（console 的分配逻辑）
- 不跨机房操作：sentinel 校验与 master 采集都在本机房完成

## 2. console 对 keeper / keepercontainer 的操作

### 2.1 keepercontainer 元数据管理

| 操作 | 位置 | 说明 |
|---|---|---|
| 登记 | `KeeperContainerServiceImpl.addKeeperContainerByInfoModel:319-326` | 按 (ip, port) 查重 + checkIpAndPort 校验 |
| 更新 | `KeeperContainerServiceImpl:408` | 按 `findByIpPort(ip, port)` 定位行 |
| 查询/展示 | `KeeperContainerServiceImpl:635/676/723` | 组装 `KeeperContainerInfoModel`，地址为 HostPort(ip, port) |
| portal 接口 | `KeeperContainerInfoController` | 容器增删改查、按 DC/AZ/org/tag 筛选 |

### 2.2 keeper 分配与落库

| 操作 | 位置 | 说明 |
|---|---|---|
| 选容器 | `DefaultKeeperAdvancedService.findBestKeepers:62` | BU/org/公共池三级降级、tag 匹配、负载升序、磁盘类型过滤、AZ 打散 |
| 分配地址 | `fillInResult:175` + `findAvailablePort:185` | keeper 的 host 取容器 IP；端口从 6380 起按容器 IP 记账（`getIpAndPortsWithSameIpAsKC:223`） |
| 校验 | `RedisServiceImpl.validateKeepers:644` | 硬校验 keeper.ip == 容器 IP |
| 落库 | `RedisServiceImpl.insertKeepers` → `RedisDao.createRedisesBatch` | 生成 runId 写入 redis_tbl |
| active 回写 | `RedisMetaServiceImpl.updateKeeperStatus:97-112` | metaserver 回传，按 keeperContainerId 匹配翻转，与 IP 无关 |

所有触发入口（建 shard、补 keeper、迁移、rebalance、DR 绑定）都收敛到上述分配逻辑。

### 2.3 meta 组装与下发

| 操作 | 位置 | 说明 |
|---|---|---|
| 容器信息入 DcMeta | `KeepercontainerMetaServiceImpl:27` | 容器 IP 取自 DB |
| keeper 信息入 ShardMeta | `RedisMetaServiceImpl:82-88` | keeper IP 取自 DB |
| 对 metaserver | `ConsoleController` | getDcMeta / getClusterMeta / getShardMeta |
| 对 checker | `ConsoleCheckerController:107-126` | getDividedMeta（含 long pull） |
| keeper 地址集合 | `AbstractMetaCache.getAllKeepers:378` | HostPort 集合，checker sentinel 流程排除 keeper 用 |

### 2.4 直连 keepercontainer（管理面，经 1.4 的客户端类）

| 操作 | 调用方 | 用途 |
|---|---|---|
| getKeeperDiskInfo | `KeeperContainerDiskInfoCollector:56-64` | 定时采集本 DC 各容器磁盘信息 |
| setKeeperContainerDiskIOLimit | `KeeperContainerDiskIOLimitDispatcher:64-74` | 按配置下发 IO 限流 |
| getAllKeepers | `KeeperContainerServiceImpl:563` | portal 按 IP 查询容器内 keeper 实例 |
| resetKeeper | `KeeperContainerController:110`、`KeeperResetCommand:27` | 运维接口、迁移流程 |
| releaseRdb | `KeeperContainerController:120` | 运维接口 |

### 2.5 容器负载分析与迁移

| 环节 | 位置 | 说明 |
|---|---|---|
| 数据聚合 | `KeeperContainerUsedInfoMsgCollector` | 合并 checker 上报的 master 指标与本地采集的磁盘信息 |
| 跨机房获取 | `KeeperContainerUsedInfoMsgCollector:115-139` | 本 DC 走本地缓存；其他 DC 调对应 console 的 `/api/keepercontainer/redis/msg`；另有 extraSyncDC 机制 |
| 分析 | `DefaultKeeperContainerUsedInfoAnalyzer`、`DefaultKeeperContainerMigrationAnalyzer` | 判断是否过载、生成迁移计划 |
| 过载处理 | 5 个 handler（KeeperContainerPairOverloadHandler、KeeperContainerOverloadHandler、KeeperDataOverloadHandler、HostDiskOverloadHandler、HostActiveHandler） | |
| 迁移执行 | `DefaultKeeperContainerMigrationService` | 删旧 keeper、走 2.2 的分配逻辑建新 keeper |

### 2.6 运维与 portal API

| 接口 | 位置 | 说明 |
|---|---|---|
| POST `/keepers/election/reset` | `api/KeeperContainerController:107` | 参数 KeeperRestElectionModel{ip, port, shardId}，外部系统调用 |
| POST `/keepers/release/rdb` | `api/KeeperContainerController:117` | 同上 |
| GET `/keepercontainer/redis/msg` | `api/KeeperContainerController:52` | 跨 console 转发的服务端 |
| GET `/keepercontainer/keepers/{ip}` | `consoleportal/KeeperContainerInfoController:143` | portal 按 IP 查实例 |
| 迁移流程控制 | `consoleportal/KeeperContainerInfoController:86-126` | 过载列表、迁移开始/终止、进度查询 |

### 2.7 console 内嵌检查

- `KeeperAvailableZoneCheck`：检查 keeper 的 AZ 分布（用容器 meta，间接依赖 IP）
- `KeeperContainerDiskIOLimitDispatcher`：见 2.4

## 3. 对容器化改造的影响汇总

| 操作组 | 当前寻址/标识方式 | 改造影响 |
|---|---|---|
| 1.2 sentinel 校验 | 按 meta 中 keeper 的 ip:port 比对 | meta 改带 hostname 后，需先解析再比对，或改比对键 |
| 1.3 master 指标采集上报 | HostPort 为 redis master 地址 | master 是固定 IP 的真 redis，不在改造面 |
| 1.4 / 2.4 直连容器 | IP 直连，4 个方法端口硬编码 8080 | 改为 hostname 经 ZK 解析；端口统一从登记信息取 |
| 2.1 容器元数据 | (ip, port) 即身份 | 加 host_name，登记/更新/查重全部改键 |
| 2.2 keeper 分配落库 | host = 容器 IP，端口按 IP 记账 | 分配逻辑与校验规则改造，详见影响面文档 B 组 |
| 2.3 meta 下发 | IP 从 DB 原样吐出 | 两个注入点改填 hostname |
| 2.5 负载分析迁移 | 按 IP 标识容器做聚合 | 聚合主键换 hostname；迁移执行复用 2.2 收敛点 |
| 2.6 运维 API | 参数带 ip/port | 接口契约调整，需协调外部调用方 |
