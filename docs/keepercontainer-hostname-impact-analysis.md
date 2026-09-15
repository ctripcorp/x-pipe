# KeeperContainer 容器化改造 —— IP 依赖影响面分析

> 日期：2026-09-14 初版；2026-09-15 修订（checker 事实修正、补充已确认的设计决策方向）
> 背景：keepercontainer 迁移到容器（Cilium 网络）后无固定 IP，身份改为 pod hostname；hostname→动态 IP 注册到 ZK，console/metaserver/checker watch ZK 感知变更。
> 本文档梳理当前代码中所有依赖"确定 IP"的位置，评估改造影响。**只做影响面，不含设计方案。**

## 0. 核心结论

1. **IP 依赖点约 25 处，但"地址真相注入点"只有 3 个**：
   - `KeepercontainerMetaServiceImpl:27` —— 容器地址写入 DcMeta（改造后填 hostname）
   - `RedisMetaServiceImpl:82-88` —— keeper 地址写入 ShardMeta（改造后填 hostname，metaserver 透传与 checker sentinel 比对基准的源头）
   - keepercontainer 进程自注册 ZK（**新增**，hostname→实时 IP 映射的权威来源，IP 由容器自探测写入）
2. **下游消费链大多按 `keeperContainerId` 关联，不碰 IP**（DefaultXpipeMetaManager:675 按 id 匹配容器）→ 理论上只需在注入点用 ZK 实时 IP 覆盖 DB 值，metaserver 寻址、checker 健康检查自动获得正确地址。
3. **真正绕不开要改的**：console 分配/校验/落库逻辑、所有"直连容器"的调用方（console 3 处、checker 1 个客户端类、metaserver 间接经 meta）、keeper 自注册逻辑、DB schema。
4. **DB 层无硬障碍**：`KEEPERCONTAINER_TBL` 无 (ip,port) 唯一键（只有主键），唯一性靠代码校验；`REDIS_TBL` 有 `UNIQUE(redis_ip, redis_port, deleted_at)`，改造时需斟酌。

## 1. 详单：按链路分组

### A. 容器身份与登记（console）

| # | 位置 | 依赖 | 容器化后的问题 |
|---|------|------|----------------|
| A1 | `KEEPERCONTAINER_TBL` schema | `keepercontainer_ip`+`port` 即身份（无 DB 唯一键） | 需加 `host_name` 列；身份语义迁移 |
| A2 | `KeeperContainerServiceImpl.addKeeperContainerByInfoModel:319-326` | `keeperContainerAlreadyExists(ip,port)` 查重 + `checkIpAndPort` 校验 | 登记时容器可能未调度、无 IP；查重键要换 |
| A3 | `KeeperContainerServiceImpl:408` `updateKeeperContainerByInfoModel` | 按 `findByIpPort(ip,port)` 定位行 | **IP 漂移后更新入口失效**；需按 hostname/id 定位 |
| A4 | `KeeperContainerServiceImpl:635/676/723` | 组装 `KeeperContainerInfoModel.addr = HostPort(ip,port)` | 展示/返回模型要带 hostname |

### B. keeper 分配与落库（console 写路径）

| # | 位置 | 依赖 | 容器化后的问题 |
|---|------|------|----------------|
| B1 | `DefaultKeeperAdvancedService.fillInResult:175` | `keeperSelected.setHost(容器IP)` | **分配那一刻容器 IP 可能未知/将过期** |
| B2 | `DefaultKeeperAdvancedService.getIpAndPortsWithSameIpAsKC:223-236` | 按容器 IP 分组记账端口（`findAllRedisWithSameIP`） | 每 pod 独立 IP 后端口冲突消失，端口策略可简化（固定 6380？）；混布期两套逻辑 |
| B3 | `RedisServiceImpl.validateKeepers:644` | 硬校验 `keeper.ip == container.ip` | 校验维度改 hostname |
| B4 | `REDIS_TBL` `UNIQUE(redis_ip,redis_port,deleted_at)` | 唯一键 | IP 可变后唯一键语义需重审（host_name+port?） |
| B5 | 所有触发分配的入口（建 shard、补 keeper、迁移、rebalance、DR 绑定） | —— | **收敛点**：都走 B1-B3，流程本身不用改 ✓ |

### C. meta 组装与下发（console → metaserver/checker）

| # | 位置 | 依赖 | 说明 |
|---|------|------|------|
| C1 | `KeepercontainerMetaServiceImpl:27` | 容器 IP 从 DB 入 DcMeta | **注入点 ①**：enrich 时可用 ZK 实时值覆盖 |
| C2 | `RedisMetaServiceImpl:82-88` | keeper IP 从 DB 入 ShardMeta | **注入点 ②**：同上 |
| C3 | `AbstractMetaCache.getAllKeepers():378` | 返回 `Set<HostPort>`（ip:port） | checker sentinel 流程用它排除 keeper；IP 漂移期语义受影响 |
| C4 | `ConsoleCheckerController:107-126` getDividedMeta/long pull | checker 的 meta 来源 | 透传，C1/C2 enrich 后自动正确 ✓ |

### D. metaserver

| # | 位置 | 依赖 | 说明 |
|---|------|------|------|
| D1 | `DefaultDcMetaCache.getKeeperContainer:320` → `DefaultXpipeMetaManager:675` | **按 keeperContainerId 关联** | 关联逻辑不用改 ✓ |
| D2 | `DefaultKeeperContainerService` / `DefaultKeeperContainerServiceFactory` | 用容器 meta 的 (ip,port) 发 REST 命令 | 地址来自 C1；**IP 过期则命令打错目标**——走 console enrich 还是自己 watch ZK 是设计分歧点 |
| D3 | `DefaultKeeperStateController:95` | 同上（addKeeper/removeKeeper 寻址） | 同 D2 |
| D4 | `StrategyAwareKeeperActiveElectAlgorithm:76-79` | 容器 meta 仅用于取 diskType 判 TFS | 按 id 关联，免疫 ✓ |
| D5 | survive keepers / active 选举 / `DeadKeeperChecker` | ZK `/keepers/repl_*` 临时节点 | **天然动态，免疫** ✓ |
| D6 | `keeperActiveChanged` → console `updateKeeperStatus:97-112` | 按 `keeperContainerId` 翻转 active | 免疫 ✓ |
| D7 | `AllMetaServerService.getKeepersByKeeperContainer:22` | metaserver 间互查，按容器 meta | 地址源头仍是 C1 |
| D8 | `TfsKeeperUtils:29` / `TfsForceCloseDirCommand:60` | TFS 运维命令按容器地址 | 同 D2 |

### E. keeper / keepercontainer 进程自身

| # | 位置 | 依赖 | 说明 |
|---|------|------|------|
| E1 | `DefaultRedisKeeperServer.createLeaderElector():206-213` | ZK 选主节点的 `KeeperMeta.ip` 来自 console 下发的 meta，非自探测 | 改造后 `KeeperMeta` 增加 hostname 字段（`keepercontainer_id` 已有）；选主后 metaserver 回传 console，console 按 hostname + id 定位 DB 行翻转 active，**不依赖 IP** |
| E2 | `DefaultRedisKeeperServer:565` `b.bind(port)` | 绑 0.0.0.0 | 免疫 ✓ |
| E3 | keepercontainer 自注册 ZK（**新增组件**） | 与 keeper 同 JVM，已有 ZK client（leader latch）可复用 | 启动时注册到 hostname 路径下，节点数据带自探测的实时 IP；console/metaserver/checker watch 这套节点获得 hostname→IP 映射 |

### F. checker

| # | 位置 | 依赖 | 说明 |
|---|------|------|------|
| F1 | ~~keeper 实例健康检查~~ | **不存在**：checker 只遍历 `shardMeta.getRedises()`（DefaultHealthCheckInstanceManager:177、DefaultHealthChecker:146），不给 keeper 建健康检查实例 | 不在改造面 ✓ |
| F2 | `DefaultKeeperContainerService`（checker 模块定义，**调用方全部在 console 进程**，见 G 组） | `getKeeperDiskInfo(ip)`、`getAllKeepers(ip)`、`setKeeperContainerDiskIOLimit(ip,port)`、`resetKeeper(ip)`、`releaseRdb(ip)` | **IP 直连，且端口硬编码 8080**（:24/:41/:52/:63），与 DB 的 keepercontainer_port 脱节，现状就有的隐患 |
| F3 | sentinel 校验：`SentinelCollector4Keeper`、`SentinelInvalidSlaves`、`ResetSentinels:338` | 不直连 keeper，但按 meta 中 keeper 的 ip:port 做身份比对 | meta 改带 hostname 后，比对键需调整 |
| F4 | used info 上报 `ConsoleCheckerController:171 updateKeeperContainerUsedInfo` | `KeeperContainerUsedInfoModel` 按 IP 标识容器 | 上报主键要换 hostname，否则 console 聚合错乱 |
| F5 | master 指标采集 `RedisMsgCollector` → `CheckerConsoleService.reportKeeperContainerInfo` | 采集对象是 **one-way 集群的 redis master**（内存/offset/流量），`Map<HostPort, RedisMsg>` 的 HostPort 是 master 地址；类名与上报方法里的 "Keeper" 字样指**数据用途**（console 侧容器负载分析），非采集对象 | **不在改造面** ✓（名实辨析，勿误判） |

### G. console 管理面直连容器

| # | 位置 | 依赖 | 说明 |
|---|------|------|------|
| G1 | `KeeperContainerDiskInfoCollector:56-64` | 按 meta 容器 IP 调 `/keepers/disk` | console watch ZK 后解析 |
| G2 | `KeeperContainerDiskIOLimitDispatcher:64-74` | 按 (ip,port) 下发 `/keepers/limit/totalIO` | 同上 |
| G3 | `KeeperContainerServiceImpl.getAllKeepers(keeperIp):563` + portal `/keepercontainer/keepers/{ip}` | 按 IP 查容器实例列表 | 入参语义改 hostname |
| G4 | `KeeperContainerController:110` + `KeeperResetCommand:27`（resetKeeper）、`KeeperContainerController:120`（releaseRdb） | 按 IP 直连容器执行**写操作** | 对外契约不变（决策 9），入站 IP 经缓存反查 hostname，命令带 hostname 自校验（决策 8） |

### H. 运维 API 入口

| # | 位置 | 依赖 | 说明 |
|---|------|------|------|
| H1 | `api/KeeperContainerController:107` `/keepers/election/reset` | `KeeperRestElectionModel{ip, port, shardId}` | **契约不变**（决策 9）：外部只认 IP，console 入站自行 IP→hostname 反查 |
| H2 | `api/KeeperContainerController:117` `/keepers/release/rdb` | 同上 | 同上 |
| H3 | `KeeperAvailableZoneCheck`（console 内部检查） | 用容器 meta | 间接依赖，跟随 enrich |
| H4 | 迁移分析链：`DefaultKeeperContainerUsedInfoAnalyzer`/`DefaultKeeperContainerMigrationAnalyzer`/overload handlers | 按 IP 标识容器 | 主键换 hostname；迁移执行本身走 B 收敛点 ✓ |

## 2. 天然免疫清单（确认不用改）

- `updateKeeperStatus` 按 keeperContainerId 匹配（RedisMetaServiceImpl:105）
- keeper↔容器关联、meta 树导航全部按 id（DefaultXpipeMetaManager:675）
- survive 发现 / active 选举 / DeadKeeperChecker（ZK 临时节点语义）
- keeper bind 0.0.0.0，不依赖 meta 里的 IP
- KEEPERCONTAINER_TBL 无 (ip,port) DB 唯一键，加列/改身份无 DDL 障碍
- checker 不给 keeper 实例建健康检查（只遍历 `shardMeta.getRedises()`），ping/delay 等实例级检查与 keeper 无关
- checker 的 keeper 相关操作全部在本机房内（按机房分组部署、与 master 同机房），不存在跨机房寻址

## 3. 风险清单

- **R1（既有隐患）**：checker 直连容器端口硬编码 8080，与 DB keepercontainer_port 脱节；改造时应统一从登记信息取
- **R2**：分配 keeper 时容器 IP 未知 → redis_ip 写什么的取舍（空值/当时解析值/hostname 占位），以及 `UNIQUE(redis_ip,redis_port)` 约束冲突
- **R3**：IP 变更瞬间，命令可能打到「旧 IP 的新租户」（Cilium IP 复用场景）——**已定：命令携带 hostname、容器自校验拒绝**（决策 8）
- **R4（已定）**：metaserver 自 watch 本机房 ZK、经共享组件反查寻址（决策 1、5），不经 console enrich 中转
- **R5**：同 DC 物理机（IP 身份）与容器（hostname 身份）**长期并存**（决策 9，非过渡态），分配/校验/展示双轨逻辑常驻
- **R6**：appliercontainer 是同构孪生（AppliercontainerServiceImpl.findByIpPort 等），本次不在范围，但方案应可复用

## 4. 已确认的设计决策（2026-09-15）

1. **meta 下发不含 IP**：console 下发给 metaserver/checker 的 xpipeMeta/dcMeta 中，keeper 不带 IP、只带 hostname；metaserver 和 checker 自己 watch ZK，用 hostname 反查 IP。
2. **keeper 自注册与选主流程不变**：keeper 启动后自注册 ZK 选主，metaserver 监听并通知 console 落库 active keeper（现状流程保持）。
3. **redis_tbl 双字段共存**：唯一键增加 hostname 字段；所有查 keeper 的地方 IP 优先，IP 无值再用 hostname。
4. **keepercontainer 双字段共存**（本次新增确认）：
   - 存量容器以 IP 为身份，增量容器以 hostname 为身份，两者长期共存（混布期）。
   - `KEEPERCONTAINER_TBL` 增加 `keepercontainer_hostname` 列；`KeeperContainerMeta` 增加 `hostname` 属性。
   - 所有使用容器地址的逻辑统一规则：**IP 有值优先用 IP；IP 无值时用 hostname 反查 IP**（经 ZK 解析）。
5. **hostname→IP 映射的注册方式**（本次新增确认）：
   - keepercontainer 启动时自注册到 ZK 的 hostname 路径下（如 `/keepercontainers/{hostname}`），节点数据带自探测的实时 IP；xpipe 组件 watch 这套节点感知 IP 变更。
   - keeper 选主节点的 `KeeperMeta` 增加 hostname 字段（`keepercontainer_id` 已有），**不承担 IP 传递**；metaserver 选主后回传 console，console 按 hostname + id 定位 DB 行落库 active 状态。
   - hostname 获取：keepercontainer 启动时通过 `Foundation.net().getHostName()` 拿自身 hostname（foundation-service 现有 API，`AbstractChangePrimaryDcAction:105` 已有使用先例）。
   - watch + 缓存做成 **core 模块共享组件**：watch 本机房 ZK 的 `/keepercontainers` 路径，维护 `Map<hostname, HostPort>` 本地缓存，反查只读缓存、miss 时同步读 ZK 兜底；session 重连后全量重建缓存。console/metaserver/checker 直接注入复用（三者现状均已有 ZK 客户端：metaserver curator、checker 经 AbstractCheckerLeaderElector、console `Production.java:21` ZkClient Bean）。
6. **跨机房 IP 解析走 console 间转发**（本次新增确认）：
   - 每套 console 只 watch 本机房 ZK，不建跨机房 ZK 连接；需要其他机房的 hostname→IP 时，按 `consoleConfig.getConsoleDomains()` 找到目标 DC 的 console，发 HTTP 请求由对方查本地缓存解析返回。
   - 现状先例：`KeeperContainerUsedInfoMsgCollector:115-139` 跨机房拉取 redis master 指标，走 `/api/keepercontainer/redis/msg`（`KeeperContainerController:52`）。
   - 典型场景：portal/运维操作指向其他机房容器、跨机房负载数据聚合。均为秒级不敏感操作，可接受多一跳 HTTP 延迟。
7. **schema 与端口分配细则**（2026-09-15 A 组确认）：
   - `redis_ip` 保持非空：容器 keeper 落库不赋值，DB 默认 `0.0.0.0`；`host_name` 列定义 `NOT NULL DEFAULT ''`，**物理机行给空串而非 NULL**（保证新唯一键 `(redis_ip, redis_port, host_name, deleted_at)` 对物理机行的去重保护不失效——MySQL 唯一键视 NULL 互不相等）。
   - keepercontainer 管理面端口固定 8080，不变；checker 直连客户端 4 处硬编码 8080 的现状隐患（R1）随改造统一。
   - keeper 端口仍从 6380 起在同一容器内递增分配；**记账主键双轨**：分配到物理机容器（有 IP）按容器 IP 查已用端口，分配到 pod 容器按 hostname 查已用端口（`getIpAndPortsWithSameIpAsKC:223` 改造点）。
   - ZK 节点用**临时节点**（session 过期自动删除，pod 销毁映射自动失效；与 keeper 选主节点同模式）；session 重连后由共享组件自动重建；节点数据格式 `{"IpAddress": "***"}`。
   - **注册时机只有进程启动一处，不做运行时 IP 探测**：pod IP 与 pod 生命周期绑定，IP 变化必伴随 pod 重建与进程重启，启动注册即可覆盖；进程活着但 session 过期（网络抖动）的场景由"重连自动重建"兜底。
8. **直连命令防误伤**（2026-09-15 B 组确认）：
   - 所有按 IP 直连 keepercontainer 的 REST 命令（metaserver 的 addKeeper/removeKeeper，console 的 disk info / IO 限流 / getAllKeepers / resetKeeper / releaseRdb）在请求中携带**目标 hostname**；keepercontainer 收到后与 `Foundation.net().getHostName()` 比对，不匹配返回 4xx 拒绝。
   - 防护对象：IP 变更瞬间缓存未刷新 + Cilium IP 复用导致命令打到"旧 IP 的新租户"，写类命令（removeKeeper/限流/reset）打错即事故。
   - （B2 meta 字段兼容性：归为发布期事项，方案不考虑；B3 sentinel 比对 miss：不加容忍逻辑，checker 定时检测会自动 reset 自愈。）
9. **对外契约与范围**（2026-09-15 C 组确认）：
   - **运维 API 契约不变**：`/keepers/election/reset`、`/keepers/release/rdb` 等对外接口入参出参保持 IP。外部系统不知道 hostname——console 对外返回的都是已反查好的 IP，外部传入的也是 IP，**console 入站后自行做 IP→hostname 反查**（缓存中维护双向映射；查不到说明外部持有的是 pod 重建前的旧 IP，返回错误、外部重新拉取）。
   - **appliercontainer 不在本次范围**：applier 全部是物理机，方案不覆盖；设计上保证同构复用的可能性即可。
   - **不做灰度开关**：IP 与 hostname 双轨并存是**长期终态**而非过渡态，混布两套逻辑常驻。

## 5. 设计阶段待决策问题（记录，不在本文展开）

已定项见第 4 节（A 组 schema/端口/注册方式、B 组命令防误伤、C 组对外契约与范围均已定；B2 meta 兼容性归为发布期事项，B3 明确不加防抖逻辑、依赖 checker 自动 reset 自愈）。剩余未定：

无。设计决策已全部闭环，后续为发布期与实现期事项。

## 6. 工作量分级（粗估）

| 模块 | 改动性质 | 量级 |
|------|---------|------|
| console DB schema + DAL（REDIS_TBL、KEEPERCONTAINER_TBL 加 host_name，唯一键扩展） | 加列、查询调整（决策 3、4、7） | 小 |
| console 分配/校验/落库（B1-B3，含端口记账双轨） | 逻辑改造 + 双轨分支 | **中-大** |
| core：ZK watch + hostname↔IP 双向缓存共享组件 | **新增组件**，console/metaserver/checker 复用（决策 5） | 中 |
| keepercontainer 自注册（E3，仅启动时）+ 命令 hostname 自校验（决策 8） | **新增组件** | 中 |
| keeper 选主链路（E1：KeeperMeta 加 hostname，回传按 hostname+id 定位） | 字段扩展 + 匹配键调整 | 小-中 |
| console meta 注入点（C1-C2 改填 hostname） | 定点改造 | 小 |
| console 直连容器（G1-G4） | 寻址经缓存解析，端口统一，命令带 hostname | 小-中 |
| console 运维 API（H1-H2） | 契约不变，入站 IP 反查（决策 9） | 小 |
| metaserver 寻址（D2-D3） | 接入共享组件解析 + 命令带 hostname | 中 |
| checker sentinel 比对（F3/C3） | meta 解析后比对，比对键调整 | 小-中 |
| 负载聚合与迁移分析链（F4 上报主键、H4 聚合主键换 hostname） | 模型/主键换装 | 中 |
