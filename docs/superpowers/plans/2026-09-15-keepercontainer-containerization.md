# KeeperContainer 容器化改造实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** keepercontainer 迁移到容器后无固定 IP，身份改为 pod hostname；hostname→动态 IP 由 keepercontainer 自注册到 ZK，console/metaserver/checker watch ZK 反查 IP，存量物理机（IP 身份）与容器（hostname 身份）长期双轨并存。

**Architecture:** 全部设计决策已定稿于 `docs/keepercontainer-hostname-impact-analysis.md` 第 4 节（9 条决策），IP 依赖详单在该文档第 1 节（A-H 组编号）；checker/console 操作清单在 `docs/checker-console-keeper-operations.md`。本计划是该方案的任务拆分，行号引用与影响面文档一致。

**Tech Stack:** Java 8 + Spring + unidal DAL（代码生成）+ Curator(ZK) + JUnit；构建 `mvn`，单模块测试 `mvn test -pl <module> -Dtest=XxxTest`。

**依赖顺序（不可乱序）：** Phase 0（schema/模型）→ Phase 1（core 共享组件 + 容器自注册）→ Phase 2（console）→ Phase 3（metaserver）→ Phase 4（checker）。

---

## Phase 0：DB schema 与 meta 模型（地基）

### Task 1: REDIS_TBL / KEEPERCONTAINER_TBL 加 host_name 列

**Files:**
- Modify: `redis/dockerPackage/xpipe-console/sql/xpipedemodbtables.sql`（KEEPERCONTAINER_TBL :189、REDIS_TBL :161）
- Create: `redis/dockerPackage/xpipe-console/sql/alter_keepercontainer_hostname.sql`（线上变更脚本，交付 DBA）
- Modify: `redis/redis-console/src/main/resources/META-INF/dal/jdbc/meta-codegen.xml`（keepercontainer-tbl :201、redis-tbl 实体）
- Modify（重新生成）: `redis/redis-console/target/generated-sources/dal-jdbc/`（构建自动生成，不手改）

- [ ] **Step 1: 改 demo 建表 SQL**

`KEEPERCONTAINER_TBL` 增加列（放在 `keepercontainer_port` 之后）：

```sql
  `keepercontainer_host_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'keepercontainer pod hostname, empty for physical machine',
```

`REDIS_TBL` 增加列并把唯一键 `ip_port_deleted_at` 由 `(redis_ip, redis_port, deleted_at)` 改为：

```sql
  `host_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'keeper pod hostname, empty for physical machine',
  ...
  UNIQUE KEY `ip_port_hostname_deleted_at` (`redis_ip`, `redis_port`, `host_name`, `deleted_at`),
```

注意：物理机行 `host_name` 必须为空串而非 NULL（决策 7，MySQL 唯一键视 NULL 互不相等，会丢去重保护）。

- [ ] **Step 2: 写线上 alter 脚本**

```sql
ALTER TABLE `KEEPERCONTAINER_TBL`
  ADD COLUMN `keepercontainer_host_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'keepercontainer pod hostname, empty for physical machine' AFTER `keepercontainer_port`;

ALTER TABLE `REDIS_TBL`
  ADD COLUMN `host_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'keeper pod hostname, empty for physical machine' AFTER `redis_ip`,
  DROP INDEX `ip_port_deleted_at`,
  ADD UNIQUE KEY `ip_port_hostname_deleted_at` (`redis_ip`, `redis_port`, `host_name`, `deleted_at`);
```

- [ ] **Step 3: 改 DAL 定义**

`meta-codegen.xml` 的 `keepercontainer-tbl` 实体（:201）在 `keepercontainer-port` member 后加：

```xml
    <member name="keepercontainer-host-name" field="keepercontainer_host_name" value-type="String" length="128" nullable="false" />
```

`redis-tbl` 实体同样位置加：

```xml
    <member name="host-name" field="host_name" value-type="String" length="128" nullable="false" />
```

并把 redis-tbl 的 index 定义从 `ip_port_deleted_at` 更新为新唯一键（同文件 index 段，仿照现有 `<index>` 声明改 members）。

- [ ] **Step 4: 重新生成并验证**

Run: `mvn generate-sources -pl redis/redis-console`
Expected: 生成的 `KeepercontainerTbl.java` 有 `getKeepercontainerHostName()/setKeepercontainerHostName()`，`RedisTbl.java` 有 `getHostName()/setHostName()`；`mvn compile -pl redis/redis-console` 通过。

- [ ] **Step 5: Commit**

```bash
git add redis/dockerPackage/xpipe-console/sql/ redis/redis-console/src/main/resources/META-INF/dal/jdbc/meta-codegen.xml
git commit -m "feat(console): add host_name column to REDIS_TBL and KEEPERCONTAINER_TBL for keepercontainer containerization"
```

### Task 2: KeeperMeta / KeeperContainerMeta 加 hostname 属性

**Files:**
- Modify: `redis/redis-core/src/main/resources/META-INF/dal/model/keeper-model.xml`（keeper 实体 :84、keeperContainer 实体 :74）
- Modify（重新生成）: `keeper-codegen.xml` 与 `redis/redis-core/target/generated-sources/dal-model/`（构建自动生成，不手改）
- Test: `redis/redis-core/src/test/java/com/ctrip/xpipe/redis/core/entity/KeeperMetaTest.java`（新建）

- [ ] **Step 1: 改模型定义**

`keeperContainer` 实体（:74-76）加：

```xml
	<entity name="keeperContainer" implements="java.io.Serializable">
		<attribute name="id" value-type="Long" />
		<attribute name="hostname" value-type="String" />
	</entity>
```

`keeper` 实体（:84-93）加：

```xml
		<attribute name="hostname" value-type="String" />
```

- [ ] **Step 2: 重新生成**

Run: `mvn generate-sources -pl redis/redis-core`
Expected: `keeper-codegen.xml` 中 keeper/keeperContainer 实体多出 `<attribute name="hostname" value-type="String" />`；生成的 `KeeperMeta.java` 有 `m_hostname` + `getHostname()/setHostname()`，`KeeperContainerMeta.java` 同理；`mvn compile -pl redis/redis-core` 通过。

注：DcMeta 为 unidal XML 序列化，未知属性在老版本 consumer 端由 BaseEntity 忽略，新老混跑兼容（B2 归为发布期事项）。

- [ ] **Step 3: 写回归测试**

新建 `KeeperMetaTest`：

```java
package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.redis.core.AbstractRedisCoreTest;
import org.junit.Assert;
import org.junit.Test;

public class KeeperMetaTest extends AbstractRedisCoreTest {

    @Test
    public void testHostnameAttribute() {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setId("runid1");
        keeperMeta.setHostname("pod-abc-0");
        Assert.assertEquals("pod-abc-0", keeperMeta.getHostname());
    }

    @Test
    public void testKeeperContainerHostname() {
        KeeperContainerMeta containerMeta = new KeeperContainerMeta();
        containerMeta.setHostname("pod-abc-0");
        Assert.assertEquals("pod-abc-0", containerMeta.getHostname());
    }
}
```

若 `AbstractRedisCoreTest` 不存在，直接继承 `AbstractTest`（redis-core 测试基类，参考同模块现有 entity 测试的继承方式）。

- [ ] **Step 4: 跑测试**

Run: `mvn test -pl redis/redis-core -Dtest=KeeperMetaTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add redis/redis-core/src/main/resources/META-INF/dal/model/ redis/redis-core/src/test/java/com/ctrip/xpipe/redis/core/entity/KeeperMetaTest.java
git commit -m "feat(core): add hostname attribute to KeeperMeta and KeeperContainerMeta"
```

---

## Phase 1：core 共享组件与容器自注册

### Task 3: KeeperContainerAddressService（ZK watch + 双向缓存）

决策依据：影响面文档决策 5。各组件 watch 本机房 ZK，复用现有 ZkClient Bean（console `Production.java:21`、metaserver curator、checker 经 AbstractCheckerLeaderElector）。

**Files:**
- Create: `redis/redis-core/src/main/java/com/ctrip/xpipe/redis/core/keeper/container/KeeperContainerAddressService.java`
- Create: `redis/redis-core/src/main/java/com/ctrip/xpipe/redis/core/keeper/container/DefaultKeeperContainerAddressService.java`
- Create: `redis/redis-core/src/main/java/com/ctrip/xpipe/redis/core/keeper/container/KeeperContainerAddressInfo.java`
- Test: `redis/redis-core/src/test/java/com/ctrip/xpipe/redis/core/keeper/container/DefaultKeeperContainerAddressServiceTest.java`

- [ ] **Step 1: 写失败测试**

测试用 curator `TestingServer`（仓库已有使用先例：`redis/redis-meta/src/test/java/com/ctrip/xpipe/redis/meta/server/StartZk.java`）：

```java
package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.*;

public class DefaultKeeperContainerAddressServiceTest extends AbstractTest {

    private TestingServer zkServer;
    private ZkClient zkClient;
    private DefaultKeeperContainerAddressService service;

    @Before
    public void beforeDefaultKeeperContainerAddressServiceTest() throws Exception {
        zkServer = new TestingServer();
        zkClient = new DefaultZkClient();
        zkClient.setZkAddress(zkServer.getConnectString());
        service = new DefaultKeeperContainerAddressService(zkClient, FoundationService.DEFAULT.getDataCenter());
        service.initialize();
        service.start();
    }

    @After
    public void afterDefaultKeeperContainerAddressServiceTest() throws Exception {
        service.stop();
        zkServer.close();
    }

    @Test
    public void testResolveAfterRegister() throws Exception {
        CuratorFramework client = zkClient.get();
        client.create().creatingParentsIfNeeded()
                .forPath(ZKPaths.makePath(DefaultKeeperContainerAddressService.ROOT_PATH, "pod-a-0"),
                        "{\"IpAddress\": \"10.0.0.5\"}".getBytes());
        waitConditionUntilTimeOut(() -> service.resolve("pod-a-0") != null, 3000);

        Assert.assertEquals(new HostPort("10.0.0.5", 8080), service.resolve("pod-a-0"));
        Assert.assertEquals("pod-a-0", service.resolveHostname("10.0.0.5"));
    }

    @Test
    public void testIpChange() throws Exception {
        CuratorFramework client = zkClient.get();
        String path = ZKPaths.makePath(DefaultKeeperContainerAddressService.ROOT_PATH, "pod-a-0");
        client.create().creatingParentsIfNeeded().forPath(path, "{\"IpAddress\": \"10.0.0.5\"}".getBytes());
        waitConditionUntilTimeOut(() -> new HostPort("10.0.0.5", 8080).equals(service.resolve("pod-a-0")), 3000);

        client.setData().forPath(path, "{\"IpAddress\": \"10.0.0.9\"}".getBytes());
        waitConditionUntilTimeOut(() -> new HostPort("10.0.0.9", 8080).equals(service.resolve("pod-a-0")), 3000);

        Assert.assertNull(service.resolveHostname("10.0.0.5"));
        Assert.assertEquals("pod-a-0", service.resolveHostname("10.0.0.9"));
    }

    @Test
    public void testNodeRemoved() throws Exception {
        CuratorFramework client = zkClient.get();
        String path = ZKPaths.makePath(DefaultKeeperContainerAddressService.ROOT_PATH, "pod-a-0");
        client.create().creatingParentsIfNeeded().forPath(path, "{\"IpAddress\": \"10.0.0.5\"}".getBytes());
        waitConditionUntilTimeOut(() -> service.resolve("pod-a-0") != null, 3000);

        client.delete().forPath(path);
        waitConditionUntilTimeOut(() -> service.resolve("pod-a-0") == null, 3000);
        Assert.assertNull(service.resolveHostname("10.0.0.5"));
    }
}
```

注：`waitConditionUntilTimeOut` 为 `AbstractTest`（core 模块 `com.ctrip.xpipe.AbstractTest`）现有工具方法。`DefaultZkClient` 若构造函数/初始化方式与上面不同，按 `core/src/main/java/com/ctrip/xpipe/zk/impl/DefaultZkClient.java` 实际 API 调整装配。

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl redis/redis-core -Dtest=DefaultKeeperContainerAddressServiceTest`
Expected: 编译失败，`DefaultKeeperContainerAddressService` 不存在。

- [ ] **Step 3: 实现**

`KeeperContainerAddressInfo.java`（节点数据模型）：

```java
package com.ctrip.xpipe.redis.core.keeper.container;

public class KeeperContainerAddressInfo {

    public static final int KEEPER_CONTAINER_PORT = 8080;

    private String IpAddress;

    public String getIpAddress() {
        return IpAddress;
    }

    public void setIpAddress(String ipAddress) {
        IpAddress = ipAddress;
    }
}
```

`KeeperContainerAddressService.java`（接口）：

```java
package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.endpoint.HostPort;

/**
 * hostname <-> IP 映射，数据来源为本机房 ZK /keepercontainers 下的临时节点。
 * 所有"用 hostname 反查 IP"或"用 IP 反查 hostname"的入口只调本接口，不直接访问 ZK。
 */
public interface KeeperContainerAddressService extends Lifecycle {

    /** hostname -> HostPort(ip, 8080)；未注册或已下线返回 null */
    HostPort resolve(String hostname);

    /** IP -> hostname；查不到返回 null（物理机 IP 或过期 IP） */
    String resolveHostname(String ip);
}
```

`DefaultKeeperContainerAddressService.java`（实现，PathChildrenCache watch + 双 Map + 断连全量重建）：

```java
package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.utils.JsonUtils;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultKeeperContainerAddressService extends AbstractLifecycle
        implements KeeperContainerAddressService, PathChildrenCacheListener, ConnectionStateListener {

    public static final String ROOT_PATH = "/keepercontainers";

    private final ZkClient zkClient;
    private final String currentDc;
    private PathChildrenCache cache;

    private final Map<String, HostPort> hostnameToAddr = new ConcurrentHashMap<>();
    private final Map<String, String> ipToHostname = new ConcurrentHashMap<>();

    public DefaultKeeperContainerAddressService(ZkClient zkClient, String currentDc) {
        this.zkClient = zkClient;
        this.currentDc = currentDc;
    }

    @Override
    protected void doInitialize() throws Exception {
        cache = new PathChildrenCache(zkClient.get(), ROOT_PATH, true);
        cache.getListenable().addListener(this);
        zkClient.get().getConnectionStateListenable().addListener(this);
    }

    @Override
    protected void doStart() throws Exception {
        // START_POST_INITIALIZED_EVENT：buildInitial 后先触发 INITIALIZED 全量事件，再收增量
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        rebuild(cache.getCurrentData());
    }

    @Override
    protected void doStop() throws Exception {
        cache.close();
        hostnameToAddr.clear();
        ipToHostname.clear();
    }

    @Override
    public HostPort resolve(String hostname) {
        return hostnameToAddr.get(hostname);
    }

    @Override
    public String resolveHostname(String ip) {
        return ipToHostname.get(ip);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
        String path = event.getData() != null ? event.getData().getPath() : null;
        switch (event.getType()) {
            case CHILD_ADDED:
            case CHILD_UPDATED:
                put(hostnameOf(path), ipOf(event.getData()));
                break;
            case CHILD_REMOVED:
                remove(hostnameOf(event.getData().getPath()));
                break;
            case INITIALIZED:
            case CONNECTION_SUSPENDED:
            case CONNECTION_LOST:
            case CONNECTION_RECONNECTED:
            default:
                break;
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        // session 重连后 PathChildrenCache 会自动 reconcile 并补发事件；
        // 为防事件丢失，这里再做一次全量重建
        if (newState == ConnectionState.RECONNECTED) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    rebuild(cache.getCurrentData());
                }
            });
        }
    }

    private void rebuild(List<ChildData> all) {
        Map<String, HostPort> fresh = new ConcurrentHashMap<>();
        for (ChildData data : all) {
            String hostname = hostnameOf(data.getPath());
            String ip = ipOf(data);
            if (hostname != null && ip != null) {
                fresh.put(hostname, new HostPort(ip, KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT));
            }
        }
        hostnameToAddr.clear();
        hostnameToAddr.putAll(fresh);
        ipToHostname.clear();
        fresh.forEach((hostname, hostPort) -> ipToHostname.put(hostPort.getHost(), hostname));
    }

    private void put(String hostname, String ip) {
        if (hostname == null || ip == null) return;
        HostPort old = hostnameToAddr.put(hostname, new HostPort(ip, KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT));
        if (old != null) ipToHostname.remove(old.getHost(), hostname);
        ipToHostname.put(ip, hostname);
        logger.info("[put]{} -> {}", hostname, ip);
    }

    private void remove(String hostname) {
        if (hostname == null) return;
        HostPort old = hostnameToAddr.remove(hostname);
        if (old != null) ipToHostname.remove(old.getHost(), hostname);
        logger.info("[remove]{}", hostname);
    }

    private String hostnameOf(String path) {
        if (path == null || !path.startsWith(ROOT_PATH)) return null;
        return ZKPaths.getNodeFromPath(path);
    }

    private String ipOf(ChildData data) {
        if (data == null || data.getData() == null) return null;
        try {
            return JsonUtils.INSTANCE.fromJson(new String(data.getData()), KeeperContainerAddressInfo.class).getIpAddress();
        } catch (Exception e) {
            logger.error("[ipOf]{}", data.getPath(), e);
            return null;
        }
    }
}
```

实现前对照 `core` 模块现有工具类核实：`JsonUtils`（com.ctrip.xpipe.utils）、`AbstractLifecycle`、`AbstractExceptionLogTask` 的包路径与 API 签名，不一致处以仓库现状为准调整。

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl redis/redis-core -Dtest=DefaultKeeperContainerAddressServiceTest`
Expected: 3 个用例 PASS。

- [ ] **Step 5: Commit**

```bash
git add redis/redis-core/src/main/java/com/ctrip/xpipe/redis/core/keeper/container/ redis/redis-core/src/test/java/com/ctrip/xpipe/redis/core/keeper/container/
git commit -m "feat(core): add KeeperContainerAddressService for hostname<->ip mapping via ZK watch"
```

### Task 4: keepercontainer 自注册 + 命令 hostname 自校验

决策依据：影响面文档决策 5（仅启动时注册、临时节点、`{"IpAddress": "***"}`）与决策 8（命令带 hostname 自校验）。

**Files:**
- Create: `redis/redis-keeper/src/main/java/com/ctrip/xpipe/redis/keeper/container/KeeperContainerZkRegistrar.java`
- Create: `redis/redis-keeper/src/main/java/com/ctrip/xpipe/redis/keeper/container/HostNameVerifyFilter.java`
- Modify: `redis/redis-keeper/src/main/java/com/ctrip/xpipe/redis/keeper/spring/Production.java`（注册 Bean 与 Filter）
- Test: `redis/redis-keeper/src/test/java/com/ctrip/xpipe/redis/keeper/container/KeeperContainerZkRegistrarTest.java`

- [ ] **Step 1: 写失败测试（注册器核心逻辑）**

```java
@Test
public void testRegister() throws Exception {
    TestingServer zkServer = new TestingServer();
    ZkClient zkClient = new DefaultZkClient();
    zkClient.setZkAddress(zkServer.getConnectString());

    KeeperContainerZkRegistrar registrar = new KeeperContainerZkRegistrar(zkClient, "pod-a-0", "10.0.0.5");
    registrar.initialize();
    registrar.start();

    Stat stat = zkClient.get().checkExists()
            .forPath(ZKPaths.makePath(KeeperContainerZkRegistrar.ROOT_PATH, "pod-a-0"));
    Assert.assertNotNull(stat);
    Assert.assertEquals(CreateMode.EPHEMERAL, CreateMode.fromId(stat.getEphemeralOwner() != 0 ? 1 : 0) != null ? true : true);
    String data = new String(zkClient.get().getData()
            .forPath(ZKPaths.makePath(KeeperContainerZkRegistrar.ROOT_PATH, "pod-a-0")));
    Assert.assertTrue(data.contains("10.0.0.5"));

    registrar.stop();
    zkServer.close();
}
```

（断言简化为"节点存在且数据含 IP"；临时性可通过 `stat.getEphemeralOwner() != 0` 断言。）

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl redis/redis-keeper -Dtest=KeeperContainerZkRegistrarTest`
Expected: 编译失败。

- [ ] **Step 3: 实现注册器**

```java
package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.utils.JsonUtils;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import java.util.HashMap;
import java.util.Map;

/**
 * keepercontainer 启动时把自身 hostname->IP 注册到本机房 ZK（临时节点，仅此一个注册点）。
 * pod IP 与 pod 生命周期绑定，IP 变化必伴随进程重启，无需运行时探测。
 */
public class KeeperContainerZkRegistrar extends AbstractLifecycle implements ConnectionStateListener {

    public static final String ROOT_PATH = "/keepercontainers";

    private final ZkClient zkClient;
    private final String hostname;
    private final String ip;

    public KeeperContainerZkRegistrar(ZkClient zkClient, String hostname, String ip) {
        this.zkClient = zkClient;
        this.hostname = hostname;
        this.ip = ip;
    }

    @Override
    protected void doInitialize() throws Exception {
        zkClient.get().getConnectionStateListenable().addListener(this);
    }

    @Override
    protected void doStart() throws Exception {
        register();
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        // 临时节点随 session 过期被删，重连后必须重建
        if (newState == ConnectionState.RECONNECTED) {
            try {
                register();
            } catch (Exception e) {
                logger.error("[stateChanged][register]", e);
            }
        }
    }

    private void register() throws Exception {
        Map<String, String> data = new HashMap<>();
        data.put("IpAddress", ip);
        String path = ZKPaths.makePath(ROOT_PATH, hostname);
        CuratorFramework client = zkClient.get();
        if (client.checkExists().forPath(path) != null) {
            client.delete().forPath(path);
        }
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .forPath(path, JsonUtils.INSTANCE.toJson(data).getBytes());
        logger.info("[register]{} -> {}", path, ip);
    }
}
```

生产装配（`redis-keeper/spring/Production.java` 追加 Bean）：

```java
@Bean
public KeeperContainerZkRegistrar keeperContainerZkRegistrar(ZkClient zkClient) throws Exception {
    String hostname = Foundation.net().getHostName();
    String ip = InetAddress.getByName(hostname).getHostAddress();
    KeeperContainerZkRegistrar registrar = new KeeperContainerZkRegistrar(zkClient, hostname, ip);
    registrar.initialize();
    registrar.start();
    return registrar;
}
```

（若 redis-keeper 的 Production 已有生命周期管理器统一托管 Bean，按现有模式交给托管器，不手动 initialize/start。）

- [ ] **Step 4: 实现自校验 Filter**

```java
package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.framework.foundation.Foundation;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 决策 8：直连命令携带目标 hostname 时校验是否打对本容器，防 Cilium IP 复用误伤。
 * 不带 hostname 的旧调用方放行（物理机时代存量调用）。
 */
public class HostNameVerifyFilter implements Filter {

    public static final String HEADER_HOSTNAME = "X-Keeper-Container-Host";

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String targetHost = httpRequest.getHeader(HEADER_HOSTNAME);
        if (targetHost != null && !targetHost.isEmpty()) {
            String self = Foundation.net().getHostName();
            if (!targetHost.equalsIgnoreCase(self)) {
                ((HttpServletResponse) response).sendError(HttpServletResponse.SC_BAD_REQUEST,
                        "hostname mismatch: target=" + targetHost + ", self=" + self);
                return;
            }
        }
        chain.doFilter(request, response);
    }
}
```

在 keepercontainer 的 web 配置（redis-keeper 模块的 ServletContextInitializer/WebConfig，参照现有 Filter 注册方式）注册到 `/keepers/**`。

- [ ] **Step 5: 跑测试 + 编译**

Run: `mvn test -pl redis/redis-keeper -Dtest=KeeperContainerZkRegistrarTest && mvn compile -pl redis/redis-keeper`
Expected: PASS / BUILD SUCCESS。

- [ ] **Step 6: Commit**

```bash
git add redis/redis-keeper/src/main/java/com/ctrip/xpipe/redis/keeper/container/ redis/redis-keeper/src/main/java/com/ctrip/xpipe/redis/keeper/spring/Production.java redis/redis-keeper/src/test/java/com/ctrip/xpipe/redis/keeper/container/
git commit -m "feat(keeper): keepercontainer self-register hostname->ip to ZK; verify hostname on direct commands"
```

---

## Phase 2：console 接入

### Task 5: keeper 分配与校验双轨（影响面 B 组）

**Files:**
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/entity/KeeperBasicInfo.java`（加 hostname 字段）
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/service/impl/DefaultKeeperAdvancedService.java:163-260`（fillInResult / findAvailablePort / 端口记账）
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/service/impl/RedisServiceImpl.java`（insertKeepers 转换 :225-252、validateKeepers :612-645）
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/dao/RedisDao.java` + `meta-dal.xml`（新增 findAllRedisWithSameHostName 查询）
- Test: `redis/redis-console/src/test/java/com/ctrip/xpipe/redis/console/service/impl/DefaultKeeperAdvancedServiceTest.java`（现有，补容器用例）

- [ ] **Step 1: KeeperBasicInfo 加字段并补失败测试**

```java
// KeeperBasicInfo 增加
private String hostName; // 容器 keeper 的 pod hostname；物理机为空串
// + getter/setter
```

新增测试用例（容器容器分配）：

```java
@Test
public void testFillInResultForPodContainer() {
    KeepercontainerTbl pod = new KeepercontainerTbl()
            .setKeepercontainerId(1L)
            .setKeepercontainerIp("")           // 容器登记时无 IP
            .setKeepercontainerHostName("pod-a-0");
    List<KeepercontainerTbl> containers = Collections.singletonList(pod);
    List<KeeperBasicInfo> result = new LinkedList<>();

    // 端口记账按 hostname 查：mock redisService.findAllRedisWithSameHostName("pod-a-0") 返回 6380/6381 已占用
    when(redisService.findAllRedisWithSameHostName("pod-a-0")).thenReturn(...);

    fillInResult(containers, result, 6380, (host, port) -> true, 1);

    Assert.assertEquals("", result.get(0).getHost());        // redis_ip 落库不赋值 -> 0.0.0.0（DB 默认）
    Assert.assertEquals("pod-a-0", result.get(0).getHostName());
    Assert.assertEquals(6382, result.get(0).getPort());      // 从 6380 起跳过已占用
}
```

存量物理机用例（现有测试）必须保持绿。

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl redis/redis-console -Dtest=DefaultKeeperAdvancedServiceTest#testFillInResultForPodContainer`
Expected: 编译失败（getHostName/setHostName 不存在）。

- [ ] **Step 3: 实现分配双轨**

`fillInResult`（:163）改动：

```java
keeperSelected.setKeeperContainerId(keepercontainerTbl.getKeepercontainerId());
keeperSelected.setHost(keepercontainerTbl.getKeepercontainerIp());          // 物理机=真实IP；容器=空串/0.0.0.0
keeperSelected.setHostName(keepercontainerTbl.getKeepercontainerHostName()); // 物理机=空串；容器=hostname
```

`getIpAndPortsWithSameIpAsKC`（:223）改为按键记账，键 = IP 非空用 IP，否则用 hostname：

```java
private Map<String, Set<Integer>> getUsedPortsByContainerKey(List<KeepercontainerTbl> keeperCount) {
    Map<String, Set<Integer>> map = Maps.newHashMap();
    keeperCount.forEach(kc -> map.putIfAbsent(containerKey(kc), new HashSet<>()));
    // 对每个 key：含 IP 形态走 redisService.findAllRedisWithSameIP(ip)；
    // hostname 形态走 redisService.findAllRedisWithSameHostName(hostname)
    // 并发查询结构与现状一致（executor.submit + future.get）
}

private String containerKey(KeepercontainerTbl kc) {
    String ip = kc.getKeepercontainerIp();
    return (ip != null && !ip.isEmpty() && !"0.0.0.0".equals(ip))
            ? ip : kc.getKeepercontainerHostName();
}
```

`findAvailablePort` 与 `alreadySelected` 同步改用 containerKey；(ip, port) 判重语义变为 (key, port)。`keeperGood` BiPredicate 的入参传 key（调用处是端口探活逻辑，物理机传 IP 行为不变；容器 keeper 的探活本就无法用 IP 做，容器分支直接返回 true，由 metaserver 流程兜底——与"容器分配时 IP 未知"的事实一致）。

`RedisServiceImpl` 改动：

1. insertKeepers 转换处（:234-247）补 `keeper.setHostName(info.getHostName())`。
2. validateKeepers（:612）双轨：

```java
if (StringUtil.isEmpty(keeper.getHostName())) {
    if (!keeper.getRedisIp().equals(keepercontainer.getKeepercontainerIp())) {
        throw new BadRequestException("Keeper's ip should be equal to keepercontainer's ip");
    }
} else {
    if (!keeper.getHostName().equals(keepercontainer.getKeepercontainerHostName())) {
        throw new BadRequestException("Keeper's hostname should be equal to keepercontainer's hostname");
    }
}
```

3. 端口冲突查询（:653 findWithIpPort）双轨：有 hostname 走新查询 `findWithHostNamePort(hostName, port)`，否则维持 findWithIpPort（容器 keeper 的 ip 全是 0.0.0.0，老查询会把所有 pod 当成同一台机器误判冲突，必须换键）。
4. RedisDao 与 meta-dal.xml 加 `findAllRedisWithSameHostName` / `findWithHostNamePort` 两个查询（仿 findAllRedisWithSameIP / findWithIpPort，`WHERE host_name = ${host-name} AND deleted = 0`）。

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl redis/redis-console -Dtest=DefaultKeeperAdvancedServiceTest,RedisServiceImplTest`
Expected: 新用例 PASS，存量用例全绿。

- [ ] **Step 5: Commit**

```bash
git add redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/ redis/redis-console/src/main/resources/META-INF/dal/jdbc/meta-dal.xml
git commit -m "feat(console): dual-track keeper allocation and validation by ip/hostname"
```

### Task 6: 容器登记/更新/查询双轨（影响面 A 组）

**Files:**
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/service/impl/KeeperContainerServiceImpl.java`（addKeeperContainerByInfoModel :319、updateKeeperContainerByInfoModel :408、findByIpPort 定位、InfoModel 组装 :635/676/723）
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/model/KeeperContainerInfoModel.java`（加 hostname 字段）
- Modify: `redis/redis-console/src/main/resources/META-INF/dal/jdbc/meta-dal.xml` + `KeeperContainerDao`（findByHostName 查询）
- Test: `redis/redis-console/src/test/java/com/ctrip/xpipe/redis/console/service/impl/KeeperContainerServiceImplTest.java`（现有，补容器登记用例）

- [ ] **Step 1: 补失败测试**

```java
@Test
public void testAddPodKeeperContainer() {
    KeeperContainerInfoModel model = new KeeperContainerInfoModel();
    model.setDcName(dcNames[0]);
    model.setKeepercontainerHostName("pod-a-0");   // 容器登记只给 hostname，不给 IP
    model.setKeepercontainerPort(8080);
    keeperContainerService.addKeeperContainerByInfoModel(model);

    KeepercontainerTbl tbl = keeperContainerService.findByHostName("pod-a-0");
    Assert.assertNotNull(tbl);
    Assert.assertEquals("", tbl.getKeepercontainerIp());   // IP 空，等自注册/watch 解析
}
```

- [ ] **Step 2: 跑测试确认失败** → **Step 3: 实现**

- `keeperContainerAlreadyExists`：hostname 非空按 findByHostName 查重，否则维持 (ip, port) 查重。
- `checkIpAndPort`：hostname 登记路径跳过 IP 校验。
- `updateKeeperContainerByInfoModel`：hostname 非空按 hostname 定位行，否则维持 findByIpPort。
- 展示模型组装：InfoModel 增加 `keepercontainerHostName` 透传 DB 值。
- 规则统一入口：写一个私有方法 `isPodContainer(tbl)` = `!StringUtil.isEmpty(tbl.getKeepercontainerHostName())`，本任务及后续 console 任务的"物理机/容器"判断全部走它。

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl redis/redis-console -Dtest=KeeperContainerServiceImplTest`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/service/ redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/model/ redis/redis-console/src/main/resources/META-INF/dal/jdbc/meta-dal.xml redis/redis-console/src/test/java/com/ctrip/xpipe/redis/console/service/impl/KeeperContainerServiceImplTest.java
git commit -m "feat(console): keepercontainer registration dual-track by ip/hostname"
```

### Task 7: meta 注入点改填 hostname（影响面 C 组注入点 ①②）

**Files:**
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/service/meta/impl/KeepercontainerMetaServiceImpl.java:27`
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/service/meta/impl/RedisMetaServiceImpl.java:82-88`
- Test: 两文件对应的现有 `*Test`（补容器行用例）

- [ ] **Step 1: 补失败测试**

```java
@Test
public void testKeeperMetaWithHostname() {
    // RedisTbl: redisIp="0.0.0.0", hostName="pod-a-0", keepercontainerId=1
    KeeperMeta keeperMeta = redisMetaService.getKeeperMeta(shardMeta, redisTbl);
    Assert.assertEquals("0.0.0.0", keeperMeta.getIp());   // 决策 1：容器 keeper 不带真实 IP
    Assert.assertEquals("pod-a-0", keeperMeta.getHostname());
}
```

- [ ] **Step 2: 跑失败 → Step 3: 实现**

`KeepercontainerMetaServiceImpl:27`：

```java
keeperContainerMeta.setHostname(keeperContainer.getKeepercontainerHostName());
```

`RedisMetaServiceImpl:82-88` 组 KeeperMeta 处补：

```java
keeperMeta.setHostname(redisTbl.getHostName());
```

（ip 字段维持现状原样透传 DB 值：物理机为真实 IP，容器为 0.0.0.0；消费方按"hostname 非空走反查"处理，决策 1。）

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl redis/redis-console -Dtest=*MetaService*Test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/service/meta/
git commit -m "feat(console): fill hostname in KeeperContainerMeta and KeeperMeta"
```

### Task 8: console 直连容器寻址 + 对外 API 反查（影响面 F2/G/H 组）

**Files:**
- Modify: `redis/redis-checker/src/main/java/com/ctrip/xpipe/redis/checker/impl/DefaultKeeperContainerService.java`（5 个直连方法；该类定义在 checker 模块、运行在 console 进程）
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/controller/api/KeeperContainerController.java:107-125`（resetKeeper/releaseRdb 入站反查）
- Modify: console console 装配处（给 DefaultKeeperContainerService 注入 KeeperContainerAddressService）
- Test: `redis/redis-checker/src/test/java/com/ctrip/xpipe/redis/checker/impl/DefaultKeeperContainerServiceTest.java`

- [ ] **Step 1: 补失败测试**

```java
@Test
public void testResolveByHostname() {
    when(addressService.resolve("pod-a-0")).thenReturn(new HostPort("10.0.0.5", 8080));
    // 传入 hostname 形态，实际请求应打到 http://10.0.0.5:8080/keepers/disk，且带 X-Keeper-Container-Host: pod-a-0
}
```

- [ ] **Step 2: 跑失败 → Step 3: 实现**

`DefaultKeeperContainerService` 每个方法统一前置解析：

```java
private HostPort resolveAddress(String ipOrHostname, int port) {
    // 已有真实 IP（物理机路径）直接用；空/0.0.0.0/hostname 形态走 ZK 缓存反查
    if (isIpAddress(ipOrHostname) && !"0.0.0.0".equals(ipOrHostname)) {
        return new HostPort(ipOrHostname, port);
    }
    HostPort resolved = keeperContainerAddressService.resolve(ipOrHostname);
    if (resolved == null) {
        throw new IllegalStateException("no alive keepercontainer for hostname: " + ipOrHostname);
    }
    return resolved;
}
```

5 个方法的端口硬编码 8080 统一收敛到 `KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT`（顺带消 R1 隐患）；每个 HTTP 请求加 header `X-Keeper-Container-Host: <hostname>`（对应 Task 4 的自校验）。

`KeeperContainerController`（决策 9，契约不变）：

```java
// resetKeeper/releaseRdb 收到 ip 后：
// 1) keeperContainerService 按 ip 查容器行；若行是容器（hostname 非空），
//    用 keeperContainerAddressService.resolveHostname(ip) 校验 IP 仍有效；查不到说明外部持有旧 IP，返回 4xx。
// 2) 调 DefaultKeeperContainerService 时传 hostname，由其解析真实 IP 并带自校验 header。
```

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl redis/redis-checker -Dtest=DefaultKeeperContainerServiceTest`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add redis/redis-checker/src/main/java/com/ctrip/xpipe/redis/checker/impl/ redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/controller/api/KeeperContainerController.java
git commit -m "feat(console): resolve keepercontainer address via ZK cache; verify hostname on admin APIs"
```

### Task 9: 负载聚合主键换装 + updateKeeperStatus（影响面 F4/H4、E1 console 侧）

**Files:**
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/keeper/impl/KeeperContainerUsedInfoMsgCollector.java`
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/keeper/DefaultKeeperContainerUsedInfoAnalyzer.java` 及 overload handlers（聚合/展示主键）
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/model/KeeperContainerUsedInfoModel.java`（标识字段换 hostname，保留 id）
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/service/meta/impl/RedisMetaServiceImpl.java:97-112`（updateKeeperStatus）
- Test: 对应现有 `*Test`

- [ ] **Step 1: 补失败测试**——聚合按 hostname 归并：同名输入（checker 上报 + 磁盘采集）应聚合到同一 hostname 的 model；物理机容器按 IP 归并的存量用例保持绿。

- [ ] **Step 2: 跑失败 → Step 3: 实现**

- 聚合 key 统一：`hostname 非空 ? hostname : ip`（与 Task 6 的 isPodContainer 规则一致）。
- `KeeperContainerUsedInfoModel` 加 `hostName`，对外返回时容器行填充**反查好的实时 IP**（决策 9：外部只认 IP）。
- `updateKeeperStatus`（:97-112）维持按 keeperContainerId 匹配（天然免疫），仅检查并去掉任何对 keeper ip 的隐含依赖；metaserver 回传的 KeeperMeta 带 hostname 后，按 (hostname, keeperContainerId) 落库定位——若现状按 id 定位已无 ip 依赖，本步骤退化为回归测试保障。

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl redis/redis-console -Dtest=*UsedInfo*Test,*KeeperContainer*Test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/
git commit -m "feat(console): aggregate keepercontainer stats by hostname; return resolved ip to external"
```

### Task 10: console 装配 ZK watch 共享组件

**Files:**
- Modify: `redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/spring/Production.java`（已有 ZkClient Bean :21，追加 KeeperContainerAddressService Bean）

- [ ] **Step 1: 装配**

```java
@Bean
public KeeperContainerAddressService keeperContainerAddressService(ZkClient zkClient, ConsoleConfig consoleConfig) throws Exception {
    DefaultKeeperContainerAddressService service =
            new DefaultKeeperContainerAddressService(zkClient, FoundationService.DEFAULT.getDataCenter());
    service.initialize();
    service.start();
    return service;
}
```

- [ ] **Step 2: 编译 + console 启动冒烟**

Run: `mvn compile -pl redis/redis-console`，本地起 console 确认 Bean 装配无循环依赖。

- [ ] **Step 3: Commit**

```bash
git add redis/redis-console/src/main/java/com/ctrip/xpipe/redis/console/spring/Production.java
git commit -m "feat(console): wire KeeperContainerAddressService"
```

---

## Phase 3：metaserver 接入

### Task 11: 命令下发寻址（影响面 D2/D3）

**Files:**
- Modify: `redis/redis-meta/src/main/java/com/ctrip/xpipe/redis/meta/server/keeper/container/DefaultKeeperContainerService.java` 与 `DefaultKeeperContainerServiceFactory.java`
- Modify: `redis/redis-meta/src/main/java/com/ctrip/xpipe/redis/meta/server/spring/Production.java`（装配 addressService）
- Test: `redis/redis-meta/src/test/java/com/ctrip/xpipe/redis/meta/server/keeper/container/DefaultKeeperContainerServiceTest.java`

- [ ] **Step 1: 补失败测试**——KeeperContainerMeta 无 IP 有 hostname 时，命令 URL 应为反查后的 IP，且请求带 hostname 自校验头。

- [ ] **Step 2: 跑失败 → Step 3: 实现**

寻址逻辑（与 Task 8 console 同款规则，建议把 `resolveAddress` 公共小工具放 redis-core 复用）：

```java
HostPort address(KeeperContainerMeta containerMeta) {
    if (containerMeta.getIp() != null && !containerMeta.getIp().isEmpty()
            && !"0.0.0.0".equals(containerMeta.getIp())) {
        return new HostPort(containerMeta.getIp(), containerMeta.getPort());
    }
    HostPort resolved = addressService.resolve(containerMeta.getHostname());
    if (resolved == null) {
        throw new IllegalStateException("no alive keepercontainer: " + containerMeta.getHostname());
    }
    return resolved;
}
```

`DefaultKeeperStateController:95` 的 addKeeper/removeKeeper 走该寻址；命令 payload 中的 `KeeperMeta` 已带 hostname（Task 2 模型 + Task 7 console 注入自动带来），REST 请求补 hostname header。

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl redis/redis-meta -Dtest=*KeeperContainer*Test,*KeeperState*Test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add redis/redis-meta/src/main/java/com/ctrip/xpipe/redis/meta/server/
git commit -m "feat(meta): resolve keepercontainer address via ZK cache for keeper commands"
```

### Task 12: 选举与 active 回传链路回归（影响面 D5/D6/E1）

**Files:**
- Modify: `redis/redis-meta/src/main/java/com/ctrip/xpipe/redis/meta/server/keeper/elect/DefaultKeeperElectorManager.java`（仅当链路存在 ip 依赖时；预期只读）
- Modify: `redis/redis-keeper/src/main/java/com/ctrip/xpipe/redis/keeper/impl/DefaultRedisKeeperServer.java:206-213`（确认注册进 ZK 的 KeeperMeta 携带 hostname）
- Test: 选举链路现有集成测试（redis-meta keeper elect 包）

- [ ] **Step 1: 链路核查并写回归测试**

- keeper 注册 ZK 的 KeeperMeta 源头是 keeper 持有的 currentKeeperMeta（来自 addKeeper 命令），确认 hostname 字段随对象序列化进 ZK 节点数据；若存在手工拷贝字段的转换层（transferKeeperMeta 之类），补拷贝。
- 选举算法按 priority+ZK 顺序（StrategyAwareKeeperActiveElectAlgorithm），与地址无关——补一个"KeeperMeta 带 hostname 不影响选举结果"的用例。
- keeperActiveChanged 回传 console 的匹配按 keeperContainerId（天然免疫），补回归用例验证带 hostname 的 KeeperMeta 回传后 active 翻转正确。

- [ ] **Step 2: 跑测试确认通过**

Run: `mvn test -pl redis/redis-meta -Dtest=*Elect*Test`
Expected: PASS。

- [ ] **Step 3: Commit**

```bash
git add redis/redis-meta/ redis/redis-keeper/
git commit -m "test(meta): keeper election and active-feedback regression with hostname"
```

---

## Phase 4：checker 接入

### Task 13: sentinel 比对解析（影响面 F3/C3）

**Files:**
- Modify: `redis/redis-checker/src/main/java/com/ctrip/xpipe/redis/checker/healthcheck/actions/sentinel/collector/SentinelCollector4Keeper.java`
- Modify: `redis/redis-checker/src/main/java/com/ctrip/xpipe/redis/checker/healthcheck/actions/sentinel/collector/ResetSentinels.java:338`（排除集合的来源 `metaCache.getAllKeepers()`）
- Modify: `redis/redis-checker/src/main/java/com/ctrip/xpipe/redis/checker/impl/AbstractMetaCache.java:378`（getAllKeepers 解析容器 keeper 为实时 IP）
- Modify: checker 装配处（注入 KeeperContainerAddressService，watch 本机房 ZK）
- Test: `redis/redis-checker/src/test/java/com/ctrip/xpipe/redis/checker/healthcheck/actions/sentinel/collector/SentinelCollector4KeeperTest.java`

- [ ] **Step 1: 补失败测试**——meta 中 keeper 只有 hostname 时，比对基准经 addressService 解析后与 sentinel 视角 master 地址一致，不误告警。

- [ ] **Step 2: 跑失败 → Step 3: 实现**

统一在 `AbstractMetaCache.getAllKeepers()` 一处做解析，下游比对逻辑零改动：

```java
// keeper 有真实 IP：原样；hostname 非空：addressService.resolve(hostname) 得实时 IP；
// 解析失败（容器未注册/下线）：该 keeper 不进排除集合，并打 warn 日志（checker 会按现状流程告警/reset 自愈，决策 B3）
```

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl redis/redis-checker -Dtest=SentinelCollector4KeeperTest,*MetaCache*Test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add redis/redis-checker/src/main/java/com/ctrip/xpipe/redis/checker/
git commit -m "feat(checker): resolve keeper hostname via ZK cache in sentinel flows"
```

---

## 收尾：全量回归

- [ ] 全模块测试：`mvn test -pl redis/redis-core,redis/redis-console,redis/redis-checker,redis/redis-meta,redis/redis-keeper`
- [ ] 集成冒烟（本地 dockerPackage 环境）：
  1. 模拟容器 keepercontainer 启动 → ZK `/keepercontainers/{hostname}` 出现且 IP 正确
  2. console 建 shard 分配 keeper 到该容器 → redis_tbl 行 ip=0.0.0.0、host_name=hostname、端口 6380
  3. metaserver 拉到 meta → addKeeper 命中反查后的 IP，keeper 正常启动注册选主
  4. 改 ZK 节点 IP 模拟 pod 漂移 → 三组件缓存秒级刷新
  5. 用旧 IP 发 resetKeeper → 被 hostname 自校验拒绝
  6. 存量物理机路径全回归（登记/分配/meta/选举/迁移）

## Self-Review 结论

- 决策 1-9 全部有对应 Task：1(meta 无 IP 带 hostname)→T2/T7；2(选主流程不变)→T12；3(redis_tbl 双字段)→T1/T5；4(keepercontainer 双字段)→T1/T6/T7；5(ZK 注册+共享组件)→T3/T4；6(跨机房转发)→沿用现状通道不需改动（已有 `/api/keepercontainer/redis/msg`），仅聚合主键在 T9 换装；7(schema/端口/节点细节)→T1/T5/T4；8(命令自校验)→T4/T8/T11；9(对外契约不变)→T8/T9。
- 已知实现期注意点：DAL/模型代码生成物的提交策略跟随仓库现状；`resolveAddress` 双轨小工具在 T8 与 T11 复用，落 redis-core。
