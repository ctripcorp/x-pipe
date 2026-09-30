package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultCurrentDcKeeperContainerAddressServiceTest extends AbstractTest {

    private static final String POD_A = "keepercontainer-pod-a-0";
    private static final String POD_B = "keepercontainer-pod-b-0";

    private TestingServer zkServer;
    private DefaultZkClient zkClient;
    private DefaultCurrentDcKeeperContainerAddressService service;
    /** hostname -> 最近一次 register 创建的 latch 子节点路径，供 setData / delete 定位。 */
    private final Map<String, String> latchPaths = new HashMap<>();
    /** 可拨时钟的实例，由 {@link #startClocked} 创建、@After 关闭。 */
    private ClockedAddressService clocked;

    @Before
    public void beforeDefaultCurrentDcKeeperContainerAddressServiceTest() throws Exception {
        zkServer = new TestingServer();
        zkClient = new DefaultZkClient(new DefaultZkConfig(zkServer.getConnectString()));
        zkClient.initialize();
        zkClient.start();

        service = new DefaultCurrentDcKeeperContainerAddressService(zkClient);
        service.initialize();
        service.start();
    }

    @After
    public void afterDefaultCurrentDcKeeperContainerAddressServiceTest() throws Exception {
        try {
            if (clocked != null) {
                clocked.stop();
            }
            service.stop();
            zkClient.stop();
        } finally {
            zkServer.close();
        }
    }

    @Test
    public void testResolveAfterRegister() throws Exception {
        register(POD_A, "10.0.0.5");

        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);

        // 未注册的 hostname 应解析不到，而非返回脏值
        Assert.assertNull(service.getIpByHostname("not-registered"));
    }

    @Test
    public void testIpChange() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);

        setData(POD_A, "10.0.0.9");
        waitConditionUntilTimeOut(() -> "10.0.0.9".equals(service.getIpByHostname(POD_A)), 3000);
    }

    /**
     * pod 重建后 hostname 不变，真实流程是旧 pod 的 latch 子节点随 session 消失、
     * 新 pod 在同一 hostname 父节点下创建新的 latch 子节点，事件序列是 NODE_DELETED 后跟 NODE_CREATED，
     * 而不是单个 NODE_CHANGED。两者的最终状态都必须收敛到新 IP。
     */
    @Test
    public void testPodRecreateKeepsHostnameAndSwitchesIp() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> POD_A.equals(service.getHostnameByIp("10.0.0.5")), 3000);

        delete(POD_A);
        // 节点消失到新节点建立之间：正向由 tombstone 给出 last-known IP，误发命令由容器端 hostname 校验挡住；
        // 反向只认存活节点，旧 IP 立即失去归属
        waitConditionUntilTimeOut(() -> null == service.getHostnameByIp("10.0.0.5"), 3000);
        Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_A));

        register(POD_A, "10.0.0.9");
        waitConditionUntilTimeOut(() -> "10.0.0.9".equals(service.getIpByHostname(POD_A)), 3000);
        Assert.assertEquals(POD_A, service.getHostnameByIp("10.0.0.9"));
        Assert.assertTrue("hostname 恢复存活后 tombstone 应被丢弃", service.tombstoneHostnames().isEmpty());
    }

    @Test
    public void testNodeRemoved() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> POD_A.equals(service.getHostnameByIp("10.0.0.5")), 3000);

        delete(POD_A);

        waitConditionUntilTimeOut(() -> null == service.getHostnameByIp("10.0.0.5"), 3000);
        Assert.assertEquals("失去存活映射后转为 tombstone", "10.0.0.5", service.getIpByHostname(POD_A));
        Assert.assertEquals(Collections.singleton(POD_A), service.tombstoneHostnames());
        // hostname 父节点是容器节点，要等 ZK 定期回收；它还在时也不能产生存活映射
        Assert.assertNotNull(zkClient.get().checkExists().forPath(path(POD_A)));
    }

    /**
     * hostname 父节点存在但下面没有 latch 子节点（容器节点尚未被 ZK 回收）时不产生映射，
     * 也不影响其余 hostname。
     */
    @Test
    public void testHostnameWithoutLatchNodesHasNoMapping() throws Exception {
        zkClient.get().create().creatingParentContainersIfNeeded().withMode(CreateMode.CONTAINER)
                .forPath(path(POD_A));
        register(POD_B, "10.0.0.6");

        waitConditionUntilTimeOut(() -> "10.0.0.6".equals(service.getIpByHostname(POD_B)), 3000);
        service.rebuild();
        Assert.assertEquals(Collections.singletonMap(POD_B, "10.0.0.6"), service.getAllHostnameToIp());
    }

    @Test
    public void testMultipleContainers() throws Exception {
        register(POD_A, "10.0.0.5");
        register(POD_B, "10.0.0.6");

        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A))
                && "10.0.0.6".equals(service.getIpByHostname(POD_B)), 3000);

        // 单个容器下线不影响其他容器
        delete(POD_A);
        waitConditionUntilTimeOut(() -> null == service.getHostnameByIp("10.0.0.5"), 3000);
        Assert.assertEquals("10.0.0.6", service.getIpByHostname(POD_B));
        Assert.assertEquals(POD_B, service.getHostnameByIp("10.0.0.6"));
        Assert.assertEquals(Collections.singleton(POD_A), service.tombstoneHostnames());
    }

    @Test
    public void testRebuildIsIdempotent() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);

        // 定时兜底路径：即使 watch 事件全部丢失，全量重建也要能纠正缓存
        service.rebuild();
        Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_A));
    }

    @Test
    public void testMalformedNodeDataIgnored() throws Exception {
        registerRaw("bad-pod", "not-a-json".getBytes());
        register(POD_A, "10.0.0.5");

        // 坏节点不能拖垮整批解析
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);
        Assert.assertNull(service.getIpByHostname("bad-pod"));
    }

    /**
     * 首节点 data 坏时该 hostname 不产生映射，不回落到次节点（次节点是更晚注册、可能未就绪的 pod），
     * 且不影响其余 hostname。
     */
    @Test
    public void testMalformedFirstLatchNodeDoesNotFallBackToNext() throws Exception {
        registerRaw(POD_A, "not-a-json".getBytes());
        register(POD_A, "10.0.0.9");
        register(POD_B, "10.0.0.6");

        waitConditionUntilTimeOut(() -> "10.0.0.6".equals(service.getIpByHostname(POD_B)), 3000);
        waitConditionUntilTimeOut(() -> latchNodesInSnapshot(service, POD_A) == 2, 3000);
        service.rebuild();
        Assert.assertNull(service.getIpByHostname(POD_A));
        Assert.assertEquals(Collections.singletonMap(POD_B, "10.0.0.6"), service.getAllHostnameToIp());
        // 从未有过存活正向映射，不产生 tombstone
        Assert.assertTrue(service.tombstoneHostnames().isEmpty());
        // 反向覆盖全部存活子节点：次节点 data 完好，其 IP 仍归属 POD_A
        Assert.assertEquals(POD_A, service.getHostnameByIp("10.0.0.9"));
    }

    /**
     * 同一 hostname 下多个 pod 并存（滚动替换）：取 seq 最小的首节点；首节点消失后落到次节点。
     */
    @Test
    public void testMultipleLatchNodesResolveToFirst() throws Exception {
        String first = register(POD_A, "10.0.0.5");
        register(POD_A, "10.0.0.9");

        waitConditionUntilTimeOut(() -> latchNodesInSnapshot(service, POD_A) == 2, 3000);
        service.rebuild();
        Assert.assertEquals("取首节点，不因新 pod 注册而切换", "10.0.0.5", service.getIpByHostname(POD_A));
        // 反向覆盖全部存活子节点，不止首节点
        Assert.assertEquals(POD_A, service.getHostnameByIp("10.0.0.5"));
        Assert.assertEquals(POD_A, service.getHostnameByIp("10.0.0.9"));

        zkClient.get().delete().forPath(first);
        waitConditionUntilTimeOut(() -> "10.0.0.9".equals(service.getIpByHostname(POD_A)), 3000);
        Assert.assertNull("已消失 pod 的 IP 不再归属", service.getHostnameByIp("10.0.0.5"));
        Assert.assertTrue("hostname 仍有存活映射，首节点换人不产生 tombstone", service.tombstoneHostnames().isEmpty());
    }

    /**
     * 排序只看 latch- 之后的 seq，不看 protection 前缀。构造「全名字典序与 seq 顺序相反」的两个节点：
     * seq 小的节点前缀是 _c_zzz，seq 大的是 _c_aaa，按全名排会误取后者。
     */
    @Test
    public void testLatchNodesSortedBySequenceNotFullName() throws Exception {
        createSequential(POD_A, "_c_zzz-latch-", "10.0.0.5");
        createSequential(POD_A, "_c_aaa-latch-", "10.0.0.9");

        waitConditionUntilTimeOut(() -> latchNodesInSnapshot(service, POD_A) == 2, 3000);
        service.rebuild();
        Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_A));
    }

    /**
     * hostname 父节点下名字不含 latch- 的子节点不参与排序。名为 "0" 的节点按全名比较会排在
     * "0000000001" 之前，不过滤就会被误取为首节点。
     */
    @Test
    public void testNonLatchChildIgnored() throws Exception {
        register(POD_A, "10.0.0.5");
        zkClient.get().create().withMode(CreateMode.EPHEMERAL)
                .forPath(ZKPaths.makePath(path(POD_A), "0"),
                        Codec.DEFAULT.encodeAsBytes(new KeeperContainerAddressInfo("9.9.9.9")));

        waitConditionUntilTimeOut(() -> service.currentChildren().stream()
                .anyMatch(child -> child.getPath().endsWith(POD_A + "/0")), 3000);
        service.rebuild();
        Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_A));
    }

    /**
     * D-I4：旧的一层结构（hostname 节点自身带地址 data、无 latch 子节点）不再解析。
     */
    @Test
    public void testLegacyFlatNodeIsNotParsed() throws Exception {
        zkClient.get().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .forPath(path("legacy-pod"), Codec.DEFAULT.encodeAsBytes(new KeeperContainerAddressInfo("10.0.0.7")));
        register(POD_A, "10.0.0.5");

        waitConditionUntilTimeOut(() -> service.currentChildren().stream()
                .anyMatch(child -> path("legacy-pod").equals(child.getPath())), 3000);
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);
        service.rebuild();
        Assert.assertEquals(Collections.singletonMap(POD_A, "10.0.0.5"), service.getAllHostnameToIp());
    }

    /**
     * 用真实 LeaderLatch 注册，固定消费侧与 LeaderLatch 节点格式（protection 前缀、latch- 名、id 作 data）的契约。
     */
    @Test
    public void testResolvesRealLeaderLatchNodes() throws Exception {
        LeaderLatch older = new LeaderLatch(zkClient.get(), path(POD_A),
                Codec.DEFAULT.encode(new KeeperContainerAddressInfo("10.0.0.5")));
        LeaderLatch newer = new LeaderLatch(zkClient.get(), path(POD_A),
                Codec.DEFAULT.encode(new KeeperContainerAddressInfo("10.0.0.9")));
        try {
            older.start();
            // 等 older 的节点建好再启动 newer，保证 seq 顺序确定
            waitConditionUntilTimeOut(() -> older.getOurPath() != null, 3000);
            newer.start();
            waitConditionUntilTimeOut(() -> newer.getOurPath() != null, 3000);

            waitConditionUntilTimeOut(() -> latchNodesInSnapshot(service, POD_A) == 2, 3000);
            service.rebuild();
            Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_A));

            older.close();
            waitConditionUntilTimeOut(() -> "10.0.0.9".equals(service.getIpByHostname(POD_A)), 3000);
        } finally {
            closeQuietly(older);
            closeQuietly(newer);
        }
    }

    @Test
    public void testMultipleHostnamesCanShareSameIp() throws Exception {
        // Cilium 回收 IP 后立刻分给新 pod，旧 pod 的临时节点还没随 session 过期被清理
        // 此时多个 hostname 可能对应同一个 IP，正向解析各自仍可用
        register(POD_A, "10.0.0.5");
        register(POD_B, "10.0.0.5");

        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A))
                && "10.0.0.5".equals(service.getIpByHostname(POD_B)), 3000);

        // 两个正向映射都应保留，误发命令由容器端 hostname 自校验拦下
        Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_A));
        Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_B));
    }

    /**
     * 同 IP 命中多个 hostname 时反向按 hostname 字典序取首，与注册先后无关。
     */
    @Test
    public void testReverseLookupPicksSmallestHostnameWhenIpShared() throws Exception {
        // 字典序大的先注册，确认结果不取决于注册顺序或遍历顺序
        register(POD_B, "10.0.0.5");
        register(POD_A, "10.0.0.5");

        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A))
                && "10.0.0.5".equals(service.getIpByHostname(POD_B)), 3000);
        Assert.assertEquals(POD_A, service.getHostnameByIp("10.0.0.5"));

        delete(POD_A);
        waitConditionUntilTimeOut(() -> POD_B.equals(service.getHostnameByIp("10.0.0.5")), 3000);

        Assert.assertNull(service.getHostnameByIp("10.0.0.99"));
        Assert.assertNull(service.getHostnameByIp(null));
        Assert.assertNull(service.getHostnameByIp(""));
    }

    /**
     * tombstone 到期精确失效：读路径按到期时间过滤，不依赖下一次 rebuild；rebuild 再把它清出快照。
     * getAllHostnameToIp 与逐个 getIpByHostname 一致，保留期内包含 tombstone（T-I.27）。
     */
    @Test
    public void testTombstoneExpiresAfterRetention() throws Exception {
        long retention = TimeUnit.MINUTES.toMillis(10);
        ClockedAddressService clocked = startClocked(retention);
        register(POD_A, "10.0.0.5");
        register(POD_B, "10.0.0.6");
        waitConditionUntilTimeOut(() -> POD_A.equals(clocked.getHostnameByIp("10.0.0.5"))
                && POD_B.equals(clocked.getHostnameByIp("10.0.0.6")), 3000);

        delete(POD_A);
        waitConditionUntilTimeOut(() -> null == clocked.getHostnameByIp("10.0.0.5"), 3000);
        Map<String, String> expected = new HashMap<>();
        expected.put(POD_A, "10.0.0.5");
        expected.put(POD_B, "10.0.0.6");
        Assert.assertEquals("保留期内批量结果含 tombstone", expected, clocked.getAllHostnameToIp());

        clocked.advanceMillis(retention - 1);
        Assert.assertEquals("10.0.0.5", clocked.getIpByHostname(POD_A));
        clocked.rebuild();
        Assert.assertEquals("未到期的 tombstone 不被 rebuild 清掉", Collections.singleton(POD_A), clocked.tombstoneHostnames());

        clocked.advanceMillis(2);
        Assert.assertNull("到期后读路径立即不可见", clocked.getIpByHostname(POD_A));
        Assert.assertEquals(Collections.singletonMap(POD_B, "10.0.0.6"), clocked.getAllHostnameToIp());
        clocked.rebuild();
        Assert.assertTrue("到期后 rebuild 清出快照", clocked.tombstoneHostnames().isEmpty());
        Assert.assertEquals("10.0.0.6", clocked.getIpByHostname(POD_B));
    }

    /**
     * hostname 恢复存活后丢弃 tombstone：之后即使过了原保留期，也按存活映射解析。
     */
    @Test
    public void testTombstoneDroppedWhenHostnameComesBack() throws Exception {
        long retention = TimeUnit.MINUTES.toMillis(10);
        ClockedAddressService clocked = startClocked(retention);
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> POD_A.equals(clocked.getHostnameByIp("10.0.0.5")), 3000);

        delete(POD_A);
        waitConditionUntilTimeOut(() -> null == clocked.getHostnameByIp("10.0.0.5"), 3000);
        Assert.assertEquals(Collections.singleton(POD_A), clocked.tombstoneHostnames());

        register(POD_A, "10.0.0.9");
        waitConditionUntilTimeOut(() -> "10.0.0.9".equals(clocked.getIpByHostname(POD_A)), 3000);
        Assert.assertTrue(clocked.tombstoneHostnames().isEmpty());

        clocked.advanceMillis(retention * 2);
        clocked.rebuild();
        Assert.assertEquals("10.0.0.9", clocked.getIpByHostname(POD_A));
    }

    /**
     * 首节点 data 变坏同样算失去存活映射：正向转为 tombstone 给出 last-known IP，反向不再认该 IP。
     */
    @Test
    public void testUnparseableFirstLatchNodeAlsoLeavesTombstone() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> POD_A.equals(service.getHostnameByIp("10.0.0.5")), 3000);

        zkClient.get().setData().forPath(latchPaths.get(POD_A), "not-a-json".getBytes());
        waitConditionUntilTimeOut(() -> null == service.getHostnameByIp("10.0.0.5"), 3000);
        Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_A));
        Assert.assertEquals(Collections.singleton(POD_A), service.tombstoneHostnames());
    }

    /**
     * D-I3 残留风险的现状固定：保留期内 IP 被别的 pod 占用时，正向仍返回旧 hostname 的 tombstone IP（不做即刻失效），
     * 反向只认存活节点，指向新的持有者。
     */
    @Test
    public void testIpReusedDuringTombstone() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> POD_A.equals(service.getHostnameByIp("10.0.0.5")), 3000);

        delete(POD_A);
        waitConditionUntilTimeOut(() -> null == service.getHostnameByIp("10.0.0.5"), 3000);

        register(POD_B, "10.0.0.5");
        waitConditionUntilTimeOut(() -> POD_B.equals(service.getHostnameByIp("10.0.0.5")), 3000);
        Assert.assertEquals("tombstone 不因 IP 被复用而失效（D-I3）", "10.0.0.5", service.getIpByHostname(POD_A));
        Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_B));
    }

    /**
     * latch 节点的绝对路径是 /{namespace}/keepercontainers/{hostname}/{latch}，但代码里不出现 namespace：
     * namespace 由 curator 在 CuratorFrameworkFactory.builder().namespace(...) 上统一挂载，
     * 读写路径与 ChildData.getPath() 都是剥离 namespace 后的相对路径。
     * hostname 则是 latch 子节点的父节点名，由 rebuild 从 ChildData 的路径倒数第二段取回。
     * 这个分工是 hostname 解析正确性的前提，用测试固定下来。
     */
    @Test
    public void testNamespaceIsTransparentAndHostnameComesFromParentNodeName() throws Exception {
        String latchPath = register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);

        // 相对路径下能读到节点
        Assert.assertNotNull(zkClient.get().checkExists().forPath(latchPath));
        // 且 curator 视角的路径不含 namespace 前缀，hostname 是 latch 节点的父节点
        Assert.assertEquals("/keepercontainers/" + POD_A, ZKPaths.getPathAndNode(latchPath).getPath());

        // 换一个原生 client 直连，验证节点真实落在 namespace 之下
        try (CuratorFramework rawClient = CuratorFrameworkFactory.builder()
                .connectString(zkServer.getConnectString())
                .retryPolicy(new RetryNTimes(1, 100))
                .build()) {
            rawClient.start();
            rawClient.blockUntilConnected(5, TimeUnit.SECONDS);

            String namespace = new DefaultZkConfig(zkServer.getConnectString()).getZkNamespace();
            String absolutePath = "/" + namespace + latchPath;
            Assert.assertNotNull("节点应落在 namespace 之下: " + absolutePath,
                    rawClient.checkExists().forPath(absolutePath));
        }
    }

    @Test
    public void testNodeDataContractIsCapitalizedIpAddress() {
        String encoded = Codec.DEFAULT.encode(new KeeperContainerAddressInfo("10.0.0.5"));
        Assert.assertTrue("节点数据契约必须是 IpAddress: " + encoded, encoded.contains("\"IpAddress\""));

        KeeperContainerAddressInfo decoded =
                Codec.DEFAULT.decode("{\"IpAddress\": \"10.0.0.5\"}", KeeperContainerAddressInfo.class);
        Assert.assertEquals("10.0.0.5", decoded.getIpAddress());
    }

    /**
     * 池里 1 个在跑 + 1 个排队，其余提交被丢掉。挡住第一次 rebuild，连续投递必须只再跑一次。
     */
    @Test
    public void testRebuildSubmissionsCoalesceToAtMostTwo() throws Exception {
        HoldingAddressService probe = new HoldingAddressService(zkClient);
        probe.initialize();
        boolean started = false;
        try {
            probe.start();
            started = true;
            waitUntilInitialRebuildDone(probe);
            probe.hold = true;
            probe.submitRebuild();
            Assert.assertTrue(probe.entered.await(5, TimeUnit.SECONDS));

            for (int i = 0; i < 20; i++) {
                probe.submitRebuild();
            }
            Assert.assertEquals("n 次提交在一次 rebuild 未完成时最多再排队一次",
                    1, probe.startedWhileHolding.get());

            probe.release.countDown();
            waitConditionUntilTimeOut(() -> probe.startedWhileHolding.get() == 2, 3000);
            Assert.assertEquals(2, probe.startedWhileHolding.get());
        } finally {
            probe.release.countDown();
            if (started) {
                probe.stop();
            }
        }
    }

    /**
     * 被丢掉的 watch 事件不能丢更新：rebuild 执行时才读快照，所以末次 IP 必须进映射，
     * 且从按住第一次 rebuild 到松开，启动次数不超过两次。
     */
    @Test
    public void testDiscardedWatchEventsStillPublishLastSnapshot() throws Exception {
        HoldingAddressService probe = new HoldingAddressService(zkClient);
        probe.initialize();
        boolean started = false;
        try {
            probe.start();
            started = true;
            register(POD_A, "10.0.0.1");
            waitConditionUntilTimeOut(() -> "10.0.0.1".equals(probe.getIpByHostname(POD_A)), 3000);
            waitUntilInitialRebuildDone(probe);

            probe.hold = true;
            probe.submitRebuild();
            Assert.assertTrue(probe.entered.await(5, TimeUnit.SECONDS));

            String lastIp = "10.0.0.8";
            for (int i = 2; i <= 8; i++) {
                setData(POD_A, "10.0.0." + i);
            }
            // 快照先于 listener 可见。等提交数稳定，避免松开时还有 listener 没投递，多跑出第三次 rebuild。
            waitUntilWatchQuiescent(probe, lastIp);
            Assert.assertEquals("watch 事件必须被合并，不能一次事件一次 rebuild",
                    1, probe.startedWhileHolding.get());

            probe.release.countDown();
            waitConditionUntilTimeOut(() -> lastIp.equals(probe.getIpByHostname(POD_A)), 3000);
            waitConditionUntilTimeOut(() -> probe.startedWhileHolding.get() >= 2, 3000);
            Assert.assertTrue("末次事件由已入队的 rebuild 覆盖，总次数不超过两次，actual="
                            + probe.startedWhileHolding.get(),
                    probe.startedWhileHolding.get() <= 2);
        } finally {
            probe.release.countDown();
            if (started) {
                probe.stop();
            }
        }
    }

    @Test
    public void testScheduledRefreshRebuildsFromZk() throws Exception {
        register(POD_A, "10.0.0.5");
        DefaultCurrentDcKeeperContainerAddressService fast =
                new DefaultCurrentDcKeeperContainerAddressService(zkClient, 400);
        fast.initialize();
        boolean started = false;
        try {
            fast.start();
            started = true;
            waitConditionUntilTimeOut(() -> "10.0.0.5".equals(fast.getIpByHostname(POD_A)), 3000);
            waitUntilInitialRebuildDone(fast);

            Map<String, String> stale = new HashMap<>();
            stale.put(POD_A, "1.1.1.1");
            fast.overwriteHostnameToIp(stale);
            Assert.assertEquals("1.1.1.1", fast.getIpByHostname(POD_A));

            waitConditionUntilTimeOut(() -> "10.0.0.5".equals(fast.getIpByHostname(POD_A)), 3000);
        } finally {
            if (started) {
                fast.stop();
            }
        }
    }

    /**
     * CuratorCache.start() 是异步的，start 之前已存在的节点要靠后台加载 + initialized 回调进映射。
     */
    @Test
    public void testNodesRegisteredBeforeStartAreLoaded() throws Exception {
        register(POD_A, "10.0.0.5");
        register(POD_B, "10.0.0.6");

        DefaultCurrentDcKeeperContainerAddressService late = new DefaultCurrentDcKeeperContainerAddressService(zkClient);
        late.initialize();
        boolean started = false;
        try {
            late.start();
            started = true;
            waitConditionUntilTimeOut(late::cacheInitialized, 3000);
            waitConditionUntilTimeOut(() -> "10.0.0.5".equals(late.getIpByHostname(POD_A))
                    && "10.0.0.6".equals(late.getIpByHostname(POD_B)), 3000);
        } finally {
            if (started) {
                late.stop();
            }
        }
    }

    /**
     * 递归 CuratorCache 会把注册根节点自身也放进 storage。即使根节点上恰好有合法的地址 JSON，
     * 根节点名也不能被当成 hostname。
     */
    @Test
    public void testRegisterRootNodeIsNotTreatedAsHostname() throws Exception {
        String root = MetaZkConfig.getKeeperContainerRegisterPath();
        register(POD_A, "10.0.0.5");
        zkClient.get().setData().forPath(root, Codec.DEFAULT.encodeAsBytes(new KeeperContainerAddressInfo("9.9.9.9")));

        waitConditionUntilTimeOut(() -> service.currentChildren().stream()
                .anyMatch(child -> root.equals(child.getPath()) && child.getStat().getVersion() > 0), 3000);
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);

        service.rebuild();
        Assert.assertEquals(Collections.singletonMap(POD_A, "10.0.0.5"), service.getAllHostnameToIp());
    }

    /**
     * spec §5.4 要求实测：CuratorCache 在 SUSPENDED / LOST 期间不清空 storage，映射继续可用；
     * 断连期间跑 rebuild（定时兜底路径）读到的仍是断连前快照，不会把映射冲空。
     * 重连后 persistent watch 重新挂上：旧 session 的临时节点被服务端过期清理后从映射移除，新注册能被感知。
     */
    @Test
    public void testMappingSurvivesSuspendedAndLostThenRecoversAfterReconnect() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);

        CountDownLatch suspended = new CountDownLatch(1);
        CountDownLatch lost = new CountDownLatch(1);
        CountDownLatch reconnected = new CountDownLatch(1);
        zkClient.get().getConnectionStateListenable().addListener((client, state) -> {
            if (state == ConnectionState.SUSPENDED) {
                suspended.countDown();
            } else if (state == ConnectionState.LOST) {
                lost.countDown();
            } else if (state == ConnectionState.RECONNECTED) {
                reconnected.countDown();
            }
        });

        zkServer.stop();
        Assert.assertTrue("未进入 SUSPENDED", suspended.await(15, TimeUnit.SECONDS));
        assertMappingKeptWhileDisconnected("SUSPENDED", POD_A, "10.0.0.5");
        Assert.assertTrue("未进入 LOST", lost.await(30, TimeUnit.SECONDS));
        assertMappingKeptWhileDisconnected("LOST", POD_A, "10.0.0.5");

        zkServer.restart();
        Assert.assertTrue("未重连", reconnected.await(30, TimeUnit.SECONDS));
        // 旧 session 已过期，其临时节点由服务端清理；重连后 cache 复查已缓存路径并移除它。
        // 存活映射随之消失，但 tombstone 兜住 pod 重新注册前的窗口（spec §5.7 附带收益）
        waitConditionUntilTimeOut(() -> null == service.getHostnameByIp("10.0.0.5"), 30000);
        Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_A));

        register(POD_A, "10.0.0.9");
        waitConditionUntilTimeOut(() -> "10.0.0.9".equals(service.getIpByHostname(POD_A)), 5000);
    }

    private void assertMappingKeptWhileDisconnected(String state, String hostname, String ip) {
        Assert.assertEquals(state + " 期间 cache storage 不应被清空", ip, ipInSnapshot(service, hostname));
        service.rebuild();
        Assert.assertEquals(state + " 期间 rebuild 不应冲空映射", ip, service.getIpByHostname(hostname));
    }

    /**
     * 等首次加载的 initialized 回调投递完、且那次 rebuild 跑完。
     * 否则晚到的 initialized 会在计数窗口里多投递一次，或在打过期值之后把映射修回去。
     */
    private void waitUntilInitialRebuildDone(DefaultCurrentDcKeeperContainerAddressService target) throws Exception {
        waitConditionUntilTimeOut(() -> target.cacheInitialized() && target.rebuildExecutorIdle(), 3000);
    }

    private void waitUntilWatchQuiescent(HoldingAddressService probe, String ip) throws Exception {
        long deadline = System.currentTimeMillis() + 3000;
        int previousSubmits = -1;
        long stableSince = -1;
        while (System.currentTimeMillis() < deadline) {
            boolean snapshotReady = ip.equals(ipInSnapshot(probe, POD_A));
            int submits = probe.submitsWhileHolding.get();
            if (snapshotReady && submits == previousSubmits) {
                if (stableSince < 0) {
                    stableSince = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - stableSince >= 150) {
                    return;
                }
            } else {
                stableSince = -1;
            }
            previousSubmits = submits;
            Thread.sleep(20);
        }
        Assert.fail("watch 没有在超时前稳定到 " + ip + ", submits=" + probe.submitsWhileHolding.get());
    }

    /** 快照里该 hostname 下的 latch 子节点数，用来确认事件已进 cache 再手动 rebuild。 */
    private long latchNodesInSnapshot(DefaultCurrentDcKeeperContainerAddressService target, String hostname) {
        return target.currentChildren().stream().filter(child -> isLatchNodeOf(child, hostname)).count();
    }

    private boolean isLatchNodeOf(ChildData child, String hostname) {
        String path = child.getPath();
        if (path == null) {
            return false;
        }
        ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
        return path(hostname).equals(pathAndNode.getPath()) && pathAndNode.getNode().contains("latch-");
    }

    /** 只用于单 latch 子节点场景。 */
    private String ipInSnapshot(DefaultCurrentDcKeeperContainerAddressService target, String hostname) {
        for (ChildData child : target.currentChildren()) {
            if (!isLatchNodeOf(child, hostname)) {
                continue;
            }
            byte[] data = child.getData();
            if (data == null || data.length == 0) {
                return null;
            }
            KeeperContainerAddressInfo info =
                    Codec.DEFAULT.decode(new String(data), KeeperContainerAddressInfo.class);
            return info == null ? null : info.getIpAddress();
        }
        return null;
    }

    private String path(String hostname) {
        return MetaZkConfig.getKeeperContainerRegisterPath() + "/" + hostname;
    }

    /** 按 LeaderLatch 的建节点方式注册：容器父节点 + protection + 临时顺序 latch- 子节点。返回实际路径。 */
    private String register(String hostname, String ip) throws Exception {
        return registerRaw(hostname, Codec.DEFAULT.encodeAsBytes(new KeeperContainerAddressInfo(ip)));
    }

    private String registerRaw(String hostname, byte[] data) throws Exception {
        String created = zkClient.get().create().creatingParentContainersIfNeeded().withProtection()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(ZKPaths.makePath(path(hostname), "latch-"), data);
        latchPaths.put(hostname, created);
        return created;
    }

    /** 不带 protection，节点名前缀由调用方指定，用来构造排序场景。 */
    private void createSequential(String hostname, String nodePrefix, String ip) throws Exception {
        zkClient.get().create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(ZKPaths.makePath(path(hostname), nodePrefix),
                        Codec.DEFAULT.encodeAsBytes(new KeeperContainerAddressInfo(ip)));
    }

    private void setData(String hostname, String ip) throws Exception {
        zkClient.get().setData()
                .forPath(latchPaths.get(hostname), Codec.DEFAULT.encodeAsBytes(new KeeperContainerAddressInfo(ip)));
    }

    private void delete(String hostname) throws Exception {
        zkClient.get().delete().forPath(latchPaths.remove(hostname));
    }

    private void closeQuietly(LeaderLatch latch) {
        if (latch.getState() != LeaderLatch.State.STARTED) {
            return;
        }
        try {
            latch.close();
        } catch (Exception e) {
            logger.warn("[closeQuietly]{}", latch.getOurPath(), e);
        }
    }

    private ClockedAddressService startClocked(long tombstoneRetentionMilli) throws Exception {
        ClockedAddressService created = new ClockedAddressService(zkClient, tombstoneRetentionMilli);
        created.initialize();
        created.start();
        clocked = created;
        waitUntilInitialRebuildDone(created);
        return created;
    }

    /**
     * 时钟冻结、只随 {@link #advanceMillis} 前进，验证 tombstone 到期边界而不用真等，也不受用例实际耗时干扰。
     * 定时周期拉得很长，rebuild 只由 watch 事件与用例手动触发。
     */
    private static final class ClockedAddressService extends DefaultCurrentDcKeeperContainerAddressService {

        private final AtomicLong nowNanos = new AtomicLong(System.nanoTime());

        private ClockedAddressService(ZkClient zkClient, long tombstoneRetentionMilli) {
            super(zkClient, TimeUnit.MINUTES.toMillis(10), tombstoneRetentionMilli);
        }

        private void advanceMillis(long millis) {
            nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
        }

        @Override
        protected long nowNanos() {
            return nowNanos.get();
        }
    }

    /**
     * 把第一次进入的 rebuild 按在读快照之前，用来制造「1 个在跑 + 1 个排队」的窗口。
     * 定时周期拉得很长，避免兜底任务混进计数。
     */
    private static final class HoldingAddressService extends DefaultCurrentDcKeeperContainerAddressService {

        private final AtomicInteger startedWhileHolding = new AtomicInteger();
        private final AtomicInteger submitsWhileHolding = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean hold;

        private HoldingAddressService(ZkClient zkClient) {
            super(zkClient, TimeUnit.MINUTES.toMillis(10));
        }

        @Override
        protected void submitRebuild() {
            if (hold) {
                submitsWhileHolding.incrementAndGet();
            }
            super.submitRebuild();
        }

        @Override
        protected synchronized void rebuild() {
            if (hold) {
                int n = startedWhileHolding.incrementAndGet();
                if (n == 1) {
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            super.rebuild();
        }
    }
}
