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
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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

public class DefaultCurrentDcKeeperContainerAddressServiceTest extends AbstractTest {

    private static final String POD_A = "keepercontainer-pod-a-0";
    private static final String POD_B = "keepercontainer-pod-b-0";

    private TestingServer zkServer;
    private DefaultZkClient zkClient;
    private DefaultCurrentDcKeeperContainerAddressService service;

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
     * pod 重建后 hostname 不变，所以真实流程是同名节点先随旧 session 消失、
     * 再由新 pod 重新创建，事件序列是 CHILD_REMOVED 后跟 CHILD_ADDED，
     * 而不是单个 CHILD_UPDATED。两者的最终状态都必须收敛到新 IP。
     */
    @Test
    public void testPodRecreateKeepsHostnameAndSwitchesIp() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);

        delete(POD_A);
        // 节点消失到新节点建立之间存在窗口，此时解析不到是正确行为：
        // 宁可让调用方拿到 null 走拒绝路径，也不能返回已失效的旧 IP
        waitConditionUntilTimeOut(() -> null == service.getIpByHostname(POD_A), 3000);

        register(POD_A, "10.0.0.9");
        waitConditionUntilTimeOut(() -> "10.0.0.9".equals(service.getIpByHostname(POD_A)), 3000);
    }

    @Test
    public void testNodeRemoved() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);

        delete(POD_A);

        waitConditionUntilTimeOut(() -> null == service.getIpByHostname(POD_A), 3000);
    }

    @Test
    public void testMultipleContainers() throws Exception {
        register(POD_A, "10.0.0.5");
        register(POD_B, "10.0.0.6");

        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A))
                && "10.0.0.6".equals(service.getIpByHostname(POD_B)), 3000);

        // 单个容器下线不影响其他容器
        delete(POD_A);
        waitConditionUntilTimeOut(() -> null == service.getIpByHostname(POD_A), 3000);
        Assert.assertEquals("10.0.0.6", service.getIpByHostname(POD_B));
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
        CuratorFramework client = zkClient.get();
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .forPath(path("bad-pod"), "not-a-json".getBytes());
        register(POD_A, "10.0.0.5");

        // 坏节点不能拖垮整批解析
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);
        Assert.assertNull(service.getIpByHostname("bad-pod"));
    }

    /**
     * 正向映射不可能一对多：hostname 是 ZK 节点名，同名节点只能存在一个，
     * 重复 create 会被 ZK 以 NodeExists 拒绝。
     */
    @Test
    public void testHostnameIsStructurallyUnique() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);

        try {
            register(POD_A, "10.0.0.9");
            Assert.fail("同名节点重复注册应被 ZK 拒绝");
        } catch (KeeperException.NodeExistsException expected) {
            // 正是期望的行为
        }

        Assert.assertEquals(1,
                zkClient.get().getChildren().forPath(MetaZkConfig.getKeeperContainerRegisterPath()).size());
        Assert.assertEquals("10.0.0.5", service.getIpByHostname(POD_A));
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
     * 节点的绝对路径是 /{namespace}/keepercontainers/{hostname}，但代码里只出现后两段：
     * namespace 由 curator 在 CuratorFrameworkFactory.builder().namespace(...) 上统一挂载，
     * 读写路径与 ChildData.getPath() 都是剥离 namespace 后的相对路径。
     * hostname 则是 watch 路径下的子节点名，由 rebuild 从 ChildData 的路径末段取回。
     * 这个分工是 hostname 解析正确性的前提，用测试固定下来。
     */
    @Test
    public void testNamespaceIsTransparentAndHostnameComesFromChildNodeName() throws Exception {
        register(POD_A, "10.0.0.5");
        waitConditionUntilTimeOut(() -> "10.0.0.5".equals(service.getIpByHostname(POD_A)), 3000);

        // 相对路径下能读到节点
        Assert.assertNotNull(zkClient.get().checkExists().forPath(path(POD_A)));
        // 且 curator 视角的路径不含 namespace 前缀
        Assert.assertEquals("/keepercontainers/" + POD_A, path(POD_A));

        // 换一个原生 client 直连，验证节点真实落在 namespace 之下
        try (CuratorFramework rawClient = CuratorFrameworkFactory.builder()
                .connectString(zkServer.getConnectString())
                .retryPolicy(new RetryNTimes(1, 100))
                .build()) {
            rawClient.start();
            rawClient.blockUntilConnected(5, TimeUnit.SECONDS);

            String namespace = new DefaultZkConfig(zkServer.getConnectString()).getZkNamespace();
            String absolutePath = "/" + namespace + "/keepercontainers/" + POD_A;
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
        // 旧 session 已过期，其临时节点由服务端清理；重连后 cache 复查已缓存路径并移除它
        waitConditionUntilTimeOut(() -> null == service.getIpByHostname(POD_A), 30000);

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

    private String ipInSnapshot(DefaultCurrentDcKeeperContainerAddressService target, String hostname) {
        for (ChildData child : target.currentChildren()) {
            String path = child.getPath();
            if (path == null || !path.endsWith("/" + hostname)) {
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

    private void register(String hostname, String ip) throws Exception {
        zkClient.get().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .forPath(path(hostname), Codec.DEFAULT.encodeAsBytes(new KeeperContainerAddressInfo(ip)));
    }

    private void setData(String hostname, String ip) throws Exception {
        zkClient.get().setData()
                .forPath(path(hostname), Codec.DEFAULT.encodeAsBytes(new KeeperContainerAddressInfo(ip)));
    }

    private void delete(String hostname) throws Exception {
        zkClient.get().delete().forPath(path(hostname));
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
