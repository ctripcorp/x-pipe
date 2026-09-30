package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.cluster.LeaderLatchProbeStatus.Reason;
import com.ctrip.xpipe.zk.BlackholeTcpProxy;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 断链探测（keeper-dynamic-ip spec §5.6 / T-I.21）。
 * <p>
 * 断链靠真实 ZK 故障构造：latch 父路径的 ACL 拿掉 CREATE 让 reset 的 create 失败（A），
 * 拿掉 READ 让 getChildren 失败（C-其余），前驱节点拿掉 READ 让前驱 watch 的 getData 失败（B）。
 * ZK 的 exists 不校验 ACL，探测本身不受这些权限影响。
 * <p>
 * 每个参与者用独立 client，即独立 session。探测节奏压缩为 100ms / 1s。
 */
public class RebuildableLeaderLatchProbeTest extends AbstractTest {

    private static final String ID = "{\"IpAddress\":\"10.0.0.5\"}";
    private static final long INTERVAL = 100;
    private static final long THRESHOLD = 1000;

    private TestingServer zkServer;
    private ScheduledExecutorService probeScheduler;
    private final List<CuratorFramework> clients = new ArrayList<>();
    private final List<RebuildableLeaderLatch> wrappers = new ArrayList<>();
    private final List<BlackholeTcpProxy> proxies = new ArrayList<>();
    private CuratorFramework admin;
    private String latchPath;

    @Before
    public void beforeRebuildableLeaderLatchProbeTest() throws Exception {
        zkServer = new TestingServer();
        probeScheduler = Executors.newSingleThreadScheduledExecutor();
        latchPath = "/" + getTestName();
        admin = newClient();
        // 预建为持久节点：latch 自建的是容器节点，子节点删空后可能被回收，改过的 ACL 会随之丢失
        admin.create().forPath(latchPath);
    }

    @After
    public void afterRebuildableLeaderLatchProbeTest() throws Exception {
        try {
            for (RebuildableLeaderLatch wrapper : wrappers) {
                wrapper.close();
            }
            for (CuratorFramework client : clients) {
                client.close();
            }
            for (BlackholeTcpProxy proxy : proxies) {
                proxy.close();
            }
            probeScheduler.shutdownNow();
        } finally {
            zkServer.close();
        }
    }

    @Test
    public void testHealthyProbeReportsNodeOwnedBySession() throws Exception {
        CuratorFramework client = newClient();
        TrackingLatch wrapper = start(client, THRESHOLD);

        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 3000);
        LeaderLatchProbeStatus status = wrapper.getProbeStatus();
        Assert.assertEquals(Reason.OK, status.getReason());
        Assert.assertEquals(LeaderLatch.State.STARTED, status.getState());
        Assert.assertEquals(wrapper.getOurPath(), status.getOurPath());
        Assert.assertEquals(Boolean.TRUE, status.getPathExists());
        Assert.assertEquals(Long.valueOf(sessionId(client)), status.getEphemeralOwner());
        Assert.assertEquals(status.getEphemeralOwner(), status.getSessionId());
        Assert.assertEquals(0, status.getUnhealthyDurationMillis());
    }

    /**
     * A：reset 的 create 回调非 OK 只打日志，ourPath 停在 null。满阈值才重建；ZK 仍拒绝建节点时
     * 每次重建都落回 A，按阈值节奏持续重试，直到 ZK 恢复。
     */
    @Test
    public void testCreateFailureLeavesNullOurPathAndIsRebuilt() throws Exception {
        setAcl(ZooDefs.Perms.ALL & ~ZooDefs.Perms.CREATE);
        CuratorFramework client = newClient();
        long begin = System.nanoTime();
        TrackingLatch wrapper = start(client, THRESHOLD);

        waitConditionUntilTimeOut(() -> Reason.OUR_PATH_NULL == wrapper.getProbeStatus().getReason(), 3000);
        Assert.assertNull(wrapper.getProbeStatus().getPathExists());
        Assert.assertNull(wrapper.getOurPath());

        waitConditionUntilTimeOut(() -> wrapper.created.size() >= 2, 5000);
        Assert.assertTrue("连续不健康满阈值才重建",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin) >= THRESHOLD);

        setAcl(ZooDefs.Perms.ALL);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 10000);
        Assert.assertTrue(ownedBy(client, wrapper.getOurPath()));
    }

    /**
     * D：首节点自检 owner 不符走 reset，出口是 A。session 过期重连后服务端旧 session 尚未过期，
     * getChildren 仍列出旧节点、排首位，getData 发现 owner 不是新 session → reset → create 被拒。
     * latch 自身不会再重试，恢复只能来自重建。
     */
    @Test
    public void testSelfCheckResetIntoCreateFailureIsRebuilt() throws Exception {
        CuratorFramework client = newClient();
        TrackingLatch wrapper = start(client, THRESHOLD);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 3000);

        setAcl(ZooDefs.Perms.ALL & ~ZooDefs.Perms.CREATE);
        long oldSession = sessionId(client);
        KillSession.kill(client.getZookeeperClient().getZooKeeper());

        waitConditionUntilTimeOut(() -> sessionId(client) != oldSession
                && Reason.OUR_PATH_NULL == wrapper.getProbeStatus().getReason(), 15000);

        setAcl(ZooDefs.Perms.ALL);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy()
                && ownedBy(client, wrapper.getOurPath()), 10000);
        Assert.assertTrue(wrapper.created.size() >= 2);
    }

    /**
     * C-其余：重连后 getChildren 非 OK 非 NONODE 只打日志，latch 停在旧 session 的节点上——
     * 服务端旧 session 未过期时 owner 不符，过期后节点不存在，两者连续计时。
     * 重建后 getChildren 依旧失败，但新节点完好、归属当前 session，判为健康，不再反复重建。
     */
    @Test
    public void testStaleNodeAfterGetChildrenFailureIsRebuilt() throws Exception {
        CuratorFramework client = newClient();
        TrackingLatch wrapper = start(client, THRESHOLD);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 3000);
        String oldPath = wrapper.getOurPath();

        setAcl(ZooDefs.Perms.ALL & ~ZooDefs.Perms.READ);
        long oldSession = sessionId(client);
        KillSession.kill(client.getZookeeperClient().getZooKeeper());

        waitConditionUntilTimeOut(() -> {
            LeaderLatchProbeStatus status = wrapper.getProbeStatus();
            return sessionId(client) != oldSession && oldPath.equals(status.getOurPath())
                    && (Reason.OWNER_MISMATCH == status.getReason() || Reason.PATH_NOT_EXIST == status.getReason());
        }, 15000);

        waitConditionUntilTimeOut(() -> wrapper.created.size() >= 2, 5000);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy()
                && ownedBy(client, wrapper.getOurPath()), 10000);
        Assert.assertNotEquals(oldPath, wrapper.getOurPath());

        int created = wrapper.created.size();
        sleep((int) (THRESHOLD * 2));
        Assert.assertEquals("节点完好时不因领导权通知丢失而重建", created, wrapper.created.size());
    }

    /**
     * B：前驱 watch 的 getData 非 NONODE 时回调什么都不做，watch 也没挂上（ZK 只在 rc==0 时注册）。
     * 本实例节点完好，注册语义不受影响，探测判健康、不重建。代价是前驱离开后领导权通知永远不到——
     * 这是判据刻意不覆盖的边界（spec §5.6），复用到依赖 hasLeadership 的选主前需另加判据。
     */
    @Test
    public void testPredecessorWatchFailureKeepsNodeAndIsNotRebuilt() throws Exception {
        CuratorFramework other = newClient();
        String predecessor = other.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .withACL(acl(ZooDefs.Perms.ALL & ~ZooDefs.Perms.READ))
                .forPath(ZKPaths.makePath(latchPath, "x-latch-"));

        CuratorFramework client = newClient();
        TrackingLatch wrapper = start(client, THRESHOLD);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 3000);

        other.delete().forPath(predecessor);
        sleep((int) (THRESHOLD * 2));

        Assert.assertTrue(wrapper.getProbeStatus().isHealthy());
        Assert.assertEquals(1, wrapper.created.size());
        Assert.assertTrue(ownedBy(client, wrapper.getOurPath()));
        Assert.assertFalse("前驱 watch 丢失，领导权通知不会到达", wrapper.hasLeadership());
    }

    /**
     * 宽限期：重连后 reset 先 setNode(null) 再异步 create，ourPath 短暂为 null。
     * 用 LeaderLatch 的 debug latch 把 create 回调挂住，拉长这段窗口但不超过阈值，不得触发重建。
     */
    @Test
    public void testTransientNullOurPathOnReconnectDoesNotTriggerRebuild() throws Exception {
        long threshold = 3000;
        CuratorFramework client = newClient();
        TrackingLatch wrapper = start(client, threshold);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 3000);

        CountDownLatch hold = new CountDownLatch(1);
        setDebugResetWaitLatch(wrapper.created.get(0), hold);
        long oldSession = sessionId(client);
        KillSession.kill(client.getZookeeperClient().getZooKeeper());

        try {
            waitConditionUntilTimeOut(() -> sessionId(client) != oldSession
                    && Reason.OUR_PATH_NULL == wrapper.getProbeStatus().getReason(), 15000);
            sleep((int) (threshold / 3));
            Assert.assertEquals(Reason.OUR_PATH_NULL, wrapper.getProbeStatus().getReason());
            Assert.assertTrue(wrapper.getProbeStatus().getUnhealthyDurationMillis() < threshold);
        } finally {
            hold.countDown();
        }

        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy()
                && ownedBy(client, wrapper.getOurPath()), 5000);
        Assert.assertEquals(1, wrapper.created.size());
    }

    /** 连接不健康时不下结论：不计时、不重建。 */
    @Test
    public void testDisconnectedIsInconclusiveAndDoesNotRebuild() throws Exception {
        CuratorFramework client = newClient();
        TrackingLatch wrapper = start(client, THRESHOLD);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 3000);

        zkServer.stop();
        try {
            waitConditionUntilTimeOut(() -> LeaderLatchProbeStatus.Verdict.INCONCLUSIVE
                    == wrapper.getProbeStatus().getVerdict(), 5000);
            sleep((int) (THRESHOLD + 500));
            Assert.assertEquals(LeaderLatchProbeStatus.Verdict.INCONCLUSIVE, wrapper.getProbeStatus().getVerdict());
            Assert.assertEquals(0, wrapper.getProbeStatus().getUnhealthyDurationMillis());
            Assert.assertEquals(1, wrapper.created.size());
        } finally {
            zkServer.restart();
        }
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 10000);
        Assert.assertEquals(1, wrapper.created.size());
    }

    /**
     * 静默分区：连接不断、请求没有响应，客户端要等读超时（约 sessionTimeout 的 2/3）才察觉，
     * 这段时间 isConnected() 仍为 true。判据②③依赖 exists 的响应，拿不到响应就得不出「不健康」，
     * 回调最终以 CONNECTIONLOSS 收尾，判无结论。用例要求这段窗口比阈值长，否则说明不了问题。
     */
    @Test
    public void testSilentPartitionBeforeClientNoticesIsNotJudgedUnhealthy() throws Exception {
        BlackholeTcpProxy proxy = newProxy();
        CuratorFramework client = newClient(proxy.getConnectString());
        TrackingLatch wrapper = start(client, THRESHOLD);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 3000);

        proxy.blackhole();
        long begin = System.nanoTime();
        Set<LeaderLatchProbeStatus.Verdict> seen = EnumSet.noneOf(LeaderLatchProbeStatus.Verdict.class);
        long silentWindowMillis = 0;
        while (client.getZookeeperClient().isConnected()) {
            seen.add(wrapper.getProbeStatus().getVerdict());
            silentWindowMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
            Assert.assertTrue("客户端迟迟未察觉断连", silentWindowMillis < 15000);
            sleep(20);
        }
        Assert.assertTrue("静默窗口 " + silentWindowMillis + "ms 须长于阈值加两个探测间隔",
                silentWindowMillis > THRESHOLD + INTERVAL * 2);

        // 察觉断连后再观察一个阈值，赶在服务端 session 过期前恢复
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(THRESHOLD);
        while (System.nanoTime() < deadline) {
            seen.add(wrapper.getProbeStatus().getVerdict());
            sleep(20);
        }
        Assert.assertFalse("分区期间不得判为不健康: " + seen, seen.contains(LeaderLatchProbeStatus.Verdict.UNHEALTHY));
        Assert.assertEquals(1, wrapper.created.size());

        proxy.restore();
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy()
                && ownedBy(client, wrapper.getOurPath()), 15000);
        Assert.assertEquals(1, wrapper.created.size());
    }

    /**
     * 分区长过 session 超时：恢复后换了新 session，由 latch 自己 reset 重建节点，
     * ourPath 短暂为 null / owner 不符都远不到阈值，不触发包装类重建。
     */
    @Test
    public void testPartitionLongerThanSessionTimeoutSelfHealsWithoutRebuild() throws Exception {
        BlackholeTcpProxy proxy = newProxy();
        CuratorFramework client = newClient(proxy.getConnectString());
        TrackingLatch wrapper = start(client, THRESHOLD);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 3000);
        String oldPath = wrapper.getOurPath();
        long oldSession = sessionId(client);
        int sessionTimeout = client.getZookeeperClient().getZooKeeper().getSessionTimeout();

        proxy.blackhole();
        waitConditionUntilTimeOut(() -> !client.getZookeeperClient().isConnected(), 15000);
        // 服务端收不到心跳，满 session 超时后过期并删掉旧临时节点
        waitConditionUntilTimeOut(() -> !exists(oldPath), sessionTimeout + 10000);
        Assert.assertEquals(1, wrapper.created.size());

        proxy.restore();
        waitConditionUntilTimeOut(() -> sessionId(client) != oldSession
                && wrapper.getProbeStatus().isHealthy() && ownedBy(client, wrapper.getOurPath()), 20000);
        Assert.assertNotEquals(oldPath, wrapper.getOurPath());
        Assert.assertEquals("恢复由 latch 自身 reset 完成", 1, wrapper.created.size());
    }

    @Test
    public void testCloseStopsProbing() throws Exception {
        TrackingLatch wrapper = start(newClient(), THRESHOLD);
        waitConditionUntilTimeOut(() -> wrapper.getProbeStatus().isHealthy(), 3000);

        wrapper.close();
        long probeTime = wrapper.getProbeStatus().getProbeTimeMillis();
        sleep((int) (INTERVAL * 5));
        Assert.assertEquals(probeTime, wrapper.getProbeStatus().getProbeTimeMillis());
    }

    private TrackingLatch start(CuratorFramework client, long thresholdMilli) throws Exception {
        TrackingLatch wrapper = new TrackingLatch(client, thresholdMilli);
        wrappers.add(wrapper);
        wrapper.start();
        return wrapper;
    }

    private CuratorFramework newClient() throws Exception {
        return newClient(zkServer.getConnectString());
    }

    private CuratorFramework newClient(String connectString) throws Exception {
        CuratorFramework client = new DefaultZkConfig(connectString).create();
        clients.add(client);
        return client;
    }

    private BlackholeTcpProxy newProxy() throws Exception {
        BlackholeTcpProxy proxy = new BlackholeTcpProxy(zkServer.getPort()).start();
        proxies.add(proxy);
        return proxy;
    }

    /** 走 admin 直连 ZK，不经代理。 */
    private boolean exists(String path) {
        try {
            return null != admin.checkExists().forPath(path);
        } catch (Exception e) {
            return true;
        }
    }

    private void setAcl(int perms) throws Exception {
        admin.setACL().withACL(acl(perms)).forPath(latchPath);
    }

    private static List<ACL> acl(int perms) {
        return Collections.singletonList(new ACL(perms, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    }

    private long sessionId(CuratorFramework client) {
        try {
            return client.getZookeeperClient().getZooKeeper().getSessionId();
        } catch (Exception e) {
            return -1;
        }
    }

    private boolean ownedBy(CuratorFramework client, String path) {
        try {
            if (null == path) {
                return false;
            }
            Stat stat = client.checkExists().forPath(path);
            return stat != null && stat.getEphemeralOwner() == sessionId(client);
        } catch (Exception e) {
            return false;
        }
    }

    /** LeaderLatch.debugResetWaitLatch 是包内可见的 @VisibleForTesting 字段。 */
    private static void setDebugResetWaitLatch(LeaderLatch latch, CountDownLatch value) throws Exception {
        Field field = LeaderLatch.class.getDeclaredField("debugResetWaitLatch");
        field.setAccessible(true);
        field.set(latch, value);
    }

    /** 记录每次新建的内部实例，用于断言重建次数。 */
    private class TrackingLatch extends RebuildableLeaderLatch {

        private final List<LeaderLatch> created = new CopyOnWriteArrayList<>();

        TrackingLatch(CuratorFramework client, long thresholdMilli) {
            super(client, latchPath, ID, probeScheduler, INTERVAL, thresholdMilli);
        }

        @Override
        protected LeaderLatch newLatch() {
            LeaderLatch latch = super.newLatch();
            created.add(latch);
            return latch;
        }
    }
}
