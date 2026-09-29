package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 重建能力（keeper-dynamic-ip spec §5.6 / T-I.18）。每个参与者用独立 client，即独立 session。
 */
public class RebuildableLeaderLatchTest extends AbstractTest {

    private static final String ID = "{\"IpAddress\":\"10.0.0.5\"}";

    private TestingServer zkServer;
    private final List<CuratorFramework> clients = new ArrayList<>();
    private final List<RebuildableLeaderLatch> wrappers = new ArrayList<>();
    private String latchPath;

    @Before
    public void beforeRebuildableLeaderLatchTest() throws Exception {
        zkServer = new TestingServer();
        latchPath = "/" + getTestName();
    }

    @After
    public void afterRebuildableLeaderLatchTest() throws Exception {
        try {
            for (RebuildableLeaderLatch wrapper : wrappers) {
                wrapper.close();
            }
            for (CuratorFramework client : clients) {
                client.close();
            }
        } finally {
            zkServer.close();
        }
    }

    @Test
    public void testStartRegistersNodeOwnedBySession() throws Exception {
        CuratorFramework client = newClient();
        TrackingLatch wrapper = startWrapper(client);

        Assert.assertEquals(LeaderLatch.State.STARTED, wrapper.getState());
        Assert.assertTrue(ownedBy(client, wrapper.getOurPath()));
        Assert.assertEquals(latchPath, ZKPaths.getPathAndNode(wrapper.getOurPath()).getPath());
        waitConditionUntilTimeOut(wrapper::hasLeadership, 3000);
    }

    /**
     * 重建即主动放弃领导权：旧节点删除、新节点排到队尾；此后仍作为参与者竞争，前面的人离开后重新当选。
     */
    @Test
    public void testRebuildRejoinsLeaderElection() throws Exception {
        CuratorFramework clientA = newClient();
        TrackingLatch a = startWrapper(clientA);
        TrackingLatch b = startWrapper(newClient());
        waitConditionUntilTimeOut(a::hasLeadership, 3000);
        String oldPathA = a.getOurPath();

        Assert.assertTrue(a.rebuild("test"));

        waitConditionUntilTimeOut(() -> a.getOurPath() != null && !a.getOurPath().equals(oldPathA), 3000);
        Assert.assertTrue(ownedBy(clientA, a.getOurPath()));
        waitConditionUntilTimeOut(() -> !children().contains(oldPathA), 3000);
        waitConditionUntilTimeOut(b::hasLeadership, 3000);
        Assert.assertFalse(a.hasLeadership());
        Assert.assertEquals(2, a.created.size());
        Assert.assertEquals(LeaderLatch.State.CLOSED, a.created.get(0).getState());

        b.close();
        waitConditionUntilTimeOut(a::hasLeadership, 3000);
        Assert.assertEquals(Collections.singletonList(a.getOurPath()), children());
    }

    /**
     * 旧实例 close 抛异常不阻断重建。模拟 close 已完成内部失活、但仍向外抛 IOException 的情形
     * （LeaderLatch.internalClose 在 CAS 之后、finally 之前抛出）。
     */
    @Test
    public void testRebuildCompletesEvenIfCloseThrows() throws Exception {
        CuratorFramework client = newClient();
        TrackingLatch wrapper = startWrapper(client, true);
        String oldPath = wrapper.getOurPath();

        Assert.assertTrue(wrapper.rebuild("test"));

        waitConditionUntilTimeOut(() -> wrapper.getOurPath() != null && !wrapper.getOurPath().equals(oldPath), 3000);
        Assert.assertTrue(ownedBy(client, wrapper.getOurPath()));
        waitConditionUntilTimeOut(() -> Collections.singletonList(wrapper.getOurPath()).equals(children()), 3000);
        waitConditionUntilTimeOut(wrapper::hasLeadership, 3000);
    }

    /**
     * 并发重建必须串行：每个被替换下来的实例都已关闭，ZK 上最终只剩当前实例一个节点。
     * 若不加锁，两个线程会各自 new 一个实例，被覆盖的那个无人 close，其节点就是孤儿。
     */
    @Test
    public void testConcurrentRebuildLeavesNoOrphanNode() throws Exception {
        TrackingLatch wrapper = startWrapper(newClient());

        int threads = 8, rounds = 5;
        runConcurrently(threads, () -> {
            for (int i = 0; i < rounds; i++) {
                Assert.assertTrue(wrapper.rebuild("concurrent"));
            }
        });

        Assert.assertEquals(1 + threads * rounds, wrapper.created.size());
        List<LeaderLatch> replaced = wrapper.created.subList(0, wrapper.created.size() - 1);
        for (LeaderLatch latch : replaced) {
            Assert.assertEquals(LeaderLatch.State.CLOSED, latch.getState());
        }
        waitConditionUntilTimeOut(() -> wrapper.getOurPath() != null
                && Collections.singletonList(wrapper.getOurPath()).equals(children()), 10000);
    }

    /**
     * 重建与 close 并发：close 之后的重建一律不生效，所有实例最终都已关闭，节点全部删除。
     */
    @Test
    public void testRebuildRacingWithCloseLeavesNoNode() throws Exception {
        TrackingLatch wrapper = startWrapper(newClient());

        CountDownLatch someRebuilt = new CountDownLatch(4);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int t = 0; t < 4; t++) {
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < 20; i++) {
                        wrapper.rebuild("race");
                        someRebuilt.countDown();
                    }
                }));
            }
            someRebuilt.await(5, TimeUnit.SECONDS);
            wrapper.close();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        int createdAfterClose = wrapper.created.size();
        Assert.assertFalse(wrapper.rebuild("after close"));
        Assert.assertEquals(createdAfterClose, wrapper.created.size());
        for (LeaderLatch latch : wrapper.created) {
            Assert.assertEquals(LeaderLatch.State.CLOSED, latch.getState());
        }
        waitConditionUntilTimeOut(() -> children().isEmpty(), 10000);
    }

    @Test
    public void testRebuildIsNoopBeforeStartAndAfterClose() throws Exception {
        TrackingLatch wrapper = new TrackingLatch(newClient(), false);
        wrappers.add(wrapper);

        Assert.assertFalse(wrapper.rebuild("not started"));
        Assert.assertEquals(LeaderLatch.State.LATENT, wrapper.getState());
        Assert.assertTrue(wrapper.created.isEmpty());

        wrapper.start();
        wrapper.close();
        wrapper.close();
        Assert.assertEquals(LeaderLatch.State.CLOSED, wrapper.getState());
        Assert.assertFalse(wrapper.rebuild("closed"));
        Assert.assertEquals(1, wrapper.created.size());
        waitConditionUntilTimeOut(() -> children().isEmpty(), 3000);
    }

    @Test
    public void testDefaultProbeSchedulerIsSharedAcrossInstances() throws Exception {
        CuratorFramework client = newClient();
        RebuildableLeaderLatch first = new RebuildableLeaderLatch(client, latchPath, ID);
        RebuildableLeaderLatch second = new RebuildableLeaderLatch(client, latchPath + "-other", ID);
        Assert.assertSame(RebuildableLeaderLatch.sharedProbeScheduler(), first.getProbeScheduler());
        Assert.assertSame(first.getProbeScheduler(), second.getProbeScheduler());
    }

    private void runConcurrently(int threads, Runnable task) throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(() -> {
                    gate.await();
                    task.run();
                    return null;
                }));
            }
            gate.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private TrackingLatch startWrapper(CuratorFramework client) throws Exception {
        return startWrapper(client, false);
    }

    /** 启动并等节点建好，保证多参与者场景下 seq 顺序与启动顺序一致。 */
    private TrackingLatch startWrapper(CuratorFramework client, boolean closeThrows) throws Exception {
        TrackingLatch wrapper = new TrackingLatch(client, closeThrows);
        wrappers.add(wrapper);
        wrapper.start();
        waitConditionUntilTimeOut(() -> wrapper.getOurPath() != null, 5000);
        return wrapper;
    }

    private CuratorFramework newClient() throws Exception {
        CuratorFramework client = new DefaultZkConfig(zkServer.getConnectString()).create();
        clients.add(client);
        return client;
    }

    private boolean ownedBy(CuratorFramework client, String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        return stat != null
                && stat.getEphemeralOwner() == client.getZookeeperClient().getZooKeeper().getSessionId();
    }

    /** latchPath 下全部子节点的完整路径，latchPath 不存在时为空。 */
    private List<String> children() {
        try {
            List<String> paths = new ArrayList<>();
            for (String child : clients.get(0).getChildren().forPath(latchPath)) {
                paths.add(ZKPaths.makePath(latchPath, child));
            }
            return paths;
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (Exception e) {
            logger.info("[children]{}", e.toString());
            return Collections.emptyList();
        }
    }

    /** 记录每次新建的内部实例，用于断言被替换的实例都已关闭。 */
    private class TrackingLatch extends RebuildableLeaderLatch {

        private final CuratorFramework client;
        private final boolean closeThrows;
        private final List<LeaderLatch> created = new CopyOnWriteArrayList<>();

        TrackingLatch(CuratorFramework client, boolean closeThrows) {
            super(client, latchPath, ID);
            this.client = client;
            this.closeThrows = closeThrows;
        }

        @Override
        protected LeaderLatch newLatch() {
            LeaderLatch latch = closeThrows && created.isEmpty() ? new CloseThrowingLatch(client) : super.newLatch();
            created.add(latch);
            return latch;
        }
    }

    private class CloseThrowingLatch extends LeaderLatch {

        CloseThrowingLatch(CuratorFramework client) {
            super(client, latchPath, ID);
        }

        @Override
        public void close() throws IOException {
            super.close();
            throw new IOException("simulated close failure");
        }
    }
}
