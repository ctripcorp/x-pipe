package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.zk.ZkConfig;
import com.ctrip.xpipe.zk.ZkTestServer;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.KillSession;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wenchao.meng
 *
 * Sep 5, 2016
 */
public class DefaultLeaderElectorTest extends AbstractTest {

    private ZkTestServer zkTestServer;
    private CuratorFramework client;

    @Before
    public void beforeDefaultLeaderElectorTest() throws InterruptedException {
        zkTestServer = startRandomZk();

        ZkConfig config = new DefaultZkConfig(String.format("localhost:%d", zkTestServer.getZkPort()));
        client = config.create();
    }

    @After
    public void afterDefaultLeaderElectorTest() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                logger.error("[afterDefaultLeaderElectorTest][close client]", e);
            }
            client = null;
        }
    }

    @Test
    public void testElect() throws Exception {
        String zkPath = "/" + getTestName();
        ElectContext electContext = new ElectContext(zkPath, "hi");
        DefaultLeaderElector leaderElector = new DefaultLeaderElector(electContext, client);
        leaderElector.initialize();
        leaderElector.start();

        try {
            waitForOneChild(zkPath);
            waitConditionUntilTimeOut(leaderElector::hasLeaderShip);
            Assert.assertNotNull(client.checkExists().forPath(zkPath));
        } finally {
            leaderElector.stop();
            leaderElector.dispose();
        }
    }

    @Test
    public void testPublishElectionDataInPlaceAndIdempotently() throws Exception {
        String zkPath = "/" + getTestName();
        AtomicReference<ElectContext> context = new AtomicReference<>(new ElectContext(zkPath, "before"));
        DefaultLeaderElector leaderElector = new DefaultLeaderElector(context::get, client);
        leaderElector.initialize();
        leaderElector.start();

        try {
            waitForOneChild(zkPath);
            List<String> childrenBefore = client.getChildren().forPath(zkPath);
            String childPath = zkPath + "/" + childrenBefore.get(0);
            Stat beforeStat = new Stat();
            Assert.assertEquals("before", new String(client.getData().storingStatIn(beforeStat)
                    .forPath(childPath), StandardCharsets.UTF_8));

            context.set(new ElectContext(zkPath, "after"));
            Assert.assertTrue(leaderElector.publishElectionData());

            List<String> childrenAfter = client.getChildren().forPath(zkPath);
            Assert.assertEquals(childrenBefore, childrenAfter);
            Stat afterStat = new Stat();
            Assert.assertEquals("after", new String(client.getData().storingStatIn(afterStat)
                    .forPath(childPath), StandardCharsets.UTF_8));
            Assert.assertEquals(beforeStat.getVersion() + 1, afterStat.getVersion());

            Assert.assertTrue(leaderElector.publishElectionData());
            Stat unchangedStat = client.checkExists().forPath(childPath);
            Assert.assertEquals(afterStat.getVersion(), unchangedStat.getVersion());
        } finally {
            leaderElector.stop();
            leaderElector.dispose();
        }
    }

    @Test
    public void testRepublishLatestDataAfterSessionLost() throws Exception {
        String zkPath = "/" + getTestName();
        AtomicReference<ElectContext> context = new AtomicReference<>(new ElectContext(zkPath, "before"));
        DefaultLeaderElector leaderElector = new DefaultLeaderElector(context::get, client);
        leaderElector.initialize();
        leaderElector.start();

        try {
            waitForOneChild(zkPath);
            String oldChild = client.getChildren().forPath(zkPath).get(0);
            context.set(new ElectContext(zkPath, "after"));
            Assert.assertTrue(leaderElector.publishElectionData());

            KillSession.kill(client.getZookeeperClient().getZooKeeper(),
                    "localhost:" + zkTestServer.getZkPort());

            waitConditionUntilTimeOut(() -> {
                try {
                    List<String> children = client.getChildren().forPath(zkPath);
                    if (children.size() != 1 || oldChild.equals(children.get(0))) {
                        return false;
                    }
                    byte[] data = client.getData().forPath(zkPath + "/" + children.get(0));
                    return "after".equals(new String(data, StandardCharsets.UTF_8));
                } catch (Exception e) {
                    return false;
                }
            });
        } finally {
            leaderElector.stop();
            leaderElector.dispose();
        }
    }

    private void waitForOneChild(String zkPath) throws Exception {
        waitConditionUntilTimeOut(() -> {
            try {
                return client.getChildren().forPath(zkPath).size() == 1;
            } catch (Exception e) {
                return false;
            }
        });
    }
}
