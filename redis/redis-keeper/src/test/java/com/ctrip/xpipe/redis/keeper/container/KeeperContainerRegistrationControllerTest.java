package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.cluster.LeaderLatchProbeStatus;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.keeper.container.resp.RestResp;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * 注册运维接口（keeper-dynamic-ip spec §5.6 Ih / T-I.24）。
 * <p>
 * 自动重建阈值拉到 10 分钟，用例里的重建只可能来自接口调用。
 */
public class KeeperContainerRegistrationControllerTest extends AbstractTest {

    private static final String HOSTNAME = "keepercontainer-pod-a-0";
    private static final String IP = "10.0.0.5";
    private static final long NEVER = TimeUnit.MINUTES.toMillis(10);

    private TestingServer zkServer;
    private DefaultZkClient zkClient;
    private KeeperContainerZkRegistrar registrar;
    private KeeperContainerRegistrationController controller;

    @Before
    public void beforeKeeperContainerRegistrationControllerTest() throws Exception {
        zkServer = new TestingServer();
        zkClient = new DefaultZkClient(new DefaultZkConfig(zkServer.getConnectString()));
        zkClient.initialize();
        zkClient.start();
    }

    @After
    public void afterKeeperContainerRegistrationControllerTest() throws Exception {
        try {
            if (null != registrar && registrar.getLifecycleState().isStarted()) {
                registrar.stop();
            }
            zkClient.stop();
        } finally {
            zkServer.close();
        }
    }

    @Test
    public void testStatusReportsProbeFields() throws Exception {
        startRegistrar(100);
        waitConditionUntilTimeOut(() -> healthy(controller.status()), 3000);

        RestResp<LeaderLatchProbeStatus> resp = controller.status();
        LeaderLatchProbeStatus status = resp.getData();
        Assert.assertEquals(0, resp.getCode());
        Assert.assertEquals(LeaderLatchProbeStatus.Verdict.HEALTHY, status.getVerdict());
        Assert.assertEquals(LeaderLatchProbeStatus.Reason.OK, status.getReason());
        Assert.assertEquals(LeaderLatch.State.STARTED, status.getState());
        Assert.assertEquals(registrar.ourPath(), status.getOurPath());
        Assert.assertEquals(latchPath(), status.getLatchPath());
        Assert.assertTrue(status.getId().contains(IP));
        Assert.assertEquals(Boolean.TRUE, status.getPathExists());
        Assert.assertEquals(Long.valueOf(sessionId()), status.getEphemeralOwner());
        Assert.assertEquals(status.getEphemeralOwner(), status.getSessionId());
        Assert.assertEquals(0, status.getUnhealthyDurationMillis());
    }

    /** 节点被外部删掉时 latch 自己察觉不到（没有 watch 自身节点），状态接口要能给出原因。 */
    @Test
    public void testStatusReportsUnhealthyReason() throws Exception {
        startRegistrar(100);
        String ourPath = registrar.ourPath();
        zkClient.get().delete().forPath(ourPath);

        waitConditionUntilTimeOut(() -> {
            LeaderLatchProbeStatus s = controller.status().getData();
            return s.isUnhealthy() && s.getUnhealthyDurationMillis() > 0;
        }, 3000);
        LeaderLatchProbeStatus status = controller.status().getData();
        Assert.assertEquals(LeaderLatchProbeStatus.Reason.PATH_NOT_EXIST, status.getReason());
        Assert.assertEquals(ourPath, status.getOurPath());
        Assert.assertEquals(Boolean.FALSE, status.getPathExists());
        Assert.assertNull(status.getEphemeralOwner());
    }

    @Test
    public void testResetHealthyWithoutForceIsRejected() throws Exception {
        startRegistrar(100);
        waitConditionUntilTimeOut(() -> healthy(controller.status()), 3000);
        String ourPath = registrar.ourPath();

        RestResp<LeaderLatchProbeStatus> resp = controller.reset(false);

        Assert.assertEquals(KeeperContainerRegistrationController.CODE_REJECTED, resp.getCode());
        Assert.assertEquals(ourPath, registrar.ourPath());
        Assert.assertTrue(ownedBySession(ourPath));
    }

    /** 尚未探测出结论时同样视为「不是 UNHEALTHY」，必须带 force。 */
    @Test
    public void testResetBeforeFirstProbeWithoutForceIsRejected() throws Exception {
        startRegistrar(NEVER);
        Assert.assertEquals(LeaderLatchProbeStatus.Verdict.NOT_PROBED, controller.status().getData().getVerdict());

        Assert.assertEquals(KeeperContainerRegistrationController.CODE_REJECTED, controller.reset(false).getCode());
    }

    @Test
    public void testResetHealthyWithForceRebuilds() throws Exception {
        startRegistrar(100);
        waitConditionUntilTimeOut(() -> healthy(controller.status()), 3000);
        String oldPath = registrar.ourPath();

        RestResp<LeaderLatchProbeStatus> resp = controller.reset(true);

        Assert.assertEquals(0, resp.getCode());
        waitConditionUntilTimeOut(() -> registrar.ourPath() != null && !registrar.ourPath().equals(oldPath)
                && ownedBySession(registrar.ourPath()), 3000);
        waitConditionUntilTimeOut(() -> !exists(oldPath), 3000);
        Assert.assertEquals(latchPath(), ZKPaths.getPathAndNode(registrar.ourPath()).getPath());
        waitConditionUntilTimeOut(() -> healthy(controller.status()), 3000);
    }

    @Test
    public void testResetUnhealthyWithoutForceRebuilds() throws Exception {
        startRegistrar(100);
        String oldPath = registrar.ourPath();
        zkClient.get().delete().forPath(oldPath);
        waitConditionUntilTimeOut(() -> controller.status().getData().isUnhealthy(), 3000);

        Assert.assertEquals(0, controller.reset(false).getCode());

        waitConditionUntilTimeOut(() -> registrar.ourPath() != null && !registrar.ourPath().equals(oldPath)
                && ownedBySession(registrar.ourPath()), 3000);
        waitConditionUntilTimeOut(() -> healthy(controller.status()), 3000);
    }

    @Test
    public void testNotRunning() throws Exception {
        KeeperContainerRegistrationController noRegistrar = new KeeperContainerRegistrationController(null);
        Assert.assertEquals(KeeperContainerRegistrationController.CODE_NOT_RUNNING, noRegistrar.status().getCode());
        Assert.assertEquals(KeeperContainerRegistrationController.CODE_NOT_RUNNING, noRegistrar.reset(true).getCode());

        startRegistrar(100);
        registrar.stop();
        Assert.assertEquals(KeeperContainerRegistrationController.CODE_NOT_RUNNING, controller.status().getCode());
        Assert.assertEquals(KeeperContainerRegistrationController.CODE_NOT_RUNNING, controller.reset(true).getCode());
    }

    private void startRegistrar(long probeIntervalMilli) throws Exception {
        registrar = new KeeperContainerZkRegistrar(zkClient, HOSTNAME, IP, probeIntervalMilli, NEVER);
        registrar.initialize();
        registrar.start();
        controller = new KeeperContainerRegistrationController(registrar);
        waitConditionUntilTimeOut(() -> registrar.ourPath() != null, 5000);
    }

    private static boolean healthy(RestResp<LeaderLatchProbeStatus> resp) {
        return 0 == resp.getCode() && resp.getData().isHealthy();
    }

    private long sessionId() {
        try {
            return zkClient.get().getZookeeperClient().getZooKeeper().getSessionId();
        } catch (Exception e) {
            return -1;
        }
    }

    private boolean ownedBySession(String path) {
        try {
            Stat stat = zkClient.get().checkExists().forPath(path);
            return stat != null && stat.getEphemeralOwner() == sessionId();
        } catch (Exception e) {
            return false;
        }
    }

    private boolean exists(String path) {
        try {
            return null != zkClient.get().checkExists().forPath(path);
        } catch (Exception e) {
            return true;
        }
    }

    private String latchPath() {
        return MetaZkConfig.getKeeperContainerRegisterPath() + "/" + HOSTNAME;
    }
}
