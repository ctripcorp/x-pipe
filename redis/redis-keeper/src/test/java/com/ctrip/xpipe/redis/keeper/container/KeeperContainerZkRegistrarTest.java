package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressInfo;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.zookeeper.data.Stat;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class KeeperContainerZkRegistrarTest extends AbstractTest {

    private static final String HOSTNAME = "keepercontainer-pod-a-0";
    private static final String IP = "10.0.0.5";

    private TestingServer zkServer;
    private DefaultZkClient zkClient;
    private KeeperContainerZkRegistrar registrar;

    @Before
    public void beforeKeeperContainerZkRegistrarTest() throws Exception {
        zkServer = new TestingServer();
        zkClient = new DefaultZkClient(new DefaultZkConfig(zkServer.getConnectString()));
        zkClient.initialize();
        zkClient.start();

        registrar = new KeeperContainerZkRegistrar(zkClient, HOSTNAME, IP);
    }

    @After
    public void afterKeeperContainerZkRegistrarTest() throws Exception {
        try {
            if (registrar.getLifecycleState().isStarted()) registrar.stop();
            zkClient.stop();
        } finally {
            zkServer.close();
        }
    }

    @Test
    public void testRegisterCreatesEphemeralNodeWithIp() throws Exception {
        registrar.initialize();
        registrar.start();

        Stat stat = zkClient.get().checkExists().forPath(path());
        Assert.assertNotNull("注册节点应存在", stat);
        // 临时节点必须绑定 session，pod 销毁后映射才会自动失效
        Assert.assertTrue("应为临时节点", stat.getEphemeralOwner() != 0);

        byte[] data = zkClient.get().getData().forPath(path());
        KeeperContainerAddressInfo info = Codec.DEFAULT.decode(new String(data), KeeperContainerAddressInfo.class);
        Assert.assertEquals(IP, info.getIpAddress());
    }

    @Test
    public void testRegisterIsIdempotentOverExistingNode() throws Exception {
        // 上一次进程残留的节点（session 尚未过期）不应阻塞本次注册
        zkClient.get().create().creatingParentsIfNeeded().forPath(path(),
                Codec.DEFAULT.encodeAsBytes(new KeeperContainerAddressInfo("10.0.0.99")));

        registrar.initialize();
        registrar.start();

        byte[] data = zkClient.get().getData().forPath(path());
        KeeperContainerAddressInfo info = Codec.DEFAULT.decode(new String(data), KeeperContainerAddressInfo.class);
        Assert.assertEquals("应覆盖为本进程的 IP", IP, info.getIpAddress());
    }

    @Test
    public void testNodeGoesAwayAfterStop() throws Exception {
        registrar.initialize();
        registrar.start();
        Assert.assertNotNull(zkClient.get().checkExists().forPath(path()));

        registrar.stop();

        Assert.assertNull("停止后注册节点应被移除", zkClient.get().checkExists().forPath(path()));
    }

    @Test
    public void testStartSucceedsEvenWhenZkUnavailable() throws Exception {
        // keeper 是数据面组件，ZK 不可用时不能让进程起不来，
        // 否则一次 ZK 故障会变成整机房 keepercontainer 的重启风暴
        DefaultZkConfig unreachable = new DefaultZkConfig("127.0.0.1:1") {
            @Override
            public int waitForZkConnectedMillis() {
                // 默认 5 秒且无 setter，本用例本就期望连不上，没必要干等
                return 200;
            }
        };
        unreachable.setZkConnectionTimeoutMillis(200);
        unreachable.setZkSessionTimeoutMillis(1000);
        unreachable.setZkRetries(1);
        DefaultZkClient brokenClient = new DefaultZkClient(unreachable);
        brokenClient.initialize();
        brokenClient.start();

        KeeperContainerZkRegistrar isolated = new KeeperContainerZkRegistrar(brokenClient, HOSTNAME, IP);
        try {
            isolated.initialize();
            isolated.start();
            Assert.assertTrue("ZK 不可用时启动仍应成功", isolated.getLifecycleState().isStarted());
        } finally {
            try {
                isolated.stop();
            } finally {
                brokenClient.stop();
            }
        }
    }

    @Test
    public void testScheduledCheckReRegistersMissingNode() throws Exception {
        registrar = new KeeperContainerZkRegistrar(zkClient, HOSTNAME, IP, 100);
        registrar.initialize();
        registrar.start();
        waitConditionUntilTimeOut(() -> nodeExists(), 3000);

        // 模拟节点意外消失（session 过期而连接状态回调未触发等情形）
        zkClient.get().delete().forPath(path());

        waitConditionUntilTimeOut(() -> nodeExists(), 5000);
        Assert.assertTrue("定时兜底应把缺失的注册节点补回来", nodeExists());
    }

    /**
     * 真正的 session 过期场景：ZK 长时间不可达导致 session 超时，临时节点被服务端删除。
     * 与"节点被误删"不同，这里连接状态会经历 SUSPENDED/LOST 再到 RECONNECTED，
     * 注册端必须在重连回调里重建节点 —— 注册节点是自己创建的，没有别的来源能补回来。
     * <p>
     * 定时兜底刻意设得远大于用例时长，确保验证的是连接状态回调这条路径。
     */
    @Test
    public void testReRegistersAfterSessionExpired() throws Exception {
        int zkPort = zkServer.getPort();
        registrar = new KeeperContainerZkRegistrar(zkClient, HOSTNAME, IP, TimeUnit.MINUTES.toMillis(10));
        registrar.initialize();
        registrar.start();
        waitConditionUntilTimeOut(this::nodeExists, 3000);

        long sessionIdBefore = zkClient.get().getZookeeperClient().getZooKeeper().getSessionId();

        // 停掉 ZK 并静默超过 session 超时，迫使服务端在恢复后判定 session 已过期
        zkServer.stop();
        sleep((int) (zkClient.get().getZookeeperClient().getZooKeeper().getSessionTimeout() * 2));
        zkServer = new TestingServer(zkPort, true);

        waitConditionUntilTimeOut(this::nodeExists, 30000);

        Assert.assertTrue("session 过期重连后注册节点必须被重建", nodeExists());
        byte[] data = zkClient.get().getData().forPath(path());
        Assert.assertEquals(IP,
                Codec.DEFAULT.decode(new String(data), KeeperContainerAddressInfo.class).getIpAddress());

        Stat stat = zkClient.get().checkExists().forPath(path());
        Assert.assertTrue("重建后仍应是临时节点", stat.getEphemeralOwner() != 0);
        Assert.assertNotEquals("节点应归属重连后的新 session，否则旧 session 过期会把它带走",
                sessionIdBefore, stat.getEphemeralOwner());
    }

    private boolean nodeExists() {
        try {
            return null != zkClient.get().checkExists().forPath(path());
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    public void testBlankHostNameIsRejected() {
        KeeperContainerZkRegistrar bad = new KeeperContainerZkRegistrar(zkClient, "", IP);
        try {
            bad.initialize();
            bad.start();
            Assert.fail("hostname 为空时必须显式失败，否则会注册出一个无法被反查的节点");
        } catch (Exception expected) {
            logger.info("[testBlankHostNameIsRejected]{}", expected.toString());
        }
    }

    @Test
    public void testBlankIpIsRejected() {
        KeeperContainerZkRegistrar bad = new KeeperContainerZkRegistrar(zkClient, HOSTNAME, "");
        try {
            bad.initialize();
            bad.start();
            Assert.fail("IP 为空时必须显式失败，否则会注册出一个解析不出地址的节点");
        } catch (Exception expected) {
            logger.info("[testBlankIpIsRejected]{}", expected.toString());
        }
    }

    private String path() {
        return MetaZkConfig.getKeeperContainerRegisterPath() + "/" + HOSTNAME;
    }
}
