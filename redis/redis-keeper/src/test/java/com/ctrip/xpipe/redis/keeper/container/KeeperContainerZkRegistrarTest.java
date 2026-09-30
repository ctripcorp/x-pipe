package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.keeper.container.DefaultCurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressInfo;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 注册侧基于 LeaderLatch（spec §5.5）。每个 pod 用独立 ZkClient，即独立 session。
 * <p>
 * 消费方定时兜底拉到 10 分钟，远大于用例时长：所有「切换到次节点」「重建后可解析」的断言
 * 都只能由 watch 事件驱动完成。注册侧本身已没有定时补注册。
 * <p>
 * 不测「新 pod 注册后立即切换」：按 D-I1 该行为不应存在。
 */
public class KeeperContainerZkRegistrarTest extends AbstractTest {

    private static final String HOSTNAME = "keepercontainer-pod-a-0";
    private static final String IP_A = "10.0.0.5";
    private static final String IP_B = "10.0.0.9";

    private TestingServer zkServer;
    private DefaultZkClient consumerClient;
    private DefaultCurrentDcKeeperContainerAddressService consumer;
    /** 按创建顺序记录，@After 逆序关闭：先 registrar 再 client。 */
    private final List<DefaultZkClient> podClients = new ArrayList<>();
    private final List<KeeperContainerZkRegistrar> registrars = new ArrayList<>();

    @Before
    public void beforeKeeperContainerZkRegistrarTest() throws Exception {
        zkServer = new TestingServer();
        consumerClient = newZkClient();
        consumer = new DefaultCurrentDcKeeperContainerAddressService(consumerClient, TimeUnit.MINUTES.toMillis(10));
        consumer.initialize();
        consumer.start();
    }

    @After
    public void afterKeeperContainerZkRegistrarTest() throws Exception {
        try {
            for (KeeperContainerZkRegistrar registrar : registrars) {
                if (registrar.getLifecycleState().isStarted()) registrar.stop();
            }
            for (DefaultZkClient client : podClients) {
                client.stop();
            }
            consumer.stop();
            consumerClient.stop();
        } finally {
            zkServer.close();
        }
    }

    @Test
    public void testSinglePodRegistersEphemeralLatchNodeAndResolves() throws Exception {
        DefaultZkClient podA = newPodClient();
        KeeperContainerZkRegistrar a = startRegistrar(podA, IP_A);

        String ourPath = a.ourPath();
        Assert.assertEquals("latch 子节点挂在 hostname 父节点下", path(), ZKPaths.getPathAndNode(ourPath).getPath());
        Assert.assertTrue(ZKPaths.getNodeFromPath(ourPath).contains("latch-"));

        Stat stat = podA.get().checkExists().forPath(ourPath);
        Assert.assertNotNull(stat);
        // 临时节点必须绑定本进程 session，pod 销毁后映射才会自动失效
        Assert.assertEquals(sessionId(podA), stat.getEphemeralOwner());
        Assert.assertEquals(IP_A, ipOf(podA, ourPath));

        waitConditionUntilTimeOut(() -> IP_A.equals(consumer.getIpByHostname(HOSTNAME)), 3000);
    }

    /**
     * 滚动替换期间新旧 pod 并存：两者都注册成功，互不覆盖；任何时刻启动的消费者都解析到首节点。
     */
    @Test
    public void testTwoPodsWithSameHostnameBothRegisterAndResolveToFirst() throws Exception {
        KeeperContainerZkRegistrar a = startRegistrar(newPodClient(), IP_A);
        KeeperContainerZkRegistrar b = startRegistrar(newPodClient(), IP_B);

        Assert.assertEquals(asSet(a.ourPath(), b.ourPath()), asSet(latchChildren()));
        Assert.assertEquals("后注册的 pod 不能覆盖先注册者的 data", IP_A, ipOf(consumerClient, a.ourPath()));
        Assert.assertEquals(IP_B, ipOf(consumerClient, b.ourPath()));

        waitConditionUntilTimeOut(() -> IP_A.equals(consumer.getIpByHostname(HOSTNAME)), 3000);

        // 两个 pod 都已在 ZK 上时才启动的消费者，结论必须一致
        DefaultCurrentDcKeeperContainerAddressService late =
                new DefaultCurrentDcKeeperContainerAddressService(consumerClient, TimeUnit.MINUTES.toMillis(10));
        late.initialize();
        late.start();
        try {
            waitConditionUntilTimeOut(() -> IP_A.equals(late.getIpByHostname(HOSTNAME)), 3000);
        } finally {
            late.stop();
        }
        Assert.assertEquals(IP_A, consumer.getIpByHostname(HOSTNAME));
    }

    /**
     * 首节点 session 过期：消费者由 watch 驱动落到次节点，不依赖任何定时周期。
     * 过期的 pod 自己重连后由 latch 重新注册，新 seq 更大，排到次节点之后，不会抢回首位（spec §5.5 注）。
     */
    @Test
    public void testFirstNodeSessionExpiredFallsToNextWithoutWaitingRefresh() throws Exception {
        DefaultZkClient podA = newPodClient();
        KeeperContainerZkRegistrar a = startRegistrar(podA, IP_A);
        KeeperContainerZkRegistrar b = startRegistrar(newPodClient(), IP_B);
        waitConditionUntilTimeOut(() -> IP_A.equals(consumer.getIpByHostname(HOSTNAME)), 3000);

        String oldPathA = a.ourPath();
        long oldSessionA = sessionId(podA);
        KillSession.kill(podA.get().getZookeeperClient().getZooKeeper());

        waitConditionUntilTimeOut(() -> IP_B.equals(consumer.getIpByHostname(HOSTNAME)), 15000);

        // pod A 重连后自动重新注册：新节点、新 session
        waitConditionUntilTimeOut(() -> {
            String p = a.ourPath();
            return p != null && !p.equals(oldPathA) && ownedBy(podA, p, sessionId(podA));
        }, 15000);
        Assert.assertNotEquals(oldSessionA, sessionId(podA));
        waitConditionUntilTimeOut(() -> !latchChildren().contains(oldPathA), 15000);
        Assert.assertEquals(asSet(a.ourPath(), b.ourPath()), asSet(latchChildren()));

        waitConditionUntilTimeOut(() -> consumer.getAllHostnameToIp().containsKey(HOSTNAME), 3000);
        Assert.assertEquals("重新注册的 pod seq 更大，不抢回首位", IP_B, consumer.getIpByHostname(HOSTNAME));
    }

    /**
     * stop 只删自己的 latch 节点，不动同 hostname 下别的 pod；消费者随即落到剩下的节点。
     */
    @Test
    public void testStopOnlyDeletesOwnNode() throws Exception {
        KeeperContainerZkRegistrar a = startRegistrar(newPodClient(), IP_A);
        KeeperContainerZkRegistrar b = startRegistrar(newPodClient(), IP_B);
        String pathA = a.ourPath();
        String pathB = b.ourPath();
        waitConditionUntilTimeOut(() -> IP_A.equals(consumer.getIpByHostname(HOSTNAME)), 3000);

        a.stop();

        // LeaderLatch.close 的删除是后台 guaranteed delete
        waitConditionUntilTimeOut(() -> !latchChildren().contains(pathA), 3000);
        Assert.assertEquals(Collections.singletonList(pathB), latchChildren());
        waitConditionUntilTimeOut(() -> IP_B.equals(consumer.getIpByHostname(HOSTNAME)), 3000);

        b.stop();
        waitConditionUntilTimeOut(() -> latchChildren().isEmpty(), 3000);
        // 存活判定看反向：两个 pod 都退出后 IP 不再归属；正向保留 last-known IP（tombstone，spec §5.7）
        waitConditionUntilTimeOut(() -> null == consumer.getHostnameByIp(IP_B), 3000);
        Assert.assertNull(consumer.getHostnameByIp(IP_A));
        Assert.assertEquals(IP_B, consumer.getIpByHostname(HOSTNAME));
    }

    /**
     * CURATOR-724 回归（验证 Ia 升级有效）：ZK 整体故障且数据全部丢失（缩容到 0 再恢复），
     * latch 父路径不复存在。重连后 getChildren 收到 NONODE，5.7.0 会静默丢弃、永不重建；
     * 5.8.0 起走 reset 重新注册。
     */
    @Test
    public void testReRegistersAfterWholeZkLossWithParentPathMissing() throws Exception {
        DefaultZkClient podA = newPodClient();
        KeeperContainerZkRegistrar a = startRegistrar(podA, IP_A);
        String oldPath = a.ourPath();
        long oldSession = sessionId(podA);

        int port = zkServer.getPort();
        zkServer.close();
        // 同端口起一个全新数据目录的 ZK：旧 session 与所有节点（含父路径）都不存在
        zkServer = new TestingServer(port, true);

        waitConditionUntilTimeOut(() -> {
            String p = a.ourPath();
            return p != null && !p.equals(oldPath) && ownedBy(podA, p, sessionId(podA));
        }, 30000);
        Assert.assertNotEquals(oldSession, sessionId(podA));
        Assert.assertEquals(IP_A, ipOf(podA, a.ourPath()));
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

        KeeperContainerZkRegistrar isolated = new KeeperContainerZkRegistrar(brokenClient, HOSTNAME, IP_A);
        try {
            isolated.initialize();
            isolated.start();
            Assert.assertTrue("ZK 不可用时启动仍应成功", isolated.getLifecycleState().isStarted());
            Assert.assertNull("连不上时不会有注册节点", isolated.ourPath());
        } finally {
            try {
                isolated.stop();
            } finally {
                brokenClient.stop();
            }
        }
    }

    @Test
    public void testBlankHostNameIsRejected() throws Exception {
        KeeperContainerZkRegistrar bad = new KeeperContainerZkRegistrar(newPodClient(), "", IP_A);
        try {
            bad.initialize();
            bad.start();
            Assert.fail("hostname 为空时必须显式失败，否则会注册出一个无法被反查的节点");
        } catch (Exception expected) {
            logger.info("[testBlankHostNameIsRejected]{}", expected.toString());
        }
    }

    @Test
    public void testBlankIpIsRejected() throws Exception {
        KeeperContainerZkRegistrar bad = new KeeperContainerZkRegistrar(newPodClient(), HOSTNAME, "");
        try {
            bad.initialize();
            bad.start();
            Assert.fail("IP 为空时必须显式失败，否则会注册出一个解析不出地址的节点");
        } catch (Exception expected) {
            logger.info("[testBlankIpIsRejected]{}", expected.toString());
        }
    }

    /** 启动并等 latch 子节点建好，保证多 pod 场景下 seq 顺序与启动顺序一致。 */
    private KeeperContainerZkRegistrar startRegistrar(DefaultZkClient client, String ip) throws Exception {
        KeeperContainerZkRegistrar registrar = new KeeperContainerZkRegistrar(client, HOSTNAME, ip);
        registrars.add(registrar);
        registrar.initialize();
        registrar.start();
        waitConditionUntilTimeOut(() -> registrar.ourPath() != null, 5000);
        return registrar;
    }

    private DefaultZkClient newPodClient() throws Exception {
        DefaultZkClient client = newZkClient();
        podClients.add(client);
        return client;
    }

    private DefaultZkClient newZkClient() throws Exception {
        DefaultZkClient client = new DefaultZkClient(new DefaultZkConfig(zkServer.getConnectString()));
        client.initialize();
        client.start();
        return client;
    }

    private long sessionId(DefaultZkClient client) {
        try {
            return client.get().getZookeeperClient().getZooKeeper().getSessionId();
        } catch (Exception e) {
            return -1;
        }
    }

    private boolean ownedBy(DefaultZkClient client, String path, long sessionId) {
        try {
            Stat stat = client.get().checkExists().forPath(path);
            return stat != null && stat.getEphemeralOwner() == sessionId;
        } catch (Exception e) {
            return false;
        }
    }

    /** hostname 父节点下的全部子节点完整路径，父节点不存在时为空。 */
    private List<String> latchChildren() {
        try {
            CuratorFramework client = consumerClient.get();
            if (null == client.checkExists().forPath(path())) {
                return Collections.emptyList();
            }
            List<String> paths = new ArrayList<>();
            for (String child : client.getChildren().forPath(path())) {
                paths.add(ZKPaths.makePath(path(), child));
            }
            return paths;
        } catch (Exception e) {
            logger.info("[latchChildren]{}", e.toString());
            return Collections.emptyList();
        }
    }

    private String ipOf(DefaultZkClient client, String latchPath) throws Exception {
        byte[] data = client.get().getData().forPath(latchPath);
        return Codec.DEFAULT.decode(new String(data), KeeperContainerAddressInfo.class).getIpAddress();
    }

    private String path() {
        return MetaZkConfig.getKeeperContainerRegisterPath() + "/" + HOSTNAME;
    }

    private static Set<String> asSet(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    private static Set<String> asSet(List<String> values) {
        return new HashSet<>(values);
    }
}
