package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

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
}
