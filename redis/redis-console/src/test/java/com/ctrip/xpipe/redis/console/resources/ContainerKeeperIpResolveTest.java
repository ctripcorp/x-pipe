package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.entity.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 容器 Keeper 地址的两段填充：Console 侧把实时 IP 写入 KeeperContainerMeta，
 * Checker 侧按 keepercontainer_id 据此补齐 Keeper 自身的 IP。
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class ContainerKeeperIpResolveTest {

    private static final String DC = "jq";
    private static final String CLUSTER = "cluster1";
    private static final String SHARD = "shard1";
    private static final String PLACEHOLDER = XPipeConsoleConstant.DEFAULT_IP;

    @Mock private CrossDcKeeperContainerAddressService addressService;

    private DefaultMetaCache metaCache;

    @Before
    public void setUp() {
        metaCache = new DefaultMetaCache();
        metaCache.setKeeperContainerAddressService(addressService);
    }

    // ---------- Console 侧：填 KeeperContainerMeta.ip ----------

    @Test
    public void testContainerIpFilledByHostname() {
        when(addressService.getAllHostnameToIp(DC)).thenReturn(hostnameToIp("pod-a", "10.0.0.1"));
        DcMeta dcMeta = dcMeta(container(1L, "pod-a", PLACEHOLDER), container(2L, "", "10.9.9.9"));

        createMeta(dcMeta);

        Assert.assertEquals("10.0.0.1", dcMeta.getKeeperContainers().get(0).getIp());
        // 物理机 hostname 为空，IP 原样保留
        Assert.assertEquals("10.9.9.9", dcMeta.getKeeperContainers().get(1).getIp());
    }

    /** 远端机房快照未就绪、容器已下线等情况下查不到，保留占位 IP，不得让整轮加载失败。 */
    @Test
    public void testUnresolvedContainerKeepsPlaceholder() {
        when(addressService.getAllHostnameToIp(DC)).thenReturn(hostnameToIp("other-pod", "10.0.0.2"));
        DcMeta dcMeta = dcMeta(container(1L, "pod-a", PLACEHOLDER));

        createMeta(dcMeta);

        Assert.assertEquals(PLACEHOLDER, dcMeta.getKeeperContainers().get(0).getIp());
    }

    @Test
    public void testAddressServiceFailureDoesNotBreakLoad() {
        when(addressService.getAllHostnameToIp(DC)).thenThrow(new RuntimeException("zk down"));
        DcMeta dcMeta = dcMeta(container(1L, "pod-a", PLACEHOLDER));

        XpipeMeta result = createMeta(dcMeta);

        Assert.assertNotNull(result);
        Assert.assertEquals(PLACEHOLDER, dcMeta.getKeeperContainers().get(0).getIp());
    }

    /** 无地址服务时也不能静默跳过 —— 有容器却填不上必须可观测。 */
    @Test
    public void testNoAddressServiceStillReportsUnresolved() {
        metaCache.setKeeperContainerAddressService(null);
        DcMeta dcMeta = dcMeta(container(1L, "pod-a", PLACEHOLDER));

        createMeta(dcMeta);

        Assert.assertEquals(PLACEHOLDER, dcMeta.getKeeperContainers().get(0).getIp());
    }

    /** 纯物理机机房（hostname 全空）不该去取地址缓存。 */
    @Test
    public void testPhysicalOnlyDcSkipsAddressLookup() {
        DcMeta dcMeta = dcMeta(container(1L, "", "10.9.9.9"), container(2L, "", "10.9.9.10"));

        createMeta(dcMeta);

        verify(addressService, never()).getAllHostnameToIp(anyString());
        Assert.assertEquals("10.9.9.9", dcMeta.getKeeperContainers().get(0).getIp());
    }

    /** 有容器时才取缓存 —— 与上一条互为对照。 */
    @Test
    public void testContainerPresentTriggersAddressLookup() {
        when(addressService.getAllHostnameToIp(DC)).thenReturn(hostnameToIp("pod-a", "10.0.0.1"));
        DcMeta dcMeta = dcMeta(container(1L, "pod-a", PLACEHOLDER), container(2L, "", "10.9.9.9"));

        createMeta(dcMeta);

        verify(addressService).getAllHostnameToIp(DC);
    }

    // ---------- Checker 侧：按 container id 补齐 KeeperMeta.ip ----------

    @Test
    public void testKeeperIpResolvedFromContainerById() {
        DcMeta dcMeta = dcMeta(container(1L, "pod-a", "10.0.0.1"), container(2L, "pod-b", "10.0.0.2"));
        // 两个容器 Keeper 共享占位 IP，只有 container id 能区分
        KeeperMeta first = containerKeeper("pod-a", 1L);
        KeeperMeta second = containerKeeper("pod-b", 2L);
        addShard(dcMeta, first, second);

        metaCache.resolveContainerKeeperIp(new XpipeMeta().addDc(dcMeta));

        Assert.assertEquals("10.0.0.1", first.getIp());
        Assert.assertEquals("10.0.0.2", second.getIp());
    }

    @Test
    public void testPhysicalKeeperIpUntouched() {
        DcMeta dcMeta = dcMeta(container(1L, "", "10.9.9.9"));
        KeeperMeta physical = new KeeperMeta().setIp("10.9.9.9").setPort(6380);
        physical.setKeeperContainerId(1L);
        addShard(dcMeta, physical);

        metaCache.resolveContainerKeeperIp(new XpipeMeta().addDc(dcMeta));

        Assert.assertEquals("10.9.9.9", physical.getIp());
    }

    /**
     * 容器 IP 自身未解析成功时不得把占位值当作有效地址 —— 用一个非占位的旧 IP 做哨兵，
     * 否则「写入占位」与「不写入」无法区分。
     */
    @Test
    public void testPlaceholderContainerIpNotPropagated() {
        DcMeta dcMeta = dcMeta(container(1L, "pod-a", PLACEHOLDER));
        KeeperMeta keeper = containerKeeper("pod-a", 1L);
        keeper.setIp("10.1.1.1");
        addShard(dcMeta, keeper);

        metaCache.resolveContainerKeeperIp(new XpipeMeta().addDc(dcMeta));

        Assert.assertEquals("占位 IP 不得覆盖 Keeper 现有地址", "10.1.1.1", keeper.getIp());
    }

    /** 一个容器承载多个 Keeper 时，补不到地址只按 hostname 打一次，不得刷出成片打点。 */
    @Test
    public void testUnresolvedKeepersDedupedByHostname() {
        DcMeta dcMeta = dcMeta(container(1L, "pod-a", PLACEHOLDER));
        // 同一容器上的三个 Keeper，分属不同 shard
        ShardMeta first = new ShardMeta().setId("shard1");
        first.addKeeper(containerKeeper("pod-a", 1L));
        first.addKeeper(containerKeeper("pod-a", 1L));
        ShardMeta second = new ShardMeta().setId("shard2");
        second.addKeeper(containerKeeper("pod-a", 1L));
        ClusterMeta clusterMeta = new ClusterMeta().setId(CLUSTER);
        clusterMeta.addShard(first);
        clusterMeta.addShard(second);
        dcMeta.addCluster(clusterMeta);

        // EventMonitor.DEFAULT 是静态常量，无法注入替身；直接验证去重集合的构造结果
        Set<String> unresolved = new LinkedHashSet<>();
        Map<Long, String> containerIpById = new HashMap<>();
        containerIpById.put(1L, PLACEHOLDER);
        for (ShardMeta shardMeta : Arrays.asList(first, second)) {
            for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
                ReflectionTestUtils.invokeMethod(metaCache, "resolveContainerKeeperIp",
                        dcMeta, clusterMeta, shardMeta, keeperMeta, containerIpById, unresolved);
            }
        }

        Assert.assertEquals("三个 Keeper 同属一个容器，只应去重出一项",
                Collections.singleton("pod-a"), unresolved);
    }

    @Test
    public void testMissingContainerLeavesKeeperUnchanged() {
        DcMeta dcMeta = dcMeta(container(1L, "pod-a", "10.0.0.1"));
        // 指向不存在的容器
        KeeperMeta keeper = containerKeeper("pod-x", 99L);
        addShard(dcMeta, keeper);

        metaCache.resolveContainerKeeperIp(new XpipeMeta().addDc(dcMeta));

        Assert.assertEquals(PLACEHOLDER, keeper.getIp());
    }

    // ---------- helpers ----------

    private XpipeMeta createMeta(DcMeta... dcMetas) {
        return (XpipeMeta) ReflectionTestUtils.invokeMethod(metaCache, "createXpipeMeta",
                Arrays.asList(dcMetas), Collections.<RedisCheckRuleMeta>emptyList());
    }

    private Map<String, String> hostnameToIp(String hostname, String ip) {
        Map<String, String> map = new HashMap<>();
        map.put(hostname, ip);
        return map;
    }

    private DcMeta dcMeta(KeeperContainerMeta... containers) {
        DcMeta dcMeta = new DcMeta().setId(DC);
        for (KeeperContainerMeta container : containers) {
            dcMeta.addKeeperContainer(container);
        }
        return dcMeta;
    }

    private KeeperContainerMeta container(Long id, String hostname, String ip) {
        KeeperContainerMeta meta = new KeeperContainerMeta();
        meta.setId(id);
        meta.setHostname(hostname);
        meta.setIp(ip);
        meta.setPort(8080);
        return meta;
    }

    private KeeperMeta containerKeeper(String hostname, Long containerId) {
        KeeperMeta keeperMeta = new KeeperMeta().setIp(PLACEHOLDER).setPort(6380);
        keeperMeta.setHostname(hostname);
        keeperMeta.setKeeperContainerId(containerId);
        return keeperMeta;
    }

    private void addShard(DcMeta dcMeta, KeeperMeta... keepers) {
        ShardMeta shardMeta = new ShardMeta().setId(SHARD);
        for (KeeperMeta keeper : keepers) {
            shardMeta.addKeeper(keeper);
        }
        ClusterMeta clusterMeta = new ClusterMeta().setId(CLUSTER);
        clusterMeta.addShard(shardMeta);
        dcMeta.addCluster(clusterMeta);
    }

}
