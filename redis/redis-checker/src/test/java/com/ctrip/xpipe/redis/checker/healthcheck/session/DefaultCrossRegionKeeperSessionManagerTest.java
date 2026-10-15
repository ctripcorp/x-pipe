package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.KeeperCheckSelector;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 跨 region Keeper 会话：in-use 口径、周期回收边界、以及必须经 endpointFactory 的代理路由。
 *
 * 清理由 {@link AbstractInstanceSessionManager#removeUnusedInstances()} 直接驱动，不真实等待 4 秒周期。
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultCrossRegionKeeperSessionManagerTest {

    private static final String CURRENT_DC = "fra";
    private static final String ACTIVE_DC_CROSS_REGION = "jq";

    private static final HostPort CROSS_REGION_KEEPER_1 = new HostPort("10.0.2.1", 6380);
    private static final HostPort CROSS_REGION_KEEPER_2 = new HostPort("10.0.2.2", 6380);
    private static final HostPort SAME_REGION_KEEPER = new HostPort("10.0.1.1", 6380);
    private static final HostPort BI_DIRECTION_KEEPER = new HostPort("10.0.3.1", 6380);
    private static final HostPort BLANK_ACTIVE_DC_KEEPER = new HostPort("10.0.4.1", 6380);

    @Mock
    private MetaCache metaCache;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Mock
    private ScheduledExecutorService scheduled;

    @Mock
    private HealthCheckEndpointFactory endpointFactory;

    @SuppressWarnings("unchecked")
    private final SimpleObjectPool<NettyClient> clientPool = Mockito.mock(SimpleObjectPool.class);

    private DefaultCrossRegionKeeperSessionManager sessionManager;

    /** Mirrors the production endpoint factory: one stable Endpoint instance per address. */
    private final Map<HostPort, Endpoint> endpoints = new HashMap<>();

    @Before
    public void before() {
        Mockito.when(keyedObjectPool.getKeyPool(Mockito.any(Endpoint.class))).thenReturn(clientPool);
        Mockito.when(endpointFactory.getOrCreateEndpoint(Mockito.any(HostPort.class)))
                .thenAnswer(invocation -> {
                    HostPort hostPort = invocation.getArgument(0);
                    return endpoints.computeIfAbsent(hostPort,
                            key -> new DefaultEndPoint(key.getHost(), key.getPort()));
                });
        // same region unless explicitly told otherwise
        Mockito.when(metaCache.isCrossRegion(Mockito.anyString(), Mockito.anyString())).thenReturn(false);
        Mockito.when(metaCache.isCrossRegion(CURRENT_DC, ACTIVE_DC_CROSS_REGION)).thenReturn(true);
        Mockito.when(metaCache.isCrossRegion(ACTIVE_DC_CROSS_REGION, CURRENT_DC)).thenReturn(true);

        sessionManager = new DefaultCrossRegionKeeperSessionManager();
        sessionManager.metaCache = metaCache;
        sessionManager.currentDcId = CURRENT_DC;
        sessionManager.checkerConfig = checkerConfig;
        sessionManager.setKeyedObjectPool(keyedObjectPool)
                .setScheduled(scheduled)
                .setEndpointFactory(endpointFactory);
        sessionManager.setConfig(checkerConfig);

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(fullMeta());
    }

    // ---------- in-use 集合口径 ----------

    @Test
    public void crossRegionKeepersAreInUse() {
        Set<HostPort> inUse = sessionManager.getInUseInstances();

        // Keeper 挂在本 DC 的 cluster 条目下，但 activeDc 在别的 region：正是 InfoReplId 要读的那批
        Assert.assertEquals(hostPorts(CROSS_REGION_KEEPER_1, CROSS_REGION_KEEPER_2), inUse);
    }

    @Test
    public void sameRegionBiDirectionAndBlankActiveDcAreIgnored() {
        Set<HostPort> inUse = sessionManager.getInUseInstances();

        Assert.assertFalse(inUse.contains(SAME_REGION_KEEPER));
        Assert.assertFalse(inUse.contains(BI_DIRECTION_KEEPER));
        // activeDc 为空必须在 isCrossRegion 之前短路，不能拿 null 去查 zone
        Assert.assertFalse(inUse.contains(BLANK_ACTIVE_DC_KEEPER));
        Mockito.verify(metaCache, Mockito.never()).isCrossRegion(Mockito.eq(CURRENT_DC), Mockito.isNull());
    }

    @Test
    public void inUseIsDisjointFromSameRegionKeeperSessionManager() {
        DefaultKeeperSessionManager sameRegionManager = sameRegionKeeperSessionManager();

        Set<HostPort> crossRegion = sessionManager.getInUseInstances();
        Set<HostPort> sameRegion = sameRegionManager.getInUseInstances();

        // 两个 Manager 各管各的：跨 region 的 Keeper 绝不进 DefaultKeeperSessionManager 的 in-use
        Assert.assertEquals(hostPorts(SAME_REGION_KEEPER), sameRegion);
        Assert.assertTrue(Collections.disjoint(crossRegion, sameRegion));
    }

    // ---------- 周期回收边界 ----------

    @Test
    public void crossRegionKeeperSurvivesCleanupAndOthersAreRecycled() {
        createSessions(CROSS_REGION_KEEPER_1, SAME_REGION_KEEPER, BI_DIRECTION_KEEPER);

        sessionManager.removeUnusedInstances();

        // 改造前这些会话每 4s 就被判未使用而关闭重建；现在必须原地复用
        Assert.assertEquals(hostPorts(CROSS_REGION_KEEPER_1), sessionAddresses(sessionManager));
    }

    @Test
    public void emptyInUseRecyclesEverySession() {
        createSessions(CROSS_REGION_KEEPER_1, CROSS_REGION_KEEPER_2);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(metaWithoutCrossRegionCluster());
        Assert.assertTrue(sessionManager.getInUseInstances().isEmpty());

        sessionManager.removeUnusedInstances();

        Assert.assertTrue(sessionManager.getSessions().isEmpty());
    }

    @Test
    public void metaUnavailableIsNullAndKeepsEverySession() {
        createSessions(CROSS_REGION_KEEPER_1, SAME_REGION_KEEPER);

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(null);
        Assert.assertNull(sessionManager.getInUseInstances());
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta());
        Assert.assertNull(sessionManager.getInUseInstances());

        sessionManager.removeUnusedInstances();

        Assert.assertEquals(2, sessionManager.getSessions().size());
    }

    @Test
    public void currentDcMissingFromMetaIsNullAndKeepsEverySession() {
        createSessions(CROSS_REGION_KEEPER_1);

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(metaWithoutCurrentDc());
        Assert.assertNull(sessionManager.getInUseInstances());

        sessionManager.removeUnusedInstances();

        Assert.assertEquals(1, sessionManager.getSessions().size());
    }

    @Test
    public void repeatedMetaChurnDoesNotAccumulateSessions() {
        for (int round = 0; round < 5; round++) {
            Mockito.when(metaCache.getXpipeMeta()).thenReturn(fullMeta());
            createSessions(CROSS_REGION_KEEPER_1, CROSS_REGION_KEEPER_2);
            sessionManager.removeUnusedInstances();
            Assert.assertEquals(2, sessionManager.getSessions().size());

            Mockito.when(metaCache.getXpipeMeta()).thenReturn(metaWithoutCrossRegionCluster());
            sessionManager.removeUnusedInstances();
            Assert.assertTrue(sessionManager.getSessions().isEmpty());
        }
    }

    // ---------- 路由与空集合语义 ----------

    @Test
    public void findOrCreateSessionRoutesThroughEndpointFactory() {
        // Keeper 是本机房的，不需要 proxy；这里锁定的是端点解析走 endpointFactory（与配对 redis 一致），
        // 而不是 DefaultKeeperSessionManager 的 DefaultEndPoint 直连
        Endpoint endpoint = new DefaultEndPoint(CROSS_REGION_KEEPER_1.getHost(), CROSS_REGION_KEEPER_1.getPort());
        Mockito.when(endpointFactory.getOrCreateEndpoint(CROSS_REGION_KEEPER_1)).thenReturn(endpoint);

        RedisSession session = sessionManager.findOrCreateSession(CROSS_REGION_KEEPER_1);

        Mockito.verify(endpointFactory).getOrCreateEndpoint(CROSS_REGION_KEEPER_1);
        Assert.assertSame(session, sessionManager.getSessions().get(endpoint));
    }

    @Test
    public void emptyInUseIsAPreciseExpectation() {
        Assert.assertTrue(sessionManager.cleanUpOnEmptyInUseInstances());
    }

    // ---------- helpers ----------

    private DefaultKeeperSessionManager sameRegionKeeperSessionManager() {
        DefaultKeeperSessionManager manager = new DefaultKeeperSessionManager();
        manager.metaCache = metaCache;
        manager.currentDcId = CURRENT_DC;
        manager.checkerConfig = checkerConfig;
        manager.setKeeperSelector(new KeeperCheckSelector(metaCache, CURRENT_DC));
        manager.setKeyedObjectPool(keyedObjectPool)
                .setScheduled(scheduled)
                .setEndpointFactory(endpointFactory);
        manager.setConfig(checkerConfig);
        return manager;
    }

    private void createSessions(HostPort... hostPorts) {
        for (HostPort hostPort : hostPorts) {
            sessionManager.findOrCreateSession(hostPort);
        }
    }

    private Set<HostPort> sessionAddresses(AbstractInstanceSessionManager manager) {
        Set<HostPort> addresses = new HashSet<>();
        for (Endpoint endpoint : manager.getSessions().keySet()) {
            addresses.add(new HostPort(endpoint.getHost(), endpoint.getPort()));
        }
        return addresses;
    }

    private Set<HostPort> hostPorts(HostPort... hostPorts) {
        return new HashSet<>(Arrays.asList(hostPorts));
    }

    private XpipeMeta fullMeta() {
        XpipeMeta meta = new XpipeMeta();

        DcMeta currentDc = new DcMeta().setId(CURRENT_DC);
        currentDc.addCluster(cluster("cluster-cross", ClusterType.ONE_WAY, ACTIVE_DC_CROSS_REGION,
                shard("shard1", CROSS_REGION_KEEPER_1), shard("shard2", CROSS_REGION_KEEPER_2)));
        currentDc.addCluster(cluster("cluster-same", ClusterType.ONE_WAY, CURRENT_DC,
                shard("shard1", SAME_REGION_KEEPER)));
        currentDc.addCluster(cluster("cluster-bi", ClusterType.BI_DIRECTION, CURRENT_DC,
                shard("shard-bi", BI_DIRECTION_KEEPER)));
        currentDc.addCluster(cluster("cluster-blank", ClusterType.ONE_WAY, null,
                shard("shard-blank", BLANK_ACTIVE_DC_KEEPER)));
        meta.addDc(currentDc);

        return meta;
    }

    private XpipeMeta metaWithoutCrossRegionCluster() {
        XpipeMeta meta = new XpipeMeta();

        DcMeta currentDc = new DcMeta().setId(CURRENT_DC);
        currentDc.addCluster(cluster("cluster-same", ClusterType.ONE_WAY, CURRENT_DC,
                shard("shard1", SAME_REGION_KEEPER)));
        meta.addDc(currentDc);

        return meta;
    }

    private XpipeMeta metaWithoutCurrentDc() {
        XpipeMeta meta = new XpipeMeta();

        DcMeta otherDc = new DcMeta().setId("oy");
        otherDc.addCluster(cluster("cluster-cross", ClusterType.ONE_WAY, ACTIVE_DC_CROSS_REGION,
                shard("shard1", CROSS_REGION_KEEPER_1)));
        meta.addDc(otherDc);

        return meta;
    }

    private ClusterMeta cluster(String clusterId, ClusterType type, String activeDc, ShardMeta... shards) {
        ClusterMeta clusterMeta = new ClusterMeta().setId(clusterId);
        clusterMeta.setType(type.toString());
        clusterMeta.setActiveDc(activeDc);
        for (ShardMeta shard : shards) {
            clusterMeta.addShard(shard);
        }
        return clusterMeta;
    }

    private ShardMeta shard(String shardId, HostPort... keepers) {
        ShardMeta shardMeta = new ShardMeta().setId(shardId);
        for (HostPort keeper : keepers) {
            shardMeta.addKeeper(new KeeperMeta().setIp(keeper.getHost()).setPort(keeper.getPort()));
        }
        return shardMeta;
    }
}
