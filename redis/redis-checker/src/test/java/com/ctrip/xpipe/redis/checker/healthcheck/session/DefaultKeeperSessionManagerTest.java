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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Phase KS — Keeper session 保活与空集合回收。
 *
 * 清理由 {@link AbstractInstanceSessionManager#removeUnusedInstances()} 直接驱动，不真实等待 4 秒周期。
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultKeeperSessionManagerTest {

    private static final String CURRENT_DC = "jq";
    private static final String SAME_REGION_DC = "oy";
    private static final String CROSS_REGION_DC = "fra";

    private static final HostPort LOCAL_KEEPER_1 = new HostPort("10.0.0.1", 6380);
    private static final HostPort LOCAL_KEEPER_2 = new HostPort("10.0.0.2", 6380);
    private static final HostPort SAME_REGION_KEEPER = new HostPort("10.0.1.1", 6380);
    private static final HostPort CROSS_REGION_KEEPER = new HostPort("10.0.2.1", 6380);
    private static final HostPort BI_DIRECTION_KEEPER = new HostPort("10.0.3.1", 6380);

    private static final HostPort SHARED_ADDRESS = new HostPort("10.0.9.9", 6379);

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

    private DefaultKeeperSessionManager keeperSessionManager;

    private DefaultRedisSessionManager redisSessionManager;

    @Before
    public void before() {
        Mockito.when(keyedObjectPool.getKeyPool(Mockito.any(Endpoint.class))).thenReturn(clientPool);
        // same region unless explicitly told otherwise
        Mockito.when(metaCache.isCrossRegion(Mockito.anyString(), Mockito.anyString())).thenReturn(false);
        Mockito.when(metaCache.isCrossRegion(CROSS_REGION_DC, CURRENT_DC)).thenReturn(true);
        Mockito.when(metaCache.isCrossRegion(CURRENT_DC, CROSS_REGION_DC)).thenReturn(true);

        keeperSessionManager = new DefaultKeeperSessionManager();
        keeperSessionManager.metaCache = metaCache;
        keeperSessionManager.checkerConfig = checkerConfig;
        keeperSessionManager.setKeeperSelector(new KeeperCheckSelector(metaCache, CURRENT_DC));
        prepareManager(keeperSessionManager);

        redisSessionManager = new DefaultRedisSessionManager();
        redisSessionManager.metaCache = metaCache;
        redisSessionManager.checkerConfig = checkerConfig;
        prepareManager(redisSessionManager);

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(fullMeta());
    }

    private void prepareManager(AbstractInstanceSessionManager manager) {
        manager.setKeyedObjectPool(keyedObjectPool)
                .setScheduled(scheduled)
                .setEndpointFactory(endpointFactory);
        manager.setConfig(checkerConfig);
    }

    // ---------- T-KS.1 in-use 集合口径 ----------

    @Test
    public void inUseInstancesFollowLoadingSelector() {
        Set<HostPort> inUse = keeperSessionManager.getInUseInstances();

        // ONE_WAY + Checker 位于 ActiveDc + Keeper DC 与 ActiveDc 同 Region 才保活；
        // selector 不看 TFS，两个本 DC Keeper 都不带 keeperContainerId 也照样命中
        Assert.assertEquals(hostPorts(LOCAL_KEEPER_1, LOCAL_KEEPER_2, SAME_REGION_KEEPER), inUse);
        Assert.assertFalse(inUse.contains(CROSS_REGION_KEEPER));
        Assert.assertFalse(inUse.contains(BI_DIRECTION_KEEPER));
    }

    @Test
    public void metaUnavailableIsNullAndKeepsEverySession() {
        createKeeperSessions(LOCAL_KEEPER_1, CROSS_REGION_KEEPER);

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(null);
        Assert.assertNull(keeperSessionManager.getInUseInstances());
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta());
        Assert.assertNull(keeperSessionManager.getInUseInstances());

        keeperSessionManager.removeUnusedInstances();

        Assert.assertEquals(2, keeperSessionManager.getSessions().size());
    }

    // ---------- T-KS.3 周期回收 ----------

    @Test
    public void targetKeeperSurvivesCleanupAndOthersAreRecycled() {
        createKeeperSessions(LOCAL_KEEPER_1, SAME_REGION_KEEPER, CROSS_REGION_KEEPER, BI_DIRECTION_KEEPER);

        keeperSessionManager.removeUnusedInstances();

        Assert.assertEquals(hostPorts(LOCAL_KEEPER_1, SAME_REGION_KEEPER), sessionAddresses(keeperSessionManager));
    }

    @Test
    public void emptyInUseRecyclesEveryKeeperSession() {
        createKeeperSessions(LOCAL_KEEPER_1, LOCAL_KEEPER_2);
        // 最后一个目标 Keeper 也从 Meta 摘掉：in-use 是空集合而不是 null
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(metaWithoutAnyLoadableKeeper());
        Assert.assertTrue(keeperSessionManager.getInUseInstances().isEmpty());

        keeperSessionManager.removeUnusedInstances();

        Assert.assertTrue(keeperSessionManager.getSessions().isEmpty());
    }

    @Test
    public void emptyInUseKeepsEveryRedisSession() {
        // v1.74 回归：DC 存在但 shard 里没有 redis 时 Redis in-use 是空集合，Redis 侧必须维持现网语义
        Endpoint redisEndpoint = new DefaultEndPoint(SHARED_ADDRESS.getHost(), SHARED_ADDRESS.getPort());
        redisSessionManager.findOrCreateSession(redisEndpoint);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(metaWithoutAnyLoadableKeeper());
        Assert.assertTrue(redisSessionManager.getInUseInstances().isEmpty());

        redisSessionManager.removeUnusedInstances();

        Assert.assertEquals(1, redisSessionManager.getSessions().size());
        Assert.assertFalse(redisSessionManager.cleanUpOnEmptyInUseInstances());
        Assert.assertTrue(keeperSessionManager.cleanUpOnEmptyInUseInstances());
    }

    @Test
    public void sameHostPortRedisAndKeeperUseIndependentSessions() {
        Endpoint sharedEndpoint = new DefaultEndPoint(SHARED_ADDRESS.getHost(), SHARED_ADDRESS.getPort());
        Mockito.when(endpointFactory.getOrCreateEndpoint(SHARED_ADDRESS)).thenReturn(sharedEndpoint);

        RedisSession redisSession = redisSessionManager.findOrCreateSession(SHARED_ADDRESS);
        RedisSession keeperSession = keeperSessionManager.findOrCreateSession(SHARED_ADDRESS);
        Assert.assertNotSame(redisSession, keeperSession);

        // 该地址不在 Keeper selector 命中集合里，Keeper 侧回收它
        keeperSessionManager.removeUnusedInstances();

        Assert.assertTrue(keeperSessionManager.getSessions().isEmpty());
        Assert.assertEquals(1, redisSessionManager.getSessions().size());
        Assert.assertSame(redisSession, redisSessionManager.getSessions().get(sharedEndpoint));
    }

    // ---------- T-KS.4 add-only 契约、反复增删与异常隔离 ----------

    @Test
    public void sameAddressRemoveThenAddReusesSession() {
        // 同地址 Keeper 字段变更走 remove-before-add：地址仍在 in-use 集合里，session 必须原地复用，
        // 不能被 instance 拆除顺手关掉再重连
        Endpoint endpoint = new DefaultEndPoint(LOCAL_KEEPER_1.getHost(), LOCAL_KEEPER_1.getPort());
        RedisSession before = Mockito.mock(RedisSession.class);
        keeperSessionManager.getSessions().put(endpoint, before);

        keeperSessionManager.removeUnusedInstances();
        RedisSession after = keeperSessionManager.findOrCreateSession(LOCAL_KEEPER_1);

        Assert.assertSame(before, after);
        Mockito.verify(before, Mockito.never()).closeConnection();
    }

    @Test
    public void sessionManagerIsAddOnly() {
        // 回收只有周期任务一个 owner：Keeper Manager 不得暴露 release / remove / close 类 API
        for (Method method : KeeperSessionManager.class.getMethods()) {
            String name = method.getName().toLowerCase();
            Assert.assertFalse("KeeperSessionManager must stay add-only, found: " + method.getName(),
                    name.contains("release") || name.contains("remove") || name.contains("close"));
        }
    }

    @Test
    public void repeatedMetaChurnDoesNotAccumulateSessions() {
        for (int round = 0; round < 5; round++) {
            Mockito.when(metaCache.getXpipeMeta()).thenReturn(fullMeta());
            createKeeperSessions(LOCAL_KEEPER_1, LOCAL_KEEPER_2, SAME_REGION_KEEPER);
            keeperSessionManager.removeUnusedInstances();
            Assert.assertEquals(3, keeperSessionManager.getSessions().size());

            Mockito.when(metaCache.getXpipeMeta()).thenReturn(metaWithoutAnyLoadableKeeper());
            keeperSessionManager.removeUnusedInstances();
            Assert.assertTrue(keeperSessionManager.getSessions().isEmpty());
        }
    }

    @Test
    public void delayCheckSwitchDoesNotChangeSessionSet() {
        createKeeperSessions(LOCAL_KEEPER_1, LOCAL_KEEPER_2, SAME_REGION_KEEPER);

        // doReturn/when 不算一次 mock 调用，后面的 never() 才有意义
        Mockito.doReturn(false).when(checkerConfig).isKeeperDelayCheckEnabled();
        keeperSessionManager.removeUnusedInstances();
        Set<HostPort> whenDisabled = sessionAddresses(keeperSessionManager);

        Mockito.doReturn(true).when(checkerConfig).isKeeperDelayCheckEnabled();
        keeperSessionManager.removeUnusedInstances();
        Set<HostPort> whenEnabled = sessionAddresses(keeperSessionManager);

        Assert.assertEquals(hostPorts(LOCAL_KEEPER_1, LOCAL_KEEPER_2, SAME_REGION_KEEPER), whenDisabled);
        Assert.assertEquals(whenDisabled, whenEnabled);
        // in-use 判定完全不读 Delay 开关
        Mockito.verify(checkerConfig, Mockito.never()).isKeeperDelayCheckEnabled();
    }

    @Test
    public void closeFailureDoesNotBlockOtherRecycles() {
        Endpoint failing = new DefaultEndPoint(CROSS_REGION_KEEPER.getHost(), CROSS_REGION_KEEPER.getPort());
        Endpoint healthy = new DefaultEndPoint(BI_DIRECTION_KEEPER.getHost(), BI_DIRECTION_KEEPER.getPort());
        RedisSession failingSession = Mockito.mock(RedisSession.class);
        RedisSession healthySession = Mockito.mock(RedisSession.class);
        Mockito.doThrow(new RuntimeException("close boom")).when(failingSession).closeConnection();
        keeperSessionManager.getSessions().put(failing, failingSession);
        keeperSessionManager.getSessions().put(healthy, healthySession);

        keeperSessionManager.removeUnusedInstances();

        Assert.assertTrue(keeperSessionManager.getSessions().isEmpty());
        Mockito.verify(failingSession).closeConnection();
        Mockito.verify(healthySession).closeConnection();
    }

    // ---------- helpers ----------

    private void createKeeperSessions(HostPort... hostPorts) {
        for (HostPort hostPort : hostPorts) {
            keeperSessionManager.findOrCreateSession(hostPort);
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
        currentDc.addCluster(oneWayCluster("cluster1", shardWithKeepers("shard1", LOCAL_KEEPER_1, LOCAL_KEEPER_2)));
        currentDc.addCluster(biDirectionCluster("cluster-bi", shardWithKeepers("shard-bi", BI_DIRECTION_KEEPER)));
        meta.addDc(currentDc);

        DcMeta sameRegionDc = new DcMeta().setId(SAME_REGION_DC);
        sameRegionDc.addCluster(oneWayCluster("cluster1", shardWithKeepers("shard1", SAME_REGION_KEEPER)));
        meta.addDc(sameRegionDc);

        DcMeta crossRegionDc = new DcMeta().setId(CROSS_REGION_DC);
        crossRegionDc.addCluster(oneWayCluster("cluster1", shardWithKeepers("shard1", CROSS_REGION_KEEPER)));
        meta.addDc(crossRegionDc);

        return meta;
    }

    /**
     * DC 仍在，但没有任何会被装载的 Keeper，也没有任何 redis —— 两个 Manager 的 in-use 都是空集合。
     */
    private XpipeMeta metaWithoutAnyLoadableKeeper() {
        XpipeMeta meta = new XpipeMeta();

        DcMeta currentDc = new DcMeta().setId(CURRENT_DC);
        currentDc.addCluster(biDirectionCluster("cluster-bi", shardWithKeepers("shard-bi", BI_DIRECTION_KEEPER)));
        meta.addDc(currentDc);

        return meta;
    }

    private ClusterMeta oneWayCluster(String clusterId, ShardMeta shardMeta) {
        ClusterMeta clusterMeta = new ClusterMeta().setId(clusterId);
        clusterMeta.setType(ClusterType.ONE_WAY.toString());
        clusterMeta.setActiveDc(CURRENT_DC);
        clusterMeta.addShard(shardMeta);
        return clusterMeta;
    }

    private ClusterMeta biDirectionCluster(String clusterId, ShardMeta shardMeta) {
        ClusterMeta clusterMeta = new ClusterMeta().setId(clusterId);
        clusterMeta.setType(ClusterType.BI_DIRECTION.toString());
        clusterMeta.setActiveDc(CURRENT_DC);
        clusterMeta.setDcs(CURRENT_DC);
        clusterMeta.addShard(shardMeta);
        return clusterMeta;
    }

    private ShardMeta shardWithKeepers(String shardId, HostPort... keepers) {
        ShardMeta shardMeta = new ShardMeta().setId(shardId);
        for (HostPort keeper : keepers) {
            shardMeta.addKeeper(new KeeperMeta().setIp(keeper.getHost()).setPort(keeper.getPort()));
        }
        return shardMeta;
    }
}
