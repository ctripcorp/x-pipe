package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.capability.KeeperCapabilityCache;
import com.ctrip.xpipe.redis.checker.healthcheck.capability.KeeperCapabilityRefreshManager;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultKeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultKeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.KeeperCheckSelector;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.DefaultKeeperSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractConfigCommand.REDIS_CONFIG_TYPE;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

import static org.mockito.Mockito.*;

/**
 * Phase KV — Checker 组件与阶段验收（AC-19 / AC-19b / AC-19c / AC-20）。selector / session manager /
 * capability cache / capability refresh manager / delay action / controller / metric listener 全用真实件，
 * 只有网络出口（RedisSession、MetricProxy）与调度器是替身；capability tick 与 check 周期手工驱动，
 * 不真实等待。真实 TFS Keeper 闭环归 Phase KE。
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class KeeperDelayCheckAcceptanceTest {

    private static final String PREPARE_WATCH = REDIS_CONFIG_TYPE.PREPARE_WATCH.getConfigName();
    private static final String PUBSUB_PARSE = REDIS_CONFIG_TYPE.PUBSUB_PARSE.getConfigName();

    private static final String CURRENT_DC = "jq";
    private static final String OTHER_ACTIVE_DC = "oy";
    private static final String CROSS_REGION_DC = "fra";
    private static final String CLUSTER = "cluster";
    private static final String SHARD = "shard";
    private static final long SHARD_DB_ID = 42L;

    private static final HostPort TFS_KEEPER = new HostPort("10.0.0.1", 6380);
    private static final HostPort NON_TFS_KEEPER = new HostPort("10.0.0.2", 6380);
    private static final HostPort CROSS_REGION_KEEPER = new HostPort("10.0.2.1", 6380);
    private static final HostPort BI_DIRECTION_KEEPER = new HostPort("10.0.3.1", 6380);
    private static final HostPort BACKUP_DC_KEEPER = new HostPort("10.0.4.1", 6380);

    @Mock private MetaCache metaCache;
    @Mock private CheckerConfig checkerConfig;
    @Mock private FoundationService foundationService;
    @Mock private MetricProxy proxy;
    @Mock private HealthCheckConfig healthCheckConfig;
    @Mock private HealthCheckInstanceManager instanceManager;
    @Mock private ScheduledExecutorService scheduled;
    @Mock private ScheduledFuture<?> capabilityFuture;
    @Mock private ExecutorService executors;

    private ControlledSession session;
    private DefaultKeeperHealthCheckInstance instance;
    private KeeperCapabilityCache capabilityCache;
    private KeeperCapabilityRefreshManager refreshManager;
    private KeeperDelayAction action;
    private final List<MetricData> points = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        session = new ControlledSession();
        instance = keeperInstance(TFS_KEEPER, true, session);

        when(foundationService.getDataCenter()).thenReturn(CURRENT_DC);
        when(foundationService.getLocalIp()).thenReturn("10.0.0.100");
        when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(60000);
        when(healthCheckConfig.checkIntervalMilli()).thenReturn(60000);
        when(healthCheckConfig.getDelayConfig(anyString(), anyString(), anyString())).thenReturn(delayConfig(1000));
        when(metaCache.isCrossRegion(anyString(), anyString())).thenReturn(false);
        when(metaCache.isCrossRegion(CROSS_REGION_DC, CURRENT_DC)).thenReturn(true);
        when(metaCache.isCrossRegion(CURRENT_DC, CROSS_REGION_DC)).thenReturn(true);
        when(instanceManager.getAllKeeperInstance()).thenReturn(Collections.singletonList(instance));
        doReturn(capabilityFuture).when(scheduled).scheduleWithFixedDelay(any(Runnable.class), anyLong(),
                anyLong(), eq(TimeUnit.MILLISECONDS));
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executors).execute(any(Runnable.class));
        doAnswer(invocation -> {
            points.add(invocation.getArgument(0, MetricData.class));
            return null;
        }).when(proxy).writeBinMultiDataPoint(any(MetricData.class));

        capabilityCache = new KeeperCapabilityCache();
        refreshManager = newRefreshManager();
        action = newAction(instance);
        refreshManager.start();
    }

    @After
    public void tearDown() throws Exception {
        if (action.getLifecycleState().isStarted()) action.stop();
        if (refreshManager.isStarted()) refreshManager.stop();
    }

    // ---------- T-KV.1 组件闭环 ----------

    @Test
    public void redisDelayMessageReachesKeeperMetricThroughRealComponents() {
        refreshTick();
        answerCapability(true, true);
        Assert.assertEquals(KeeperCapabilityCache.Capability.SUPPORTED, capabilityCache.get(TFS_KEEPER));

        driveCheck(action);
        Assert.assertEquals(1, session.callbacks.size());
        Assert.assertTrue("INIT 首轮不上报", points.isEmpty());
        session.deliver(Long.toHexString(System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(2)));

        driveCheck(action);

        Assert.assertEquals(1, points.size());
        MetricData point = points.get(0);
        Assert.assertEquals("delay", point.getMetricType());
        Assert.assertEquals("keeper", point.getTags().get("type"));
        Assert.assertEquals("0", point.getTags().get("isNew"));
        Assert.assertNull(point.getTags().get("state"));
        Assert.assertNull(point.getTags().get("srcShardId"));
        Assert.assertEquals(TFS_KEEPER, point.getHostPort());
        Assert.assertTrue("delay 必须是正的微秒值", point.getValue() > 0);
        Assert.assertEquals("Keeper 链路不发 publish", 0, session.publishCount);
        Assert.assertEquals("check 周期不得发 CONFIG GET", 1, session.configCount(PREPARE_WATCH));
        Assert.assertEquals(1, session.configCount(PUBSUB_PARSE));
    }

    @Test
    public void capabilityDowngradeClosesSubscriptionAndDropsFreshSample() {
        refreshTick();
        answerCapability(true, true);
        driveCheck(action);
        session.deliver(Long.toHexString(System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(2)));

        refreshTick();
        answerCapability(true, false);
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNSUPPORTED, capabilityCache.get(TFS_KEEPER));
        driveCheck(action);

        Assert.assertEquals(1, session.closeCount);
        Assert.assertTrue("unsupported 时不得打 delay 点", points.isEmpty());
        Assert.assertTrue(action.getLifecycleState().isStarted());
    }

    @Test
    public void lostSampleReportsRedisContractValueAsKeeperPoint() {
        when(healthCheckConfig.getDelayConfig(anyString(), anyString(), anyString())).thenReturn(delayConfig(-1001));
        refreshTick();
        answerCapability(true, true);

        driveCheck(action);
        Assert.assertTrue(points.isEmpty());
        driveCheck(action);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals(99_999_000.0, points.get(0).getValue(), 0.0);
        Assert.assertEquals("keeper", points.get(0).getTags().get("type"));
    }

    // ---------- T-KV.2 范围与隔离门禁 ----------

    @Test
    public void instanceScopeIgnoresDelaySwitchAndControllerSoftGatesNonTfs() {
        KeeperCheckSelector selector = new KeeperCheckSelector(metaCache, CURRENT_DC);
        DefaultKeeperSessionManager sessionManager = keeperSessionManager(selector);

        Set<HostPort> selected = new HashSet<>();
        selector.select(fullMeta()).forEach(keeper -> selected.add(new HostPort(keeper.getIp(), keeper.getPort())));

        // ONE_WAY ∧ Checker 位于 ActiveDc ∧ 同 Region；TFS / capability / Delay 开关都不参与
        Assert.assertEquals(new HashSet<>(Arrays.asList(TFS_KEEPER, NON_TFS_KEEPER)), selected);
        Assert.assertEquals(selected, inUseInstances(sessionManager));
        verify(checkerConfig, never()).isKeeperDelayCheckEnabled();

        KeeperDelayActionController controller = new KeeperDelayActionController();
        ControlledSession nonTfsSession = new ControlledSession();
        KeeperDelayAction nonTfsAction = newAction(keeperInstance(NON_TFS_KEEPER, false, nonTfsSession));
        Assert.assertTrue(controller.shouldCheck(instance));
        Assert.assertFalse(controller.shouldCheck(nonTfsAction.getActionInstance()));

        driveCheck(nonTfsAction);

        Assert.assertEquals("非 TFS 只 skip Action", 0, nonTfsSession.callbacks.size());
        Assert.assertEquals(0, nonTfsSession.configCount(PREPARE_WATCH));
        Assert.assertTrue(nonTfsAction.getLifecycleState().isStarted());
        Assert.assertTrue(points.isEmpty());
    }

    @Test
    public void keeperChainStaysOutOfRedisStateMachineAlertAndProxy() throws Exception {
        String chain = source("healthcheck/actions/keeperdelay/KeeperDelayAction.java")
                + source("healthcheck/actions/keeperdelay/KeeperDelayActionController.java")
                + source("healthcheck/actions/keeperdelay/KeeperMetricDelayListener.java")
                + source("healthcheck/capability/KeeperCapabilityCache.java")
                + source("healthcheck/capability/KeeperCapabilityRefreshManager.java")
                + source("healthcheck/meta/KeeperCheckSelector.java")
                + source("healthcheck/session/DefaultKeeperSessionManager.java");
        for (String forbidden : Arrays.asList("DelayPingActionCollector", "createHealthStatus", "HealthStateService",
                "AlertManager", "ProxyRegistry", "HealthEventProcessor", "migration")) {
            Assert.assertFalse("Keeper delay 链路不得触达 " + forbidden, chain.contains(forbidden));
        }

        // isTfs 快照由 Factory 在 action 装配之前写入，Controller 只读该字段
        String factory = source("healthcheck/impl/DefaultHealthCheckInstanceFactory.java");
        Assert.assertTrue(factory.contains("setTfs(isTfsKeeper("));
        Assert.assertTrue(factory.indexOf("setTfs(isTfsKeeper(") < factory.indexOf("initActionsForKeeper(instance)"));
    }

    // ---------- T-KV.3 生命周期 ----------

    @Test
    public void capabilityRecoversOnNextTickAndSwitchToggleKeepsTopology() throws Exception {
        refreshTick();
        session.fail(PREPARE_WATCH, new TimeoutException("keeper port not ready"));
        session.success(PUBSUB_PARSE, "1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, capabilityCache.get(TFS_KEEPER));
        driveCheck(action);
        Assert.assertEquals(0, session.callbacks.size());

        // Meta 无 diff，仅靠下一次独立 capability tick 恢复
        refreshTick();
        answerCapability(true, true);
        Assert.assertEquals(KeeperCapabilityCache.Capability.SUPPORTED, capabilityCache.get(TFS_KEEPER));
        driveCheck(action);
        Assert.assertEquals(1, session.callbacks.size());

        when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(false);
        driveCheck(action);
        refreshTick();
        Assert.assertEquals(1, session.closeCount);
        Assert.assertEquals("开关关闭不发能力命令", 2, session.configCount(PREPARE_WATCH));
        Assert.assertEquals(KeeperCapabilityCache.Capability.SUPPORTED, capabilityCache.get(TFS_KEEPER));
        Assert.assertSame(session, instance.getRedisSession());

        when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        driveCheck(action);
        Assert.assertEquals("重新开启后复用现有 instance 与 cache 恢复", 2, session.callbacks.size());
        Assert.assertEquals(2, session.configCount(PREPARE_WATCH));

        refreshManager.stop();
        verify(capabilityFuture).cancel(true);
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, capabilityCache.get(TFS_KEEPER));
        driveCheck(action);
        Assert.assertEquals(2, session.closeCount);

        // 停止后的在途 callback 不得恢复订阅或打点
        session.deliver(Long.toHexString(System.nanoTime()));
        driveCheck(action);
        Assert.assertEquals(2, session.callbacks.size());
        Assert.assertTrue(points.isEmpty());
    }

    @Test
    public void emptyInUseSetRecyclesEveryKeeperSession() {
        DefaultKeeperSessionManager sessionManager =
                keeperSessionManager(new KeeperCheckSelector(metaCache, CURRENT_DC));
        sessionManager.findOrCreateSession(TFS_KEEPER);
        sessionManager.findOrCreateSession(NON_TFS_KEEPER);
        Assert.assertEquals(2, sessions(sessionManager).size());

        when(metaCache.getXpipeMeta())
                .thenReturn(new XpipeMeta().addDc(new DcMeta(CURRENT_DC).setZone("SHA")));
        ReflectionTestUtils.invokeMethod(sessionManager, "removeUnusedInstances");

        Assert.assertTrue(sessions(sessionManager).isEmpty());
    }

    // ---------- T-KV.4 开关默认关闭 ----------

    @Test
    public void disabledSwitchAssemblesTopologyWithoutAnyCommand() {
        when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(false);
        KeeperCheckSelector selector = new KeeperCheckSelector(metaCache, CURRENT_DC);
        DefaultKeeperSessionManager sessionManager = keeperSessionManager(selector);
        sessionManager.findOrCreateSession(TFS_KEEPER);

        refreshTick();
        driveCheck(action);
        ReflectionTestUtils.invokeMethod(sessionManager, "removeUnusedInstances");

        Assert.assertEquals(2, selector.select(fullMeta()).size());
        Assert.assertEquals(1, sessions(sessionManager).size());
        Assert.assertEquals(1, instance.getHealthCheckActions().size());
        Assert.assertTrue(action.getLifecycleState().isStarted());
        Assert.assertEquals(0, session.configCount(PREPARE_WATCH));
        Assert.assertEquals(0, session.configCount(PUBSUB_PARSE));
        Assert.assertEquals(0, session.callbacks.size());
        Assert.assertTrue(points.isEmpty());
    }

    // ---------- T-KV.5 公共 visitor 与套件登记 ----------

    @Test
    public void publicVisitorsUntouchedAndRegisteredInCheckerSuite() throws Exception {
        for (String visitor : Arrays.asList("ShardMetaVisitor", "ShardMetaCrossRegionVisitor",
                "ShardMetaComparatorCollector")) {
            Assert.assertFalse("公共 visitor 不得引入 Keeper: " + visitor,
                    source("healthcheck/meta/" + visitor + ".java").contains("Keeper"));
        }

        File suite = new File("src/test/java/com/ctrip/xpipe/redis/checker/AllTests.java");
        String suiteSource = new String(Files.readAllBytes(suite.toPath()), StandardCharsets.UTF_8);
        Assert.assertTrue(suiteSource.contains(getClass().getSimpleName() + ".class"));
    }

    // ---------- harness ----------

    /** 生产件的测试构造器是包私有的；KV 是 test-only Phase，不为验收放宽生产可见性。 */
    private KeeperCapabilityRefreshManager newRefreshManager() throws Exception {
        Constructor<KeeperCapabilityRefreshManager> constructor = KeeperCapabilityRefreshManager.class
                .getDeclaredConstructor(HealthCheckInstanceManager.class, KeeperCapabilityCache.class,
                        CheckerConfig.class, MetaCache.class, String.class, ScheduledExecutorService.class);
        constructor.setAccessible(true);
        return constructor.newInstance(instanceManager, capabilityCache, checkerConfig, metaCache, CURRENT_DC, scheduled);
    }

    private KeeperDelayAction newAction(DefaultKeeperHealthCheckInstance target) {
        KeeperMetricDelayListener listener = new KeeperMetricDelayListener();
        ReflectionTestUtils.setField(listener, "foundationService", foundationService);
        ReflectionTestUtils.setField(listener, "metaCache", metaCache);
        ReflectionTestUtils.setField(listener, "proxy", proxy);
        KeeperDelayAction created = new KeeperDelayAction(scheduled, target, executors, foundationService,
                checkerConfig, capabilityCache);
        created.addController(new KeeperDelayActionController());
        created.addListener(listener);
        target.register(created);
        try {
            created.initialize();
            created.start();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return created;
    }

    private void driveCheck(KeeperDelayAction target) {
        target.new ScheduledHealthCheckTask().run();
    }

    private void refreshTick() {
        ReflectionTestUtils.invokeMethod(refreshManager, "refreshOnce");
    }

    private void answerCapability(boolean prepareWatch, boolean pubsubParse) {
        session.success(PREPARE_WATCH, prepareWatch ? "1" : "0");
        session.success(PUBSUB_PARSE, pubsubParse ? "1" : "0");
    }

    @SuppressWarnings("unchecked")
    private Set<HostPort> inUseInstances(DefaultKeeperSessionManager manager) {
        return (Set<HostPort>) ReflectionTestUtils.invokeMethod(manager, "getInUseInstances");
    }

    @SuppressWarnings("unchecked")
    private Map<Endpoint, RedisSession> sessions(DefaultKeeperSessionManager manager) {
        return (Map<Endpoint, RedisSession>) ReflectionTestUtils.invokeMethod(manager, "getSessions");
    }

    private DefaultKeeperHealthCheckInstance keeperInstance(HostPort address, boolean tfs, RedisSession keeperSession) {
        DefaultKeeperInstanceInfo info = new DefaultKeeperInstanceInfo(CURRENT_DC, CLUSTER, SHARD, SHARD_DB_ID,
                address, CURRENT_DC, ClusterType.ONE_WAY);
        return (DefaultKeeperHealthCheckInstance) new DefaultKeeperHealthCheckInstance()
                .setSession(keeperSession).setTfs(tfs)
                .setInstanceInfo(info).setHealthCheckConfig(healthCheckConfig);
    }

    private DefaultKeeperSessionManager keeperSessionManager(KeeperCheckSelector selector) {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = mock(XpipeNettyClientKeyedObjectPool.class);
        @SuppressWarnings("unchecked")
        SimpleObjectPool<NettyClient> clientPool = mock(SimpleObjectPool.class);
        when(keyedObjectPool.getKeyPool(any(Endpoint.class))).thenReturn(clientPool);
        DefaultKeeperSessionManager manager = new DefaultKeeperSessionManager();
        ReflectionTestUtils.setField(manager, "metaCache", metaCache);
        manager.setKeeperSelector(selector);
        manager.setKeyedObjectPool(keyedObjectPool).setScheduled(scheduled)
                .setEndpointFactory(mock(HealthCheckEndpointFactory.class));
        manager.setConfig(checkerConfig);
        when(metaCache.getXpipeMeta()).thenReturn(fullMeta());
        return manager;
    }

    /** 同 Region 的 TFS / 非 TFS Keeper 应装载；跨 Region、非 ONE_WAY、Checker 不在 ActiveDc 的都不装载。 */
    private XpipeMeta fullMeta() {
        DcMeta local = new DcMeta(CURRENT_DC).setZone("SHA");
        local.addCluster(cluster(CLUSTER, ClusterType.ONE_WAY, CURRENT_DC, SHARD, TFS_KEEPER, NON_TFS_KEEPER));
        local.addCluster(cluster("bi_cluster", ClusterType.BI_DIRECTION, CURRENT_DC, "bi_shard", BI_DIRECTION_KEEPER));
        local.addCluster(cluster("backup_cluster", ClusterType.ONE_WAY, OTHER_ACTIVE_DC, "backup_shard", BACKUP_DC_KEEPER));
        DcMeta crossRegion = new DcMeta(CROSS_REGION_DC).setZone("FRA");
        crossRegion.addCluster(cluster(CLUSTER, ClusterType.ONE_WAY, CURRENT_DC, SHARD, CROSS_REGION_KEEPER));
        return new XpipeMeta().addDc(local).addDc(crossRegion);
    }

    private ClusterMeta cluster(String clusterId, ClusterType type, String activeDc, String shardId,
                                HostPort... keepers) {
        ShardMeta shard = new ShardMeta().setId(shardId).setDbId(SHARD_DB_ID);
        for (HostPort keeper : keepers) {
            shard.addKeeper(new KeeperMeta().setIp(keeper.getHost()).setPort(keeper.getPort()));
        }
        ClusterMeta cluster = new ClusterMeta().setId(clusterId).setType(type.toString()).setActiveDc(activeDc);
        cluster.addShard(shard);
        return cluster;
    }

    private DelayConfig delayConfig(int dcHealthyDelay) {
        return new DelayConfig(CLUSTER, CURRENT_DC, CURRENT_DC)
                .setClusterLevelHealthyDelayMilli(-1).setDcLevelHealthyDelayMilli(dcHealthyDelay);
    }

    private String source(String relativePath) throws Exception {
        File file = new File("src/main/java/com/ctrip/xpipe/redis/checker/" + relativePath);
        Assert.assertTrue("source missing: " + file.getAbsolutePath(), file.isFile());
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    /** 只替换网络出口：CONFIG GET / SUBSCRIBE 的回调由用例手工驱动。 */
    private static final class ControlledSession extends RedisSession {

        private final Map<String, List<Callbackable<String>>> configCallbacks = new HashMap<>();
        private final Map<String, Integer> configCounts = new HashMap<>();
        private final Set<SubscribeCallback> callbacks = new LinkedHashSet<>();
        private int closeCount;
        private int publishCount;

        @Override
        public synchronized void ConfigGet(Callbackable<String> callback, String key) {
            configCounts.put(key, configCount(key) + 1);
            configCallbacks.computeIfAbsent(key, unused -> new ArrayList<>()).add(callback);
        }

        @Override
        public synchronized void subscribeIfAbsent(SubscribeCallback callback, String... channel) {
            callbacks.add(callback);
        }

        @Override
        public synchronized void closeSubscribedChannel(String... channel) {
            closeCount++;
        }

        @Override
        public synchronized void publish(String channel, String message) {
            publishCount++;
        }

        private synchronized int configCount(String key) {
            return configCounts.getOrDefault(key, 0);
        }

        private void success(String key, String value) {
            take(key).success(value);
        }

        private void fail(String key, Throwable throwable) {
            take(key).fail(throwable);
        }

        private synchronized Callbackable<String> take(String key) {
            List<Callbackable<String>> pending = configCallbacks.get(key);
            Assert.assertTrue("no pending CONFIG GET for " + key, pending != null && !pending.isEmpty());
            return pending.remove(0);
        }

        /** 模拟同进程 Redis DelayAction 发布的消息经 Keeper 订阅回调抵达。 */
        private synchronized void deliver(String message) {
            new ArrayList<>(callbacks).forEach(callback -> callback.message("xpipe-health-check", message));
        }
    }
}
