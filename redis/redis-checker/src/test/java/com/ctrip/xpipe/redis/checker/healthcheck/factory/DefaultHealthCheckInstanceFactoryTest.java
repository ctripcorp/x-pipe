package com.ctrip.xpipe.redis.checker.healthcheck.factory;

import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay.KeeperDelayActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay.KeeperDelayActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckInstanceFactory;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class DefaultHealthCheckInstanceFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    protected DefaultHealthCheckInstanceFactory factory;

    @Autowired
    private DefaultHealthCheckEndpointFactory endpointFactory;

    private MetaCache metaCache;

    private MetaCache oldMetaCache;

    private TestKeeperActionFactory keeperActionFactory;

    @Before
    public void beforeDefaultHealthCheckRedisInstanceFactoryTest() {
        oldMetaCache = endpointFactory.getMetaCache();
        metaCache = mock(MetaCache.class);
        endpointFactory.setMetaCache(metaCache);
        keeperActionFactory = new TestKeeperActionFactory(false);
        factory.setKeeperHealthCheckActionFactories(Collections.singletonList(keeperActionFactory));
    }

    @After
    public void afterDefaultHealthCheckRedisInstanceFactoryTest() {
        factory.setKeeperHealthCheckActionFactories(Collections.emptyList());
        endpointFactory.setMetaCache(oldMetaCache);
    }

    @Test
    public void testCreate() {
        RedisMeta redisMeta = normalRedisMeta();
        when(metaCache.getDc(new HostPort(redisMeta.getIp(), redisMeta.getPort()))).thenReturn("oy");
        RedisHealthCheckInstance instance = factory.create(redisMeta);

        Assert.assertNotNull(instance.getEndpoint());
        Assert.assertNotNull(instance.getHealthCheckConfig());
        Assert.assertNotNull(instance.getCheckInfo());
        Assert.assertNotNull(instance.getCheckInfo().getRedisCheckRules());
        Assert.assertNotNull(instance.getRedisSession());

        Assert.assertEquals(instance.getEndpoint(), new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort()));
        Assert.assertTrue(instance.getLifecycleState().isStarted());
        factory.remove(instance);
    }

    @Test
    public void testCreateKeeper() {
        KeeperMeta keeperMeta = normalKeeperMeta();
        KeeperHealthCheckInstance first = factory.create(keeperMeta);
        ShardMeta keeperShard = keeperMeta.parent();
        ClusterMeta keeperCluster = keeperShard.parent();
        DcMeta dcMeta = keeperCluster.parent();
        dcMeta.getKeeperContainers().get(0).setDiskType("DEFAULT");
        KeeperHealthCheckInstance second = factory.create(keeperMeta);

        Assert.assertEquals(new DefaultEndPoint(keeperMeta.getIp(), keeperMeta.getPort()), first.getEndpoint());
        Assert.assertEquals(first.getEndpoint(), second.getEndpoint());
        Assert.assertSame(first.getRedisSession(), second.getRedisSession());
        Assert.assertNotNull(first.getHealthCheckConfig());
        Assert.assertTrue(first.isTfs());
        Assert.assertFalse(second.isTfs());
        Assert.assertEquals(1, first.getHealthCheckActions().size());
        Assert.assertEquals(1, second.getHealthCheckActions().size());
        Assert.assertTrue(first.getLifecycleState().isStarted());
        Assert.assertTrue(second.getLifecycleState().isStarted());
        TestKeeperAction firstAction = (TestKeeperAction) first.getHealthCheckActions().get(0);
        Assert.assertTrue(firstAction.getLifecycleState().isStarted());

        KeeperInstanceInfo info = first.getCheckInfo();
        Assert.assertEquals("cluster", info.getClusterId());
        Assert.assertEquals(42, info.getClusterOrgId());
        Assert.assertEquals("shard", info.getShardId());
        Assert.assertEquals(Long.valueOf(100L), info.getShardDbId());
        Assert.assertEquals("jq", info.getDcId());
        Assert.assertEquals("oy", info.getActiveDc());
        Assert.assertEquals(ClusterType.ONE_WAY, info.getClusterType());
        Assert.assertEquals(new HostPort("127.0.0.1", 6380), info.getHostPort());
        Assert.assertEquals("normal", info.getStatus());
        Assert.assertFalse(RedisHealthCheckInstance.class.isAssignableFrom(first.getClass()));
        Assert.assertFalse(RedisInstanceInfo.class.isAssignableFrom(info.getClass()));
        Assert.assertFalse(Arrays.stream(KeeperInstanceInfo.class.getMethods())
                .anyMatch(method -> "getCreateTime".equals(method.getName())));

        factory.remove(first);
        Assert.assertTrue(first.getLifecycleState().isStopped());
        Assert.assertTrue(firstAction.getLifecycleState().isStopped());
        Assert.assertTrue(first.getHealthCheckActions().isEmpty());
        Assert.assertNull(first.getEndpoint());
        Assert.assertNull(first.getRedisSession());
        factory.remove(second);
    }

    /**
     * D47 / AC-19d：TFS 判定是「keeperContainerId → 同 DcMeta 的 KeeperContainer」点查，container 缺失时只能静默
     * 退化为非 TFS（Controller 直接 skip，既不打点也不告警）。故分片 Meta 必须整份下发 container，本例锁住该失败模式。
     */
    @Test
    public void testKeeperNotTfsWhenItsContainerMissingFromDcMeta() {
        KeeperMeta keeperMeta = normalKeeperMeta();
        DcMeta dcMeta = ((ClusterMeta) keeperMeta.parent().parent()).parent();
        dcMeta.getKeeperContainers().clear();

        KeeperHealthCheckInstance instance = factory.create(keeperMeta);

        Assert.assertFalse(instance.isTfs());
        factory.remove(instance);
    }

    @Test
    public void testKeeperCreateFailureRollsBack() {
        TestKeeperActionFactory failingFactory = new TestKeeperActionFactory(true);
        factory.setKeeperHealthCheckActionFactories(Collections.singletonList(failingFactory));

        try {
            factory.create(normalKeeperMeta());
            Assert.fail("Keeper create should fail when its action cannot start");
        } catch (IllegalStateException expected) {
            Assert.assertNotNull(expected.getCause());
        }

        Assert.assertEquals(1, failingFactory.actions.size());
        TestKeeperAction action = failingFactory.actions.get(0);
        KeeperHealthCheckInstance instance = action.getActionInstance();
        Assert.assertTrue(action.destroyed);
        Assert.assertFalse(action.resourceAllocated);
        Assert.assertTrue(action.disposed);
        Assert.assertTrue(action.getLifecycleState().isDisposed());
        Assert.assertTrue(instance.getLifecycleState().isDisposed());
        Assert.assertTrue(instance.getHealthCheckActions().isEmpty());
        Assert.assertNull(instance.getEndpoint());
        Assert.assertNull(instance.getRedisSession());
    }

    @Test
    public void testKeeperInitializeFailureUsesFactoryDestroy() {
        TestKeeperActionFactory failingFactory = new TestKeeperActionFactory(true, false, false);
        factory.setKeeperHealthCheckActionFactories(Collections.singletonList(failingFactory));

        try {
            factory.create(normalKeeperMeta());
            Assert.fail("Keeper create should fail when its action cannot initialize");
        } catch (IllegalStateException expected) {
            Assert.assertNotNull(expected.getCause());
        }

        TestKeeperAction action = failingFactory.actions.get(0);
        KeeperHealthCheckInstance instance = action.getActionInstance();
        Assert.assertTrue(action.destroyed);
        Assert.assertFalse(action.resourceAllocated);
        Assert.assertTrue(instance.getHealthCheckActions().isEmpty());
        Assert.assertNull(instance.getEndpoint());
        Assert.assertNull(instance.getRedisSession());
    }

    @Test
    public void testKeeperRemoveFailureRetainsCleanupOwnership() {
        TestKeeperActionFactory failingFactory = new TestKeeperActionFactory(false, false, true);
        factory.setKeeperHealthCheckActionFactories(Collections.singletonList(failingFactory));
        KeeperHealthCheckInstance instance = factory.create(normalKeeperMeta());
        TestKeeperAction action = failingFactory.actions.get(0);

        try {
            factory.remove(instance);
            Assert.fail("Keeper remove should expose action stop failure");
        } catch (IllegalStateException expected) {
            Assert.assertNotNull(expected.getCause());
        }

        Assert.assertSame(action, instance.getHealthCheckActions().get(0));
        Assert.assertNotNull(instance.getEndpoint());
        Assert.assertNotNull(instance.getRedisSession());
        Assert.assertTrue(action.getLifecycleState().isStarted());
        Assert.assertFalse(action.destroyed);

        action.failOnStop = false;
        factory.remove(instance);
        Assert.assertTrue(action.destroyed);
        Assert.assertTrue(instance.getHealthCheckActions().isEmpty());
        Assert.assertNull(instance.getEndpoint());
        Assert.assertNull(instance.getRedisSession());
    }

    @Test
    public void testKeeperGenericChainAndAssemblySourceIsolation() throws Exception {
        String keeperTypes = source("healthcheck/KeeperHealthCheckActionFactory.java")
                + source("healthcheck/actions/keeperdelay/KeeperDelayActionContext.java")
                + source("healthcheck/actions/keeperdelay/KeeperDelayActionListener.java");
        for (String forbidden : Arrays.asList("RedisHealthCheckInstance", "RedisInstanceInfo", "DelayActionContext")) {
            Assert.assertFalse("Keeper generic chain must not reference " + forbidden,
                    Pattern.compile("\\b" + Pattern.quote(forbidden) + "\\b").matcher(keeperTypes).find());
        }

        String assembly = source("healthcheck/impl/DefaultHealthCheckInstanceFactory.java");
        Assert.assertTrue(assembly.contains("initActionsForKeeper"));
        for (String forbidden : Arrays.asList("DelayPingActionCollector", "createHealthStatus", "HealthStateService", "AlertManager")) {
            Assert.assertFalse("Keeper assembly must not enter " + forbidden, assembly.contains(forbidden));
        }

        String keeperListenerType = Arrays.toString(KeeperDelayActionListener.class.getGenericInterfaces());
        String redisListenerType = Arrays.toString(DelayActionListener.class.getGenericInterfaces());
        Assert.assertFalse(keeperListenerType.contains("Redis"));
        Assert.assertFalse(redisListenerType.contains("Keeper"));

        KeeperHealthCheckInstance instance = mock(KeeperHealthCheckInstance.class);
        KeeperDelayActionContext context = new KeeperDelayActionContext(instance, 123L);
        Assert.assertSame(instance, context.instance());
        Assert.assertEquals(Long.valueOf(123L), context.getResult());
    }

    @Test
    public void testCreateRedisInstanceInfoWithCreateTime() {
        long createTimeMillis = System.currentTimeMillis();
        RedisMeta redisMeta = normalRedisMeta().setCreateTime(createTimeMillis);
        when(metaCache.getDc(new HostPort(redisMeta.getIp(), redisMeta.getPort()))).thenReturn("oy");

        RedisHealthCheckInstance instance = factory.create(redisMeta);

        Assert.assertEquals(new java.util.Date(createTimeMillis), instance.getCheckInfo().getCreateTime());
        factory.remove(instance);
    }

    @Test
    public void testKeeperRemovalDoesNotAffectRedisProxyAtSameAddress() {
        XpipeMeta meta = new XpipeMeta();
        DcMeta local = newDcMeta(FoundationService.DEFAULT.getDataCenter());
        meta.addDc(local);
        DcMeta target = newDcMeta("target");
        RedisMeta redisMeta = target.getClusters().get("cluster").getShards().get("shard").getRedises().get(0);
        meta.addDc(target);

        String routeInfo = "PROXYTCP://127.0.0.1:8008,PROXYTCP://127.0.0.1:8009";
        local.addRoute(new RouteMeta().setSrcDc(FoundationService.DEFAULT.getDataCenter())
                .setDstDc("target").setTag(Route.TAG_CONSOLE).setRouteInfo(routeInfo).setIsPublic(true)
                .setClusterType("").setOrgId(0));

        ClusterMeta clusterMeta = redisMeta.parent().parent();
        HostPort address = new HostPort(redisMeta.getIp(), redisMeta.getPort());
        when(metaCache.getCurrentDcConsoleRoutes()).thenReturn(local.getRoutes());
        when(metaCache.getXpipeMeta()).thenReturn(meta);
        when(metaCache.findMetaDesc(address))
                .thenReturn(new XpipeMetaManager.MetaDesc(clusterMeta.parent(), clusterMeta, redisMeta.parent(), redisMeta));
        when(metaCache.getDc(address)).thenReturn("target");

        endpointFactory.updateRoutes();
        RedisHealthCheckInstance redisInstance = factory.create(redisMeta);
        Assert.assertTrue(redisInstance.getEndpoint() instanceof DefaultEndPoint);
        Assert.assertEquals(AbstractRedisCommand.PROXYED_REDIS_CONNECTION_COMMAND_TIME_OUT_MILLI,
                redisInstance.getRedisSession().getCommandTimeOut());
        KeeperMeta keeperMeta = normalKeeperMeta().setIp(redisMeta.getIp()).setPort(redisMeta.getPort());
        KeeperHealthCheckInstance keeperInstance = null;
        try {
            ProxyResourceManager redisProxy = ProxyRegistry.getProxy(address.getHost(), address.getPort());
            Assert.assertNotNull(redisProxy);

            keeperInstance = factory.create(keeperMeta);
            Assert.assertSame(redisProxy, ProxyRegistry.getProxy(address.getHost(), address.getPort()));
            Assert.assertNotSame(redisInstance.getRedisSession(), keeperInstance.getRedisSession());

            factory.remove(keeperInstance);
            keeperInstance = null;
            Assert.assertSame(redisProxy, ProxyRegistry.getProxy(address.getHost(), address.getPort()));
            Assert.assertSame(redisInstance.getEndpoint(), endpointFactory.getOrCreateEndpoint(redisMeta));
        } finally {
            if (keeperInstance != null) {
                factory.remove(keeperInstance);
            }
            factory.remove(redisInstance);
        }
    }

    private String source(String relativePath) throws Exception {
        File file = new File("src/main/java/com/ctrip/xpipe/redis/checker/" + relativePath);
        Assert.assertTrue("source missing: " + file.getAbsolutePath(), file.isFile());
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    protected DcMeta newDcMeta(String dcId) {
        DcMeta dcMeta = new DcMeta().setId(dcId);
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster").setParent(dcMeta)
                .setType(ClusterType.ONE_WAY.toString()).setOrgId(0);
        dcMeta.addCluster(clusterMeta);
        ShardMeta shardMeta = new ShardMeta().setParent(clusterMeta).setId("shard");
        clusterMeta.addShard(shardMeta);
        RedisMeta redisMeta = new RedisMeta().setParent(shardMeta).setIp("localhost").setPort(randomPort());
        shardMeta.addRedis(redisMeta);
        return dcMeta;
    }

    protected RedisMeta normalRedisMeta() {
        DcMeta dcMeta = new DcMeta().setId("dc");
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster").setParent(dcMeta)
                .setType(ClusterType.ONE_WAY.toString()).setOrgId(0).setActiveRedisCheckRules("0,1");
        ShardMeta shardMeta = new ShardMeta().setParent(clusterMeta).setId("shard");
        return new RedisMeta().setParent(shardMeta).setIp("localhost").setPort(randomPort());
    }

    protected KeeperMeta normalKeeperMeta() {
        DcMeta dcMeta = new DcMeta().setId("jq");
        long keeperContainerId = 1L;
        dcMeta.addKeeperContainer(new KeeperContainerMeta().setId(keeperContainerId).setDiskType("TFS"));
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster").setType(ClusterType.ONE_WAY.toString())
                .setActiveDc("oy").setOrgId(42).setStatus("normal");
        dcMeta.addCluster(clusterMeta);
        ShardMeta shardMeta = new ShardMeta().setId("shard").setDbId(100L);
        clusterMeta.addShard(shardMeta);
        KeeperMeta keeperMeta = new KeeperMeta().setIp("127.0.0.1").setPort(6380)
                .setKeeperContainerId(keeperContainerId)
                .setActive(true).setMaster("127.0.0.2:6379");
        shardMeta.addKeeper(keeperMeta);
        return keeperMeta;
    }

    private static class TestKeeperActionFactory implements KeeperHealthCheckActionFactory<TestKeeperAction> {

        private final boolean failOnInitialize;

        private final boolean failOnStart;

        private final boolean failOnStop;

        private final List<TestKeeperAction> actions = new ArrayList<>();

        private TestKeeperActionFactory(boolean failOnStart) {
            this(false, failOnStart, false);
        }

        private TestKeeperActionFactory(boolean failOnInitialize, boolean failOnStart, boolean failOnStop) {
            this.failOnInitialize = failOnInitialize;
            this.failOnStart = failOnStart;
            this.failOnStop = failOnStop;
        }

        @Override
        public TestKeeperAction create(KeeperHealthCheckInstance instance) {
            TestKeeperAction action = new TestKeeperAction(instance, failOnInitialize, failOnStart, failOnStop);
            actions.add(action);
            return action;
        }

        @Override
        public void destroy(TestKeeperAction action) throws Exception {
            action.destroyFromFactory();
        }
    }

    private static class TestKeeperAction extends AbstractLifecycle implements HealthCheckAction<KeeperHealthCheckInstance> {

        private final KeeperHealthCheckInstance instance;

        private final boolean failOnInitialize;

        private final boolean failOnStart;

        private boolean failOnStop;

        private boolean resourceAllocated;

        private boolean stopped;

        private boolean disposed;

        private boolean destroyed;

        private TestKeeperAction(KeeperHealthCheckInstance instance, boolean failOnInitialize,
                                 boolean failOnStart, boolean failOnStop) {
            this.instance = instance;
            this.failOnInitialize = failOnInitialize;
            this.failOnStart = failOnStart;
            this.failOnStop = failOnStop;
        }

        @Override
        protected void doInitialize() throws Exception {
            resourceAllocated = true;
            if (failOnInitialize) {
                throw new IllegalStateException("expected initialize failure");
            }
        }

        @Override
        protected void doStart() throws Exception {
            if (failOnStart) {
                throw new IllegalStateException("expected start failure");
            }
        }

        @Override
        protected void doStop() {
            if (failOnStop) {
                throw new IllegalStateException("expected stop failure");
            }
            stopped = true;
        }

        @Override
        protected void doDispose() {
            disposed = true;
        }

        private void destroyFromFactory() throws Exception {
            if (getLifecycleState().canStop()) {
                stop();
            }
            if (getLifecycleState().canDispose()) {
                dispose();
            }
            resourceAllocated = false;
            destroyed = true;
        }

        @Override
        public void addListener(HealthCheckActionListener listener) {
        }

        @Override
        public void removeListener(HealthCheckActionListener listener) {
        }

        @Override
        public void addListeners(List<HealthCheckActionListener> listeners) {
        }

        @Override
        public void addController(HealthCheckActionController controller) {
        }

        @Override
        public void addControllers(List<HealthCheckActionController> controllers) {
        }

        @Override
        public void removeController(HealthCheckActionController controller) {
        }

        @Override
        public KeeperHealthCheckInstance getActionInstance() {
            return instance;
        }
    }
}
