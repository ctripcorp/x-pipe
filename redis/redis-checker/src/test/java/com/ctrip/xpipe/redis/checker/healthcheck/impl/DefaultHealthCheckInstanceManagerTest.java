package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.KeeperCheckSelector;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2024/9/14
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultHealthCheckInstanceManagerTest extends AbstractCheckerTest {

    @InjectMocks
    private DefaultHealthChecker healthChecker;

    @InjectMocks
    private DefaultHealthCheckInstanceManager healthCheckInstanceManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private HealthCheckInstanceFactory instanceFactory;

    @Mock
    private KeeperCheckSelector keeperSelector;

    @Mock
    private RedisHealthCheckInstance mockCheckInstance;

    @Mock
    private ClusterHealthCheckInstance mockClusterInstance;

    @Mock
    private KeeperHealthCheckInstance mockKeeperInstance;

    @Mock
    private KeeperInstanceInfo mockKeeperInfo;

    @Before
    public void setupDefaultHealthCheckInstanceManagerTest() {
        Mockito.when(instanceFactory.create(Mockito.any(RedisMeta.class))).thenReturn(mockCheckInstance);
        Mockito.when(instanceFactory.create(Mockito.any(ClusterMeta.class))).thenReturn(mockClusterInstance);
        Mockito.when(instanceFactory.getOrCreateRedisInstanceForInfoReplIdAction(Mockito.any())).thenReturn(mockCheckInstance);
        Mockito.when(instanceFactory.create(Mockito.any(KeeperMeta.class))).thenReturn(mockKeeperInstance);
        Mockito.when(mockKeeperInstance.getCheckInfo()).thenReturn(mockKeeperInfo);
        Mockito.when(mockKeeperInfo.getDcId()).thenReturn("jq");

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.doAnswer(inv -> {
            String currentDc = inv.getArgument(0, String.class);
            String otherDc = inv.getArgument(1, String.class);
            DcMeta currentDcMeta = getXpipeMeta().findDc(currentDc);
            DcMeta otherDcMeta = getXpipeMeta().findDc(otherDc);
            if (null == currentDcMeta || null == otherDcMeta) return false;
            return !currentDcMeta.getZone().equalsIgnoreCase(otherDcMeta.getZone());
        }).when(metaCache).isCrossRegion(Mockito.anyString(), Mockito.anyString());

        healthChecker.setInstanceManager(healthCheckInstanceManager);
    }

    @Test
    public void testInstanceMatch() {
        healthChecker.generateHealthCheckInstances();
        Assert.assertTrue(healthCheckInstanceManager.checkInstancesMiss(getXpipeMeta()));
    }

    @Test
    public void testInstanceDisMatch() {
        healthChecker.generateHealthCheckInstances();
        XpipeMeta xpipeMeta = getXpipeMeta();
        xpipeMeta.getDcs().get("jq").getClusters().remove("bbz_qmq_idempotent_fra_default");
        Assert.assertFalse(healthCheckInstanceManager.checkInstancesMiss(xpipeMeta));
    }

    @Test
    public void testCrossRegionInstanceMatch() {
        healthChecker.generateHealthCheckInstances();
        Assert.assertNotNull(healthCheckInstanceManager.findRedisInstanceForInfoReplIdPingAction(new HostPort("10.43.49.173", 6379)));
        Assert.assertNotNull(healthCheckInstanceManager.findRedisInstanceForInfoReplIdPingAction(new HostPort("10.56.204.175", 6379)));
        Assert.assertNotNull(healthCheckInstanceManager.findClusterHealthCheckInstance("bbz_qmq_idempotent_fra_default"));
    }

    @Test
    public void testKeeperConcurrentGetOrCreate() throws Exception {
        KeeperMeta keeper = new KeeperMeta().setIp("127.0.0.1").setPort(6379);
        int concurrency = 16;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<KeeperHealthCheckInstance>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < concurrency; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    Assert.assertTrue(start.await(5, TimeUnit.SECONDS));
                    return healthCheckInstanceManager.getOrCreate(keeper);
                }));
            }
            Assert.assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            for (Future<KeeperHealthCheckInstance> future : futures) {
                Assert.assertSame(mockKeeperInstance, future.get(5, TimeUnit.SECONDS));
            }
        } finally {
            start.countDown();
            executor.shutdownNow();
        }

        Mockito.verify(instanceFactory, Mockito.times(1)).create(keeper);
        Assert.assertEquals(1, healthCheckInstanceManager.getAllKeeperInstance().size());
        Assert.assertSame(mockKeeperInstance,
                healthCheckInstanceManager.findKeeperHealthCheckInstance(new HostPort("127.0.0.1", 6379)));
    }

    @Test
    public void testKeeperQueryByDcAndRemove() {
        KeeperMeta jqKeeper = new KeeperMeta().setIp("127.0.0.1").setPort(6379);
        KeeperMeta oyKeeper = new KeeperMeta().setIp("127.0.0.2").setPort(6380);
        KeeperHealthCheckInstance oyInstance = Mockito.mock(KeeperHealthCheckInstance.class);
        KeeperInstanceInfo oyInfo = Mockito.mock(KeeperInstanceInfo.class);
        Mockito.when(oyInstance.getCheckInfo()).thenReturn(oyInfo);
        Mockito.when(oyInfo.getDcId()).thenReturn("oy");
        Mockito.when(instanceFactory.create(oyKeeper)).thenReturn(oyInstance);

        healthCheckInstanceManager.getOrCreate(jqKeeper);
        healthCheckInstanceManager.getOrCreate(oyKeeper);

        Assert.assertEquals(Collections.singletonList(mockKeeperInstance),
                healthCheckInstanceManager.getKeeperInstancesByDc("JQ"));
        Assert.assertEquals(Collections.singletonList(oyInstance),
                healthCheckInstanceManager.getKeeperInstancesByDc("oy"));
        Assert.assertEquals(2, healthCheckInstanceManager.getAllKeeperInstance().size());

        HostPort jqAddress = new HostPort(jqKeeper.getIp(), jqKeeper.getPort());
        Assert.assertSame(mockKeeperInstance, healthCheckInstanceManager.removeKeeper(jqAddress));
        Assert.assertNull(healthCheckInstanceManager.findKeeperHealthCheckInstance(jqAddress));
        Assert.assertNull(healthCheckInstanceManager.removeKeeper(jqAddress));
        Mockito.verify(instanceFactory, Mockito.times(1)).remove(mockKeeperInstance);
        Assert.assertSame(oyInstance,
                healthCheckInstanceManager.findKeeperHealthCheckInstance(new HostPort(oyKeeper.getIp(), oyKeeper.getPort())));
    }

    @Test
    public void testKeeperRemoveFailureRetainsInstanceForRetry() {
        KeeperMeta keeper = new KeeperMeta().setIp("127.0.0.1").setPort(6379);
        HostPort address = new HostPort(keeper.getIp(), keeper.getPort());
        healthCheckInstanceManager.getOrCreate(keeper);
        Mockito.doThrow(new IllegalStateException("expected cleanup failure"))
                .doNothing().when(instanceFactory).remove(mockKeeperInstance);

        try {
            healthCheckInstanceManager.removeKeeper(address);
            Assert.fail("remove should expose cleanup failure");
        } catch (IllegalStateException expected) {
            Assert.assertSame(mockKeeperInstance,
                    healthCheckInstanceManager.findKeeperHealthCheckInstance(address));
        }

        Assert.assertSame(mockKeeperInstance, healthCheckInstanceManager.removeKeeper(address));
        Assert.assertNull(healthCheckInstanceManager.findKeeperHealthCheckInstance(address));
        Mockito.verify(instanceFactory, Mockito.times(2)).remove(mockKeeperInstance);
    }

    @Test
    public void testKeeperMissingCheckUsesSelectorWithoutCreating() {
        healthChecker.generateHealthCheckInstances();
        KeeperMeta keeper = new KeeperMeta().setIp("127.0.0.9").setPort(6380);
        Mockito.when(keeperSelector.select(Mockito.any(XpipeMeta.class)))
                .thenReturn(Collections.singletonList(keeper));
        healthCheckInstanceManager.getOrCreate(keeper);

        Assert.assertTrue(healthCheckInstanceManager.checkInstancesMiss(getXpipeMeta()));
        healthCheckInstanceManager.removeKeeper(new HostPort(keeper.getIp(), keeper.getPort()));
        Assert.assertFalse(healthCheckInstanceManager.checkInstancesMiss(getXpipeMeta()));
        Mockito.verify(instanceFactory, Mockito.times(1)).create(keeper);
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "multi-type-health-instances.xml";
    }

}
