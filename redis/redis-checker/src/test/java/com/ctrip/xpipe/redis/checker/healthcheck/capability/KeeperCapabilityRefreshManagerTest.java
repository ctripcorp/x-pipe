package com.ctrip.xpipe.redis.checker.healthcheck.capability;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.DefaultDcMetaChangeManager;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.KeeperCheckSelector;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractConfigCommand.REDIS_CONFIG_TYPE;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KeeperCapabilityRefreshManagerTest {

    private static final String PREPARE_WATCH = REDIS_CONFIG_TYPE.PREPARE_WATCH.getConfigName();
    private static final String PUBSUB_PARSE = REDIS_CONFIG_TYPE.PUBSUB_PARSE.getConfigName();

    private HealthCheckInstanceManager instanceManager;
    private KeeperCapabilityCache capabilityCache;
    private CheckerConfig checkerConfig;
    private ScheduledExecutorService scheduled;
    private ScheduledFuture<?> future;

    @Before
    public void setUp() {
        instanceManager = Mockito.mock(HealthCheckInstanceManager.class);
        capabilityCache = Mockito.mock(KeeperCapabilityCache.class);
        checkerConfig = Mockito.mock(CheckerConfig.class);
        scheduled = Mockito.mock(ScheduledExecutorService.class);
        future = Mockito.mock(ScheduledFuture.class);
        Mockito.doReturn(future).when(scheduled).scheduleWithFixedDelay(Mockito.any(Runnable.class),
                Mockito.anyLong(), Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testStartsImmediatelyWithConfiguredFixedDelay() throws Exception {
        Mockito.when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(60000);
        Mockito.when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        KeeperHealthCheckInstance tfs = instance("127.0.0.1", true, new RedisSession());
        Mockito.when(instanceManager.getAllKeeperInstance()).thenReturn(Collections.singletonList(tfs));
        KeeperCapabilityRefreshManager manager = manager(capabilityCache);

        manager.start();

        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(scheduled).scheduleWithFixedDelay(task.capture(), Mockito.eq(0L),
                Mockito.eq(60000L), Mockito.eq(TimeUnit.MILLISECONDS));
        task.getValue().run();
        Mockito.verify(capabilityCache).refresh(tfs);
    }

    @Test
    public void testIntervalIsClampedToOneMillisecond() throws Exception {
        Mockito.when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(0);
        KeeperCapabilityRefreshManager manager = manager(capabilityCache);

        manager.start();

        Mockito.verify(scheduled).scheduleWithFixedDelay(Mockito.any(Runnable.class), Mockito.eq(0L),
                Mockito.eq(1L), Mockito.eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testOnlyTfsAndOneFailureDoesNotBlockOthersOrNextTick() throws Exception {
        Mockito.when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(60000);
        Mockito.when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        KeeperHealthCheckInstance failing = instance("127.0.0.1", true, new RedisSession());
        KeeperHealthCheckInstance succeeding = instance("127.0.0.2", true, new RedisSession());
        KeeperHealthCheckInstance nonTfs = instance("127.0.0.3", false, new RedisSession());
        Mockito.when(instanceManager.getAllKeeperInstance())
                .thenReturn(Arrays.asList(null, failing, nonTfs, succeeding));
        Mockito.doThrow(new IllegalStateException("first failed")).doNothing()
                .when(capabilityCache).refresh(failing);
        KeeperCapabilityRefreshManager manager = manager(capabilityCache);
        manager.start();
        Runnable task = scheduledTask();

        task.run();
        task.run();

        Mockito.verify(capabilityCache, Mockito.times(2)).refresh(failing);
        Mockito.verify(capabilityCache, Mockito.times(2)).refresh(succeeding);
        Mockito.verify(capabilityCache, Mockito.never()).refresh(nonTfs);
    }

    @Test
    public void testDisabledSwitchSendsNoRefresh() throws Exception {
        Mockito.when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(60000);
        Mockito.when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(false);
        KeeperCapabilityRefreshManager manager = manager(capabilityCache);
        manager.start();

        scheduledTask().run();

        Mockito.verifyNoInteractions(instanceManager, capabilityCache);
    }

    @Test
    public void testStopCancelsAndLateTickDoesNothing() throws Exception {
        Mockito.when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(60000);
        Mockito.when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        KeeperCapabilityRefreshManager manager = manager(capabilityCache);
        manager.start();
        Runnable task = scheduledTask();

        manager.stop();
        task.run();

        Mockito.verify(future).cancel(true);
        Mockito.verify(capabilityCache).invalidateAll();
        Mockito.verifyNoInteractions(instanceManager);
        Mockito.verifyNoMoreInteractions(capabilityCache);
    }

    @Test
    public void testStopInvalidatesEvenWhenCancelFails() throws Exception {
        Mockito.when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(60000);
        IllegalStateException cancelFailure = new IllegalStateException("cancel failed");
        Mockito.doThrow(cancelFailure).when(future).cancel(true);
        KeeperCapabilityRefreshManager manager = manager(capabilityCache);
        manager.start();

        try {
            manager.stop();
            Assert.fail("cancel failure should propagate");
        } catch (IllegalStateException actual) {
            Assert.assertSame(cancelFailure, actual);
        }

        Mockito.verify(capabilityCache).invalidateAll();
    }

    @Test
    public void testStopFencesOldCallbacksAndRestartCanRefreshImmediately() throws Exception {
        Mockito.when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(60000);
        Mockito.when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        KeeperCapabilityCache realCache = new KeeperCapabilityCache();
        ControlledSession session = new ControlledSession();
        HostPort address = new HostPort("127.0.0.1", 6380);
        KeeperHealthCheckInstance instance = instance(address, "dc", true, session);
        Mockito.when(instanceManager.getAllKeeperInstance()).thenReturn(Collections.singletonList(instance));
        KeeperCapabilityRefreshManager manager = manager(realCache);
        manager.start();

        scheduledTask().run();
        Assert.assertEquals(1, session.count(PREPARE_WATCH));
        Assert.assertEquals(1, session.count(PUBSUB_PARSE));

        manager.stop();
        session.success(PREPARE_WATCH, "1");
        session.success(PUBSUB_PARSE, "1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, realCache.get(address));

        manager.start();
        scheduledTask(2).run();
        Assert.assertEquals(2, session.count(PREPARE_WATCH));
        Assert.assertEquals(2, session.count(PUBSUB_PARSE));
        session.success(PREPARE_WATCH, "1");
        session.success(PUBSUB_PARSE, "1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.SUPPORTED, realCache.get(address));
    }

    @Test
    public void testScheduleFailureRollsBackAndCanRetry() throws Exception {
        Mockito.when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(60000);
        Mockito.doThrow(new RejectedExecutionException("rejected")).doReturn(future).when(scheduled)
                .scheduleWithFixedDelay(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.anyLong(),
                        Mockito.eq(TimeUnit.MILLISECONDS));
        KeeperCapabilityRefreshManager manager = manager(capabilityCache);

        try {
            manager.start();
            Assert.fail("schedule rejection should fail start");
        } catch (RejectedExecutionException expected) {
            Assert.assertFalse(manager.isStarted());
        }

        manager.start();
        Assert.assertTrue(manager.isStarted());
        manager.stop();
        Mockito.verify(scheduled, Mockito.times(2)).scheduleWithFixedDelay(Mockito.any(Runnable.class),
                Mockito.eq(0L), Mockito.eq(60000L), Mockito.eq(TimeUnit.MILLISECONDS));
        Mockito.verify(future).cancel(true);
    }

    @Test
    public void testConcurrentStopCannotBeOvertakenByAdmittedStart() throws Exception {
        Mockito.when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(60000);
        Mockito.when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        CountDownLatch scheduling = new CountDownLatch(1);
        CountDownLatch allowSchedule = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            scheduling.countDown();
            Assert.assertTrue(allowSchedule.await(5, TimeUnit.SECONDS));
            return future;
        }).when(scheduled).scheduleWithFixedDelay(Mockito.any(Runnable.class), Mockito.anyLong(),
                Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS));
        KeeperCapabilityRefreshManager manager = manager(capabilityCache);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> start = executor.submit(() -> {
                manager.start();
                return null;
            });
            Assert.assertTrue(scheduling.await(5, TimeUnit.SECONDS));
            Future<?> stop = executor.submit(() -> {
                manager.stop();
                return null;
            });
            allowSchedule.countDown();
            start.get(5, TimeUnit.SECONDS);
            stop.get(5, TimeUnit.SECONDS);

            Assert.assertFalse(manager.isStarted());
            scheduledTask().run();
            Mockito.verify(future).cancel(true);
            Mockito.verify(capabilityCache).invalidateAll();
            Mockito.verifyNoInteractions(instanceManager);
            Mockito.verifyNoMoreInteractions(capabilityCache);
        } finally {
            allowSchedule.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testFailedAttemptRetriesOnIndependentTickNotMetaCompare() throws Exception {
        Mockito.when(checkerConfig.getKeeperCapabilityRefreshIntervalMilli()).thenReturn(60000);
        Mockito.when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        KeeperCapabilityCache realCache = new KeeperCapabilityCache();
        ControlledSession session = new ControlledSession();
        HostPort address = new HostPort("127.0.0.1", 6380);
        KeeperHealthCheckInstance instance = instance(address, "dc", true, session);
        Mockito.when(instanceManager.getAllKeeperInstance()).thenReturn(Collections.singletonList(instance));
        KeeperCapabilityRefreshManager refreshManager = manager(realCache);
        refreshManager.start();
        Runnable tick = scheduledTask();

        tick.run();
        Assert.assertEquals(1, session.count(PREPARE_WATCH));
        Assert.assertEquals(1, session.count(PUBSUB_PARSE));
        session.fail(PREPARE_WATCH, new TimeoutException("not ready"));
        session.success(PUBSUB_PARSE, "1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.UNKNOWN, realCache.get(address));

        KeeperMeta keeper = new KeeperMeta().setIp(address.getHost()).setPort(address.getPort());
        DcMeta meta = new DcMeta("dc");
        KeeperCheckSelector selector = Mockito.mock(KeeperCheckSelector.class);
        Mockito.when(selector.select(meta)).thenReturn(Collections.singletonList(keeper));
        Mockito.when(instanceManager.getKeeperInstancesByDc("dc")).thenReturn(Collections.singletonList(instance));
        DefaultDcMetaChangeManager metaManager = new DefaultDcMetaChangeManager("dc", instanceManager,
                Mockito.mock(HealthCheckEndpointFactory.class), Mockito.mock(MetaCache.class), selector, realCache);
        metaManager.compare(meta);
        metaManager.compare(meta);
        Assert.assertEquals(1, session.count(PREPARE_WATCH));
        Assert.assertEquals(1, session.count(PUBSUB_PARSE));

        tick.run();
        Assert.assertEquals(2, session.count(PREPARE_WATCH));
        Assert.assertEquals(2, session.count(PUBSUB_PARSE));
        session.success(PREPARE_WATCH, "1");
        session.success(PUBSUB_PARSE, "1");
        Assert.assertEquals(KeeperCapabilityCache.Capability.SUPPORTED, realCache.get(address));
    }

    private KeeperCapabilityRefreshManager manager(KeeperCapabilityCache cache) {
        return new KeeperCapabilityRefreshManager(instanceManager, cache, checkerConfig,
                Mockito.mock(MetaCache.class), "dc", scheduled);
    }

    private Runnable scheduledTask() {
        return scheduledTask(1);
    }

    private Runnable scheduledTask(int invocationCount) {
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(scheduled, Mockito.times(invocationCount)).scheduleWithFixedDelay(task.capture(),
                Mockito.eq(0L), Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS));
        return task.getAllValues().get(invocationCount - 1);
    }

    private KeeperHealthCheckInstance instance(String ip, boolean tfs, RedisSession session) {
        return instance(new HostPort(ip, 6380), "dc", tfs, session);
    }

    private KeeperHealthCheckInstance instance(HostPort address, String dc, boolean tfs, RedisSession session) {
        KeeperInstanceInfo info = Mockito.mock(KeeperInstanceInfo.class);
        Mockito.when(info.getHostPort()).thenReturn(address);
        Mockito.when(info.getDcId()).thenReturn(dc);
        KeeperHealthCheckInstance instance = Mockito.mock(KeeperHealthCheckInstance.class);
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        Mockito.when(instance.getRedisSession()).thenReturn(session);
        Mockito.when(instance.isTfs()).thenReturn(tfs);
        return instance;
    }

    private static final class ControlledSession extends RedisSession {
        private final Map<String, List<Callbackable<String>>> callbacks = new HashMap<>();
        private final Map<String, Integer> counts = new HashMap<>();

        @Override
        public synchronized void ConfigGet(Callbackable<String> callback, String key) {
            counts.put(key, count(key) + 1);
            callbacks.computeIfAbsent(key, unused -> new ArrayList<>()).add(callback);
        }

        private synchronized int count(String key) {
            return counts.getOrDefault(key, 0);
        }

        private void success(String key, String value) {
            take(key).success(value);
        }

        private void fail(String key, Throwable throwable) {
            take(key).fail(throwable);
        }

        private synchronized Callbackable<String> take(String key) {
            return callbacks.get(key).remove(0);
        }
    }
}
