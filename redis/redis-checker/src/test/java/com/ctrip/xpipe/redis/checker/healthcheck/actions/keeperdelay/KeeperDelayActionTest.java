package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayAction;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.healthcheck.capability.KeeperCapabilityCache;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultKeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultKeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KeeperDelayActionTest {

    private static final String CURRENT_DC = "dc1";
    private static final String CLUSTER = "cluster";
    private static final String SHARD = "shard";
    private static final String KEEPER_IP = "127.0.0.1";
    private static final int KEEPER_PORT = 6379;
    private static final String LOCAL_IP = "10.0.0.1";
    private static final long SHARD_DB_ID = 42L;

    @Mock private RedisSession session;
    @Mock private HealthCheckConfig config;
    @Mock private CheckerConfig checkerConfig;
    @Mock private FoundationService foundationService;
    @Mock private KeeperCapabilityCache capabilityCache;
    @Mock private ScheduledExecutorService scheduled;
    @Mock private ScheduledFuture<Object> scheduledFuture;
    @Mock private ExecutorService executors;
    @Mock private KeeperDelayActionListener listener;

    private DefaultKeeperHealthCheckInstance instance;
    private KeeperDelayAction action;

    @Before
    public void setUp() throws Exception {
        DefaultKeeperInstanceInfo info = new DefaultKeeperInstanceInfo(CURRENT_DC, CLUSTER, SHARD,
                SHARD_DB_ID, new HostPort(KEEPER_IP, KEEPER_PORT), CURRENT_DC, ClusterType.ONE_WAY);
        instance = new DefaultKeeperHealthCheckInstance();
        instance.setInstanceInfo(info);
        instance.setSession(session);
        instance.setTfs(true);
        instance.setHealthCheckConfig(config);

        when(foundationService.getDataCenter()).thenReturn(CURRENT_DC);
        when(foundationService.getLocalIp()).thenReturn(LOCAL_IP);
        when(config.checkIntervalMilli()).thenReturn(60_000);
        when(config.getDelayConfig(anyString(), anyString(), anyString())).thenReturn(delayConfig(1000));
        when(checkerConfig.isKeeperDelayCheckEnabled()).thenReturn(true);
        when(capabilityCache.getIfPresent(info.getHostPort()))
                .thenReturn(KeeperCapabilityCache.Capability.SUPPORTED);
        doReturn(scheduledFuture).when(scheduled)
                .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executors).execute(any(Runnable.class));
        when(listener.worksfor(any())).thenReturn(true);
        when(listener.supportInstance(instance)).thenReturn(true);

        action = new KeeperDelayAction(scheduled, instance, executors, foundationService,
                checkerConfig, capabilityCache);
        action.addListener(listener);
        action.initialize();
        action.start();
    }

    @After
    public void tearDown() throws Exception {
        if (action.getLifecycleState().isStarted()) {
            action.stop();
        }
    }

    @Test
    public void testMessageChannelAndReadOnlyCapabilityPath() {
        action.doTask();
        verify(listener, never()).onAction(any());

        ArgumentCaptor<RedisSession.SubscribeCallback> callback =
                ArgumentCaptor.forClass(RedisSession.SubscribeCallback.class);
        verify(session).subscribeIfAbsent(callback.capture(), eq(action.subscribeChannel()));
        callback.getValue().message(action.subscribeChannel(), Long.toHexString(System.nanoTime() - 1_000_000));

        action.doTask();
        action.doTask();
        ArgumentCaptor<KeeperDelayActionContext> contexts = ArgumentCaptor.forClass(KeeperDelayActionContext.class);
        verify(listener, times(2)).onAction(contexts.capture());
        Assert.assertTrue(contexts.getAllValues().get(0).getResult() > 0);

        RedisHealthCheckInstance redisInstance = mock(RedisHealthCheckInstance.class);
        RedisInstanceInfo redisInfo = mock(RedisInstanceInfo.class);
        when(redisInstance.getCheckInfo()).thenReturn(redisInfo);
        when(redisInfo.getShardDbId()).thenReturn(SHARD_DB_ID);
        DelayAction redisAction = new DelayAction(scheduled, redisInstance, executors,
                mock(PingService.class), foundationService);
        Assert.assertEquals(ReflectionTestUtils.getField(redisAction, "publish_channel"), action.subscribeChannel());

        verify(capabilityCache, times(3)).getIfPresent(instance.getCheckInfo().getHostPort());
        verify(capabilityCache, never()).refresh(any());
        verify(session, never()).publish(anyString(), anyString());
        verify(session, never()).ConfigGet(any(), anyString());
    }

    /** 从未收到过样本时超时不上报任何点（不再上报 SAMPLE_LOST）。 */
    @Test
    public void testInitThenExpireWithoutPublisherReportsNothing() {
        when(config.getDelayConfig(anyString(), anyString(), anyString())).thenReturn(delayConfig(-1001));

        action.doTask();
        action.doTask();
        action.doTask();

        verify(listener, never()).onAction(any());
    }

    /** 超时后延迟取「当前时间 - 上一个样本的 recvTimeMilli」，随失联时长攀升，而非 SAMPLE_LOST。 */
    @SuppressWarnings("unchecked")
    @Test
    public void testExpireReportsElapsedSinceLastSample() {
        action.doTask();
        ArgumentCaptor<RedisSession.SubscribeCallback> callback =
                ArgumentCaptor.forClass(RedisSession.SubscribeCallback.class);
        verify(session).subscribeIfAbsent(callback.capture(), eq(action.subscribeChannel()));
        callback.getValue().message(action.subscribeChannel(), Long.toHexString(System.nanoTime() - 1_000_000));

        // 上一个样本在 5s 前到达，超过 1s 的过期窗口
        AtomicReference<KeeperDelayActionContext> context =
                (AtomicReference<KeeperDelayActionContext>) ReflectionTestUtils.getField(action, "context");
        Assert.assertNotNull(context);
        ReflectionTestUtils.setField(context.get(), "recvTimeMilli", System.currentTimeMillis() - 5000, long.class);
        action.doTask();

        ReflectionTestUtils.setField(context.get(), "recvTimeMilli", System.currentTimeMillis() - 8000, long.class);
        action.doTask();

        ArgumentCaptor<KeeperDelayActionContext> contexts = ArgumentCaptor.forClass(KeeperDelayActionContext.class);
        verify(listener, times(2)).onAction(contexts.capture());
        KeeperDelayActionContext first = contexts.getAllValues().get(0);
        KeeperDelayActionContext second = contexts.getAllValues().get(1);
        Assert.assertSame(instance, first.instance());
        assertBetweenMillis(5000, first.getResult());
        assertBetweenMillis(8000, second.getResult());
        Assert.assertTrue("失联越久延迟越大", second.getResult() > first.getResult());
        Assert.assertNotSame("超时点是新样本，带当前时间戳", context.get(), second);
    }

    /** 允许测试执行本身耗时 1s 内的误差。 */
    private void assertBetweenMillis(long expectedMilli, long actualNanos) {
        long actualMilli = TimeUnit.NANOSECONDS.toMillis(actualNanos);
        Assert.assertTrue("expected ~" + expectedMilli + "ms but was " + actualMilli + "ms",
                actualMilli >= expectedMilli && actualMilli < expectedMilli + 1000);
    }

    @Test
    public void testCapabilityDowngradeInvalidatesOldCallback() {
        action.doTask();
        ArgumentCaptor<RedisSession.SubscribeCallback> callback =
                ArgumentCaptor.forClass(RedisSession.SubscribeCallback.class);
        verify(session).subscribeIfAbsent(callback.capture(), eq(action.subscribeChannel()));
        when(capabilityCache.getIfPresent(instance.getCheckInfo().getHostPort()))
                .thenReturn(KeeperCapabilityCache.Capability.UNSUPPORTED);

        action.doTask();
        callback.getValue().message(action.subscribeChannel(), Long.toHexString(System.nanoTime() - 1_000_000));
        when(capabilityCache.getIfPresent(instance.getCheckInfo().getHostPort()))
                .thenReturn(KeeperCapabilityCache.Capability.SUPPORTED);
        action.doTask();

        verify(session).closeSubscribedChannel(action.subscribeChannel());
        verify(session, times(2)).subscribeIfAbsent(any(), eq(action.subscribeChannel()));
        verify(listener, never()).onAction(any());
        verify(session, never()).ConfigGet(any(), anyString());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStoppedActionIgnoresOldCallback() throws Exception {
        action.doTask();
        ArgumentCaptor<RedisSession.SubscribeCallback> callback =
                ArgumentCaptor.forClass(RedisSession.SubscribeCallback.class);
        verify(session).subscribeIfAbsent(callback.capture(), eq(action.subscribeChannel()));

        action.stop();
        callback.getValue().message(action.subscribeChannel(), Long.toHexString(System.nanoTime() - 1_000_000));

        AtomicReference<KeeperDelayActionContext> context =
                (AtomicReference<KeeperDelayActionContext>) ReflectionTestUtils.getField(action, "context");
        Assert.assertNotNull(context);
        Assert.assertSame(KeeperDelayAction.INIT_CONTEXT, context.get());
        verify(listener, never()).onAction(any());
    }

    @Test
    public void testStopClosesSubscriptionStartedByInFlightTask() throws Exception {
        CountDownLatch subscribeEntered = new CountDownLatch(1);
        CountDownLatch allowSubscribe = new CountDownLatch(1);
        AtomicBoolean subscribed = new AtomicBoolean();
        doAnswer(invocation -> {
            subscribeEntered.countDown();
            Assert.assertTrue(allowSubscribe.await(5, TimeUnit.SECONDS));
            subscribed.set(true);
            return null;
        }).when(session).subscribeIfAbsent(any(), eq(action.subscribeChannel()));
        doAnswer(invocation -> {
            subscribed.set(false);
            return null;
        }).when(session).closeSubscribedChannel(action.subscribeChannel());

        Thread taskThread = new Thread(action::doTask);
        Thread stopThread = new Thread(() -> {
            try {
                action.stop();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        taskThread.start();
        Assert.assertTrue(subscribeEntered.await(5, TimeUnit.SECONDS));
        stopThread.start();
        allowSubscribe.countDown();
        taskThread.join(5000);
        stopThread.join(5000);

        Assert.assertFalse(taskThread.isAlive());
        Assert.assertFalse(stopThread.isAlive());
        Assert.assertFalse(subscribed.get());
    }

    @Test
    public void testControllerAndFactoryOnlyInstallKeeperDelayChain() throws Exception {
        KeeperDelayActionController controller = new KeeperDelayActionController();
        action.addController(controller);

        instance.setTfs(false);
        action.new ScheduledHealthCheckTask().run();
        verify(capabilityCache, never()).getIfPresent(any());
        verify(session, never()).subscribeIfAbsent(any(), any(String[].class));
        Assert.assertTrue(action.getLifecycleState().isStarted());

        instance.setTfs(true);
        action.new ScheduledHealthCheckTask().run();
        verify(capabilityCache).getIfPresent(instance.getCheckInfo().getHostPort());
        verify(session).subscribeIfAbsent(any(), eq(action.subscribeChannel()));

        KeeperDelayActionFactory factory = new KeeperDelayActionFactory();
        ReflectionTestUtils.setField(factory, "scheduled", scheduled);
        ReflectionTestUtils.setField(factory, "executors", executors);
        ReflectionTestUtils.setField(factory, "foundationService", foundationService);
        ReflectionTestUtils.setField(factory, "capabilityCache", capabilityCache);
        ReflectionTestUtils.setField(factory, "controller", controller);
        ReflectionTestUtils.setField(factory, "listeners", Collections.singletonList(listener));

        KeeperDelayAction created = factory.create(instance);
        Assert.assertEquals(Collections.singletonList(listener), created.getListeners());
        Assert.assertEquals(Collections.singletonList(controller), created.getControllers());
        factory.destroy(created);
    }

    private DelayConfig delayConfig(int dcHealthyDelay) {
        return new DelayConfig(CLUSTER, CURRENT_DC, CURRENT_DC)
                .setClusterLevelHealthyDelayMilli(-1)
                .setDcLevelHealthyDelayMilli(dcHealthyDelay);
    }
}
