package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultKeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultKeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.keeperstate.KeeperStateCache;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KeeperStateActionTest {

    private static final HostPort KEEPER = new HostPort("127.0.0.1", 6380);

    @Mock private RedisSession session;
    @Mock private HealthCheckConfig config;
    @Mock private CheckerConfig checkerConfig;
    @Mock private KeeperStateActionListener listener;
    @Mock private ScheduledExecutorService scheduled;
    @Mock private ScheduledFuture<Object> scheduledFuture;
    @Mock private ExecutorService executors;

    private DefaultKeeperHealthCheckInstance instance;
    private KeeperStateAction action;

    @Before
    public void setUp() throws Exception {
        instance = new DefaultKeeperHealthCheckInstance();
        instance.setInstanceInfo(new DefaultKeeperInstanceInfo("jq", "cluster", "shard", 1L, KEEPER, "jq",
                ClusterType.ONE_WAY));
        instance.setSession(session);
        instance.setTfs(true);
        instance.setHealthCheckConfig(config);

        when(checkerConfig.getKeeperStateCheckIntervalMilli()).thenReturn(5000);
        doReturn(scheduledFuture).when(scheduled)
                .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        lenient().doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executors).execute(any(Runnable.class));
        lenient().when(listener.worksfor(any())).thenReturn(true);

        action = new KeeperStateAction(scheduled, instance, executors, checkerConfig);
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

    /** 周期取专用配置，不跟 Redis 复制检查的 checkIntervalMilli 走；KeeperStateCache 过期按同一配置计算。 */
    @Test
    public void testScheduledWithKeeperStateInterval() {
        verify(scheduled).scheduleWithFixedDelay(any(Runnable.class), anyLong(), eq(5000L),
                eq(TimeUnit.MILLISECONDS));
        verify(config, never()).checkIntervalMilli();
    }

    /** 误配为 0 / 负数时兜底到 1s，避免高频刷命令或调度报错。 */
    @Test
    public void testIntervalClampedToMinimum() throws Exception {
        when(checkerConfig.getKeeperStateCheckIntervalMilli()).thenReturn(0);
        KeeperStateAction clamped = new KeeperStateAction(scheduled, instance, executors, checkerConfig);
        clamped.initialize();
        clamped.start();
        try {
            verify(scheduled).scheduleWithFixedDelay(any(Runnable.class), anyLong(),
                    eq((long) KeeperStateAction.MIN_CHECK_INTERVAL_MILLI), eq(TimeUnit.MILLISECONDS));
        } finally {
            clamped.stop();
        }
    }

    /** 成功与失败都通知 listener，并带上请求发出时间供消费方判断新旧。 */
    @Test
    public void testNotifiesSuccessAndFailureWithRequestTime() {
        when(checkerConfig.isKeeperStateCheckEnabled()).thenReturn(true);

        long before = System.currentTimeMillis();
        action.doTask();
        action.doTask();
        long after = System.currentTimeMillis();
        List<Callbackable<KeeperState>> callbacks = captureCallbacks(2);

        callbacks.get(0).success(KeeperState.ACTIVE);
        callbacks.get(1).fail(new RuntimeException("timeout"));

        ArgumentCaptor<KeeperStateActionContext> contexts = ArgumentCaptor.forClass(KeeperStateActionContext.class);
        verify(listener, times(2)).onAction(contexts.capture());
        KeeperStateActionContext success = contexts.getAllValues().get(0);
        KeeperStateActionContext failure = contexts.getAllValues().get(1);

        Assert.assertTrue(success.isSuccess());
        Assert.assertEquals(KeeperState.ACTIVE, success.getResult());
        Assert.assertFalse(failure.isSuccess());
        Assert.assertNull(failure.getResult());
        for (KeeperStateActionContext context : Arrays.asList(success, failure)) {
            Assert.assertSame(instance, context.instance());
            Assert.assertTrue(context.getRequestTimeMilli() >= before && context.getRequestTimeMilli() <= after);
        }
    }

    /** 开关关闭只是不再采集，不通知、不发命令；已有结果由消费方按过期处理。 */
    @Test
    public void testDisabledSendsNoCommandAndNotifiesNothing() {
        when(checkerConfig.isKeeperStateCheckEnabled()).thenReturn(false);

        action.doTask();

        verify(session, never()).keeperGetState(any());
        verify(listener, never()).onAction(any());
    }

    /** 停止后到达的在途响应不得再通知，且停止时经 stopWatch 通知消费方清理。 */
    @Test
    public void testStopCallsStopWatchAndDropsInFlightResponse() throws Exception {
        when(checkerConfig.isKeeperStateCheckEnabled()).thenReturn(true);
        action.doTask();
        Callbackable<KeeperState> inFlight = captureCallbacks(1).get(0);

        action.stop();
        verify(listener).stopWatch(action);

        inFlight.success(KeeperState.ACTIVE);
        verify(listener, never()).onAction(any());
    }

    @Test
    public void testControllerOnlyTfs() {
        KeeperStateActionController controller = new KeeperStateActionController();
        Assert.assertTrue(controller.shouldCheck(instance));
        instance.setTfs(false);
        Assert.assertFalse(controller.shouldCheck(instance));
    }

    /** factory 只挂 supportInstance 的 listener，KeeperStateCache 本身即一个 listener。 */
    @Test
    public void testFactoryWiresControllerAndListeners() throws Exception {
        KeeperStateActionController controller = new KeeperStateActionController();
        KeeperStateCache cache = new KeeperStateCache(checkerConfig);
        KeeperStateActionListener unsupported = mock(KeeperStateActionListener.class);
        when(unsupported.supportInstance(instance)).thenReturn(false);

        KeeperStateActionFactory factory = new KeeperStateActionFactory();
        ReflectionTestUtils.setField(factory, "scheduled", scheduled);
        ReflectionTestUtils.setField(factory, "executors", executors);
        ReflectionTestUtils.setField(factory, "checkerConfig", checkerConfig);
        ReflectionTestUtils.setField(factory, "controller", controller);
        ReflectionTestUtils.setField(factory, "listeners", Arrays.asList(cache, unsupported));

        KeeperStateAction created = factory.create(instance);
        Assert.assertEquals(Collections.singletonList(controller), created.getControllers());
        Assert.assertEquals(Collections.singletonList(cache), created.getListeners());
        factory.destroy(created);
    }

    @SuppressWarnings("unchecked")
    private List<Callbackable<KeeperState>> captureCallbacks(int times) {
        ArgumentCaptor<Callbackable<KeeperState>> callbacks = ArgumentCaptor.forClass(Callbackable.class);
        verify(session, times(times)).keeperGetState(callbacks.capture());
        return callbacks.getAllValues();
    }
}
