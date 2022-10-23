package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DelayActionTest extends AbstractRedisTest {

    @Mock
    private RedisSession session;

    @Mock
    private PingService pingService;

    @Mock
    private HealthCheckConfig config;

    @Mock
    private RedisInstanceInfo info;

    private DefaultRedisHealthCheckInstance instance;

    private Map<String, Queue<String> > messageQueueMap;

    private Map<String, Future> futureMap;

    private static final int CHECK_INTERVAL = 2000;

    private int redisDelay = 100;

    private DelayAction action;

    private AtomicBoolean delayHealth;

    private AtomicBoolean instanceNull = new AtomicBoolean(false);

    @Before
    public void beforeDelayActionTest() {
        initSessionPubAndSub();
        initConfig();

        instance = new DefaultRedisHealthCheckInstance();
        instance.setSession(session);
        instance.setHealthCheckConfig(config);
        instance.setInstanceInfo(info);

        when(info.getClusterType()).thenReturn(ClusterType.ONE_WAY);
        action = mockAction();
    }

    @After
    public void afterDelayActionTest() {
        futureMap.forEach((key, future) -> {
            future.cancel(false);
        });

        Assert.assertFalse(instanceNull.get());
    }

    @Test
    public void testRedisUp() throws Exception {
        delayHealth.set(false);
        redisDelay = 100;
        action.initialize();
        action.start();
        sleep(3 * CHECK_INTERVAL);
        action.stop();
        action.dispose();
        Assert.assertTrue(delayHealth.get());
    }

    @Test
    public void testRedisDown() throws Exception {
        delayHealth.set(true);
        redisDelay = 10;
        action.initialize();
        action.start();
        sleep(2 * CHECK_INTERVAL);
        redisDelay = 10 * CHECK_INTERVAL;
        sleep(2 * CHECK_INTERVAL);
        Assert.assertFalse(delayHealth.get());
        action.stop();
        action.dispose();
    }

    @Test
    public void testRedisUpAfterDown() throws Exception {
        delayHealth.set(true);
        redisDelay = 10 * CHECK_INTERVAL;
        action.initialize();
        action.start();
        sleep(3 * CHECK_INTERVAL);
        Assert.assertFalse(delayHealth.get());
        redisDelay = 10;
        sleep(2 * CHECK_INTERVAL);
        Assert.assertTrue(delayHealth.get());
        action.stop();
        action.dispose();
    }

    @Test
    public void testConnectTimeoutSinceBeginning() {
        AtomicBoolean result = new AtomicBoolean(false);
        action.addListener(new DelayActionListener() {
            @Override
            public void onAction(DelayActionContext actionContext) {
                if (null == actionContext.instance()) {
                    instanceNull.set(true);
                    Assert.fail();
                }
                result.set(true);
            }

            @Override
            public boolean worksfor(ActionContext t) {
                return true;
            }

            @Override
            public void stopWatch(HealthCheckAction action) {

            }

            @Override
            public boolean supportInstance(RedisHealthCheckInstance instance) {
                return true;
            }
        });
        action.doTask();
        action.doTask();
        action.doTask();
        sleep(2 * CHECK_INTERVAL);
        action.doTask();
        Assert.assertTrue(result.get());
    }

    private void initSessionPubAndSub() {
        messageQueueMap = new HashMap<>();
        futureMap = new HashMap<>();

        Mockito.doAnswer(invocationOnMock -> {
            String channel = invocationOnMock.getArgument(1, String.class);
            RedisSession.SubscribeCallback callback = invocationOnMock.getArgument(0, RedisSession.SubscribeCallback.class);

            if (futureMap.containsKey(channel)) futureMap.get(channel).cancel(false);
            futureMap.put(channel, scheduled.scheduleWithFixedDelay(() -> {
                if (messageQueueMap.containsKey(channel) && !messageQueueMap.get(channel).isEmpty()) {
                    synchronized (messageQueueMap.get(channel)) {
                        if (!messageQueueMap.get(channel).isEmpty()) callback.message(channel, messageQueueMap.get(channel).poll());
                    }
                }
            }, 10, 100, TimeUnit.MILLISECONDS));
            return null;
        }).when(session).subscribeIfAbsent(Mockito.any(), Mockito.anyString());

        Mockito.doAnswer(invocationOnMock -> {
            String channel = invocationOnMock.getArgument(0, String.class);
            String message = invocationOnMock.getArgument(1, String.class);
            if (!messageQueueMap.containsKey(channel)) messageQueueMap.put(channel, new ConcurrentLinkedQueue<>());
            scheduled.schedule(() -> {
                messageQueueMap.get(channel).offer(message);
            }, redisDelay, TimeUnit.MILLISECONDS);
            return null;
        }).when(session).publish(Mockito.anyString(), Mockito.anyString());
    }

    private void initConfig() {
        when(config.checkIntervalMilli()).thenReturn(CHECK_INTERVAL);
        when(config.getHealthyDelayMilli()).thenReturn(CHECK_INTERVAL);
        when(info.isMaster()).thenReturn(true);
        when(info.getDcId()).thenReturn(FoundationService.DEFAULT.getDataCenter());
    }

    private DelayAction mockAction() {
        DelayAction action = new DelayAction(scheduled, instance, executors, pingService, FoundationService.DEFAULT);
        HealthCheckActionListener<DelayActionContext, HealthCheckAction> listener = Mockito.mock(HealthCheckActionListener.class);
        action.addListener(listener);
        delayHealth = new AtomicBoolean(false);

        Mockito.doAnswer(invocationOnMock -> {
            DelayActionContext context = invocationOnMock.getArgument(0, DelayActionContext.class);
            if (null == context.instance()) {
                instanceNull.set(true);
                Assert.fail();
            }
            Long result = context.getResult();
            delayHealth.set(result > 0 && result < DelayAction.SAMPLE_LOST_BUT_PONG);
            return null;
        }).when(listener).onAction(Mockito.any());
        when(listener.worksfor(Mockito.any())).thenReturn(true);

        return action;
    }
}
