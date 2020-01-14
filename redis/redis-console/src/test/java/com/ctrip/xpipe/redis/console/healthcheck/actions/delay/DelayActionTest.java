package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    @Before
    public void beforeDelayActionTest() {
        initSessionPubAndSub();
        initConfig();

        instance = new DefaultRedisHealthCheckInstance();
        instance.setSession(session);
        instance.setHealthCheckConfig(config);
        instance.setRedisInstanceInfo(info);

        action = mockAction();
    }

    @After
    public void afterDelayActionTest() {
        futureMap.forEach((key, future) -> {
            future.cancel(false);
        });
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

    private void initSessionPubAndSub() {
        messageQueueMap = new HashMap<>();
        futureMap = new HashMap<>();

        Mockito.doAnswer(invocationOnMock -> {
            String channel = invocationOnMock.getArgumentAt(0, String.class);
            RedisSession.SubscribeCallback callback = invocationOnMock.getArgumentAt(1, RedisSession.SubscribeCallback.class);

            if (futureMap.containsKey(channel)) futureMap.get(channel).cancel(false);
            futureMap.put(channel, scheduled.scheduleWithFixedDelay(() -> {
                if (messageQueueMap.containsKey(channel) && !messageQueueMap.get(channel).isEmpty()) {
                    synchronized (messageQueueMap.get(channel)) {
                        if (!messageQueueMap.get(channel).isEmpty()) callback.message(channel, messageQueueMap.get(channel).poll());
                    }
                }
            }, 10, 100, TimeUnit.MILLISECONDS));
            return null;
        }).when(session).subscribeIfAbsent(Mockito.anyString(), Mockito.any());

        Mockito.doAnswer(invocationOnMock -> {
            String channel = invocationOnMock.getArgumentAt(0, String.class);
            String message = invocationOnMock.getArgumentAt(1, String.class);
            if (!messageQueueMap.containsKey(channel)) messageQueueMap.put(channel, new ConcurrentLinkedQueue<>());
            scheduled.schedule(() -> {
                messageQueueMap.get(channel).offer(message);
            }, redisDelay, TimeUnit.MILLISECONDS);
            return null;
        }).when(session).publish(Mockito.anyString(), Mockito.anyString());
    }

    private void initConfig() {
        Mockito.when(config.checkIntervalMilli()).thenReturn(CHECK_INTERVAL);
        Mockito.when(config.getHealthyDelayMilli()).thenReturn(CHECK_INTERVAL);
        Mockito.when(info.isMaster()).thenReturn(true);
    }

    private DelayAction mockAction() {
        DelayAction action = new DelayAction(scheduled, instance, executors, pingService);
        HealthCheckActionListener<DelayActionContext> listener = Mockito.mock(HealthCheckActionListener.class);
        action.addListener(listener);
        delayHealth = new AtomicBoolean(false);

        Mockito.doAnswer(invocationOnMock -> {
            DelayActionContext context = invocationOnMock.getArgumentAt(0, DelayActionContext.class);
            Long result = context.getResult();
            delayHealth.set(result > 0 && result < DelayAction.SAMPLE_LOST_BUT_PONG);
            return null;
        }).when(listener).onAction(Mockito.any());
        Mockito.when(listener.worksfor(Mockito.any())).thenReturn(true);

        return action;
    }
}
