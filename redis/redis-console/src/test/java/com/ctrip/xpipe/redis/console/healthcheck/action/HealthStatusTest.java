package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceUp;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
public class HealthStatusTest extends AbstractRedisTest {

    private RedisHealthCheckInstance instance;

    private HealthStatus healthStatus;

    private HealthCheckConfig config;

    @Before
    public void beforeHealthStatusTest() {
        instance = mock(RedisHealthCheckInstance.class);
        RedisInstanceInfo info = new DefaultRedisInstanceInfo("dc", "cluster", "shard", localHostport(randomPort()));
        when(instance.getRedisInstanceInfo()).thenReturn(info);

        config = mock(HealthCheckConfig.class);
        when(config.getHealthyDelayMilli()).thenReturn(2000);
        when(config.downAfterMilli()).thenReturn(2000 * 8);
        when(instance.getHealthCheckConfig()).thenReturn(config);
        healthStatus = new HealthStatus(instance, scheduled);

        healthStatus = new HealthStatus(instance, scheduled);
        System.gc();
    }

    @Test
    public void testStateSwitchFromUnknowToUp() {
        int N = 100;
        for(int i = 0; i < N; i++) {
            healthStatus.delay(1000);
        }
        Assert.assertEquals(HEALTH_STATE.UNKNOWN, healthStatus.getState());

        for(int i = 0; i < N; i++) {
            healthStatus.pong();
        }
        Assert.assertEquals(HEALTH_STATE.INSTANCEOK, healthStatus.getState());

        healthStatus.delay(1000);
        Assert.assertEquals(HEALTH_STATE.UP, healthStatus.getState());
    }

    @Test
    public void testStateSwitchFromUpToDown() throws Exception {
        markup();

        HealthStatus.PING_DOWN_AFTER_MILLI = 10;
        Thread.sleep(10);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());

        healthStatus.pong();
        Assert.assertEquals(HEALTH_STATE.INSTANCEOK, healthStatus.getState());

        healthStatus.delay(3000);
        Assert.assertEquals(HEALTH_STATE.INSTANCEOK, healthStatus.getState());

        healthStatus.delay(1000);
        Assert.assertEquals(HEALTH_STATE.UP, healthStatus.getState());

    }

    @Test
    public void testStateSwitchFromUpToUnhealthyToSick() throws Exception {
        markup();
        when(config.downAfterMilli()).thenReturn(10);

        healthStatus.delay(1000);
        Thread.sleep(5);
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());

        Thread.sleep(5);
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        healthStatus.delay(1000);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.UP, healthStatus.getState());
    }

    @Test
    public void testMarkUpMarkDown() throws Exception {
        AtomicInteger markup = new AtomicInteger(0);
        AtomicInteger markdown = new AtomicInteger(0);
        HealthStatus.PING_DOWN_AFTER_MILLI = 30 * 1000;

        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                if(args instanceof InstanceUp) {
                    markup.incrementAndGet();
                } else {
                    markdown.incrementAndGet();
                }
            }
        });

        int N = 100;
        for(int i = 0; i < N; i++) {
            healthStatus.delay(1000);
        }
        Assert.assertEquals(0, markup.get());
        Assert.assertEquals(0, markdown.get());

        healthStatus.pong();
        Assert.assertEquals(0, markup.get());
        Assert.assertEquals(0, markdown.get());

        healthStatus.delay(1000);
        Assert.assertEquals(1, markup.get());
        Assert.assertEquals(0, markdown.get());

        when(config.downAfterMilli()).thenReturn(10);
        Thread.sleep(15);
        healthStatus.healthStatusUpdate();

        Assert.assertEquals(1, markup.get());
        Assert.assertEquals(1, markdown.get());
        logger.info("[ping-down-time] {}", HealthStatus.PING_DOWN_AFTER_MILLI);
        Assert.assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        healthStatus.delay(1000);
        Assert.assertEquals(HEALTH_STATE.UP, healthStatus.getState());
        Assert.assertEquals(2, markup.get());
        Assert.assertEquals(1, markdown.get());

        HealthStatus.PING_DOWN_AFTER_MILLI = 10;
        Thread.sleep(10);
        logger.info("[ping-down-time] {}", HealthStatus.PING_DOWN_AFTER_MILLI);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        Assert.assertEquals(2, markup.get());
        Assert.assertEquals(2, markdown.get());

        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        Assert.assertEquals(2, markup.get());
        Assert.assertEquals(2, markdown.get());

        for(int i = 0; i < N; i++) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    healthStatus.healthStatusUpdate();
                }
            });
        }
        Thread.sleep(1);
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        Assert.assertEquals(2, markup.get());
        Assert.assertEquals(2, markdown.get());

        HealthStatus.PING_DOWN_AFTER_MILLI = 30 * 1000;
        healthStatus.pong();
        Assert.assertEquals(HEALTH_STATE.INSTANCEOK, healthStatus.getState());
        Assert.assertEquals(2, markup.get());
        Assert.assertEquals(2, markdown.get());

        for(int i = 0; i < N; i++) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    healthStatus.pong();
                    healthStatus.delay(1000);
                    healthStatus.healthStatusUpdate();
                }
            });
        }
        Thread.sleep(2);
        Assert.assertEquals(HEALTH_STATE.UP, healthStatus.getState());
        Assert.assertEquals(3, markup.get());
        Assert.assertEquals(2, markdown.get());

        Thread.sleep(10);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(3, markup.get());
        Assert.assertEquals(3, markdown.get());
        logger.info("[ping-down-time] {}", HealthStatus.PING_DOWN_AFTER_MILLI);
        Assert.assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        HealthStatus.PING_DOWN_AFTER_MILLI = 10;
        logger.info("[ping-down-time] {}", HealthStatus.PING_DOWN_AFTER_MILLI);
        healthStatus.healthStatusUpdate();

        Assert.assertEquals(3, markup.get());
        Assert.assertEquals(3, markdown.get());
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
    }

    @Test
    public void testMultiThread() throws Exception {
        markup();

        int baseInterval = 20;

        HealthStatus.PING_DOWN_AFTER_MILLI = 30;
        when(config.getHealthyDelayMilli()).thenReturn(baseInterval);
        when(config.downAfterMilli()).thenReturn(baseInterval * 5);

        AtomicInteger markup = new AtomicInteger(0);
        AtomicInteger markdown = new AtomicInteger(0);

        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                if(args instanceof InstanceUp) {
                    markup.incrementAndGet();
                } else {
                    markdown.incrementAndGet();
                }
            }
        });
        Assert.assertEquals(0, markdown.get());
        Assert.assertEquals(0, markup.get());

        ScheduledFuture routine = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                healthStatus.pong();
                healthStatus.delay(baseInterval/2);
            }
        }, 0, baseInterval, TimeUnit.MILLISECONDS);

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                healthStatus.healthStatusUpdate();
            }
        }, 0, baseInterval, TimeUnit.MILLISECONDS);

        //todo: set sleep more to test when do test manually
        Thread.sleep(1000);
        Assert.assertEquals(HEALTH_STATE.UP, healthStatus.getState());
        logger.info("[markup][count] {}", markup.get());
        logger.info("[markdown][count] {}", markdown.get());
        Assert.assertEquals(0, markdown.get());
        Assert.assertEquals(0, markup.get());

        routine.cancel(true);
        waitConditionUntilTimeOut(()->routine.isCancelled(), 30);
        Thread.sleep(Math.max(HealthStatus.PING_DOWN_AFTER_MILLI, baseInterval) + 10);
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        Assert.assertEquals(1, markdown.get());
        Assert.assertEquals(0, markup.get());
    }

    @Test
    public void testMarkUpMarkDownSequenceMultiThreadBug() throws Exception {
        int baseInterval = 20;

        HealthStatus.PING_DOWN_AFTER_MILLI = 50;
        when(config.getHealthyDelayMilli()).thenReturn(1000);
        when(config.downAfterMilli()).thenReturn(baseInterval);

        AtomicInteger markup = new AtomicInteger(0);
        AtomicInteger markdown = new AtomicInteger(0);
        AtomicBoolean status = new AtomicBoolean(false);

        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                if(args instanceof InstanceUp) {
                    markup.incrementAndGet();
                    status.set(true);
                } else {
                    markdown.incrementAndGet();
                    scheduled.schedule(new AbstractExceptionLogTask() {
                        @Override
                        protected void doRun() throws Exception {
                            if(!healthStatus.getState().equals(HEALTH_STATE.UP)) {
                                status.set(false);
                            }
                        }
                    }, HealthStatus.PING_DOWN_AFTER_MILLI/5, TimeUnit.MILLISECONDS);
                }
            }
        });
        markup();
        Assert.assertTrue(status.get());

        Thread.sleep(baseInterval + 5);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        healthStatus.delay(10);
        Assert.assertEquals(HEALTH_STATE.UP, healthStatus.getState());
        Assert.assertTrue(status.get());

        Thread.sleep(100);
        Assert.assertTrue(status.get());
    }

    private void markup() {
        healthStatus.pong();
        healthStatus.delay(1000);
        Assert.assertEquals(HEALTH_STATE.UP, healthStatus.getState());
    }

}