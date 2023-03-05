package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceLongDelay;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 * testStateSwitchFromUpToDown() and testMarkUpMarkDown() manual test
 */
public class HealthStatusTest extends AbstractRedisTest {

    private RedisHealthCheckInstance instance;

    private HealthStatus healthStatus;

    private HealthCheckConfig config;

    @Before
    public void beforeHealthStatusTest() {
        instance = mock(RedisHealthCheckInstance.class);
        RedisInstanceInfo info = new DefaultRedisInstanceInfo("dc", "cluster", "shard", localHostport(randomPort()), "dc2", ClusterType.ONE_WAY);
        when(instance.getCheckInfo()).thenReturn(info);

        config = mock(HealthCheckConfig.class);
        when(config.getDelayConfig(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(
                new DelayConfig("test", "test", "test").
                        setDcLevelHealthyDelayMilli(2000).setClusterLevelHealthyDelayMilli(2000).
                        setDcLevelDelayDownAfterMilli(2000 * 8).setClusterLevelDelayDownAfterMilli(2000 * 8));
        when(config.pingDownAfterMilli()).thenReturn(12 * 1000);
        when(instance.getHealthCheckConfig()).thenReturn(config);
        healthStatus = new HealthStatus(instance, scheduled);
        System.gc();
        sleep(10);
    }

    @Test
    public void testStateSwitchFromUnknowToUp() {
        int N = 100;
        for(int i = 0; i < N; i++) {
            healthStatus.delay(1000);
        }
        assertEquals(UNKNOWN, healthStatus.getState());

        for(int i = 0; i < N; i++) {
            healthStatus.pong();
        }
        assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());

        healthStatus.delay(1000);
        assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
    }

    @Ignore
    @Test
    public void testStateSwitchFromUpToDown() throws Exception {
        markup();

        when(config.pingDownAfterMilli()).thenReturn(10);
        Thread.sleep(10);
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());

        healthStatus.pong();
        assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());

        healthStatus.delay(config.getDelayConfig("test","test","test").getClusterLevelHealthyDelayMilli() + 1);
        assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());

        healthStatus.delay(config.getDelayConfig("test","test","test").getClusterLevelHealthyDelayMilli()/2);
        assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());

    }

    @Test
    public void testStateSwitchFromUpToUnhealthyToSick() throws Exception {
        markup();
        when(config.instanceLongDelayMilli()).thenReturn(10);
        when(config.getDelayConfig(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(
                new DelayConfig("test", "test", "test").
                        setDcLevelHealthyDelayMilli(2000).setClusterLevelHealthyDelayMilli(2000).
                        setDcLevelDelayDownAfterMilli(100).setClusterLevelDelayDownAfterMilli(100));

        healthStatus.delay(config.getDelayConfig("test","test","test").getDcLevelHealthyDelayMilli()/2);
        Thread.sleep(config.instanceLongDelayMilli());
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        waitConditionUntilTimeOut(()->HEALTH_STATE.UNHEALTHY == healthStatus.getState(), 1000);
        assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());

        Thread.sleep(config.getDelayConfig("test","test","test").getDcLevelHealthyDelayMilli()/2);
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        waitConditionUntilTimeOut(()->HEALTH_STATE.SICK == healthStatus.getState(), 1000);
        assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        healthStatus.delay(config.getDelayConfig("test","test","test").getDcLevelHealthyDelayMilli()/2);
        healthStatus.healthStatusUpdate();
        waitConditionUntilTimeOut(()->HEALTH_STATE.HEALTHY == healthStatus.getState(), 1000);
        assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
    }

    @Ignore
    @Test
    public void testMarkUpMarkDown() throws Exception {
        AtomicInteger markup = new AtomicInteger(0);
        AtomicInteger markdown = new AtomicInteger(0);
        when(config.pingDownAfterMilli()).thenReturn(30 * 1000);

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
            healthStatus.delay(config.getDelayConfig("test","test","test").getClusterLevelHealthyDelayMilli()/2);
        }
        assertEquals(0, markup.get());
        assertEquals(0, markdown.get());

        healthStatus.pong();
        assertEquals(0, markup.get());
        assertEquals(0, markdown.get());

        healthStatus.delay(config.getDelayConfig("test","test","test").getClusterLevelHealthyDelayMilli()/2);
        assertEquals(1, markup.get());
        assertEquals(0, markdown.get());

        when(config.getDelayConfig("test","test","test").getClusterLevelDelayDownAfterMilli()).thenReturn(10);
        Thread.sleep(12);
        healthStatus.healthStatusUpdate();

        assertEquals(1, markup.get());
        assertEquals(1, markdown.get());
        logger.info("[ping-down-time] {}", config.pingDownAfterMilli());
        assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        healthStatus.delay(1000);
        assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
        assertEquals(2, markup.get());
        assertEquals(1, markdown.get());

        when(config.pingDownAfterMilli()).thenReturn(10);
        Thread.sleep(10);
        logger.info("[ping-down-time] {}", config.pingDownAfterMilli());
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        assertEquals(2, markup.get());
        assertEquals(2, markdown.get());

        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        assertEquals(2, markup.get());
        assertEquals(2, markdown.get());

        for(int i = 0; i < N; i++) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    healthStatus.healthStatusUpdate();
                }
            });
        }
        Thread.sleep(1);
        assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        assertEquals(2, markup.get());
        assertEquals(2, markdown.get());

        when(config.pingDownAfterMilli()).thenReturn(30 * 1000);
        healthStatus.pong();
        assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());
        assertEquals(2, markup.get());
        assertEquals(2, markdown.get());

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
        assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
        assertEquals(3, markup.get());
        assertEquals(2, markdown.get());

        Thread.sleep(10);
        healthStatus.healthStatusUpdate();
        assertEquals(3, markup.get());
        assertEquals(3, markdown.get());
        logger.info("[ping-down-time] {}", config.pingDownAfterMilli());
        assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        when(config.pingDownAfterMilli()).thenReturn(10);
        logger.info("[ping-down-time] {}", config.pingDownAfterMilli());
        healthStatus.healthStatusUpdate();

        assertEquals(3, markup.get());
        assertEquals(3, markdown.get());
        assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
    }

    @Test
    public void testMultiThread() throws Exception {
        markup();

        int baseInterval = 20;

        when(config.pingDownAfterMilli()).thenReturn(20);
        when(config.instanceLongDelayMilli()).thenReturn(baseInterval * 2);
        when(config.getDelayConfig(any(),any(),any())).thenReturn(
                new DelayConfig("test","test","test").
                        setClusterLevelDelayDownAfterMilli(baseInterval * 5).setClusterLevelHealthyDelayMilli(baseInterval));

        AtomicInteger markup = new AtomicInteger(0);
        AtomicInteger markdown = new AtomicInteger(0);

        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                if(args instanceof InstanceUp) {
                    markup.incrementAndGet();
                } else if (args instanceof InstanceLongDelay) {
                    markdown.get();
                } else {
                    markdown.incrementAndGet();
                }
            }
        });
        assertEquals(0, markdown.get());
        assertEquals(0, markup.get());

        ScheduledFuture routine = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                healthStatus.pong();
                healthStatus.delay(baseInterval/2);
            }
        }, 0, 5, TimeUnit.MILLISECONDS);

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                healthStatus.healthStatusUpdate();
            }
        }, 0, 5, TimeUnit.MILLISECONDS);

        //todo: set sleep more to test when do test manually
        waitConditionUntilTimeOut(()->HEALTH_STATE.HEALTHY == healthStatus.getState(), 1500);
        assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
        logger.info("[markup][count] {}", markup.get());
        logger.info("[markdown][count] {}", markdown.get());
        assertEquals(0, markdown.get());
        assertEquals(0, markup.get());

        routine.cancel(true);
        waitConditionUntilTimeOut(()->routine.isCancelled(), 30);
        Thread.sleep(50);
        assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        assertEquals(1, markdown.get());
        assertEquals(0, markup.get());
    }

    @Test
    public void testMarkUpMarkDownSequenceMultiThreadBug() throws Exception {
        int baseInterval = 20;

        when(config.pingDownAfterMilli()).thenReturn(50);
        when(config.getDelayConfig(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(
                new DelayConfig("test", "test", "test").
                        setDcLevelHealthyDelayMilli(1000).setClusterLevelHealthyDelayMilli(1000).
                        setDcLevelDelayDownAfterMilli(baseInterval).setClusterLevelDelayDownAfterMilli(baseInterval));

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
                            if(!healthStatus.getState().equals(HEALTH_STATE.HEALTHY)) {
                                status.set(false);
                            }
                        }
                    }, config.pingDownAfterMilli()/5, TimeUnit.MILLISECONDS);
                }
            }
        });
        markup();
        Assert.assertTrue(status.get());

        Thread.sleep(baseInterval + 5);
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        healthStatus.delay(10);
        assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
        Assert.assertTrue(status.get());

        Thread.sleep(100);
        Assert.assertTrue(status.get());
    }

    @Test
    public void testHealthyHalfDown() throws TimeoutException {
        when(config.pingDownAfterMilli()).thenReturn(50);
        when(config.getDelayConfig(any(),any(),any())).thenReturn(
                new DelayConfig("test","test","test")
                .setClusterLevelHealthyDelayMilli(50).setClusterLevelDelayDownAfterMilli(100));
        markup();

        sleep(config.pingDownAfterMilli()/2);
        healthStatus.healthStatusUpdate();
        waitConditionUntilTimeOut(()->HEALTH_STATE.UNHEALTHY == healthStatus.getState(), 1000);
        assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());
    }

    @Test
    public void testUnhealthyToHalfDown() {
        when(config.pingDownAfterMilli()).thenReturn(50);
        when(config.getDelayConfig(any(),any(),any())).thenReturn(
                new DelayConfig("test","test","test")
                        .setClusterLevelHealthyDelayMilli(50).setClusterLevelDelayDownAfterMilli(100));
        markup();

        sleep(config.getDelayConfig("test","test","test").getClusterLevelDelayDownAfterMilli()/2);
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());

        sleep(config.pingDownAfterMilli()/2);
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());
    }

    @Test
    public void testSickDownThenDownSendOnceNotify() {
        when(config.pingDownAfterMilli()).thenReturn(40);
        when(config.getDelayConfig(any(),any(),any())).thenReturn(
                new DelayConfig("test","test","test")
                        .setClusterLevelHealthyDelayMilli(10).setClusterLevelDelayDownAfterMilli(10));
        markup();

        AtomicInteger markdownCount = new AtomicInteger(0);
        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                if(!(args instanceof InstanceUp)) {
                    markdownCount.incrementAndGet();
                }
            }
        });
        healthStatus.pong();
        sleep(11);
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.SICK, healthStatus.getState());
        assertEquals(1, markdownCount.get());

        sleep(15);
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        sleep(15);
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        assertEquals(1, markdownCount.get());
    }

    @Test
    public void testNewAddedRedis() {
        when(config.pingDownAfterMilli()).thenReturn(40);
        when(config.getDelayConfig(any(),any(),any())).thenReturn(
                new DelayConfig("test","test","test")
                        .setClusterLevelHealthyDelayMilli(20).setClusterLevelDelayDownAfterMilli(60));

        assertEquals(UNKNOWN, healthStatus.getState());
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());

    }

    @Test
    public void testNewAddedRedisUnHealthy() {
        when(config.pingDownAfterMilli()).thenReturn(80);
        when(config.getDelayConfig(any(),any(),any())).thenReturn(
                new DelayConfig("test","test","test")
                        .setClusterLevelHealthyDelayMilli(40).setClusterLevelDelayDownAfterMilli(120));

        assertEquals(UNKNOWN, healthStatus.getState());
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());

        int N = 10;
        for(int i = 0; i < N; i++) {
            assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());
            healthStatus.pong();
            healthStatus.healthStatusUpdate();
            assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());
            sleep(10);
        }

        sleep(40);
        healthStatus.healthStatusUpdate();
        assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());

    }

    @Test
    public void testPingFailNotify() {
        when(config.pingDownAfterMilli()).thenReturn(40);
        when(config.getDelayConfig(any(),any(),any())).thenReturn(
                new DelayConfig("test","test","test")
                        .setClusterLevelHealthyDelayMilli(20).setClusterLevelDelayDownAfterMilli(60));

        healthStatus.pongInit();
        sleep(40 * 2 + 5);
        healthStatus.healthStatusUpdate();
        Assert.assertSame(healthStatus.getState(), HEALTH_STATE.DOWN);
    }

    @Test
    public void testInitToPingDownShouldNotifyOuterClinet() {
        when(config.pingDownAfterMilli()).thenReturn(40);
        when(config.getDelayConfig(any(),any(),any())).thenReturn(
                new DelayConfig("test","test","test")
                        .setClusterLevelHealthyDelayMilli(20).setClusterLevelDelayDownAfterMilli(60));

        AtomicInteger counter = new AtomicInteger();
        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                counter.incrementAndGet();
            }
        });
        healthStatus.pongInit();
        sleep(40 * 2 + 5);
        healthStatus.healthStatusUpdate();
        Assert.assertSame(healthStatus.getState(), HEALTH_STATE.DOWN);
        Assert.assertTrue(counter.get() >= 1);
    }

    @Test
    public void testLoading() {
        assertEquals(UNKNOWN, healthStatus.getState());
        healthStatus.loading();
        assertEquals(DOWN, healthStatus.getState());
    }

    @Test
    public void testPongAfterLoading() {
        assertEquals(UNKNOWN, healthStatus.getState());
        healthStatus.loading();
        assertEquals(DOWN, healthStatus.getState());
        healthStatus.pong();
        assertEquals(INSTANCEUP, healthStatus.getState());
    }

    @Test
    public void testDelayOKAfterLoading() {
        assertEquals(UNKNOWN, healthStatus.getState());
        healthStatus.loading();
        assertEquals(DOWN, healthStatus.getState());
        healthStatus.delay(1L);
        assertEquals(DOWN, healthStatus.getState());
    }

    @Test
    public void testDelayOkAndInstanceUpAfterLoading() {
        assertEquals(UNKNOWN, healthStatus.getState());
        healthStatus.loading();
        assertEquals(DOWN, healthStatus.getState());
        healthStatus.delay(1L);
        assertEquals(DOWN, healthStatus.getState());
        healthStatus.pong();
        assertEquals(INSTANCEUP, healthStatus.getState());
        healthStatus.delay(1L);
        assertEquals(HEALTHY, healthStatus.getState());
    }

    private void markup() {
        healthStatus.pong();
        healthStatus.delay(config.getDelayConfig("test","test","test").getClusterLevelHealthyDelayMilli()/2);
        assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
    }
}