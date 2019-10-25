package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceHalfSick;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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
 * testStateSwitchFromUpToDown() and testMarkUpMarkDown() manual test
 */
public class HealthStatusTest extends AbstractRedisTest {

    private RedisHealthCheckInstance instance;

    private HealthStatus healthStatus;

    private HealthCheckConfig config;

    @Before
    public void beforeHealthStatusTest() {
        instance = mock(RedisHealthCheckInstance.class);
        RedisInstanceInfo info = new DefaultRedisInstanceInfo("dc", "cluster", "shard", localHostport(randomPort()), "dc2");
        when(instance.getRedisInstanceInfo()).thenReturn(info);

        config = mock(HealthCheckConfig.class);
        when(config.getHealthyDelayMilli()).thenReturn(2000);
        when(config.delayDownAfterMilli()).thenReturn(2000 * 8);
        when(config.pingDownAfterMilli()).thenReturn(12 * 1000);
        when(instance.getHealthCheckConfig()).thenReturn(config);
        healthStatus = new HealthStatus(instance, scheduled);

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
        Assert.assertEquals(HEALTH_STATE.UNKNOWN, healthStatus.getState());

        for(int i = 0; i < N; i++) {
            healthStatus.pong();
        }
        Assert.assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());

        healthStatus.delay(1000);
        Assert.assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
    }

    @Ignore
    @Test
    public void testStateSwitchFromUpToDown() throws Exception {
        markup();

        when(config.pingDownAfterMilli()).thenReturn(10);
        Thread.sleep(10);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());

        healthStatus.pong();
        Assert.assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());

        healthStatus.delay(config.getHealthyDelayMilli() + 1);
        Assert.assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());

        healthStatus.delay(config.getHealthyDelayMilli()/2);
        Assert.assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());

    }

    @Test
    public void testStateSwitchFromUpToUnhealthyToSick() throws Exception {
        markup();
        when(config.delayDownAfterMilli()).thenReturn(10);

        healthStatus.delay(config.getHealthyDelayMilli()/2);
        Thread.sleep(config.delayDownAfterMilli()/2);
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());

        Thread.sleep(config.getHealthyDelayMilli()/2);
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        healthStatus.delay(config.getHealthyDelayMilli()/2);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
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
            healthStatus.delay(config.getHealthyDelayMilli()/2);
        }
        Assert.assertEquals(0, markup.get());
        Assert.assertEquals(0, markdown.get());

        healthStatus.pong();
        Assert.assertEquals(0, markup.get());
        Assert.assertEquals(0, markdown.get());

        healthStatus.delay(config.getHealthyDelayMilli()/2);
        Assert.assertEquals(1, markup.get());
        Assert.assertEquals(0, markdown.get());

        when(config.delayDownAfterMilli()).thenReturn(10);
        Thread.sleep(12);
        healthStatus.healthStatusUpdate();

        Assert.assertEquals(1, markup.get());
        Assert.assertEquals(1, markdown.get());
        logger.info("[ping-down-time] {}", config.pingDownAfterMilli());
        Assert.assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        healthStatus.delay(1000);
        Assert.assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
        Assert.assertEquals(2, markup.get());
        Assert.assertEquals(1, markdown.get());

        when(config.pingDownAfterMilli()).thenReturn(10);
        Thread.sleep(10);
        logger.info("[ping-down-time] {}", config.pingDownAfterMilli());
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

        when(config.pingDownAfterMilli()).thenReturn(30 * 1000);
        healthStatus.pong();
        Assert.assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());
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
        Assert.assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
        Assert.assertEquals(3, markup.get());
        Assert.assertEquals(2, markdown.get());

        Thread.sleep(10);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(3, markup.get());
        Assert.assertEquals(3, markdown.get());
        logger.info("[ping-down-time] {}", config.pingDownAfterMilli());
        Assert.assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        when(config.pingDownAfterMilli()).thenReturn(10);
        logger.info("[ping-down-time] {}", config.pingDownAfterMilli());
        healthStatus.healthStatusUpdate();

        Assert.assertEquals(3, markup.get());
        Assert.assertEquals(3, markdown.get());
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
    }

    @Test
    public void testMultiThread() throws Exception {
        markup();

        int baseInterval = 20;

        when(config.pingDownAfterMilli()).thenReturn(20);
        when(config.getHealthyDelayMilli()).thenReturn(baseInterval);
        when(config.delayDownAfterMilli()).thenReturn(baseInterval * 5);

        AtomicInteger markup = new AtomicInteger(0);
        AtomicInteger markdown = new AtomicInteger(0);

        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                if(args instanceof InstanceUp) {
                    markup.incrementAndGet();
                } else if (args instanceof InstanceHalfSick) {
                    markdown.get();
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
        }, 0, 5, TimeUnit.MILLISECONDS);

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                healthStatus.healthStatusUpdate();
            }
        }, 0, 5, TimeUnit.MILLISECONDS);

        //todo: set sleep more to test when do test manually
        Thread.sleep(500);
        Assert.assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
        logger.info("[markup][count] {}", markup.get());
        logger.info("[markdown][count] {}", markdown.get());
        Assert.assertEquals(0, markdown.get());
        Assert.assertEquals(0, markup.get());

        routine.cancel(true);
        waitConditionUntilTimeOut(()->routine.isCancelled(), 30);
        Thread.sleep(50);
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        Assert.assertEquals(1, markdown.get());
        Assert.assertEquals(0, markup.get());
    }

    @Test
    public void testMarkUpMarkDownSequenceMultiThreadBug() throws Exception {
        int baseInterval = 20;

        when(config.pingDownAfterMilli()).thenReturn(50);
        when(config.getHealthyDelayMilli()).thenReturn(1000);
        when(config.delayDownAfterMilli()).thenReturn(baseInterval);

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
        Assert.assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        healthStatus.delay(10);
        Assert.assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
        Assert.assertTrue(status.get());

        Thread.sleep(100);
        Assert.assertTrue(status.get());
    }


    @Test
    public void testHealthyHalfDown() {
        when(config.pingDownAfterMilli()).thenReturn(50);
        when(config.delayDownAfterMilli()).thenReturn(100);
        when(config.getHealthyDelayMilli()).thenReturn(50);
        markup();

        sleep(config.pingDownAfterMilli()/2);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());
    }

    @Test
    public void testUnhealthyToHalfDown() {
        when(config.pingDownAfterMilli()).thenReturn(50);
        when(config.delayDownAfterMilli()).thenReturn(100);
        when(config.getHealthyDelayMilli()).thenReturn(50);
        markup();

        sleep(config.delayDownAfterMilli()/2);
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());

        sleep(config.pingDownAfterMilli()/2);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());
    }

    @Test
    public void testSickDownThenDownSendOnceNotify() {
        when(config.pingDownAfterMilli()).thenReturn(40);
        when(config.delayDownAfterMilli()).thenReturn(10);
        when(config.getHealthyDelayMilli()).thenReturn(10);
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
        Assert.assertEquals(HEALTH_STATE.SICK, healthStatus.getState());
        Assert.assertEquals(1, markdownCount.get());

        sleep(15);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.SICK, healthStatus.getState());

        sleep(15);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.DOWN, healthStatus.getState());
        Assert.assertEquals(1, markdownCount.get());
    }

    @Test
    public void testNewAddedRedis() {
        when(config.pingDownAfterMilli()).thenReturn(40);
        when(config.delayDownAfterMilli()).thenReturn(60);
        when(config.getHealthyDelayMilli()).thenReturn(20);

        Assert.assertEquals(HEALTH_STATE.UNKNOWN, healthStatus.getState());
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());

    }

    @Test
    public void testNewAddedRedisUnHealthy() {
        when(config.pingDownAfterMilli()).thenReturn(40);
        when(config.delayDownAfterMilli()).thenReturn(60);
        when(config.getHealthyDelayMilli()).thenReturn(20);

        Assert.assertEquals(HEALTH_STATE.UNKNOWN, healthStatus.getState());
        healthStatus.pong();
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());

        int N = 10;
        for(int i = 0; i < N; i++) {
            Assert.assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());
            healthStatus.pong();
            healthStatus.healthStatusUpdate();
            Assert.assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState());
            sleep(10);
        }

        sleep(20);
        healthStatus.healthStatusUpdate();
        Assert.assertEquals(HEALTH_STATE.UNHEALTHY, healthStatus.getState());

    }

    private void markup() {
        healthStatus.pong();
        healthStatus.delay(config.getHealthyDelayMilli()/2);
        Assert.assertEquals(HEALTH_STATE.HEALTHY, healthStatus.getState());
    }

}