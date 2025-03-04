package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.HeteroInstanceLongDelay;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE.UNKNOWN;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HeteroHealthStatusTest extends AbstractRedisTest {

    private RedisHealthCheckInstance instance;

    private HeteroHealthStatus healthStatus;

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
        when(instance.getHealthCheckConfig()).thenReturn(config);
        healthStatus = new HeteroHealthStatus(instance, scheduled);

        System.gc();
        sleep(10);
    }

    @Test
    public void testStateSwitchFromUnknowToUp() {
        assertNull(healthStatus.getState(1L));

        healthStatus.delay(-1000,1L);
        assertEquals(UNKNOWN, healthStatus.getState(1L));

        healthStatus.delay(1000,1L);
        assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState(1L));
    }


    @Test
    public void testInstanceLongDelay() throws InterruptedException, TimeoutException {
        when(config.getDelayConfig(Mockito.any(),Mockito.any(),Mockito.any())).thenReturn(
                new DelayConfig("test","test","test")
                        .setDcLevelHealthyDelayMilli(200).setClusterLevelHealthyDelayMilli(200));
        when(config.instanceLongDelayMilli()).thenReturn(300);
        when(config.checkIntervalMilli()).thenReturn(100);

        AtomicReference<AbstractInstanceEvent> event = new AtomicReference<>();
        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                event.set((AbstractInstanceEvent) args);
            }
        });

        healthStatus.start();

        healthStatus.delay(100,1L);
        assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState(1L));
        assertTrue(event.get() instanceof InstanceUp);

        waitConditionUntilTimeOut(()->HEALTH_STATE.UNHEALTHY==healthStatus.getState(1L),500);
        waitConditionUntilTimeOut(()->(event.get() instanceof HeteroInstanceLongDelay)&&(((HeteroInstanceLongDelay)event.get()).getSrcShardDBId()==1),50);

        healthStatus.delay(100,1L);
        assertEquals(HEALTH_STATE.INSTANCEUP, healthStatus.getState(1L));
        assertTrue(event.get() instanceof InstanceUp);
    }

}
