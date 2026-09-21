package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * cross-region slave 的「拉入」状态机：
 * UNKNOWN --ping--> INSTANCEUP --subSuccess(psub 拉入通知)--> HEALTHY --ping超时--> DOWN
 */
public class CrossRegionRedisHealthStatusTest extends AbstractRedisTest {

    private RedisHealthCheckInstance instance;

    private HealthCheckConfig config;

    private CrossRegionRedisHealthStatus healthStatus;

    @Before
    public void beforeCrossRegionRedisHealthStatusTest() {
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
        healthStatus = new CrossRegionRedisHealthStatus(instance, scheduled);
    }

    private AtomicInteger countMarkUp() {
        AtomicInteger markup = new AtomicInteger();
        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                if (args instanceof InstanceUp) {
                    markup.incrementAndGet();
                }
            }
        });
        return markup;
    }

    @Test
    public void testSubSuccessMarksUpAndNotifies() {
        AtomicInteger markup = countMarkUp();

        healthStatus.pong();                            // UNKNOWN -> INSTANCEUP，不触发 markUp
        assertEquals(INSTANCEUP, healthStatus.getState());
        assertEquals(0, markup.get());

        healthStatus.subSuccess();                      // psub 收到拉入通知 -> HEALTHY + InstanceUp
        assertEquals(HEALTHY, healthStatus.getState());
        assertEquals(1, markup.get());

        healthStatus.subSuccess();                      // 已 HEALTHY，不重复触发
        assertEquals(HEALTHY, healthStatus.getState());
        assertEquals(1, markup.get());
    }

    @Test
    public void testSubSuccessWhenNotInstanceUpNoMarkUp() {
        AtomicInteger markup = countMarkUp();

        healthStatus.subSuccess();                      // UNKNOWN 状态，忽略
        assertEquals(UNKNOWN, healthStatus.getState());
        assertEquals(0, markup.get());
    }

    @Test
    public void testSubSuccessAfterPingDown() {
        AtomicInteger markup = countMarkUp();

        healthStatus.pong();
        healthStatus.subSuccess();
        assertEquals(HEALTHY, healthStatus.getState());

        when(config.pingDownAfterMilli()).thenReturn(20);   // ping 超时 -> DOWN
        sleep(30);
        healthStatus.healthStatusUpdate();
        assertEquals(DOWN, healthStatus.getState());

        healthStatus.subSuccess();                      // DOWN 状态下忽略，需重新 ping 通
        assertEquals(DOWN, healthStatus.getState());
        assertEquals(1, markup.get());

        healthStatus.pong();                            // DOWN -> INSTANCEUP
        assertEquals(INSTANCEUP, healthStatus.getState());
        healthStatus.subSuccess();                      // -> HEALTHY
        assertEquals(HEALTHY, healthStatus.getState());
        assertEquals(2, markup.get());
    }

    @Test
    public void testPongOnlyMovesUnknownOrDown() {
        healthStatus.pong();                            // UNKNOWN -> INSTANCEUP
        assertEquals(INSTANCEUP, healthStatus.getState());

        healthStatus.subSuccess();                      // -> HEALTHY
        assertEquals(HEALTHY, healthStatus.getState());

        healthStatus.pong();                            // HEALTHY 下 ping 成功不改状态
        assertEquals(HEALTHY, healthStatus.getState());
    }

    @Test
    public void testFullStateMachine() {
        assertEquals(UNKNOWN, healthStatus.getState());

        // ping 成功 -> INSTANCEUP
        healthStatus.pong();
        assertEquals(INSTANCEUP, healthStatus.getState());

        // subSuccess（psub 拉入通知）-> HEALTHY
        healthStatus.subSuccess();
        assertEquals(HEALTHY, healthStatus.getState());

        // ping 超时 -> DOWN
        when(config.pingDownAfterMilli()).thenReturn(20);
        sleep(30);
        healthStatus.healthStatusUpdate();
        assertEquals(DOWN, healthStatus.getState());

        // 再 ping 成功 -> INSTANCEUP
        healthStatus.pong();
        assertEquals(INSTANCEUP, healthStatus.getState());

        // 再收到拉入通知 -> HEALTHY
        healthStatus.subSuccess();
        assertEquals(HEALTHY, healthStatus.getState());
    }

    @Test
    public void testLoadingGoesDown() {
        assertEquals(UNKNOWN, healthStatus.getState());
        healthStatus.loading();
        assertEquals(DOWN, healthStatus.getState());
    }
}
