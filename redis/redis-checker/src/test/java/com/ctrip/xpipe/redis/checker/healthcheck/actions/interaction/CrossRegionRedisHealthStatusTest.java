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
 * UNKNOWN --ping--> INSTANCEUP --replId一致--> HEALTHY --ping超时--> DOWN
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

    @Test
    public void testReplIdMatch() {
        assertFalse(healthStatus.replIdMatch());        // 初始均为 null

        healthStatus.updateReplIds("a", null);
        assertFalse(healthStatus.replIdMatch());        // keeper null

        healthStatus.updateReplIds(null, "a");
        assertFalse(healthStatus.replIdMatch());        // slave null

        healthStatus.updateReplIds("a", "b");
        assertFalse(healthStatus.replIdMatch());        // 不相等

        healthStatus.updateReplIds("a", "a");
        assertTrue(healthStatus.replIdMatch());         // 相等
    }

    @Test
    public void testUpdateReplIdsMatchMarksUpAndNotifies() {
        AtomicInteger markup = new AtomicInteger();
        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                if (args instanceof InstanceUp) {
                    markup.incrementAndGet();
                }
            }
        });

        healthStatus.pong();                            // UNKNOWN -> INSTANCEUP，不触发 markUp
        assertEquals(INSTANCEUP, healthStatus.getState());
        assertEquals(0, markup.get());

        healthStatus.updateReplIds("a", "a");           // replId 一致 -> HEALTHY + InstanceUp
        assertEquals(HEALTHY, healthStatus.getState());
        assertEquals(1, markup.get());

        healthStatus.updateReplIds("a", "a");           // 已 HEALTHY，不重复触发
        assertEquals(HEALTHY, healthStatus.getState());
        assertEquals(1, markup.get());
    }

    @Test
    public void testUpdateReplIdsNotMatchStaysInstanceUp() {
        AtomicInteger markup = new AtomicInteger();
        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                if (args instanceof InstanceUp) {
                    markup.incrementAndGet();
                }
            }
        });

        healthStatus.pong();                            // -> INSTANCEUP
        healthStatus.updateReplIds("a", "b");           // 不一致，不拉入
        assertEquals(INSTANCEUP, healthStatus.getState());
        assertEquals(0, markup.get());

        healthStatus.updateReplIds(null, null);         // 失败清空，同样不拉入
        assertEquals(INSTANCEUP, healthStatus.getState());
        assertEquals(0, markup.get());
    }

    @Test
    public void testUpdateReplIdsWhenNotInstanceUpNoMarkUp() {
        AtomicInteger markup = new AtomicInteger();
        healthStatus.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                if (args instanceof InstanceUp) {
                    markup.incrementAndGet();
                }
            }
        });

        healthStatus.updateReplIds("a", "a");           // UNKNOWN 状态，即便 replId 一致也不拉入
        assertEquals(UNKNOWN, healthStatus.getState());
        assertEquals(0, markup.get());
    }

    @Test
    public void testFullStateMachine() {
        assertEquals(UNKNOWN, healthStatus.getState());

        // ping 成功 -> INSTANCEUP
        healthStatus.pong();
        assertEquals(INSTANCEUP, healthStatus.getState());

        // replId 一致 -> HEALTHY
        healthStatus.updateReplIds("a", "a");
        assertEquals(HEALTHY, healthStatus.getState());

        // ping 超时 -> DOWN
        when(config.pingDownAfterMilli()).thenReturn(20);
        sleep(30);
        healthStatus.healthStatusUpdate();
        assertEquals(DOWN, healthStatus.getState());

        // 再 ping 成功 -> INSTANCEUP
        healthStatus.pong();
        assertEquals(INSTANCEUP, healthStatus.getState());

        // 再 replId 一致 -> HEALTHY
        healthStatus.updateReplIds("a", "a");
        assertEquals(HEALTHY, healthStatus.getState());
    }

    @Test
    public void testLoadingGoesDown() {
        assertEquals(UNKNOWN, healthStatus.getState());
        healthStatus.loading();
        assertEquals(DOWN, healthStatus.getState());
    }
}
