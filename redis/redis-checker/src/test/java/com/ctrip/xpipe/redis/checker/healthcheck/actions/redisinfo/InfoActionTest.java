package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Verifies InfoAction.shouldCheck is gated by:
 *   1) HealthCheckConfig.supportCollectInfo(clusterType) — the collect-info switch
 *   2) InfoActionController (CurrentDcInfoController: redis in current DC)
 * and that the legacy ClusterType.supportHealthCheck() gate no longer short-circuits,
 * so SINGLE_DC/LOCAL_DC (supportHealthCheck()==false) can still be collected when the
 * config switch is on.
 */
public class InfoActionTest {

    private static final String CURRENT_DC = "SHARB";

    private DefaultRedisHealthCheckInstance newInstance(ClusterType type, String dcId, boolean configSupport) {
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(
                dcId, "cluster", "shard", new HostPort("127.0.0.1", 6379), dcId, type);
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();
        instance.setInstanceInfo(info);
        instance.setHealthCheckConfig(newConfig(configSupport));
        return instance;
    }

    private HealthCheckConfig newConfig(boolean support) {
        HealthCheckConfig config = Mockito.mock(HealthCheckConfig.class);
        Mockito.when(config.supportCollectInfo(Mockito.any(ClusterType.class))).thenReturn(support);
        return config;
    }

    private InfoAction newAction(RedisHealthCheckInstance instance) {
        ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
        ExecutorService executors = Executors.newCachedThreadPool();
        InfoAction action = new InfoAction(scheduled, instance, executors);

        FoundationService foundation = Mockito.mock(FoundationService.class);
        Mockito.when(foundation.getDataCenter()).thenReturn(CURRENT_DC);
        action.addController(new CurrentDcInfoController(foundation));
        return action;
    }

    @Test
    public void testShouldCheckSingleDcInCurrentDc() {
        RedisHealthCheckInstance instance = newInstance(ClusterType.SINGLE_DC, CURRENT_DC, true);
        Assert.assertTrue(newAction(instance).shouldCheck(instance));
    }

    @Test
    public void testShouldCheckSingleDcNotInCurrentDc() {
        RedisHealthCheckInstance instance = newInstance(ClusterType.SINGLE_DC, "FRA", true);
        Assert.assertFalse(newAction(instance).shouldCheck(instance));
    }

    @Test
    public void testShouldCheckLocalDcInCurrentDc() {
        RedisHealthCheckInstance instance = newInstance(ClusterType.LOCAL_DC, CURRENT_DC, true);
        Assert.assertTrue(newAction(instance).shouldCheck(instance));
    }

    @Test
    public void testShouldCheckLocalDcNotInCurrentDc() {
        RedisHealthCheckInstance instance = newInstance(ClusterType.LOCAL_DC, "FRA", true);
        Assert.assertFalse(newAction(instance).shouldCheck(instance));
    }

    @Test
    public void testShouldCheckOneWayInCurrentDc() {
        RedisHealthCheckInstance instance = newInstance(ClusterType.ONE_WAY, CURRENT_DC, true);
        Assert.assertTrue(newAction(instance).shouldCheck(instance));
    }

    @Test
    public void testShouldCheckOneWayNotInCurrentDc() {
        RedisHealthCheckInstance instance = newInstance(ClusterType.ONE_WAY, "FRA", true);
        Assert.assertFalse(newAction(instance).shouldCheck(instance));
    }

    @Test
    public void testSkipWhenCollectInfoSwitchOff() {
        // config switch off -> never collect, even for redis in current DC
        RedisHealthCheckInstance instance = newInstance(ClusterType.SINGLE_DC, CURRENT_DC, false);
        Assert.assertFalse(newAction(instance).shouldCheck(instance));
    }

    @Test
    public void testCollectWhenSupportHealthCheckFalseButSwitchOn() {
        // SINGLE_DC.supportHealthCheck()==false, yet the legacy gate is bypassed:
        // with the config switch on and redis in current DC, info is still collected
        Assert.assertFalse(ClusterType.SINGLE_DC.supportHealthCheck());
        RedisHealthCheckInstance instance = newInstance(ClusterType.SINGLE_DC, CURRENT_DC, true);
        Assert.assertTrue(newAction(instance).shouldCheck((HealthCheckInstance) instance));
    }
}
