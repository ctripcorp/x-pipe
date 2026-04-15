package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterInstanceInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class SentinelBeaconMigrationControllerTest extends AbstractCheckerTest {

    @Mock
    private CheckerDbConfig checkerDbConfig;

    @Mock
    private PersistenceCache persistenceCache;

    private SentinelBeaconMigrationController controller;

    private ClusterHealthCheckInstance instance;

    @Mock
    private HealthCheckConfig healthCheckConfig;

    @Before
    public void setup() {
        controller = new SentinelBeaconMigrationController(checkerDbConfig, persistenceCache);
        instance = Mockito.mock(ClusterHealthCheckInstance.class);
        ClusterInstanceInfo info = new DefaultClusterInstanceInfo("cluster1", "jq", ClusterType.ONE_WAY, 1, "20201030");
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        Mockito.when(instance.getHealthCheckConfig()).thenReturn(healthCheckConfig);
        Mockito.when(healthCheckConfig.supportSentinelBeacon(1, "cluster1")).thenReturn(true);
        Mockito.when(checkerDbConfig.shouldSentinelCheck("cluster1")).thenReturn(true);
        Mockito.when(checkerDbConfig.isSentinelAutoProcess()).thenReturn(true);
        Map<String, String> consoleDomains = new HashMap<>();
        consoleDomains.put("jq", "console-jq.domain");
    }

    @Test
    public void shouldSkipWhenSentinelCheckDisabled() {
        Mockito.when(checkerDbConfig.shouldSentinelCheck("cluster1")).thenReturn(false);
        Assert.assertFalse(controller.shouldCheck(instance));
    }

    @Test
    public void shouldSkipWhenHealthCheckConfigMissing() {
        Mockito.when(instance.getHealthCheckConfig()).thenReturn(null);
        Assert.assertFalse(controller.shouldCheck(instance));
    }

    @Test
    public void shouldSkipWhenClusterOnMigration() {
        Mockito.when(persistenceCache.isClusterOnMigration("cluster1")).thenReturn(true);
        Assert.assertFalse(controller.shouldCheck(instance));
    }

    @Test
    public void shouldCheckWhenClusterNotOnMigration() {
        Mockito.when(persistenceCache.isClusterOnMigration("cluster1")).thenReturn(false);
        Assert.assertTrue(controller.shouldCheck(instance));
    }
}
