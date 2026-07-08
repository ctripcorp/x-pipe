package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterInstanceInfo;
import com.ctrip.xpipe.redis.core.meta.ClusterMetaStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SentinelBeaconMigrationControllerTest {

    @Mock
    private CheckerDbConfig checkerDbConfig;

    private SentinelBeaconMigrationController controller;

    private ClusterHealthCheckInstance instance;

    @Mock
    private HealthCheckConfig healthCheckConfig;

    @Before
    public void setup() {
        controller = new SentinelBeaconMigrationController(checkerDbConfig);
        instance = Mockito.mock(ClusterHealthCheckInstance.class);
        ClusterInstanceInfo info = new DefaultClusterInstanceInfo("cluster1", "jq", ClusterType.ONE_WAY, 1, "20201030");
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        Mockito.when(instance.getHealthCheckConfig()).thenReturn(healthCheckConfig);
        Mockito.when(healthCheckConfig.supportSentinelBeacon(1, "cluster1")).thenReturn(true);
        Mockito.when(checkerDbConfig.shouldSentinelCheck("cluster1")).thenReturn(true);
        Mockito.when(checkerDbConfig.isSentinelAutoProcess()).thenReturn(true);
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
        instance.getCheckInfo().setStatus(ClusterMetaStatus.MIGRATING);
        Assert.assertFalse(controller.shouldCheck(instance));
    }

    @Test
    public void shouldCheckWhenClusterNotOnMigration() {
        Assert.assertTrue(controller.shouldCheck(instance));
    }
}
