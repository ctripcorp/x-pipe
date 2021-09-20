package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE.CRDT_CROSS_DC_REPLICATION_DOWN;
import static com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE.CRDT_CROSS_DC_REPLICATION_UP;

@RunWith(MockitoJUnitRunner.class)
public class MultiMasterDelayListenerTest extends AbstractCheckerTest {

    @InjectMocks
    private MultiMasterDelayListener multiMasterDelayListener;

    @Mock
    private MetaCache metaCache;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private AlertManager alertManager;

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock HealthCheckConfig healthCheckConfig;

    private MultiMasterDelayListener.CrossMasterHealthStatus status;

    private int healthDelayMilli = 100;

    private int delayDownMilli = 200;

    private RedisInstanceInfo currentMasterInfo = new DefaultRedisInstanceInfo("jq", "cluster1", "shard1",
            new HostPort("27.0.0.1", 6379), null, ClusterType.BI_DIRECTION);

    private RedisInstanceInfo oyMasterInfo = new DefaultRedisInstanceInfo("oy", "cluster1", "shard1",
            new HostPort("27.0.0.2", 6379), null, ClusterType.BI_DIRECTION);

    private RedisInstanceInfo rbMasterInfo = new DefaultRedisInstanceInfo("rb", "cluster1", "shard1",
            new HostPort("27.0.0.3", 6379), null, ClusterType.BI_DIRECTION);

    @Before
    public void setupCrossMasterHealthStatusTest() {
        Mockito.when(instance.getCheckInfo()).thenReturn(currentMasterInfo);
        Mockito.when(instance.getHealthCheckConfig()).thenReturn(healthCheckConfig);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.when(healthCheckConfig.getHealthyDelayMilli()).thenReturn(healthDelayMilli);
        Mockito.when(healthCheckConfig.delayDownAfterMilli()).thenReturn(delayDownMilli);

        status = multiMasterDelayListener.new CrossMasterHealthStatus("cluster1", "shard1");
    }

    /* ----- test for CrossMasterHealthStatus ----- */
    @Test
    public void testStatusChange() {
        // init
        status.updateTargetMasterDelay("rb", 0, instance);
        status.updateTargetMasterDelay("oy", 0, instance);
        status.refreshCurrentMasterHealthStatus();
        Assert.assertTrue(status.getHealthStatus());
        Mockito.verify(alertManager, Mockito.never()).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());

        // delay not healthy
        status.updateTargetMasterDelay("rb", healthDelayMilli, instance);
        status.updateTargetMasterDelay("oy", healthDelayMilli, instance);
        Assert.assertTrue(status.getHealthStatus());
        Mockito.verify(alertManager, Mockito.never()).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());

        // delay down
        sleep(delayDownMilli + 1);
        status.refreshCurrentMasterHealthStatus();
        Assert.assertFalse(status.getHealthStatus());
        Mockito.verify(alertManager, Mockito.times(1)).alert("cluster1", "shard1", null, CRDT_CROSS_DC_REPLICATION_DOWN, "replication unhealthy from jq to [rb, oy]");

        // delay half recovery
        status.updateTargetMasterDelay("rb", 0, instance);
        status.refreshCurrentMasterHealthStatus();
        Assert.assertFalse(status.getHealthStatus());
        Mockito.verify(alertManager, Mockito.times(1)).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());

        // delay all recovery
        status.updateTargetMasterDelay("oy", 0, instance);
        Assert.assertTrue(status.getHealthStatus());
        Mockito.verify(alertManager, Mockito.times(2)).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());
        Mockito.verify(alertManager, Mockito.times(1)).alert("cluster1", "shard1", null, CRDT_CROSS_DC_REPLICATION_UP, "replication become healthy from jq");
    }

    @Test
    public void testMetaChange() {
        status.updateTargetMasterDelay("rb", 0, instance);
        status.updateTargetMasterDelay("oy", 0, instance);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta());

        sleep(delayDownMilli + 1);
        status.refreshCurrentMasterHealthStatus();
        Assert.assertTrue(status.getHealthStatus());
        Mockito.verify(alertManager, Mockito.times(0)).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());

    }

    /* ----- test for MultiMasterDelayListener ----- */

    @Test
    public void testAllDelayHealth() {
        Mockito.when(instance.getCheckInfo()).thenReturn(rbMasterInfo);
        multiMasterDelayListener.onAction(new DelayActionContext(instance, 0L));
        Mockito.when(instance.getCheckInfo()).thenReturn(oyMasterInfo);
        multiMasterDelayListener.onAction(new DelayActionContext(instance, 0L));
        multiMasterDelayListener.checkAllHealthStatus();
        Mockito.verify(alertManager, Mockito.times(0)).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testAllDelayKeepHealthy() {
        long healthDelayNano = TimeUnit.MILLISECONDS.toNanos(healthDelayMilli/10);
        Mockito.when(instance.getCheckInfo()).thenReturn(rbMasterInfo);
        multiMasterDelayListener.onAction(new DelayActionContext(instance, healthDelayNano));
        multiMasterDelayListener.checkAllHealthStatus();

        sleep(delayDownMilli / 2 + 10);
        multiMasterDelayListener.onAction(new DelayActionContext(instance, healthDelayNano));

        sleep(delayDownMilli / 2 + 10);
        multiMasterDelayListener.onAction(new DelayActionContext(instance, healthDelayNano));
        multiMasterDelayListener.checkAllHealthStatus();

        Mockito.verify(alertManager, Mockito.times(0)).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testDelayUnhealthy() {
        testAllDelayHealth();
        sleep(delayDownMilli + 1);
        multiMasterDelayListener.checkAllHealthStatus();
        Mockito.verify(alertManager, Mockito.times(1)).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());
        Mockito.verify(alertManager, Mockito.times(1)).alert("cluster1", "shard1", null, CRDT_CROSS_DC_REPLICATION_DOWN, "replication unhealthy from jq to [rb, oy]");
    }

    @Test
    public void testDelayRecovery() {
        testDelayUnhealthy();
        Mockito.when(instance.getCheckInfo()).thenReturn(rbMasterInfo);
        multiMasterDelayListener.onAction(new DelayActionContext(instance, 0L));
        Mockito.verify(alertManager, Mockito.times(1)).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());

        Mockito.when(instance.getCheckInfo()).thenReturn(oyMasterInfo);
        multiMasterDelayListener.onAction(new DelayActionContext(instance, 0L));
        Mockito.verify(alertManager, Mockito.times(2)).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());
        Mockito.verify(alertManager, Mockito.times(1)).alert("cluster1", "shard1", null, CRDT_CROSS_DC_REPLICATION_UP, "replication become healthy from jq");

    }

    @Test
    public void testNotExistDcDelay() {
        testAllDelayHealth();
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta());

        sleep(delayDownMilli + 1);
        multiMasterDelayListener.checkAllHealthStatus();
        Mockito.verify(alertManager, Mockito.times(0)).alert(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "crdt-replication-test.xml";
    }

}
