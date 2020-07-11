package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CurrentDcDelayPingActionCollectorTest extends AbstractConsoleTest {

    @InjectMocks
    CurrentDcDelayPingActionCollector collector;

    @Mock
    private AlertManager alertManager;

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock
    private HealthCheckConfig healthCheckConfig;

    private String clusterId = "cluster1", shardId = "shard1", dcId = FoundationService.DEFAULT.getDataCenter();

    private RedisInstanceInfo currentMaster = new DefaultRedisInstanceInfo(dcId, clusterId, shardId, new HostPort("10.0.0.1", 6379), null, ClusterType.BI_DIRECTION);
    private RedisInstanceInfo remoteMaster = new DefaultRedisInstanceInfo("remote-dc", clusterId, shardId, new HostPort("10.0.0.2", 6379), null, ClusterType.BI_DIRECTION);

    private DelayActionListener delayActionListener;
    private PingActionListener pingActionListener;

    private int downAfterMilli = 200;
    private int healthyDelayMilli = 100;
    private int checkIntervalMilli = 100;

    @Before
    public void setupCurrentDcDelayPingActionCollectorTest() {
        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(remoteMaster);
        Mockito.when(instance.getHealthCheckConfig()).thenReturn(healthCheckConfig);
        Mockito.when(healthCheckConfig.pingDownAfterMilli()).thenReturn(downAfterMilli);
        Mockito.when(healthCheckConfig.delayDownAfterMilli()).thenReturn(downAfterMilli);
        Mockito.when(healthCheckConfig.getHealthyDelayMilli()).thenReturn(healthyDelayMilli);
        Mockito.when(healthCheckConfig.checkIntervalMilli()).thenReturn(checkIntervalMilli);
        delayActionListener = collector.createDelayActionListener();
        pingActionListener = collector.createPingActionListener();
        collector.setScheduled(scheduled);
    }

    @Test
    public void testSupportInstance() {
        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(currentMaster);
        Assert.assertTrue(collector.supportInstance(instance));
        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(remoteMaster);
        Assert.assertFalse(collector.supportInstance(instance));
    }

    @Test
    public void testInstanceUp() {
        pingActionListener.onAction(new PingActionContext(instance, true));
        delayActionListener.onAction(new DelayActionContext(instance, 0L));
        Mockito.verify(alertManager, Mockito.never()).alert(Mockito.any(RedisInstanceInfo.class), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testInstanceDown() {
        Mockito.doAnswer(invocation -> {
            pingActionListener.onAction(new PingActionContext(instance, false));
            delayActionListener.onAction(new DelayActionContext(instance, healthyDelayMilli * 2L));
            ALERT_TYPE type = invocation.getArgumentAt(1, ALERT_TYPE.class);
            Assert.assertEquals(ALERT_TYPE.CRDT_INSTANCE_DOWN, type);

            return null;
        }).when(alertManager).alert(Mockito.any(RedisInstanceInfo.class), Mockito.any(), Mockito.anyString());
        pingActionListener.onAction(new PingActionContext(instance, false));
        delayActionListener.onAction(new DelayActionContext(instance, healthyDelayMilli * 2L));

        sleep(downAfterMilli + checkIntervalMilli);
        Mockito.verify(alertManager, Mockito.times(1)).alert(Mockito.any(RedisInstanceInfo.class), Mockito.any(), Mockito.anyString());

        // no repeat alert
        sleep(checkIntervalMilli);
        Mockito.verify(alertManager, Mockito.times(1)).alert(Mockito.any(RedisInstanceInfo.class), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testInstanceUpAfterDown() {
        testInstanceDown();

        Mockito.doAnswer(invocation -> {
            pingActionListener.onAction(new PingActionContext(instance, false));
            delayActionListener.onAction(new DelayActionContext(instance, healthyDelayMilli * 2L));
            ALERT_TYPE type = invocation.getArgumentAt(1, ALERT_TYPE.class);
            Assert.assertEquals(ALERT_TYPE.CRDT_INSTANCE_UP, type);

            return null;
        }).when(alertManager).alert(Mockito.any(RedisInstanceInfo.class), Mockito.any(), Mockito.anyString());
        pingActionListener.onAction(new PingActionContext(instance, true));
        sleep(checkIntervalMilli);
        Mockito.verify(alertManager, Mockito.times(1)).alert(Mockito.any(RedisInstanceInfo.class), Mockito.any(), Mockito.anyString());

        pingActionListener.onAction(new PingActionContext(instance, true));
        delayActionListener.onAction(new DelayActionContext(instance, 0L));
        sleep(checkIntervalMilli);
        Mockito.verify(alertManager, Mockito.times(2)).alert(Mockito.any(RedisInstanceInfo.class), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testInstanceDownAfterUp() {
        testInstanceUp();
        testInstanceDown();
    }

}
