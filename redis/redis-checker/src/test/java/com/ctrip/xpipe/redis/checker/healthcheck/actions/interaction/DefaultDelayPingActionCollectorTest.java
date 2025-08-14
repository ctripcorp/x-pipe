package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.ClusterHealthManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2024/10/15
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultDelayPingActionCollectorTest extends AbstractCheckerTest {

    @InjectMocks
    private DefaultDelayPingActionCollector delayPingActionCollector;

    @Mock
    private ClusterHealthManager clusterHealthManager;

    @Mock
    private CheckerConfig config;

    @Mock
    private Observer observer;

    private RedisHealthCheckInstance instance;

    private int lastMarkTimeout = 20;

    @Before
    public void setupDefaultDelayPingActionCollectorTest() throws Exception {
        delayPingActionCollector.setScheduled(scheduled);
        when(clusterHealthManager.createHealthStatusObserver()).thenReturn(observer);

        instance = newRandomRedisHealthCheckInstance(6379);
        delayPingActionCollector.createOrGetHealthStatus(instance);

        when(config.getMarkdownInstanceMaxDelayMilli()).thenReturn(lastMarkTimeout/2);
        when(config.getCheckerMetaRefreshIntervalMilli()).thenReturn(lastMarkTimeout/2);
    }

    @Test
    public void setGetLastMarkTest() {
        HealthStatusDesc statusDesc = delayPingActionCollector.getHealthStatusDesc(new HostPort("10.0.0.1", 10));
        Assert.assertEquals(HEALTH_STATE.UNKNOWN, statusDesc.getState());
        Assert.assertNull(statusDesc.getLastMarkHandled());

        HostPort hostPort = new HostPort(instance.getEndpoint().getHost(), instance.getEndpoint().getPort());
        delayPingActionCollector.updateLastMarkHandled(hostPort, true);
        statusDesc = delayPingActionCollector.getHealthStatusDesc(hostPort);
        Assert.assertTrue(statusDesc.getLastMarkHandled());

        sleep(lastMarkTimeout + 1);
        statusDesc = delayPingActionCollector.getHealthStatusDesc(hostPort);
        Assert.assertNull(statusDesc.getLastMarkHandled());
    }

}
