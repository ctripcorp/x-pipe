package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.ProxyManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.HeteroInstanceLongDelay;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceLongDelay;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.HealthEventProcessorException;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class HeteroRouteHealthEventProcessorTest extends AbstractTest {

    private HeteroRouteHealthEventProcessor processor = new HeteroRouteHealthEventProcessor() {
        @Override
        protected long getHoldingMillis() {
            return 100;
        }
    };

    @Mock
    private ProxyManager proxyManager;

    @Mock
    private RedisHealthCheckInstance instance;

    private ProxyTunnelInfo proxyTunnelInfo;

    @Before
    public void beforeRouteHealthEventProcessorTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        processor.proxyManager = proxyManager;
        processor.scheduled = Executors.newScheduledThreadPool(1);
        processor = spy(processor);

        proxyTunnelInfo = new ProxyTunnelInfo();

        when(instance.getCheckInfo()).thenReturn(new DefaultRedisInstanceInfo("FRA-AWS", "cluster", "shard", new HostPort("127.0.0.3", 6379), "SHAJQ", ClusterType.ONE_WAY));
    }

    @Test
    public void testOnEventWithNormal() throws HealthEventProcessorException {
        processor.onEvent(new InstanceLongDelay(instance));
        verify(processor, never()).doOnEvent(any());
        verify(processor, never()).tryRecover(any(), any());
    }

    @Test
    public void testOnEventWithHetero() throws InterruptedException {
        doReturn(null).when(proxyManager).getProxyTunnelInfo(any(),any(),any(),any());
        processor.onEvent(new HeteroInstanceLongDelay(instance,1));
        verify(processor, times(1)).doOnEvent(any());
        verify(processor, never()).tryRecover(any(), any());

        Thread.sleep(100);
        doReturn(proxyTunnelInfo).when(proxyManager).getProxyTunnelInfo(any(),any(),any(),any());
        processor.onEvent(new HeteroInstanceLongDelay(instance,1));
        verify(processor, times(2)).doOnEvent(any());
        verify(processor, times(1)).tryRecover(any(), any());
    }
}
