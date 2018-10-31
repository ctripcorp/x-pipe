package com.ctrip.xpipe.redis.proxy.monitor.tunnel;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.session.BackendSession;
import com.ctrip.xpipe.redis.proxy.session.FrontendSession;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class DefaultTunnelMonitorTest extends AbstractRedisProxyServerTest {

    private Tunnel tunnel;

    private DefaultTunnelMonitor monitor;

    @Before
    public void beforeDefaultTunnelMonitorTest() throws Exception {
        tunnel = mock(Tunnel.class);
        TestProxyConfig testProxyConfig = (TestProxyConfig) proxyResourceManager.getProxyConfig();
        testProxyConfig.setSessionIdleTime(10);
        monitor = new DefaultTunnelMonitor(proxyResourceManager, tunnel) {
            @Override
            protected int getCheckInterval() {
                return 5;
            }
        };

    }

    @Test
    public void testMonitorShutdownTunnel() throws Exception {
        FrontendSession frontend = mock(FrontendSession.class);
        BackendSession backend = mock(BackendSession.class);
        when(tunnel.frontend()).thenReturn(frontend);
        when(tunnel.backend()).thenReturn(backend);
        monitor.start();
        sleep(50);
        verify(frontend, atLeastOnce()).release();
    }
}