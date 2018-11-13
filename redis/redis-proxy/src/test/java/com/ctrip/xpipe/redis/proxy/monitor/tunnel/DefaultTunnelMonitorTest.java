package com.ctrip.xpipe.redis.proxy.monitor.tunnel;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.session.BackendSession;
import com.ctrip.xpipe.redis.proxy.session.FrontendSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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
        monitor = new DefaultTunnelMonitor(proxyResourceManager, tunnel);

    }

    @Test
    public void testGetFrontendSessionMonitor() {
        Assert.assertNotNull(monitor.getFrontendSessionMonitor());
    }

    @Test
    public void testGetBackendSessionMonitor() {
        Assert.assertNotNull(monitor.getBackendSessionMonitor());
    }

    @Test
    public void testGetTunnelStats() {
        Assert.assertNotNull(monitor.getTunnelStats());
    }
}