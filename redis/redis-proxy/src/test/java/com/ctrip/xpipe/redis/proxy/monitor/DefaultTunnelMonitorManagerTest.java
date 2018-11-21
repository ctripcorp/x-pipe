package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.resource.TestResourceManager;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class DefaultTunnelMonitorManagerTest extends AbstractProxyIntegrationTest {

    private DefaultTunnelMonitorManager tunnelMonitorManager = new DefaultTunnelMonitorManager(new TestResourceManager());

    @Test
    public void testGetOrCreate() {
        Tunnel tunnel = mock(Tunnel.class);
        TunnelMonitor tunnelMonitor = tunnelMonitorManager.getOrCreate(tunnel);
        Assert.assertNotNull(tunnelMonitor);

        tunnelMonitorManager.getOrCreate(tunnel);
        Assert.assertEquals(1, tunnelMonitorManager.getTunnelMonitors().size());
    }

    @Test
    public void testRemove() {
        Tunnel tunnel = mock(Tunnel.class);
        TunnelMonitor tunnelMonitor = tunnelMonitorManager.getOrCreate(tunnel);
        Assert.assertNotNull(tunnelMonitor);

        Assert.assertFalse(tunnelMonitorManager.getTunnelMonitors().isEmpty());
        tunnelMonitorManager.remove(tunnel);
        Assert.assertTrue(tunnelMonitorManager.getTunnelMonitors().isEmpty());
    }
}