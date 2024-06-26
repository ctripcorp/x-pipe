package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.resource.TestResourceManager;
import com.ctrip.xpipe.redis.proxy.session.BackendSession;
import com.ctrip.xpipe.redis.proxy.session.FrontendSession;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTunnelMonitorManagerTest extends AbstractProxyIntegrationTest {

    private DefaultTunnelMonitorManager tunnelMonitorManager = new DefaultTunnelMonitorManager(new TestResourceManager());

    @Test
    public void testGetOrCreate() {
        Tunnel tunnel = mockTunnel();
        TunnelMonitor tunnelMonitor = tunnelMonitorManager.getOrCreate(tunnel);
        Assert.assertNotNull(tunnelMonitor);

        tunnelMonitorManager.getOrCreate(tunnel);
        Assert.assertEquals(1, tunnelMonitorManager.getTunnelMonitors().size());
    }

    @Test
    public void testRemove() {
        Tunnel tunnel = mockTunnel();
        TunnelMonitor tunnelMonitor = tunnelMonitorManager.getOrCreate(tunnel);
        Assert.assertNotNull(tunnelMonitor);

        Assert.assertFalse(tunnelMonitorManager.getTunnelMonitors().isEmpty());
        tunnelMonitorManager.remove(tunnel);
        Assert.assertTrue(tunnelMonitorManager.getTunnelMonitors().isEmpty());
    }

    private Tunnel mockTunnel() {
        Tunnel tunnel = mock(Tunnel.class);
        FrontendSession frontendSession = Mockito.mock(FrontendSession.class);
        BackendSession backendSession = Mockito.mock(BackendSession.class);
        SessionEstablished sessionEstablished = Mockito.mock(SessionEstablished.class);
        Mockito.when(frontendSession.getSessionState()).thenReturn(sessionEstablished);
        Mockito.when(backendSession.getSessionState()).thenReturn(sessionEstablished);
        when(tunnel.frontend()).thenReturn(frontendSession);
        when(tunnel.backend()).thenReturn(backendSession);
        return tunnel;
    }

}