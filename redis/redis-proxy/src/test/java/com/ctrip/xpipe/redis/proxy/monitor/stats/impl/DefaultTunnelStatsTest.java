package com.ctrip.xpipe.redis.proxy.monitor.stats.impl;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.session.DefaultBackendSession;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTunnelStatsTest extends AbstractProxyIntegrationTest {

    @Test
    public void testGetTunnelStatsResult() {
        Tunnel tunnel = mock(Tunnel.class);

        DefaultBackendSession backend = mock(DefaultBackendSession.class);
        when(tunnel.backend()).thenReturn(backend);
        DefaultTunnelStats tunnelStats = new DefaultTunnelStats(tunnel);
        Assert.assertNull(tunnelStats.getTunnelStatsResult());
    }
}