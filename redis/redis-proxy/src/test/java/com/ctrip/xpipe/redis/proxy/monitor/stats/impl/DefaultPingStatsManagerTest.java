package com.ctrip.xpipe.redis.proxy.monitor.stats.impl;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStats;
import com.ctrip.xpipe.redis.proxy.resource.TestResourceManager;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Test;

public class DefaultPingStatsManagerTest extends AbstractProxyIntegrationTest {

    private DefaultPingStatsManager manager = new DefaultPingStatsManager(new TestResourceManager(),
            new DefaultProxyEndpointManager(()->60000));

    @Test
    public void testGetAllPingStats() {
        Assert.assertNotNull(manager.getAllPingStats());
    }

    @Test
    public void testCreate() throws Exception {
        Server server = startEmptyServer();
        PingStats pingStats = manager.create(new DefaultProxyEndpoint(
                String.format("%s://%s:%d", ProxyEndpoint.PROXY_SCHEME.TCP.name(), "127.0.0.1", server.getPort())));
        Assert.assertNotNull(pingStats);
        server.stop();
    }

}