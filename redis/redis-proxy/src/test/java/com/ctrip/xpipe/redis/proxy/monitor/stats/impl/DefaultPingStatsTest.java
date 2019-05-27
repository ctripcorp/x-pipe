package com.ctrip.xpipe.redis.proxy.monitor.stats.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.resource.TestResourceManager;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultPingStatsTest extends AbstractProxyIntegrationTest {

    private DefaultPingStats pingStats;

    private ResourceManager resourceManager;

    private ProxyEndpoint endpoint;

    private int port;

    @Before
    public void before() {
        port = randomPort();
        resourceManager = new TestResourceManager();
        endpoint = new DefaultProxyEndpoint(String.format("%s://%s:%d",
                ProxyEndpoint.PROXY_SCHEME.TCP.name(), "127.0.0.1", port));
        pingStats = new DefaultPingStats(resourceManager.getGlobalSharedScheduled(),
                endpoint, resourceManager.getKeyedObjectPool());
    }

    @Test
    public void getEndpoint() {
        Assert.assertEquals(endpoint, pingStats.getEndpoint());
    }

    @Test
    public void getPingStatsResult() throws Exception {
        Server server = startServer(port, "+PROXY PONG 127.0.0.1:" + port + "\r\n");
        HostPort target = new HostPort("127.0.0.1", port);
        Assert.assertEquals(new PingStatsResult(-1, -1, target, target), pingStats.getPingStatsResult());
        pingStats.doTask();
        sleep(100);
        Assert.assertNotNull(pingStats.getPingStatsResult());
        server.stop();
    }

    @Test
    public void doTask() throws Exception {
        Server server = startServer(port, "+PROXY PONG 127.0.0.1:" + port + "\r\n");
        pingStats.doTask();
        waitConditionUntilTimeOut(()->pingStats.getPingStatsResult() != null, 500);
        Assert.assertEquals(new HostPort(pingStats.getEndpoint().getHost(), pingStats.getEndpoint().getPort()),
                pingStats.getPingStatsResult().getDirect());
        server.stop();
    }
}