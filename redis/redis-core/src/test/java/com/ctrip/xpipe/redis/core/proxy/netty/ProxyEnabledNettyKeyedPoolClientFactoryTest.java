package com.ctrip.xpipe.redis.core.proxy.netty;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.resource.ConsoleProxyResourceManager;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Sep 14, 2018
 */
public class ProxyEnabledNettyKeyedPoolClientFactoryTest extends AbstractRedisTest {

    private ProxyEnabledNettyKeyedPoolClientFactory factory;

    @Before
    public void beforeProxyEnabledNettyKeyedPoolClientFactoryTest() throws Exception {
        ProxyResourceManager resourceManager = new ConsoleProxyResourceManager(
                new DefaultProxyEndpointManager(()->1), new NaiveNextHopAlgorithm());
        factory = new ProxyEnabledNettyKeyedPoolClientFactory(resourceManager);
        factory.start();
    }

    @Test
    public void testMakeNormalObject() throws Exception {
        Server server = startEchoServer();
        NettyClient client = factory.makeObject(localHostEndpoint(server.getPort())).getObject();
        Assert.assertEquals(1, server.getConnected());
        Assert.assertTrue(client.channel().isActive());

        Assert.assertTrue(client instanceof DefaultNettyClient);
        client.sendRequest(Unpooled.copiedBuffer("hello".getBytes()));

    }

//    @Test
    // manually test by connect fws proxy
    public void testMakeProxyedObject() throws Exception {
        FakeRedisServer server = startFakeRedisServer();
        Endpoint endpoint = new ProxyEnabledEndpoint("localhost", server.getPort(),
                (ProxyConnectProtocol) new DefaultProxyConnectProtocolParser().read("PROXY ROUTE PROXYTCP://10.2.131.200:80 TCP://10.32.21.145:" + server.getPort()));
        NettyClient client = factory.makeObject(endpoint).getObject();

        waitConditionUntilTimeOut(()->server.getConnected() == 1, 1000);
        client.sendRequest(Unpooled.copiedBuffer(toRedisProtocalString("PING").getBytes()));

        Thread.sleep(1000 * 10);
    }

    @Test
    public void testValidateObject() {

    }
}