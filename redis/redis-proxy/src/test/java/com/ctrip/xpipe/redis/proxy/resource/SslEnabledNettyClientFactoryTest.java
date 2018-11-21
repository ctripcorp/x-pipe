package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.Unpooled;
import org.apache.commons.pool2.PooledObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class SslEnabledNettyClientFactoryTest extends AbstractProxyIntegrationTest {

    private DefaultProxyServer server;

    private SslEnabledNettyClientFactory factory;

    @Before
    public void beforeSslEnabledNettyClientFactoryTest() throws Exception {
        server = new DefaultProxyServer().setConfig(new TestProxyConfig()).setResourceManager(new TestResourceManager());
        prepare(server);
        server.start();
        factory = new SslEnabledNettyClientFactory(new TestResourceManager());
        factory.start();
    }

    @After
    public void afterSslEnabledNettyClientFactoryTest() {
        server.stop();
    }


    @Test
    public void makeObject() throws Exception {
        Server localServer = startEchoServer();
        int tlsPort = server.getConfig().frontendTlsPort();
        PooledObject<NettyClient> clientPooledObject = factory.makeObject(new DefaultProxyEndpoint(ProxyEndpoint.PROXY_SCHEME.TLS + "://127.0.0.1:" + tlsPort));
        clientPooledObject.getObject().sendRequest(Unpooled.copiedBuffer(String.format("+PROXY ROUTE TCP://127.0.0.1:%d\r\nhello", localServer.getPort()).getBytes()));
        sleep(1000 * 10);
    }
}