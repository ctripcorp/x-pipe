package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.commons.pool2.PooledObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
        server = startFirstProxy();
        factory = new SslEnabledNettyClientFactory(new TestResourceManager());
        factory.start();
    }

    @After
    public void afterSslEnabledNettyClientFactoryTest() {
        server.stop();
    }


    @Test
    public void testMakeObject() throws Exception {
        Server localServer = startEchoServer();
        int tlsPort = server.getConfig().frontendTlsPort();
        PooledObject<NettyClient> clientPooledObject = factory.makeObject(new DefaultProxyEndpoint(ProxyEndpoint.PROXY_SCHEME.PROXYTLS + "://127.0.0.1:" + tlsPort));
        clientPooledObject.getObject().sendRequest(Unpooled.copiedBuffer(String.format("+PROXY ROUTE TCP://127.0.0.1:%d\r\nhello", localServer.getPort()).getBytes()));
        sleep(1000);
    }

    @Test
    public void testTLSHandShakeError() throws Exception {
        int tlsPort = server.getConfig().frontendTlsPort();
        PooledObject<NettyClient> clientPooledObject = factory.makeObject(new DefaultProxyEndpoint(ProxyEndpoint.PROXY_SCHEME.PROXYTLS + "://127.0.0.1:" + tlsPort));
        clientPooledObject.getObject().sendRequest(Unpooled.copiedBuffer(("+PROXY MONITOR PingStats\r\n").getBytes()));
        sleep(1000);
    }

    @Ignore
    @Test
    public void manuallyTestFWS() throws Exception {
        PooledObject<NettyClient> clientPooledObject = factory.makeObject(new DefaultProxyEndpoint(ProxyEndpoint.PROXY_SCHEME.PROXYTLS + "://10.2.134.71:443"));
        clientPooledObject.getObject().sendRequest(
                Unpooled.copiedBuffer(("+PROXY MONITOR PingStats\r\n+PROXY MONITOR TunnelStats\r\n+PROXY MONITOR SocketStats\r\n").getBytes()),
                new ByteBufReceiver() {
                    @Override
                    public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                        logger.info("{}", ByteBufUtils.readToString(byteBuf));
                        return RECEIVER_RESULT.SUCCESS;
                    }

                    @Override
                    public void clientClosed(NettyClient nettyClient) {

                    }

                    @Override
                    public void clientClosed(NettyClient nettyClient, Throwable th) {

                    }
                });
        sleep(1000);
    }

    @Test
    public void manuallyTestTimeoutLog() throws Exception {
        PooledObject<NettyClient> clientPooledObject = factory.makeObject(new DefaultProxyEndpoint(ProxyEndpoint.PROXY_SCHEME.PROXYTLS + "://10.0.0.0:443"));
        clientPooledObject.getObject().sendRequest(
                Unpooled.copiedBuffer(("+PROXY MONITOR PingStats\r\n+PROXY MONITOR TunnelStats\r\n+PROXY MONITOR SocketStats\r\n").getBytes()),
                new ByteBufReceiver() {
                    @Override
                    public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                        logger.info("{}", ByteBufUtils.readToString(byteBuf));
                        return RECEIVER_RESULT.SUCCESS;
                    }

                    @Override
                    public void clientClosed(NettyClient nettyClient) {

                    }

                    @Override
                    public void clientClosed(NettyClient nettyClient, Throwable th) {

                    }
                });

        sleep(1000);
    }
}