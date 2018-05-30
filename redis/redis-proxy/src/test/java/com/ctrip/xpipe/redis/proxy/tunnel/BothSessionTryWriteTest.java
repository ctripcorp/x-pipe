package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.AbstractNettyTest;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.handler.BackendSessionHandler;
import com.ctrip.xpipe.redis.proxy.handler.FrontendSessionNettyHandler;
import com.ctrip.xpipe.redis.proxy.handler.TunnelTrafficReporter;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.session.*;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelEstablished;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * May 30, 2018
 */
public class BothSessionTryWriteTest extends AbstractProxyIntegrationTest {

    private EmbeddedChannel backendChannel;

    private EmbeddedChannel frontChannel;

    private DefaultTunnel tunnel;

    @Mock
    private DefaultTunnelManager tunnelManager;

    @Mock
    private ProxyEndpointSelector selector;

    private DefaultFrontendSession frontend;

    private DefaultBackendSession backend;

    private ProxyEndpointManager endpointManager = endpointManager();

    private NettySslHandlerFactory sslHandlerFactory = new NettyClientSslHandlerFactory(new TestProxyConfig());

    private ProxyConfig config = new TestProxyConfig();

    private EventLoopGroup eventLoopGroup1 = new NioEventLoopGroup(1, XpipeThreadFactory.create("backend"));

    private EventLoopGroup eventLoopGroup2 = new NioEventLoopGroup(1, XpipeThreadFactory.create("frontend"));

    private ProxyProtocol proxyProtocol;

    private static final String PROXY_PROTOCOL = "PROXY ROUTE TCP://127.0.0.1:6379\r\n";

    @Before
    public void beforeBothSessionTryWriteTest() {
        MockitoAnnotations.initMocks(this);
        when(tunnelManager.getOrCreate(frontChannel, proxyProtocol)).thenReturn(tunnel);
        frontChannel = new EmbeddedChannel(new LineBasedFrameDecoder(2048), new StringDecoder());
        spy(frontChannel);

        proxyProtocol = new DefaultProxyProtocolParser().read(PROXY_PROTOCOL);
        tunnel = new DefaultTunnel(frontChannel, endpointManager, proxyProtocol, sslHandlerFactory, config, eventLoopGroup2);

        frontend = new DefaultFrontendSession(tunnel, frontChannel, 300000);
        backend = new DefaultBackendSession(tunnel, 300000, selector, eventLoopGroup1, sslHandlerFactory);

        spy(frontend);
        spy(backend);

        backendChannel = new EmbeddedChannel(new LineBasedFrameDecoder(2048), new StringDecoder());
        backend.setChannel(backendChannel);


        tunnel.setFrontend(frontend);
        tunnel.setBackend(backend);

        backend.setSessionState(new SessionEstablished(backend));
        tunnel.setState(new TunnelEstablished(tunnel));

    }

    private static final String CRLF = "\r\n";

    private ByteBuf getByteBuf(String str) {
        return Unpooled.copiedBuffer((str+CRLF).getBytes());
    }

    @Test
    public void testTryWriteBothSide() throws TimeoutException {
        int N = 10000;
        Queue<String> sample = Lists.newLinkedList();
        while(N-- > 0) {
            String str = randomString();
            sample.offer(str);
            tunnel.forwardToFrontend(getByteBuf(str));
            tunnel.forwardToBackend(getByteBuf(str));
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(!frontChannel.outboundMessages().isEmpty()) {
                    frontChannel.writeInbound(frontChannel.readOutbound());
                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(!backendChannel.outboundMessages().isEmpty()) {
                    backendChannel.writeInbound(backendChannel.readOutbound());
                }
            }
        }).start();
        waitConditionUntilTimeOut(()-> backendChannel.outboundMessages().isEmpty()
                && frontChannel.outboundMessages().isEmpty(), 1500);

        Assert.assertEquals(backendChannel.inboundMessages().size(), frontChannel.inboundMessages().size());
        Assert.assertEquals(sample.size(), frontChannel.inboundMessages().size());

        while(!backendChannel.inboundMessages().isEmpty()) {
            String frontStr = (String) frontChannel.readInbound();
            String backStr = (String) backendChannel.readInbound();
            Assert.assertEquals(sample.peek(), frontStr);
            Assert.assertEquals(sample.poll(), backStr);
        }
    }
}
