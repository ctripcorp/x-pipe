package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.monitor.DefaultTunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.session.DefaultBackendSession;
import com.ctrip.xpipe.redis.proxy.session.DefaultFrontendSession;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
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

import java.util.Queue;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 30, 2018
 */
public class BothSessionTryWriteTest extends AbstractRedisProxyServerTest {

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

    private ProxyConnectProtocol proxyConnectProtocol;

    private static final String PROXY_PROTOCOL = "PROXY ROUTE TCP://127.0.0.1:6379\r\n";

    @Before
    public void beforeBothSessionTryWriteTest() {
        MockitoAnnotations.initMocks(this);
        when(tunnelManager.create(frontChannel, proxyConnectProtocol)).thenReturn(tunnel);
        frontChannel = new EmbeddedChannel(new LineBasedFrameDecoder(2048), new StringDecoder());

        proxyConnectProtocol = new DefaultProxyConnectProtocolParser().read(PROXY_PROTOCOL);
        tunnel = new DefaultTunnel(frontChannel, proxyConnectProtocol, config, proxyResourceManager, new DefaultTunnelMonitorManager(proxyResourceManager), scheduled);

        frontend = new DefaultFrontendSession(tunnel, frontChannel, 300000);
        ResourceManager resourceManager = mock(ResourceManager.class);
        when(resourceManager.createProxyEndpointSelector(any())).thenReturn(selector);
        backend = new DefaultBackendSession(tunnel, new NioEventLoopGroup(1), 300000, resourceManager);

        frontend = spy(frontend);
        backend = spy(backend);

        backendChannel = new EmbeddedChannel(new LineBasedFrameDecoder(2048), new StringDecoder());
        backend.setChannel(backendChannel);


        tunnel.setFrontend(frontend);
        tunnel.setBackend(backend);

        when(backend.getSessionState()).thenReturn(new SessionEstablished(backend));
        when(frontend.getSessionState()).thenReturn(new SessionEstablished(frontend));

    }

    private static final String CRLF = "\r\n";

    private ByteBuf getByteBuf(String str) {
        return Unpooled.copiedBuffer((str+CRLF).getBytes());
    }

    @Test
    public void preTestTryWrite() {
        frontChannel.writeAndFlush(getByteBuf("hello"));
        logger.info("[outboundMessage] {}", frontChannel.outboundMessages());
        ByteBuf buf = frontChannel.readOutbound();
        backendChannel.writeInbound(buf);
        logger.info("[inboundMessages] {}", backendChannel.inboundMessages());
        logger.info("[outboundMessage] {}", frontChannel.outboundMessages().isEmpty());
    }

    @Test
    public void testTryWriteBothSide() throws TimeoutException {
        int N = 1000;
        Queue<String> sample = Lists.newLinkedList();
        while(N-- > 0) {
            String str = randomString();
            sample.offer(str);
            tunnel.forwardToFrontend(getByteBuf(str));
            tunnel.forwardToBackend(getByteBuf(str));
        }

        while(!frontChannel.outboundMessages().isEmpty()) {
            ByteBuf buf = frontChannel.readOutbound();
            backendChannel.writeInbound(buf);
        }
        while(!backendChannel.outboundMessages().isEmpty()) {
            ByteBuf buf = backendChannel.readOutbound();
            frontChannel.writeInbound(buf);
        }
        waitConditionUntilTimeOut(()-> backendChannel.outboundMessages().isEmpty()
                && frontChannel.outboundMessages().isEmpty(), 500);

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
