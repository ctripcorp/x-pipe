package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
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
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import com.ctrip.xpipe.redis.proxy.tunnel.state.*;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyByte;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class DefaultTunnelTest extends AbstractProxyIntegrationTest {

    private EmbeddedChannel frontChannel;

    private DefaultTunnel tunnel;

    @Mock
    private DefaultTunnelManager tunnelManager;

    @Mock
    private DefaultFrontendSession frontend;

    @Mock
    private DefaultBackendSession backend;

    private ProxyEndpointManager endpointManager = endpointManager();

    private NettySslHandlerFactory sslHandlerFactory = new NettyClientSslHandlerFactory(new TestProxyConfig());

    private ProxyConfig config = new TestProxyConfig();

    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1, XpipeThreadFactory.create("Test"));

    private ProxyProtocol proxyProtocol;

    private static final String PROXY_PROTOCOL = "PROXY ROUTE TCP://127.0.0.1:6379\r\n";

    private ByteBuf testByteBuf = Unpooled.copiedBuffer("TEST".getBytes());

    @Before
    public void beforeDefaultTunnelTest() {
        MockitoAnnotations.initMocks(this);
        when(tunnelManager.getOrCreate(frontChannel, proxyProtocol)).thenReturn(tunnel);
        frontChannel = new EmbeddedChannel(new FrontendSessionNettyHandler(tunnelManager),
                new TunnelTrafficReporter(6000, frontend));

        proxyProtocol = new DefaultProxyProtocolParser().read(PROXY_PROTOCOL);
        tunnel = new DefaultTunnel(frontChannel, endpointManager, proxyProtocol, sslHandlerFactory, config, eventLoopGroup);

        tunnel.setFrontend(frontend);
        tunnel.setBackend(backend);

        EmbeddedChannel backendChannel = new EmbeddedChannel(new BackendSessionHandler(tunnel),
                new TunnelTrafficReporter(6000, backend));
        when(backend.getChannel()).thenReturn(backendChannel);
        when(frontend.getChannel()).thenReturn(frontChannel);

        when(backend.tunnel()).thenReturn(tunnel);
        when(frontend.tunnel()).thenReturn(tunnel);

        when(backend.getSessionType()).thenReturn(SESSION_TYPE.BACKEND);
        when(frontend.getSessionType()).thenReturn(SESSION_TYPE.FRONTEND);
    }

    @Test
    public void testForwardToBackend() {
        when(backend.tryWrite(testByteBuf)).then(new Answer<ChannelFuture>() {
            @Override
            public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
                Object object = invocation.getArguments()[0];
                Assert.assertTrue(object instanceof ByteBuf);
                Assert.assertEquals(testByteBuf, object);
                return null;
            }
        });
        tunnel.forwardToBackend(testByteBuf);
        verify(backend).tryWrite(testByteBuf);
        verify(frontend, never()).tryWrite(any());
    }

    @Test
    public void testForwardToFrontend() {
        when(frontend.tryWrite(testByteBuf)).then(new Answer<ChannelFuture>() {
            @Override
            public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
                Object object = invocation.getArguments()[0];
                Assert.assertTrue(object instanceof ByteBuf);
                Assert.assertEquals(testByteBuf, object);
                return frontChannel.newSucceededFuture();
            }
        });
        tunnel.forwardToFrontend(testByteBuf);
        verify(frontend).tryWrite(testByteBuf);
        verify(backend, never()).tryWrite(any());
    }

    @Test
    public void testCloseBackendRead() {
        tunnel.closeBackendRead();
        Assert.assertFalse(backend.getChannel().config().isAutoRead());
    }

    @Test
    public void testCloseFrontendRead() {
        tunnel.closeFrontendRead();
        Assert.assertFalse(frontChannel.config().isAutoRead());
    }

    @Test
    public void triggerBackendRead() {
        Assert.assertTrue(backend.getChannel().config().isAutoRead());
        backend.getChannel().config().setAutoRead(false);
        Assert.assertFalse(backend.getChannel().config().isAutoRead());
        tunnel.triggerBackendRead();
        Assert.assertTrue(backend.getChannel().config().isAutoRead());
    }

    @Test
    public void triggerFrontendRead() {
        frontChannel.config().setAutoRead(false);
        Assert.assertFalse(frontChannel.config().isAutoRead());
        tunnel.triggerFrontendRead();
        Assert.assertTrue(frontend.getChannel().config().isAutoRead());
    }

    @Test
    public void testFrontend() {
        Assert.assertEquals(frontend, tunnel.frontend());
    }

    @Test
    public void testBackend() {
        Assert.assertEquals(backend, tunnel.backend());
    }

    @Test
    public void getTunnelMeta() {
        logger.info("{}", tunnel.getTunnelMeta());
    }

    @Test
    public void getState() {
        Assert.assertEquals(new TunnelHalfEstablished(tunnel), tunnel.getState());
    }

    @Test
    public void testSetState1() {
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.setState(new TunnelHalfEstablished(tunnel));
        Assert.assertEquals(new TunnelEstablished(tunnel), tunnel.getState());
    }

    @Test
    public void testSetState2() {
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.setState(new TunnelClosing(tunnel));
        Assert.assertEquals(new TunnelClosing(tunnel), tunnel.getState());
    }

    @Test
    public void testSetState3() {
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.setState(new TunnelClosed(tunnel));
        Assert.assertEquals(new TunnelEstablished(tunnel), tunnel.getState());
    }

    @Test
    public void testSetState4() {
        tunnel.setState(new TunnelClosing(tunnel));
        tunnel.setState(new BackendClosed(tunnel));
        Assert.assertEquals(new TunnelClosing(tunnel), tunnel.getState());

        tunnel.setState(new FrontendClosed(tunnel));
        Assert.assertEquals(new TunnelClosing(tunnel), tunnel.getState());
    }

    @Test
    public void testSetState5() {
        tunnel.setState(new BackendClosed(tunnel));
        tunnel.setState(new FrontendClosed(tunnel));
        Assert.assertEquals(new BackendClosed(tunnel), tunnel.getState());

        tunnel.setState(new FrontendClosed(tunnel));
        Assert.assertEquals(new BackendClosed(tunnel), tunnel.getState());
    }

    @Test
    public void testSetState6() {
        registerTunnelMasterObserver();
        tunnel.setState(new FrontendClosed(tunnel));
        Assert.assertEquals(new TunnelClosed(tunnel), tunnel.getState());
        verify(backend).release();
    }

    @Test
    public void testSetState7() {
        registerTunnelMasterObserver();
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.setState(new BackendClosed(tunnel));
        Assert.assertEquals(new TunnelClosed(tunnel), tunnel.getState());
        verify(frontend).release();
    }

    private void registerTunnelMasterObserver() {
        tunnel.addObserver(tunnelManager);
        doCallRealMethod().when(tunnelManager).update(any(), any());
    }

    @Test
    public void getProxyProtocol() {
        Assert.assertEquals(proxyProtocol, tunnel.getProxyProtocol());
    }

    @Test
    public void testUpdate1() {
        tunnel.update(new SessionStateChangeEvent(new SessionInit(backend), new SessionEstablished(backend)), backend);
        Assert.assertEquals(new TunnelEstablished(tunnel), tunnel.getState());
    }

    @Test
    public void testUpdate2() {
        tunnel.update(new SessionStateChangeEvent(new SessionInit(frontend), new SessionEstablished(frontend)), frontend);
        Assert.assertEquals(new TunnelHalfEstablished(tunnel), tunnel.getState());
    }

    @Test
    public void testUpdate3() {
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.update(new SessionStateChangeEvent(new SessionInit(frontend), new SessionEstablished(frontend)), frontend);
        Assert.assertEquals(new TunnelEstablished(tunnel), tunnel.getState());
    }

    @Test
    public void testUpdate4() {
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.update(new SessionStateChangeEvent(new SessionClosing(frontend), new SessionClosed(frontend)), frontend);
        Assert.assertEquals(new FrontendClosed(tunnel), tunnel.getState());
    }

    @Test
    public void testUpdate5() {
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.update(new SessionStateChangeEvent(new SessionClosing(backend), new SessionClosed(backend)), backend);
        Assert.assertEquals(new BackendClosed(tunnel), tunnel.getState());
    }

    @Test
    public void testUpdate6() {
        registerTunnelMasterObserver();
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.update(new SessionStateChangeEvent(new SessionClosing(backend), new SessionClosed(backend)), backend);
        Assert.assertEquals(new TunnelClosed(tunnel), tunnel.getState());
    }

    @Test
    public void testUpdate7() {
        registerTunnelMasterObserver();
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.update(new SessionStateChangeEvent(new SessionClosing(frontend), new SessionClosed(frontend)), frontend);
        Assert.assertEquals(new TunnelClosed(tunnel), tunnel.getState());
    }

    @Test
    public void testRelease() throws Exception {
        tunnel.setState(new TunnelClosed(tunnel));
        tunnel.release();
        verify(frontend, never()).release();
        verify(backend, never()).release();
    }

    @Test
    public void testRelease2() throws Exception {
        tunnel.setState(new TunnelEstablished(tunnel));
        when(frontend.getSessionState()).thenReturn(new SessionEstablished(frontend));
        when(backend.getSessionState()).thenReturn(new SessionEstablished(backend));
        doCallRealMethod().when(frontend).isReleasable();
        doCallRealMethod().when(backend).isReleasable();
        tunnel.release();
        logger.info("[testRelease2] frontend: {}", frontend.getSessionState());
        logger.info("[testRelease2] backend: {}", backend.getSessionState());
        verify(frontend).release();
        verify(backend).release();
    }

}