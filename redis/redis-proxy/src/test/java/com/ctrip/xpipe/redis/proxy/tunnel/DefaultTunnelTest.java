package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.handler.BackendSessionHandler;
import com.ctrip.xpipe.redis.proxy.handler.FrontendSessionNettyHandler;
import com.ctrip.xpipe.redis.proxy.handler.SessionTrafficReporter;
import com.ctrip.xpipe.redis.proxy.monitor.DefaultTunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.session.DefaultBackendSession;
import com.ctrip.xpipe.redis.proxy.session.DefaultFrontendSession;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import com.ctrip.xpipe.redis.proxy.session.SessionStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import com.ctrip.xpipe.redis.proxy.tunnel.state.*;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class DefaultTunnelTest extends AbstractRedisProxyServerTest {

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

    private ProxyConnectProtocol proxyConnectProtocol;

    private static final String PROXY_PROTOCOL = "PROXY ROUTE TCP://127.0.0.1:6379;FORWARD_FOR 127.0.0.1:80\r\n";

    private ByteBuf testByteBuf = Unpooled.copiedBuffer("TEST".getBytes());

    @Before
    public void beforeDefaultTunnelTest() {
        MockitoAnnotations.initMocks(this);
        when(tunnelManager.create(frontChannel, proxyConnectProtocol)).thenReturn(tunnel);
        frontChannel = new EmbeddedChannel();

        proxyConnectProtocol = new DefaultProxyConnectProtocolParser().read(PROXY_PROTOCOL);
        tunnel = new DefaultTunnel(frontChannel, proxyConnectProtocol, config, proxyResourceManager,
                new DefaultTunnelMonitorManager(proxyResourceManager), scheduled);

        frontChannel.pipeline().addLast(new FrontendSessionNettyHandler(tunnel),
                new SessionTrafficReporter(6000, ()->true, frontend));


        tunnel.setFrontend(frontend);
        tunnel.setBackend(backend);

        EmbeddedChannel backendChannel = new EmbeddedChannel(new BackendSessionHandler(tunnel),
                new SessionTrafficReporter(6000, ()->true, backend));
        when(backend.getChannel()).thenReturn(backendChannel);
        when(frontend.getChannel()).thenReturn(frontChannel);

        when(backend.tunnel()).thenReturn(tunnel);
        when(frontend.tunnel()).thenReturn(tunnel);

        when(backend.getSessionType()).thenReturn(SESSION_TYPE.BACKEND);
        when(frontend.getSessionType()).thenReturn(SESSION_TYPE.FRONTEND);
    }

    @After
    public void afterbeforeDefaultTunnelTest() {
        backend = null;
        proxyConnectProtocol = null;
        frontend = null;
        frontChannel = null;
        tunnel = null;
        tunnelManager = null;
    }

    @Test
    public void testAddCompressOptionToProtocolIfNeeded() {
        ((TestProxyConfig)config).setCompress(true);
        ProxyConnectProtocol proxyConnectProtocol1 =
                new DefaultProxyConnectProtocolParser().read(PROXY_PROTOCOL);

        DefaultTunnel tunnel1 = new DefaultTunnel(frontChannel, proxyConnectProtocol1, config, proxyResourceManager,
                new DefaultTunnelMonitorManager(proxyResourceManager), scheduled);
        tunnel1.addCompressOptionToProtocolIfNeeded();
        Assert.assertNull(tunnel1.getProxyProtocol().getCompressAlgorithm());

        proxyConnectProtocol1 = new DefaultProxyConnectProtocolParser()
                .read("PROXY ROUTE PROXYTLS://127.0.0.1:443,PROXYTLS://127.0.0.2:443 TCP://127.0.0.1:6379;FORWARD_FOR 127.0.0.1:80\n");
        tunnel1 = new DefaultTunnel(frontChannel, proxyConnectProtocol1, config, proxyResourceManager,
                new DefaultTunnelMonitorManager(proxyResourceManager), scheduled);
        tunnel1.addCompressOptionToProtocolIfNeeded();
        Assert.assertNotNull(tunnel1.getProxyProtocol().getCompressAlgorithm());
    }

    @Test
    public void testForwardToBackend() {
        doAnswer(new Answer<ChannelFuture>() {
            @Override
            public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
                Object object = invocation.getArguments()[0];
                Assert.assertTrue(object instanceof ByteBuf);
                Assert.assertEquals(testByteBuf, object);
                return null;
            }
        }).when(backend).tryWrite(testByteBuf);
        tunnel.forwardToBackend(testByteBuf);
        verify(backend).tryWrite(testByteBuf);
        verify(frontend, never()).tryWrite(any());
    }

    @Test
    public void testForwardToFrontend() {
        doAnswer(new Answer<ChannelFuture>() {
            @Override
            public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
                Object object = invocation.getArguments()[0];
                Assert.assertTrue(object instanceof ByteBuf);
                Assert.assertEquals(testByteBuf, object);
                return frontChannel.newSucceededFuture();
            }
        }).when(frontend).tryWrite(testByteBuf);
        tunnel.forwardToFrontend(testByteBuf);
        verify(frontend).tryWrite(testByteBuf);
        verify(backend, never()).tryWrite(any());
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

    private void registerTunnelMasterObserver() {
        tunnel.addObserver(tunnelManager);
        doCallRealMethod().when(tunnelManager).update(any(), any());
    }

    @Test
    public void getProxyProtocol() {
        Assert.assertEquals(proxyConnectProtocol, tunnel.getProxyProtocol());
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
        tunnel = spy(tunnel);
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.update(new SessionStateChangeEvent(new SessionClosing(frontend), new SessionClosed(frontend)), frontend);
        Assert.assertEquals(new TunnelClosing(tunnel), tunnel.getState());
        verify(tunnel).setState(new FrontendClosed(tunnel));
    }

    @Test
    public void testUpdate5() {
        tunnel = spy(tunnel);
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.update(new SessionStateChangeEvent(new SessionClosing(backend), new SessionClosed(backend)), backend);
        Assert.assertEquals(new TunnelClosing(tunnel), tunnel.getState());
        verify(tunnel).setState(new BackendClosed(tunnel));
    }

    @Test
    public void testUpdate6() {
        frontend = new DefaultFrontendSession(tunnel, new EmbeddedChannel(), 3000);
        frontend.addObserver(tunnel);
        tunnel.setFrontend(frontend);
        registerTunnelMasterObserver();
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.update(new SessionStateChangeEvent(new SessionClosing(backend), new SessionClosed(backend)), backend);
        Assert.assertEquals(new TunnelClosed(tunnel), tunnel.getState());
    }

    @Test
    public void testUpdate7() {
        DefaultProxyEndpointSelector selector = new DefaultProxyEndpointSelector(Lists.newArrayList(), endpointManager());
        ResourceManager resourceManager = mock(ResourceManager.class);
        when(resourceManager.createProxyEndpointSelector(any())).thenReturn(selector);
        backend = new DefaultBackendSession(tunnel, new NioEventLoopGroup(1), 1, resourceManager);
        tunnel.setBackend(backend);
        backend.addObserver(tunnel);
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
        tunnel.release();
        logger.info("[testRelease2] frontend: {}", frontend.getSessionState());
        logger.info("[testRelease2] backend: {}", backend.getSessionState());
        verify(frontend).release();
        verify(backend).release();
    }

    @Test
    public void testTunnelCheck() {
        tunnel.setState(new TunnelEstablished(tunnel));
        tunnel.check();
        Mockito.verify(frontend, never()).release();
        Mockito.verify(backend, never()).release();

        tunnel.backendBlockFrom.set(System.currentTimeMillis());
        tunnel.check();
        Mockito.verify(frontend, never()).release();
        Mockito.verify(backend, never()).release();

        tunnel.frontendBlockFrom.set(1);
        tunnel.check();
        Mockito.verify(frontend, times(1)).release();
        Mockito.verify(backend, never()).release();

        tunnel.frontendBlockFrom.set(-1);
        tunnel.backendBlockFrom.set(1);
        tunnel.check();
        Mockito.verify(frontend, times(1)).release();
        Mockito.verify(backend, times(1)).release();
    }

}