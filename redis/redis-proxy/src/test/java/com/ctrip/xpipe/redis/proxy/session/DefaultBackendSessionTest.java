package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.redis.core.exception.NoResourceException;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.SelectOneCycle;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.handler.BackendSessionHandler;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInit;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import static com.ctrip.xpipe.redis.proxy.session.DefaultBackendSession.*;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class DefaultBackendSessionTest extends AbstractRedisProxyServerTest {

    private DefaultBackendSession session;

    @Mock
    private Tunnel tunnel;

    @Spy
    private DefaultProxyEndpointSelector selector = new DefaultProxyEndpointSelector(Lists.newArrayList(), endpointManager());

    private NettySslHandlerFactory sslHandlerFactory = new NettyServerSslHandlerFactory(new TestProxyConfig());

    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1, XpipeThreadFactory.create("Test-BackendSession"));

    @Before
    public void beforeDefaultBackendSessionTest() {
        MockitoAnnotations.initMocks(this);
        ResourceManager resourceManager = mock(ResourceManager.class);
        TestProxyConfig proxyConfig1 = new TestProxyConfig();
        proxyConfig1.setCompress(true);
        when(resourceManager.getProxyConfig()).thenReturn(proxyConfig1);
        when(resourceManager.createProxyEndpointSelector(any())).thenReturn(selector);
        session = new DefaultBackendSession(tunnel, new NioEventLoopGroup(1), 300000, resourceManager);

    }

    @After
    public void afterDefaultBackendSessionTest() {
        session.release();
    }

    public void testSendImmdiateAfterProtocol() throws Exception {
        session.sendAfterProtocol(testByteBuf());
        session.sendAfterProtocol(testByteBuf());
    }

    @Test
    public void getEndpoint() {
    }

    @Test
    public void onChannelEstablished1() {
        when(tunnel.getProxyProtocol()).thenReturn(new DefaultProxyConnectProtocolParser().read("PROXY ROUTE TCP://127.0.0.1:6379;FORWARD_FOR 127.0.0.1:80\n"));
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(BACKEND_SESSION_HANDLER, new BackendSessionHandler(tunnel));
        session.endpoint = new DefaultProxyEndpoint("TCP://127.0.0.1:6379");
        SessionEventHandler handler = mock(SessionEventHandler.class);
        session.addSessionEventHandler(handler);
        session.onChannelEstablished(channel);
        Assert.assertNull(channel.pipeline().get(BACKEND_COMPRESS_DECODER));
        Assert.assertNull(channel.pipeline().get(BACKEND_COMPRESS_ENCODER));
        verify(handler).onEstablished();
    }

    @Test
    public void onChannelEstablished2() {
        when(tunnel.getProxyProtocol()).thenReturn(new DefaultProxyConnectProtocolParser()
                .read("PROXY ROUTE PROXYTLS://127.0.0.1:443,PROXYTLS://127.0.0.2:443 TCP://127.0.0.1:6379;FORWARD_FOR 127.0.0.1:80\n"));
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(BACKEND_SESSION_HANDLER, new BackendSessionHandler(tunnel));
        session.endpoint = new DefaultProxyEndpoint("TCP://127.0.0.1:6379");
        SessionEventHandler handler = mock(SessionEventHandler.class);
        session.addSessionEventHandler(handler);
        session.onChannelEstablished(channel);
        Assert.assertNotNull(channel.pipeline().get(BACKEND_COMPRESS_DECODER));
        Assert.assertNotNull(channel.pipeline().get(BACKEND_COMPRESS_ENCODER));
        verify(handler).onEstablished();
    }


    @Test
    public void doStart() throws Exception {
        when(selector.selectCounts()).thenReturn(1);
        when(selector.getCandidates()).thenReturn(Lists.newArrayList());
        selector.setSelectStrategy(new SelectOneCycle(selector));
        doCallRealMethod().when(selector).nextHop();

        Throwable throwable = null;
        try {
            session.doStart();
        } catch (Exception e) {
            throwable = e;
        }
        Assert.assertNotNull(throwable);
        Assert.assertTrue(throwable instanceof NoResourceException);
    }

    @Test
    public void testDoStartWithNoNextAvailable() throws Exception {
        when(selector.selectCounts()).thenReturn(2);
        when(selector.getCandidates()).thenReturn(Lists.newArrayList(newProxyEndpoint(true, true), newProxyEndpoint(true, false)));
        selector.setSelectStrategy(new SelectOneCycle(selector));
        doCallRealMethod().when(selector).nextHop();

        Throwable throwable = null;
        try {
            session.doStart();
        } catch (Exception e) {
            throwable = e;
        }
        Assert.assertNotNull(throwable);
        Assert.assertTrue(throwable instanceof NoResourceException);
    }

    @Test
    public void testDoStartWithConnectTwoTimesLose() throws Exception {
        selector = new DefaultProxyEndpointSelector(Lists.newArrayList(newProxyEndpoint(true, false), newProxyEndpoint(true, false)), endpointManager());
        selector.setSelectStrategy(new SelectOneCycle(selector));
        selector.setNextHopAlgorithm(new NaiveNextHopAlgorithm());
        ResourceManager resourceManager = mock(ResourceManager.class);
        when(resourceManager.createProxyEndpointSelector(any())).thenReturn(selector);
        when(resourceManager.getProxyConfig()).thenReturn(config());
        session = new DefaultBackendSession(tunnel, new NioEventLoopGroup(1), 300000, resourceManager);
        session.setNioEventLoopGroup(new NioEventLoopGroup(1));
        session.doStart();
        waitConditionUntilTimeOut(()->session.getSessionState().equals(new SessionClosed(session)), 1200);
    }

    @Test
    public void testDoStart() throws Exception {
        session.endpoint = new DefaultProxyEndpoint("TCP://127.0.0.1:6379");
        session.setSessionState(new SessionEstablished(session));
        session.doStart();
        verify(selector, never()).getCandidates();
    }

    @Test
    public void doSetSessionState() {
    }

    @Test
    public void getSessionType() {
        Assert.assertEquals(SESSION_TYPE.BACKEND, session.getSessionType());
    }

    @Test
    public void getSessionState() {
        Assert.assertEquals(new SessionInit(session), session.getSessionState());
    }

    private ByteBuf testByteBuf() {
        return Unpooled.copiedBuffer("TEST".getBytes());
    }
}